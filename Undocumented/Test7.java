/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.index;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DocumentsWriterDeleteQueue.DeleteSlice;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.SetOnce;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;

final class DocumentsWriterPerThread implements Accountable, Lock {

  private Throwable abortingException;

  private void onAbortingException(Throwable throwable) {
    assert throwable != null : "aborting exception must not be null";
    assert abortingException == null : "aborting exception has already been set";
    abortingException = throwable;
  }

  final boolean isAborted() {
    return aborted;
  }

  static final class FlushedSegment {
    final SegmentCommitInfo segmentInfo;
    final FieldInfos fieldInfos;
    final FrozenBufferedUpdates segmentUpdates;
    final FixedBitSet liveDocs;
    final Sorter.DocMap sortMap;
    final int delCount;

    private FlushedSegment(
        InfoStream infoStream,
        SegmentCommitInfo segmentInfo,
        FieldInfos fieldInfos,
        BufferedUpdates segmentUpdates,
        FixedBitSet liveDocs,
        int delCount,
        Sorter.DocMap sortMap) {
      this.segmentInfo = segmentInfo;
      this.fieldInfos = fieldInfos;
      this.segmentUpdates =
          segmentUpdates != null && segmentUpdates.any()
              ? new FrozenBufferedUpdates(infoStream, segmentUpdates, segmentInfo)
              : null;
      this.liveDocs = liveDocs;
      this.delCount = delCount;
      this.sortMap = sortMap;
    }
  }

  
  void abort() throws IOException {
    aborted = true;
    pendingNumDocs.addAndGet(-numDocsInRAM);
    try {
      if (infoStream.isEnabled("DWPT")) {
        infoStream.message("DWPT", "now abort");
      }
      try {
        indexingChain.abort();
      } finally {
        pendingUpdates.clear();
      }
    } finally {
      if (infoStream.isEnabled("DWPT")) {
        infoStream.message("DWPT", "done abort");
      }
    }
  }

  private static final boolean INFO_VERBOSE = false;
  final Codec codec;
  final TrackingDirectoryWrapper directory;
  private final IndexingChain indexingChain;

  // Updates for our still-in-RAM (to be flushed next) segment
  private final BufferedUpdates pendingUpdates;
  private final SegmentInfo segmentInfo; // Current segment we are working on
  private boolean aborted = false; // True if we aborted
  private SetOnce<Boolean> flushPending = new SetOnce<>();
  private volatile long lastCommittedBytesUsed;
  private SetOnce<Boolean> hasFlushed = new SetOnce<>();

  private final FieldInfos.Builder fieldInfos;
  private final InfoStream infoStream;
  private int numDocsInRAM;
  final DocumentsWriterDeleteQueue deleteQueue;
  private final DeleteSlice deleteSlice;
  private final NumberFormat nf = NumberFormat.getInstance(Locale.ROOT);
  private final AtomicLong pendingNumDocs;
  private final LiveIndexWriterConfig indexWriterConfig;
  private final boolean enableTestPoints;
  private final ReentrantLock lock = new ReentrantLock();
  private int[] deleteDocIDs = new int[0];
  private int numDeletedDocIds = 0;
  private final int indexMajorVersionCreated;
  private final IndexingChain.ReservedField<NumericDocValuesField> parentField;

  DocumentsWriterPerThread(
      int indexMajorVersionCreated,
      String segmentName,
      Directory directoryOrig,
      Directory directory,
      LiveIndexWriterConfig indexWriterConfig,
      DocumentsWriterDeleteQueue deleteQueue,
      FieldInfos.Builder fieldInfos,
      AtomicLong pendingNumDocs,
      boolean enableTestPoints) {
    this.indexMajorVersionCreated = indexMajorVersionCreated;
    this.directory = new TrackingDirectoryWrapper(directory);
    this.fieldInfos = fieldInfos;
    this.indexWriterConfig = indexWriterConfig;
    this.infoStream = indexWriterConfig.getInfoStream();
    this.codec = indexWriterConfig.getCodec();
    this.pendingNumDocs = pendingNumDocs;
    pendingUpdates = new BufferedUpdates(segmentName);
    this.deleteQueue = Objects.requireNonNull(deleteQueue);
    assert numDocsInRAM == 0 : "num docs " + numDocsInRAM;
    deleteSlice = deleteQueue.newSlice();

    segmentInfo =
        new SegmentInfo(
            directoryOrig,
            Version.LATEST,
            Version.LATEST,
            segmentName,
            -1,
            false,
            false,
            codec,
            Collections.emptyMap(),
            StringHelper.randomId(),
            Collections.emptyMap(),
            indexWriterConfig.getIndexSort());
    assert numDocsInRAM == 0;
    if (INFO_VERBOSE && infoStream.isEnabled("DWPT")) {
      infoStream.message(
          "DWPT",
          Thread.currentThread().getName()
              + " init seg="
              + segmentName
              + " delQueue="
              + deleteQueue);
    }
    this.enableTestPoints = enableTestPoints;
    indexingChain =
        new IndexingChain(
            indexMajorVersionCreated,
            segmentInfo,
            this.directory,
            fieldInfos,
            indexWriterConfig,
            this::onAbortingException);
    if (indexWriterConfig.getParentField() != null) {
      this.parentField =
          indexingChain.markAsReserved(
              new NumericDocValuesField(indexWriterConfig.getParentField(), -1));
    } else {
      this.parentField = null;
    }
  }

  final void testPoint(String message) {
    if (enableTestPoints) {
      assert infoStream.isEnabled("TP"); // don't enable unless you need them.
      infoStream.message("TP", message);
    }
  }

  private void reserveOneDoc() {
    if (pendingNumDocs.incrementAndGet() > IndexWriter.getActualMaxDocs()) {
      // Reserve failed: put the one doc back and throw exc:
      pendingNumDocs.decrementAndGet();
      throw new IllegalArgumentException(
          "number of documents in the index cannot exceed " + IndexWriter.getActualMaxDocs());
    }
  }

  long updateDocuments(
      Iterable<? extends Iterable<? extends IndexableField>> docs,
      DocumentsWriterDeleteQueue.Node<?> deleteNode,
      DocumentsWriter.FlushNotifications flushNotifications,
      Runnable onNewDocOnRAM)
      throws IOException {
    try {
      testPoint("DocumentsWriterPerThread addDocuments start");
      assert abortingException == null : "DWPT has hit aborting exception but is still indexing";
      if (INFO_VERBOSE && infoStream.isEnabled("DWPT")) {
        infoStream.message(
            "DWPT",
            Thread.currentThread().getName()
                + " update delTerm="
                + deleteNode
                + " docID="
                + numDocsInRAM
                + " seg="
                + segmentInfo.name);
      }
      final int docsInRamBefore = numDocsInRAM;
      boolean allDocsIndexed = false;
      try {
        final Iterator<? extends Iterable<? extends IndexableField>> iterator = docs.iterator();
        while (iterator.hasNext()) {
          Iterable<? extends IndexableField> doc = iterator.next();
          if (parentField != null) {
            if (iterator.hasNext() == false) {
              doc = addParentField(doc, parentField);
            }
          } else if (segmentInfo.getIndexSort() != null
              && iterator.hasNext()
              && indexMajorVersionCreated >= Version.LUCENE_10_0_0.major) {
            
            throw new IllegalArgumentException(
                "a parent field must be set in order to use document blocks with index sorting; see IndexWriterConfig#setParentField");
          }

         
          reserveOneDoc();
          try {
            indexingChain.processDocument(numDocsInRAM++, doc);
          } finally {
            onNewDocOnRAM.run();
          }
        }
        final int numDocs = numDocsInRAM - docsInRamBefore;
        if (numDocs > 1) {
          segmentInfo.setHasBlocks();
        }
        allDocsIndexed = true;
        return finishDocuments(deleteNode, docsInRamBefore);
      } finally {
        if (!allDocsIndexed && !aborted) {
          
          deleteLastDocs(numDocsInRAM - docsInRamBefore);
        }
      }
    } finally {
      maybeAbort("updateDocuments", flushNotifications);
    }
  }

  private Iterable<? extends IndexableField> addParentField(
      Iterable<? extends IndexableField> doc, IndexableField parentField) {
    return () -> {
      final Iterator<? extends IndexableField> first = doc.iterator();
      return new Iterator<>() {
        IndexableField additionalField = parentField;

        @Override
        public boolean hasNext() {
          return additionalField != null || first.hasNext();
        }

        @Override
        public IndexableField next() {
          if (additionalField != null) {
            IndexableField field = additionalField;
            additionalField = null;
            return field;
          }
          if (first.hasNext()) {
            return first.next();
          }
          throw new NoSuchElementException();
        }
      };
    };
  }

  private long finishDocuments(DocumentsWriterDeleteQueue.Node<?> deleteNode, int docIdUpTo) {
    
    long seqNo;
    if (deleteNode != null) {
      seqNo = deleteQueue.add(deleteNode, deleteSlice);
      assert deleteSlice.isTail(deleteNode) : "expected the delete term as the tail item";
      deleteSlice.apply(pendingUpdates, docIdUpTo);
      return seqNo;
    } else {
      seqNo = deleteQueue.updateSlice(deleteSlice);
      if (seqNo < 0) {
        seqNo = -seqNo;
        deleteSlice.apply(pendingUpdates, docIdUpTo);
      } else {
        deleteSlice.reset();
      }
    }

    return seqNo;
  }

 
  private void deleteLastDocs(int docCount) {
    int from = numDocsInRAM - docCount;
    int to = numDocsInRAM;
    deleteDocIDs = ArrayUtil.grow(deleteDocIDs, numDeletedDocIds + (to - from));
    for (int docId = from; docId < to; docId++) {
      deleteDocIDs[numDeletedDocIds++] = docId;
    }
   
  }


  public int getNumDocsInRAM() {
   
    return numDocsInRAM;
  }

  
  FrozenBufferedUpdates prepareFlush() {
    assert numDocsInRAM > 0;
    final FrozenBufferedUpdates globalUpdates = deleteQueue.freezeGlobalBuffer(deleteSlice);
   
    if (deleteSlice != null) {
     
      deleteSlice.apply(pendingUpdates, numDocsInRAM);
      assert deleteSlice.isEmpty();
      deleteSlice.reset();
    }
    return globalUpdates;
  }

 
  FlushedSegment flush(DocumentsWriter.FlushNotifications flushNotifications) throws IOException {
    assert flushPending.get() == Boolean.TRUE;
    assert numDocsInRAM > 0;
    assert deleteSlice.isEmpty() : "all deletes must be applied in prepareFlush";
    segmentInfo.setMaxDoc(numDocsInRAM);
    final SegmentWriteState flushState =
        new SegmentWriteState(
            infoStream,
            directory,
            segmentInfo,
            fieldInfos.finish(),
            pendingUpdates,
            IOContext.flush(new FlushInfo(numDocsInRAM, lastCommittedBytesUsed)));
    final double startMBUsed = lastCommittedBytesUsed / 1024. / 1024.;

    
    if (numDeletedDocIds > 0) {
      flushState.liveDocs = new FixedBitSet(numDocsInRAM);
      flushState.liveDocs.set(0, numDocsInRAM);
      for (int i = 0; i < numDeletedDocIds; i++) {
        flushState.liveDocs.clear(deleteDocIDs[i]);
      }
      flushState.delCountOnFlush = numDeletedDocIds;
      deleteDocIDs = new int[0];
    }

    if (aborted) {
      if (infoStream.isEnabled("DWPT")) {
        infoStream.message("DWPT", "flush: skip because aborting is set");
      }
      return null;
    }

    long t0 = System.nanoTime();

    if (infoStream.isEnabled("DWPT")) {
      infoStream.message(
          "DWPT",
          "flush postings as segment " + flushState.segmentInfo.name + " numDocs=" + numDocsInRAM);
    }
    final Sorter.DocMap sortMap;
    try {
      DocIdSetIterator softDeletedDocs;
      if (indexWriterConfig.getSoftDeletesField() != null) {
        softDeletedDocs = indexingChain.getHasDocValues(indexWriterConfig.getSoftDeletesField());
      } else {
        softDeletedDocs = null;
      }
      sortMap = indexingChain.flush(flushState);
      if (softDeletedDocs == null) {
        flushState.softDelCountOnFlush = 0;
      } else {
        flushState.softDelCountOnFlush =
            PendingSoftDeletes.countSoftDeletes(softDeletedDocs, flushState.liveDocs);
        assert flushState.segmentInfo.maxDoc()
            >= flushState.softDelCountOnFlush + flushState.delCountOnFlush;
      }
      
      pendingUpdates.clearDeleteTerms();
      segmentInfo.setFiles(new HashSet<>(directory.getCreatedFiles()));

      final SegmentCommitInfo segmentInfoPerCommit =
          new SegmentCommitInfo(
              segmentInfo,
              0,
              flushState.softDelCountOnFlush,
              -1L,
              -1L,
              -1L,
              StringHelper.randomId());
      if (infoStream.isEnabled("DWPT")) {
        infoStream.message(
            "DWPT",
            "new segment has "
                + (flushState.liveDocs == null ? 0 : flushState.delCountOnFlush)
                + " deleted docs");
        infoStream.message(
            "DWPT", "new segment has " + flushState.softDelCountOnFlush + " soft-deleted docs");
        infoStream.message(
            "DWPT",
            "new segment has "
                + (flushState.fieldInfos.hasTermVectors() ? "vectors" : "no vectors")
                + "; "
                + (flushState.fieldInfos.hasNorms() ? "norms" : "no norms")
                + "; "
                + (flushState.fieldInfos.hasDocValues() ? "docValues" : "no docValues")
                + "; "
                + (flushState.fieldInfos.hasProx() ? "prox" : "no prox")
                + "; "
                + (flushState.fieldInfos.hasFreq() ? "freqs" : "no freqs"));
        infoStream.message("DWPT", "flushedFiles=" + segmentInfoPerCommit.files());
        infoStream.message("DWPT", "flushed codec=" + codec);
      }

      final BufferedUpdates segmentDeletes;
      if (pendingUpdates.deleteQueries.isEmpty() && pendingUpdates.numFieldUpdates.get() == 0) {
        pendingUpdates.clear();
        segmentDeletes = null;
      } else {
        segmentDeletes = pendingUpdates;
      }

      if (infoStream.isEnabled("DWPT")) {
        final double newSegmentSize = segmentInfoPerCommit.sizeInBytes() / 1024. / 1024.;
        infoStream.message(
            "DWPT",
            "flushed: segment="
                + segmentInfo.name
                + " ramUsed="
                + nf.format(startMBUsed)
                + " MB"
                + " newFlushedSize="
                + nf.format(newSegmentSize)
                + " MB"
                + " docs/MB="
                + nf.format(flushState.segmentInfo.maxDoc() / newSegmentSize));
      }

      assert segmentInfo != null;

      FlushedSegment fs =
          new FlushedSegment(
              infoStream,
              segmentInfoPerCommit,
              flushState.fieldInfos,
              segmentDeletes,
              flushState.liveDocs,
              flushState.delCountOnFlush,
              sortMap);
      sealFlushedSegment(fs, sortMap, flushNotifications);
      if (infoStream.isEnabled("DWPT")) {
        infoStream.message(
            "DWPT",
            "flush time "
                + ((System.nanoTime() - t0) / (double) TimeUnit.MILLISECONDS.toNanos(1))
                + " ms");
      }
      return fs;
    } catch (Throwable t) {
      onAbortingException(t);
      throw t;
    } finally {
      maybeAbort("flush", flushNotifications);
      hasFlushed.set(Boolean.TRUE);
    }
  }

  private void maybeAbort(String location, DocumentsWriter.FlushNotifications flushNotifications)
      throws IOException {
    if (abortingException != null && aborted == false) {
     
      try {
        abort();
      } finally {
        
        flushNotifications.onTragicEvent(abortingException, location);
      }
    }
  }

  private final Set<String> filesToDelete = new HashSet<>();

  Set<String> pendingFilesToDelete() {
    return filesToDelete;
  }

  private FixedBitSet sortLiveDocs(Bits liveDocs, Sorter.DocMap sortMap) {
    assert liveDocs != null && sortMap != null;
    FixedBitSet sortedLiveDocs = new FixedBitSet(liveDocs.length());
    sortedLiveDocs.set(0, liveDocs.length());
    for (int i = 0; i < liveDocs.length(); i++) {
      if (liveDocs.get(i) == false) {
        sortedLiveDocs.clear(sortMap.oldToNew(i));
      }
    }
    return sortedLiveDocs;
  }

  
  void sealFlushedSegment(
      FlushedSegment flushedSegment,
      Sorter.DocMap sortMap,
      DocumentsWriter.FlushNotifications flushNotifications)
      throws IOException {
    assert flushedSegment != null;
    SegmentCommitInfo newSegment = flushedSegment.segmentInfo;

    IndexWriter.setDiagnostics(newSegment.info, IndexWriter.SOURCE_FLUSH);

    IOContext context =
        IOContext.flush(new FlushInfo(newSegment.info.maxDoc(), newSegment.sizeInBytes()));

    try {
      if (indexWriterConfig.getUseCompoundFile()) {
        Set<String> originalFiles = newSegment.info.files();
        // TODO: like addIndexes, we are relying on createCompoundFile to successfully cleanup...
        IndexWriter.createCompoundFile(
            infoStream,
            new TrackingDirectoryWrapper(directory),
            newSegment.info,
            context,
            flushNotifications::deleteUnusedFiles);
        filesToDelete.addAll(originalFiles);
        newSegment.info.setUseCompoundFile(true);
      }

      
      codec.segmentInfoFormat().write(directory, newSegment.info, context);

      
      if (flushedSegment.liveDocs != null) {
        final int delCount = flushedSegment.delCount;
        assert delCount > 0;
        if (infoStream.isEnabled("DWPT")) {
          infoStream.message(
              "DWPT",
              "flush: write "
                  + delCount
                  + " deletes gen="
                  + flushedSegment.segmentInfo.getDelGen());
        }

        

        SegmentCommitInfo info = flushedSegment.segmentInfo;
        Codec codec = info.info.getCodec();
        final FixedBitSet bits;
        if (sortMap == null) {
          bits = flushedSegment.liveDocs;
        } else {
          bits = sortLiveDocs(flushedSegment.liveDocs, sortMap);
        }
        codec.liveDocsFormat().writeLiveDocs(bits, directory, info, delCount, context);
        newSegment.setDelCount(delCount);
        newSegment.advanceDelGen();
      }
    } catch (Throwable t) {
      if (infoStream.isEnabled("DWPT")) {
        infoStream.message(
            "DWPT",
            "hit exception creating compound file for newly flushed segment "
                + newSegment.info.name);
      }
      throw t;
    }
  }

  SegmentInfo getSegmentInfo() {
    return segmentInfo;
  }

  @Override
  public long ramBytesUsed() {
    assert lock.isHeldByCurrentThread();
    return (deleteDocIDs.length * (long) Integer.BYTES)
        + pendingUpdates.ramBytesUsed()
        + indexingChain.ramBytesUsed();
  }

  @Override
  public Collection<Accountable> getChildResources() {
    assert lock.isHeldByCurrentThread();
    return List.of(pendingUpdates, indexingChain);
  }

  @Override
  public String toString() {
    return "DocumentsWriterPerThread [pendingDeletes="
        + pendingUpdates
        + ", segment="
        + segmentInfo.name
        + ", aborted="
        + aborted
        + ", numDocsInRAM="
        + numDocsInRAM
        + ", deleteQueue="
        + deleteQueue
        + ", "
        + numDeletedDocIds
        + " deleted docIds"
        + "]";
  }

 
  public boolean isApproachingBufferLimit() {
    return indexingChain.isApproachingBufferLimit(
        65000); 
  }




  boolean isFlushPending() {
    return flushPending.get() == Boolean.TRUE;
  }

  boolean isQueueAdvanced() {
    return deleteQueue.isAdvanced();
  }


  void setFlushPending() {
    flushPending.set(Boolean.TRUE);
  }

  
  long getLastCommittedBytesUsed() {
    return lastCommittedBytesUsed;
  }

  
  void commitLastBytesUsed(long delta) {
    assert isHeldByCurrentThread();
    assert getCommitLastBytesUsedDelta() == delta : "delta has changed";
    lastCommittedBytesUsed += delta;
  }

  
  long getCommitLastBytesUsedDelta() {
    assert isHeldByCurrentThread();
    long delta = ramBytesUsed() - lastCommittedBytesUsed;
    return delta;
  }

  @Override
  public void lock() {
    lock.lock();
  }

  @Override
  public void lockInterruptibly() throws InterruptedException {
    lock.lockInterruptibly();
  }

  @Override
  public boolean tryLock() {
    return lock.tryLock();
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
    return lock.tryLock(time, unit);
  }

  
  boolean isHeldByCurrentThread() {
    return lock.isHeldByCurrentThread();
  }

  @Override
  public void unlock() {
    lock.unlock();
  }

  @Override
  public Condition newCondition() {
    throw new UnsupportedOperationException();
  }

  boolean hasFlushed() {
    return hasFlushed.get() == Boolean.TRUE;
  }
}
