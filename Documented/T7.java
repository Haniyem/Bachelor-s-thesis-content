```java
/**
 * @file DocumentsWriterPerThread.java
 * @module org.apache.lucene.index
 * 
 * @description
 * Implements the per-thread management of document indexing in Lucene's internal architecture.
 * Handles buffering of in-memory documents, application of deletes and updates, and the
 * logic for flushing in-memory segments to disk. This class plays a critical role in controlling
 * the indexing pipeline performance, resource utilization, and maintaining ACID properties
 * by supporting abort operations and segment isolation per indexing thread.
 * The design enables concurrent document ingestion and ensures consistency and recoverability
 * during errors or abort signals. Its synchronization and state-tracking components also underpin
 * the robustness of Lucene's multi-threaded indexing engine.
 * 
 * @dependencies
 * - Codec: Abstraction for accessing segment codecs (reading/writing index files)
 * - Directory: Provides file-system abstraction for index storage
 * - DocumentsWriterDeleteQueue: Coordinates global and per-thread document deletions
 * - InfoStream: Used for advanced logging of internal and debugging information
 * - ReentrantLock: Controls thread-safety for mutating memory and flush operations
 * - AtomicLong: Tracks document counts across concurrent updates
 * - Others (see imports): Various Lucene indexing, utility, and concurrency helpers
 * 
 * @author Apache Lucene Project Contributors
 * @version 10.x (for Lucene 10.0.0+)
 * @license Apache License 2.0
 * @lastmodified 2025-09-14: Documentation and inline commentary added for clarity and maintainability.
 */

/**
 * @class DocumentsWriterPerThread
 * @classdesc
 * Manages document indexing state and logic per indexing thread ("DWPT") in Lucene.
 * Responsible for buffering in-RAM documents, applying deletions and updates, flushing segments,
 * and coordinating with the wider indexing infrastructure for segment management.
 * This class isolates thread state for scalability and error containment.
 *
 * @example
 * // Typical instantiation within Lucene internals:
 * DocumentsWriterPerThread dwpt = new DocumentsWriterPerThread(
 *    versionCreated,
 *    "segmentName",
 *    origDirectory,
 *    directory,
 *    config,
 *    deleteQueue,
 *    fieldInfosBuilder,
 *    pendingNumDocsCounter,
 *    enableTestPoints
 * );
 * // Index documents via dwpt.updateDocuments(docs, ...);
 * // Flush or abort as needed
 *
 * @prop {Codec} codec - Codec used to encode/decode index data for this thread's segment.
 * @prop {TrackingDirectoryWrapper} directory - Directory managing segment file creation/tracking.
 * @prop {IndexingChain} indexingChain - Responsible for document-to-segment pipeline (parsing, analysis, ...).
 * @prop {BufferedUpdates} pendingUpdates - Per-thread updates/deletes still in RAM (not yet flushed).
 * @prop {SegmentInfo} segmentInfo - Metadata for the in-progress segment.
 * @prop {boolean} aborted - Whether this thread has aborted due to a tragic exception.
 * @prop {int} numDocsInRAM - Accumulated document count in this DWPT's buffer (not yet flushed).
 * @prop {ReentrantLock} lock - Thread-safety guard for critical sections and state updates.
 * ...
 */
final class DocumentsWriterPerThread implements Accountable, Lock {

  /** 
   * If set, a tragic/aborting exception occurred on this writer and further indexing must halt.
   * Used to propagate and manage fatal errors during indexing workflows.
   */
  private Throwable abortingException;

  /**
   * Sets the 'aborting exception' marker for the DWPT, indicating that an unrecoverable state
   * has occurred and future operations should be rejected.
   *
   * @param {Throwable} throwable - Non-null fatal exception causing abort.
   */
  private void onAbortingException(Throwable throwable) {
    assert throwable != null : "aborting exception must not be null";
    assert abortingException == null : "aborting exception has already been set";
    abortingException = throwable;
  }

  /**
   * Checks whether this DWPT has been marked as aborted due to a tragic event.
   *
   * @returns {boolean} True if the thread is aborted and must not continue indexing.
   */
  final boolean isAborted() {
    return aborted;
  }

  /**
   * @class FlushedSegment
   * @classdesc Immutable structure holding metadata and state of a segment after it's flushed.
   *          Includes segment information, index field mappings, applied updates, and live-docs bitset.
   *
   * @prop {SegmentCommitInfo} segmentInfo - Details and unique identity for the segment as committed on flush.
   * @prop {FieldInfos} fieldInfos - Per-segment field descriptors (types, properties, names).
   * @prop {FrozenBufferedUpdates} segmentUpdates - Frozen, applied updates on this segment.
   * @prop {FixedBitSet} liveDocs - BitSet marking 'live' document IDs (after deletes).
   * @prop {Sorter.DocMap} sortMap - Mapping for sorted/unsorted document ids, if segment is sorted.
   * @prop {int} delCount - Count of deleted docs in segment at flush.
   */
  static final class FlushedSegment {
    final SegmentCommitInfo segmentInfo;
    final FieldInfos fieldInfos;
    final FrozenBufferedUpdates segmentUpdates;
    final FixedBitSet liveDocs;
    final Sorter.DocMap sortMap;
    final int delCount;

    /**
     * @constructor
     * @description
     * Constructs a FlushedSegment, freezing updates and recording segment state post-flush.
     *
     * @param {InfoStream} infoStream - Logging for debug/instrumentation.
     * @param {SegmentCommitInfo} segmentInfo - Info about flushed segment.
     * @param {FieldInfos} fieldInfos - Segment's field information.
     * @param {BufferedUpdates} segmentUpdates - Updates queued for this segment.
     * @param {FixedBitSet} liveDocs - Document live/deleted flags.
     * @param {int} delCount - Count of docs deleted in this segment.
     * @param {Sorter.DocMap} sortMap - Doc ID sort mapping, if sorted segment.
     */
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

  /**
   * Aborts ongoing indexing in this writer due to a fatal event.
   * Ensures memory is freed, state is reset, and pending updates are dropped.
   * 
   * @throws {IOException} If errors occur during abort finalization.
   */
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

  // Various state and utility fields (see @classdesc for description)
  private static final boolean INFO_VERBOSE = false;
  final Codec codec;
  final TrackingDirectoryWrapper directory;
  private final IndexingChain indexingChain;
  private final BufferedUpdates pendingUpdates;
  private final SegmentInfo segmentInfo; 
  private boolean aborted = false;
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

  /**
   * @constructor
   * @description
   * Creates a DWPT instance responsible for a segment-in-progress, with its own directory and update state.
   * Sets up key state (segment info, updates, fieldInfos, locking) to buffer, delete, or flush documents.
   *
   * @param {int} indexMajorVersionCreated - Lucene major version this segment targets.
   * @param {String} segmentName - Unique identifier for segment in progress.
   * @param {Directory} directoryOrig - Original base directory for the segment.
   * @param {Directory} directory - Directory abstraction for segment file tracking.
   * @param {LiveIndexWriterConfig} indexWriterConfig - Global index configuration, including codecs, test points.
   * @param {DocumentsWriterDeleteQueue} deleteQueue - Source for global and per-thread deletes.
   * @param {FieldInfos.Builder} fieldInfos - Builder for accumulating segment field metadata.
   * @param {AtomicLong} pendingNumDocs - Global counter for documents pending flush.
   * @param {boolean} enableTestPoints - Enable assertion points/testing hooks.
   * 
   * @example
   * DocumentsWriterPerThread dwpt = new DocumentsWriterPerThread(...);
   */
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

  /**
   * Signals a "test point" for testing/verification during indexing.
   * Used only when test points are enabled.
   * 
   * @param {String} message - Identifier for the test point checkpoint.
   */
  final void testPoint(String message) {
    if (enableTestPoints) {
      assert infoStream.isEnabled("TP");
      infoStream.message("TP", message);
    }
  }

  /**
   * Increments the document counter, enforcing maximum allowed documents.
   * Throws if the document limit is breached.
   * 
   * @throws {IllegalArgumentException} If total doc count after increment exceeds allowed max.
   */
  private void reserveOneDoc() {
    if (pendingNumDocs.incrementAndGet() > IndexWriter.getActualMaxDocs()) {
      // If reservation fails, decrement and signal error
      pendingNumDocs.decrementAndGet();
      throw new IllegalArgumentException(
          "number of documents in the index cannot exceed " + IndexWriter.getActualMaxDocs());
    }
  }

  /**
   * Adds a batch of documents to the in-RAM segment and applies any necessary deletes/updates.
   * 
   * @param {Iterable<? extends Iterable<? extends IndexableField>>} docs - Collection of documents to buffer.
   * @param {DocumentsWriterDeleteQueue.Node<?>} deleteNode - Node representing deletes concurrent to this batch.
   * @param {DocumentsWriter.FlushNotifications} flushNotifications - Observer for flush/abort notifications.
   * @param {Runnable} onNewDocOnRAM - Callback to signal each document buffered.
   * 
   * @returns {long} Sequence number for this operation's position in the global delete queue.
   * 
   * @throws {IOException} If document ingestion or deletion fails.
   * 
   * @example
   * long seqNo = dwpt.updateDocuments(documents, deleteNode, flushObserver, () -> {});
   */
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
          // Add parent field to last doc in a block if present or enforce sorting/parent requirements
          if (parentField != null) {
            if (!iterator.hasNext()) {
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
            // Route document to the indexing chain (analysis, field writing, etc)
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
          // Delete incompletely indexed docs if operation was not fully successful
          deleteLastDocs(numDocsInRAM - docsInRamBefore);
        }
      }
    } finally {
      maybeAbort("updateDocuments", flushNotifications);
    }
  }

  /**
   * Decorates a document by adding the required parent field.
   * Used in parent/child or block-indexing scenarios.
   * 
   * @param {Iterable<? extends IndexableField>} doc - Document fields.
   * @param {IndexableField} parentField - Field to be added.
   * 
   * @returns {Iterable<? extends IndexableField>} Iterator for the combined fields.
   */
  private Iterable<? extends IndexableField> addParentField(
      Iterable<? extends IndexableField> doc, IndexableField parentField) {
    return () -> {
      final Iterator<? extends IndexableField> first = doc.iterator();
      return new Iterator<>() {
        IndexableField additionalField = parentField;

        @Override
        public boolean hasNext() {
          // Parent field comes first, then delegate to user fields
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

  /**
   * Finalizes and records the state of documents just indexed in RAM, applying any deletes.
   * Updates the delete queue and per-thread pending updates accordingly.
   * 
   * @param {DocumentsWriterDeleteQueue.Node<?>} deleteNode - Delete marker for this operation.
   * @param {int} docIdUpTo - Upper bound (exclusive) for docs to which this applies.
   * 
   * @returns {long} Sequence number marking delete/operation position.
   */
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

  /**
   * Marks the last N docs in the buffer as deleted for rollback or incomplete-batch error handling.
   * 
   * @param {int} docCount - Number of documents to delete.
   */
  private void deleteLastDocs(int docCount) {
    int from = numDocsInRAM - docCount;
    int to = numDocsInRAM;
    deleteDocIDs = ArrayUtil.grow(deleteDocIDs, numDeletedDocIds + (to - from));
    for (int docId = from; docId < to; docId++) {
      deleteDocIDs[numDeletedDocIds++] = docId;
    }
  }

  /**
   * Returns current in-RAM document count (not yet flushed).
   * 
   * @returns {int} The number of documents buffered in RAM.
   */
  public int getNumDocsInRAM() {
    return numDocsInRAM;
  }

  /**
   * Prepares a flush, freezing deletes and updates for the global buffer.
   * Ensures all deletes are applied up to the current document count.
   * 
   * @returns {FrozenBufferedUpdates} Frozen global updates buffer for use at flush.
   */
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

  /**
   * Flushes the currently indexed segment to disk.
   * Commits all in-RAM state as a durable Lucene segment, applies deletes and updates,
   * and returns the FlushedSegment metadata for downstream use.
   * 
   * @param {DocumentsWriter.FlushNotifications} flushNotifications - Observer for abort, error, or completion.
   * @returns {FlushedSegment} Immutable descriptor of the flushed segment.
   * 
   * @throws {IOException|Throwable} If flush fails or is aborted.
   */
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

    // If any docs were deleted in RAM before flush, mark and write liveDocs
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

  /**
   * Invokes an abort workflow if an aborting exception has occurred.
   * This will forcibly clean up and propagate notifications as appropriate.
   *
   * @param {String} location - String tag identifying where abort is triggered.
   * @param {DocumentsWriter.FlushNotifications} flushNotifications - Observer for error/abort propagation.
   * 
   * @throws {IOException} If abort fails.
   */
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

  /**
   * Returns the batched list of files pending deletion after flush/finalization.
   * Used to defer deletion for later cleanup.
   * 
   * @returns {Set<String>} Set of filenames to delete.
   */
  Set<String> pendingFilesToDelete() {
    return filesToDelete;
  }

  /**
   * Sorts a bitset of live document IDs using a doc ID sort mapping.
   * Used when segments are output in sorted order.
   *
   * @param {Bits} liveDocs - Source bitset for live docs.
   * @param {Sorter.DocMap} sortMap - Document ID mapping to use for sort.
   *
   * @returns {FixedBitSet} Sorted version of the live docs bitset.
   */
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

  /**
   * Completes flush finalization steps: writes segment info, applies deletes, and tracks unused files.
   * Will also advance segment generation for newly deleted docs.
   * 
   * @param {FlushedSegment} flushedSegment - Metadata and buffers of the freshly flushed segment.
   * @param {Sorter.DocMap} sortMap - Mapping for document ID sort order.
   * @param {DocumentsWriter.FlushNotifications} flushNotifications - Observer for error/abort.
   * 
   * @throws {IOException} If finalization fails.
   */
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
        // Compound files merge many files into one for I/O efficiency, requiring error-safe cleanup.
        IndexWriter.createCompoundFile(
            infoStream,
            new TrackingDirectoryWrapper(directory),
            newSegment.info,
            context,
            flushNotifications::deleteUnusedFiles);
        filesToDelete.addAll(originalFiles);
        newSegment.info.setUseCompoundFile(true);
      }

      // Write segment metadata (info) to disk
      codec.segmentInfoFormat().write(directory, newSegment.info, context);

      // If doc deletes exist, write deletions with applied BitSet
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

  /**
   * @returns {SegmentInfo} Returns the current segment info for the DWPT.
   */
  SegmentInfo getSegmentInfo() {
    return segmentInfo;
  }

  /**
   * Returns current RAM usage by this DWPT for diagnostic/accounting purposes.
   * Must be called only while lock is held.
   * 
   * @returns {long} Used RAM bytes.
   */
  @Override
  public long ramBytesUsed() {
    assert lock.isHeldByCurrentThread();
    return (deleteDocIDs.length * (long) Integer.BYTES)
        + pendingUpdates.ramBytesUsed()
        + indexingChain.ramBytesUsed();
  }

  /**
   * Returns children resources for resource accounting (RAM usage diagnostics).
   *
   * @returns {Collection<Accountable>} Child resources (pending updates, indexing pipeline).
   */
  @Override
  public Collection<Accountable> getChildResources() {
    assert lock.isHeldByCurrentThread();
    return List.of(pendingUpdates, indexingChain);
  }

  /**
   * String descriptor showing key state for diagnostics and logging.
   *
   * @returns {String} Human-readable state dump.
   */
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

  /**
   * Quickly checks if buffer is approaching per-thread imposed limit; used to rate-limit
   * or trigger preemptive flush to avoid out-of-memory errors.
   * 
   * @returns {boolean} True if buffer limit is close; warning signal.
   */
  public boolean isApproachingBufferLimit() {
    return indexingChain.isApproachingBufferLimit(
        65000); // Conservative default threshold; empirically tuned
  }

  // -- Various accessors for DWPT state (booleans, counters, flags) --

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

  /**
   * Commits the delta in RAM usage after a flush, for accurate accounting.
   * Can only be called with the lock held.
   *
   * @param {long} delta - Exact difference to commit.
   */
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

  // -- Lock/Condition API for thread-safety wrappers --

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

  /**
   * Returns whether the last flush was already executed for this DWPT instance.
   *
   * @returns {boolean} True if flush finalized.
   */
  boolean hasFlushed() {
    return hasFlushed.get() == Boolean.TRUE;
  }
}
```

[1](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/6341464/6a0a7c5c-5868-42ee-8c80-ff44200d26be/T7.txt)
