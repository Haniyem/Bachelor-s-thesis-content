/**
 * @file FreqProxTermsWriter.java
 * @module org.apache.lucene.index
 * 
 * @description
 * Implements the main in-memory postings (docs, frequencies, positions, offsets) writer for Lucene indexing, handling the collection, sorting, and serialization of per-field posting data. In addition to writing posting lists, it supports document sorting, live document bitsets, and deletion updates during the flush phase. The core responsibilities include aggregating term postings collected by per-field structures and writing out field-level indexed information with codec-specific consumers. Sorting logic ensures correct docID assignment during index sorting, and delete-mark propagation is efficiently managed on segment write.
 * 
 * @dependencies
 * - org.apache.lucene.util.*: Core Lucene utility classes for pools, collections, counters, sorting, and references.
 * - org.apache.lucene.codecs.*: Codec interfaces for writing and formatting postings and norms.
 * - org.apache.lucene.search.DocIdSetIterator: For iterating posting lists/docID sets.
 * - org.apache.lucene.store.*: IO abstractions used in buffer handling.
 * - org.apache.lucene.index.*: Common Lucene index structures (terms, postings, automata).
 * 
 * @author Apache Software Foundation (ASF)
 * @version 9.x
 * @license Apache-2.0
 * @lastmodified 2025-09-15: Documentation and comments added for enhanced maintainability.
 */

/**
 * @class FreqProxTermsWriter
 * @classdesc
 * Implements the main TermsHash consumer for postings (frequency and proximity) data in Lucene. Responsible for collecting in-memory term posting information during document indexing, then flushing/serializing the information to disk in an efficient, field-sorted, and possibly doc-sorted manner.
 * 
 * @example
 * // Typical integration (internal Lucene usage)
 * FreqProxTermsWriter writer = new FreqProxTermsWriter(intBlockAllocator, byteBlockAllocator, bytesUsed, termVectors);
 */
final class FreqProxTermsWriter extends TermsHash {

  /**
   * Constructs a new FreqProxTermsWriter.
   * 
   * @param {IntBlockPool.Allocator} intBlockAllocator - Allocator for integer block storage pools.
   * @param {ByteBlockPool.Allocator} byteBlockAllocator - Allocator for byte block storage pools.
   * @param {Counter} bytesUsed - Tracks cumulative memory used by this writer.
   * @param {TermsHash} termVectors - Downstream TermsHash instance for handling term vectors.
   */
  FreqProxTermsWriter(
      final IntBlockPool.Allocator intBlockAllocator,
      final ByteBlockPool.Allocator byteBlockAllocator,
      Counter bytesUsed,
      TermsHash termVectors) {
    super(intBlockAllocator, byteBlockAllocator, bytesUsed, termVectors);
  }

  /**
   * Applies pending term deletions to the set of live documents according to per-segment
   * delete markers, updating the segment's liveDocs bitset and deletion counters as needed.
   *
   * @function applyDeletes
   * @description
   * Iterates through all delete terms marked for this segment, updating the liveDocs bitset to reflect document deletions for matching terms and increments the deletion counter. Used during the flush to atomically apply all pending deletes before writing postings.
   * 
   * @param {SegmentWriteState} state - The segment write state including segment updates and liveDocs.
   * @param {Fields} fields - The Fields instance representing the postings (per-term) for this segment.
   * 
   * @throws {IOException} If there is an I/O failure during postings access.
   *
   * @example
   * applyDeletes(segmentWriteState, postingsFields);
   */
  private void applyDeletes(SegmentWriteState state, Fields fields) throws IOException {
    // Check if there are segment-level term deletions to process
    if (state.segUpdates != null && state.segUpdates.deleteTerms.size() > 0) {

      BufferedUpdates.DeletedTerms segDeletes = state.segUpdates.deleteTerms;
      // Iterator provides (term, docId) pairs for all delete terms
      FrozenBufferedUpdates.TermDocsIterator iterator =
          new FrozenBufferedUpdates.TermDocsIterator(fields, true);

      segDeletes.forEachOrdered(
          (term, docId) -> {
            DocIdSetIterator postings = iterator.nextTerm(term.field(), term.bytes());
            if (postings != null) {
              assert docId < PostingsEnum.NO_MORE_DOCS;
              int doc;
              // Iterate over docs less than the limit (delete up to but not including docId)
              while ((doc = postings.nextDoc()) < docId) {
                // Initialize liveDocs bitset if not present
                if (state.liveDocs == null) {
                  state.liveDocs = new FixedBitSet(state.segmentInfo.maxDoc());
                  state.liveDocs.set(0, state.segmentInfo.maxDoc());
                }
                // Only update if doc is currently live
                if (state.liveDocs.getAndClear(doc)) {
                  state.delCountOnFlush++;
                }
              }
            }
          });
    }
  }

  /**
   * Flushes all in-memory posting data to the index. Serializes fields, applies document deletes, and passes the collected postings to the codec.
   *
   * @function flush
   * @description
   * Main entry point for persisting field-level in-memory postings data on a segment flush. Sorts terms, applies deletions to liveDocs, remaps posting data if index sorting is enabled, and writes the postings data via the codec's FieldsConsumer. Invoked once per flush per segment.
   * 
   * @param {Map<String, TermsHashPerField>} fieldsToFlush - Map of field names to TermsHashPerField containers for the current segment.
   * @param {SegmentWriteState} state - Contextual data for this segment write, including deletion updates, norms, and field infos.
   * @param {Sorter.DocMap} sortMap - Optional mapping for docID remapping for sorts (null if not sorting).
   * @param {NormsProducer} norms - Producer for field norms information.
   * 
   * @throws {IOException} If an error occurs during writing or deletion application.
   */
  @Override
  public void flush(
      Map<String, TermsHashPerField> fieldsToFlush,
      final SegmentWriteState state,
      Sorter.DocMap sortMap,
      NormsProducer norms)
      throws IOException {
    super.flush(fieldsToFlush, state, sortMap, norms);

    // Aggregate and sort all fields with postings for this segment
    List<FreqProxTermsWriterPerField> allFields = new ArrayList<>();

    for (TermsHashPerField f : fieldsToFlush.values()) {
      final FreqProxTermsWriterPerField perField = (FreqProxTermsWriterPerField) f;
      if (perField.getNumTerms() > 0) {
        perField.sortTerms();
        assert perField.indexOptions != IndexOptions.NONE;
        allFields.add(perField);
      }
    }

    // No field in this segment contains postings - return early
    if (!state.fieldInfos.hasPostings()) {
      assert allFields.isEmpty();
      return;
    }
    // Sort all collected fields (by field name or metadata)
    CollectionUtil.introSort(allFields);

    Fields fields = new FreqProxFields(allFields);

    // Apply any segment delete markers before writing postings
    applyDeletes(state, fields);

    if (sortMap != null) {
      final Sorter.DocMap docMap = sortMap;
      final FieldInfos infos = state.fieldInfos;
      // Wrap postings Fields with a doc-sorting layer
      fields =
          new FilterLeafReader.FilterFields(fields) {

            /**
             * @function terms
             * @description
             * Returns Terms for a field, with wrapping logic for docID remapping according to docMap if present.
             * 
             * @param {String} field - The indexed field name
             * @returns {Terms} Possibly sorted Terms instance for this field.
             */
            @Override
            public Terms terms(final String field) throws IOException {
              Terms terms = in.terms(field);
              if (terms == null) {
                return null;
              } else {
                return new SortingTerms(terms, infos.fieldInfo(field).getIndexOptions(), docMap);
              }
            }
          };
    }

    // Invoke codec’s consumer to write out all postings and norms
    try (FieldsConsumer consumer =
        state.segmentInfo.getCodec().postingsFormat().fieldsConsumer(state)) {
      consumer.write(fields, norms);
    }
  }

  /**
   * Adds a field for postings collection.
   * 
   * @function addField
   * @description
   * Instantiates and returns a new FreqProxTermsWriterPerField for collecting postings for a field.
   * 
   * @param {FieldInvertState} invertState - Current inversion state for the field.
   * @param {FieldInfo} fieldInfo - Descriptor/info for the field.
   * @returns {TermsHashPerField} Per-field terms hash writer.
   */
  @Override
  public TermsHashPerField addField(FieldInvertState invertState, FieldInfo fieldInfo) {
    return new FreqProxTermsWriterPerField(
        invertState, this, fieldInfo, nextTermsHash.addField(invertState, fieldInfo));
  }

  /**
   * @class SortingTerms
   * @classdesc
   * Wraps a Terms instance to remap docIDs according to Sorter.DocMap, producing sorted posting enumeration.
   *
   * @prop {Sorter.DocMap} docMap - Mapping of old docIDs to new docIDs.
   * @prop {IndexOptions} indexOptions - Index options for the field (docs, freq, positions, offsets).
   */
  static class SortingTerms extends FilterLeafReader.FilterTerms {

    private final Sorter.DocMap docMap;
    private final IndexOptions indexOptions;

    /**
     * Constructs a SortingTerms.
     * 
     * @param {Terms} in - The original Terms instance.
     * @param {IndexOptions} indexOptions - Options describing postings details.
     * @param {Sorter.DocMap} docMap - Mapping of original to new docIDs.
     */
    SortingTerms(final Terms in, IndexOptions indexOptions, final Sorter.DocMap docMap) {
      super(in);
      this.docMap = docMap;
      this.indexOptions = indexOptions;
    }

    /**
     * @function iterator
     * @description Returns a TermsEnum that remaps docIDs on the fly.
     *
     * @returns {TermsEnum} Iterator over sorted terms.
     */
    @Override
    public TermsEnum iterator() throws IOException {
      return new SortingTermsEnum(in.iterator(), docMap, indexOptions);
    }

    /**
     * @function intersect
     * @description Returns a TermsEnum for terms matching the compiled automaton and remaps docIDs.
     *
     * @param {CompiledAutomaton} compiled - Automaton to intersect.
     * @param {BytesRef} startTerm - Optional starting term.
     * @returns {TermsEnum} Iterator for automaton-matching terms, with docID reordering.
     */
    @Override
    public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
      return new SortingTermsEnum(in.intersect(compiled, startTerm), docMap, indexOptions);
    }
  }

  /**
   * @class SortingTermsEnum
   * @classdesc
   * TermsEnum wrapper that produces a postings enumeration with docIDs remapped using Sorter.DocMap and offers specialized sorting enums for positional and doc-only postings.
   */
  private static class SortingTermsEnum extends FilterLeafReader.FilterTermsEnum {

    final Sorter.DocMap docMap; // Exposed for internal performance
    private final IndexOptions indexOptions;

    /**
     * Constructs a SortingTermsEnum.
     * 
     * @param {TermsEnum} in - Underlying TermsEnum positioned at sorted terms.
     * @param {Sorter.DocMap} docMap - Old-to-new docID mapping.
     * @param {IndexOptions} indexOptions - Options describing postings info needed.
     */
    SortingTermsEnum(final TermsEnum in, Sorter.DocMap docMap, IndexOptions indexOptions) {
      super(in);
      this.docMap = docMap;
      this.indexOptions = indexOptions;
    }

    /**
     * @function postings
     * @description
     * Returns a PostingsEnum for the current term, remapping docIDs and supporting positional and doc-only modes as per required posting flags.
     * 
     * @param {PostingsEnum} reuse - Reusable PostingsEnum, if available.
     * @param {int} flags - Bitwise OR of requested features (frequency, positions, offsets, payloads).
     *
     * @returns {PostingsEnum} Sorted postings enumeration for the term.
     */
    @Override
    public PostingsEnum postings(PostingsEnum reuse, final int flags) throws IOException {
      if (indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0
          && PostingsEnum.featureRequested(flags, PostingsEnum.FREQS)) {
        final PostingsEnum inReuse;
        final SortingPostingsEnum wrapReuse;
        if (reuse != null && reuse instanceof SortingPostingsEnum) {
          wrapReuse = (SortingPostingsEnum) reuse;
          inReuse = wrapReuse.getWrapped();
        } else {
          wrapReuse = new SortingPostingsEnum();
          inReuse = reuse;
        }
        final PostingsEnum inDocsAndPositions = in.postings(inReuse, flags);

        final boolean storePositions =
            indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
        final boolean storeOffsets =
            indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
        wrapReuse.reset(docMap, inDocsAndPositions, storePositions, storeOffsets);
        return wrapReuse;
      }

      final PostingsEnum inReuse;
      final SortingDocsEnum wrapReuse;
      if (reuse != null && reuse instanceof SortingDocsEnum) {
        wrapReuse = (SortingDocsEnum) reuse;
        inReuse = wrapReuse.getWrapped();
      } else {
        wrapReuse = new SortingDocsEnum();
        inReuse = reuse;
      }
      final PostingsEnum inDocs = in.postings(inReuse, flags);
      wrapReuse.reset(docMap, inDocs);
      return wrapReuse;
    }
  }

  /**
   * @class SortingDocsEnum
   * @classdesc
   * PostingsEnum for docID remapping. Aggregates document IDs for the term, remaps via docMap, sorts, and then provides doc iteration. Handles only doc-only (no positions) postings efficiently.
   */
  static class SortingDocsEnum extends PostingsEnum {

    private final LSBRadixSorter sorter;
    private PostingsEnum in;
    private int[] docs = IntsRef.EMPTY_INTS;
    private int docIt;
    private int upTo;

    /**
     * Constructs a SortingDocsEnum.
     */
    SortingDocsEnum() {
      sorter = new LSBRadixSorter();
    }

    /**
     * @function reset
     * @description
     * Fills and sorts the docs buffer using docMap; called per term for postings iteration.
     * 
     * @param {Sorter.DocMap} docMap - Map for old-to-new docID remapping.
     * @param {PostingsEnum} in - Underlying postings enumeration for the term.
     * @throws {IOException} On postings access/iteration failure.
     */
    void reset(Sorter.DocMap docMap, PostingsEnum in) throws IOException {
      this.in = in;
      int i = 0;
      for (int doc = in.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = in.nextDoc()) {
        // Grow docs array as needed
        if (docs.length <= i) {
          docs = ArrayUtil.grow(docs);
        }
        docs[i++] = docMap.oldToNew(doc);
      }
      upTo = i;
      if (docs.length == upTo) {
        docs = ArrayUtil.grow(docs);
      }
      docs[upTo] = DocIdSetIterator.NO_MORE_DOCS;
      final int maxDoc = docMap.size();
      final int numBits = PackedInts.bitsRequired(Math.max(0, maxDoc - 1));
      // Perform least-significant-bit radix sort for efficient in-place sorting of docIDs
      sorter.sort(numBits, docs, upTo);
      docIt = -1;
    }

    /**
     * Returns the wrapped underlying PostingsEnum.
     * @returns {PostingsEnum} The wrapped instance.
     */
    PostingsEnum getWrapped() {
      return in;
    }

    @Override
    public int advance(final int target) throws IOException {
      // Use sequential scan for small sorted posting lists
      return slowAdvance(target);
    }

    @Override
    public int docID() {
      return docIt < 0 ? -1 : docs[docIt];
    }

    @Override
    public int nextDoc() throws IOException {
      return docs[++docIt];
    }

    @Override
    public long cost() {
      return upTo;
    }

    @Override
    public int freq() throws IOException {
      return 1;
    }

    @Override
    public int nextPosition() throws IOException {
      return -1;
    }

    @Override
    public int startOffset() throws IOException {
      return -1;
    }

    @Override
    public int endOffset() throws IOException {
      return -1;
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return null;
    }
  }

  /**
   * @class SortingPostingsEnum
   * @classdesc
   * PostingsEnum for docID and positional sorting. Remaps docIDs, stores positions, offsets, and payloads, sorts by docID, and iterates them in order for codecs requiring document and position sorting.
   */
  static class SortingPostingsEnum extends PostingsEnum {

    /**
     * @class DocOffsetSorter
     * @classdesc
     * Helper TimSorter implementation sorting parallel arrays of docIDs and their data (offsets).
     */
    private static final class DocOffsetSorter extends TimSorter {

      private int[] docs;
      private long[] offsets;
      private int[] tmpDocs;
      private long[] tmpOffsets;

      /**
       * Constructs a DocOffsetSorter with the given number of temp slots.
       * 
       * @param {int} numTempSlots - Number of temporary slots for sorting.
       */
      public DocOffsetSorter(int numTempSlots) {
        super(numTempSlots);
        this.tmpDocs = IntsRef.EMPTY_INTS;
        this.tmpOffsets = LongsRef.EMPTY_LONGS;
      }

      /**
       * Resets the sorter with new docs and offsets arrays.
       * @param {int[]} docs - DocIDs to sort.
       * @param {long[]} offsets - Associated position/offset buffer indexes.
       */
      public void reset(int[] docs, long[] offsets) {
        this.docs = docs;
        this.offsets = offsets;
      }

      @Override
      protected int compare(int i, int j) {
        return docs[i] - docs[j];
      }

      @Override
      protected void swap(int i, int j) {
        int tmpDoc = docs[i];
        docs[i] = docs[j];
        docs[j] = tmpDoc;

        long tmpOffset = offsets[i];
        offsets[i] = offsets[j];
        offsets[j] = tmpOffset;
      }

      @Override
      protected void copy(int src, int dest) {
        docs[dest] = docs[src];
        offsets[dest] = offsets[src];
      }

      @Override
      protected void save(int i, int len) {
        if (tmpDocs.length < len) {
          tmpDocs = new int[ArrayUtil.oversize(len, Integer.BYTES)];
          tmpOffsets = new long[tmpDocs.length];
        }
        System.arraycopy(docs, i, tmpDocs, 0, len);
        System.arraycopy(offsets, i, tmpOffsets, 0, len);
      }

      @Override
      protected void restore(int i, int j) {
        docs[j] = tmpDocs[i];
        offsets[j] = tmpOffsets[i];
      }

      @Override
      protected int compareSaved(int i, int j) {
        return tmpDocs[i] - docs[j];
      }
    }

    private DocOffsetSorter sorter;
    private int[] docs = IntsRef.EMPTY_INTS;
    private long[] offsets = LongsRef.EMPTY_LONGS;
    private int upto;

    private ByteBuffersDataInput postingInput;
    private PostingsEnum in;
    private boolean storePositions, storeOffsets;

    private int docIt;
    private int pos;
    private int startOffset;
    private int endOffset;
    private final BytesRef payload = new BytesRef();
    private int currFreq;

    private final ByteBuffersDataOutput buffer = ByteBuffersDataOutput.newResettableInstance();

    /**
     * @function reset
     * @description
     * Fills arrays with all matching docIDs and position data, remapped via docMap,
     * then sorts the arrays by new docID. Serializes positional/offset data into a memory buffer.
     * 
     * @param {Sorter.DocMap} docMap - Old-to-new docID mapping.
     * @param {PostingsEnum} in - Source postings enumeration to wrap.
     * @param {boolean} storePositions - Whether to store positional data.
     * @param {boolean} storeOffsets - Whether to store offsets data.
     * @throws {IOException} On failures during postings iteration.
     */
    void reset(Sorter.DocMap docMap, PostingsEnum in, boolean storePositions, boolean storeOffsets)
        throws IOException {
      this.in = in;
      this.storePositions = storePositions;
      this.storeOffsets = storeOffsets;
      if (sorter == null) {
        final int numTempSlots = docMap.size() / 8;
        sorter = new DocOffsetSorter(numTempSlots);
      }
      docIt = -1;
      startOffset = -1;
      endOffset = -1;
      buffer.reset();
      int doc;
      int i = 0;
      while ((doc = in.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        if (i == docs.length) {
          final int newLength = ArrayUtil.oversize(i + 1, 4);
          docs = ArrayUtil.growExact(docs, newLength);
          offsets = ArrayUtil.growExact(offsets, newLength);
        }
        docs[i] = docMap.oldToNew(doc);
        offsets[i] = buffer.size();
        addPositions(in, buffer);
        i++;
      }
      upto = i;
      sorter.reset(docs, offsets);
      sorter.sort(0, upto);

      this.postingInput = buffer.toDataInput();
    }

    /**
     * @function addPositions
     * @description
     * Serializes frequency, positions, offsets, and payloads for the current document into an output buffer.
     * 
     * @param {PostingsEnum} in - The wrapped postings enum.
     * @param {DataOutput} out - Output stream for buffer serialization.
     * @throws {IOException} On underlying enum or serialization failure.
     */
    private void addPositions(final PostingsEnum in, final DataOutput out) throws IOException {
      int freq = in.freq();
      out.writeVInt(freq);
      if (storePositions) {
        int previousPosition = 0;
        int previousEndOffset = 0;
        for (int i = 0; i < freq; i++) {
          final int pos = in.nextPosition();
          final BytesRef payload = in.getPayload();

          // Position delta encoding and payload flagging
          final int token = (pos - previousPosition) << 1 | (payload == null ? 0 : 1);
          out.writeVInt(token);
          previousPosition = pos;
          if (storeOffsets) {
            final int startOffset = in.startOffset();
            final int endOffset = in.endOffset();
            out.writeVInt(startOffset - previousEndOffset);
            out.writeVInt(endOffset - startOffset);
            previousEndOffset = endOffset;
          }
          if (payload != null) {
            out.writeVInt(payload.length);
            out.writeBytes(payload.bytes, payload.offset, payload.length);
          }
        }
      }
    }

    /**
     * Returns the wrapped underlying PostingsEnum.
     * @returns {PostingsEnum} The wrapped instance.
     */
    PostingsEnum getWrapped() {
      return in;
    }

    @Override
    public int advance(final int target) throws IOException {
      // Use linear scan for advance (valid for rare doc/term sets)
      return slowAdvance(target);
    }

    @Override
    public int docID() {
      return docIt < 0 ? -1 : docIt >= upto ? NO_MORE_DOCS : docs[docIt];
    }

    @Override
    public int endOffset() throws IOException {
      return endOffset;
    }

    @Override
    public int freq() throws IOException {
      return currFreq;
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return payload.length == 0 ? null : payload;
    }

    @Override
    public int nextDoc() throws IOException {
      if (++docIt >= upto) return DocIdSetIterator.NO_MORE_DOCS;
      postingInput.seek(offsets[docIt]);
      currFreq = postingInput.readVInt();

      pos = 0;
      endOffset = 0;
      return docs[docIt];
    }

    @Override
    public int nextPosition() throws IOException {
      if (storePositions == false) {
        return -1;
      }
      final int token = postingInput.readVInt();
      pos += token >>> 1;
      if (storeOffsets) {
        startOffset = endOffset + postingInput.readVInt();
        endOffset = startOffset + postingInput.readVInt();
      }
      if ((token & 1) != 0) {
        payload.offset = 0;
        payload.length = postingInput.readVInt();
        if (payload.length > payload.bytes.length) {
          payload.bytes = new byte[ArrayUtil.oversize(payload.length, 1)];
        }
        postingInput.readBytes(payload.bytes, 0, payload.length);
      } else {
        payload.length = 0;
      }
      return pos;
    }

    @Override
    public int startOffset() throws IOException {
      return startOffset;
    }

    @Override
    public long cost() {
      return in.cost();
    }
  }
}
