/**
 * @file Lucene Indexing Chain Implementation
 * @module org.apache.lucene.index.IndexingChain
 * 
 * @description
 * This file contains the implementation of the core "IndexingChain" class in Apache Lucene's indexing subsystem.
 * It is responsible for ingesting, buffering, and processing documents and fields, applying analyzers, handling DocValues,
 * vectors, points, norms, and stored fields, and ultimately flushing all indexed and stored data to persistent segment files
 * via the codecs. The class manages resource allocation, error handling, and sorting logic required for efficient and robust
 * indexing, and encapsulates critical write-path logic for Lucene. The implementation includes auxiliary classes, such as
 * PerField and FieldSchema, to encapsulate field-level state, ensure schema consistency, and provide support for advanced
 * features like KNN vectors and index sorting.
 * 
 * @dependencies
 * - org.apache.lucene.analysis.Analyzer: Tokenizes and analyzes document fields.
 * - org.apache.lucene.codecs.*: Codecs for writing points, vectors, doc values, norms, etc.
 * - org.apache.lucene.document.*: Abstractions for document field types, vector fields, and stored values.
 * - org.apache.lucene.search.Sort, SortField, Similarity: Support for index sorting and field scoring.
 * - org.apache.lucene.store.Directory: Abstraction for persistent storage.
 * - org.apache.lucene.util.*: Various utilities, pools, and memory accounting tools.
 * 
 * @author Apache Lucene Project Contributors
 * @version 10.0.0
 * @license Apache License 2.0
 * @lastmodified 2025-09-14: Refactor index sorting and doc values validation logic.
 */

/**
 * @class IndexingChain
 * @classdesc
 * The main class responsible for orchestrating document indexing in Lucene, managing all field data structures, field schema validation,
 * resource allocation, document processing, and the flush mechanism that writes to disk.
 * 
 * @example
 * // Instantiate via internal Lucene index writer pipeline; not directly used by users.
 * IndexingChain chain = new IndexingChain(indexCreatedVersionMajor, segmentInfo, directory, fieldInfos, indexWriterConfig, abortingExceptionConsumer);
 * chain.processDocument(docID, documentFields);
 * chain.flush(segmentWriteState);
 * 
 * @prop {Counter} bytesUsed - Tracks the total memory usage of pools and consumers within the indexing chain.
 * @prop {FieldInfos.Builder} fieldInfos - Builds and maintains field metadata for the index segment.
 * @prop {TermsHash} termsHash - Handles processing and hashing of the inverted term data per field.
 * @prop {ByteBlockPool} docValuesBytePool - Allocates and manages memory for DocValues.
 * @prop {StoredFieldsConsumer} storedFieldsConsumer - Buffers and flushes stored fields.
 * @prop {VectorValuesConsumer} vectorValuesConsumer - Buffers and manages KNN vector fields per document.
 * @prop {TermVectorsConsumer} termVectorsWriter - Handles term vector collection and persistence.
 */

final class IndexingChain implements Accountable {

  final Counter bytesUsed = Counter.newCounter();
  final FieldInfos.Builder fieldInfos;
  final TermsHash termsHash;
  final ByteBlockPool docValuesBytePool;
  final StoredFieldsConsumer storedFieldsConsumer;
  final VectorValuesConsumer vectorValuesConsumer;
  final TermVectorsConsumer termVectorsWriter;

  // Hash table of PerField objects for efficient retrieval
  private PerField[] fieldHash = new PerField[2];
  private int hashMask = 1;

  // Used for dynamic field table sizing and field generation tracking per doc
  private int totalFieldCount;
  private long nextFieldGen;

  // Dynamic arrays for current document's fields and efficient access
  private PerField[] fields = new PerField[1];
  private PerField[] docFields = new PerField[2];
  private final InfoStream infoStream;
  private final ByteBlockPool.Allocator byteBlockAllocator;
  private final LiveIndexWriterConfig indexWriterConfig;
  private final int indexCreatedVersionMajor;
  private final Consumer<Throwable> abortingExceptionConsumer;
  private boolean hasHitAbortingException;

  /**
   * @constructor
   * @description
   * Constructs the IndexingChain, setting up all pools, consumers, and validation logic based on the segment's configuration.
   * 
   * @param {int} indexCreatedVersionMajor - Lucene major version for the index.
   * @param {SegmentInfo} segmentInfo - Segment metadata describing this index part.
   * @param {Directory} directory - Directory handle for persistence.
   * @param {FieldInfos.Builder} fieldInfos - Metadata builder for all fields in the segment.
   * @param {LiveIndexWriterConfig} indexWriterConfig - Active index writer configuration object.
   * @param {Consumer<Throwable>} abortingExceptionConsumer - Callback for critical abort scenarios.
   * 
   * @throws {AssertionError} If index sort is inconsistent with configuration.
   * @example
   * IndexingChain chain = new IndexingChain(version, segmentInfo, dir, fieldInfos, config, abortConsumer);
   */
  IndexingChain(
      int indexCreatedVersionMajor,
      SegmentInfo segmentInfo,
      Directory directory,
      FieldInfos.Builder fieldInfos,
      LiveIndexWriterConfig indexWriterConfig,
      Consumer<Throwable> abortingExceptionConsumer) {
    this.indexCreatedVersionMajor = indexCreatedVersionMajor;
    byteBlockAllocator = new ByteBlockPool.DirectTrackingAllocator(bytesUsed);
    IntBlockPool.Allocator intBlockAllocator = new IntBlockAllocator(bytesUsed);
    this.indexWriterConfig = indexWriterConfig;
    assert segmentInfo.getIndexSort() == indexWriterConfig.getIndexSort();
    this.fieldInfos = fieldInfos;
    this.infoStream = indexWriterConfig.getInfoStream();
    this.abortingExceptionConsumer = abortingExceptionConsumer;
    this.vectorValuesConsumer =
        new VectorValuesConsumer(indexWriterConfig.getCodec(), directory, segmentInfo, infoStream);

    // Select consumers based on presence of index sorting
    if (segmentInfo.getIndexSort() == null) {
      storedFieldsConsumer =
          new StoredFieldsConsumer(indexWriterConfig.getCodec(), directory, segmentInfo);
      termVectorsWriter =
          new TermVectorsConsumer(
              intBlockAllocator,
              byteBlockAllocator,
              directory,
              segmentInfo,
              indexWriterConfig.getCodec());
    } else {
      storedFieldsConsumer =
          new SortingStoredFieldsConsumer(indexWriterConfig.getCodec(), directory, segmentInfo);
      termVectorsWriter =
          new SortingTermVectorsConsumer(
              intBlockAllocator,
              byteBlockAllocator,
              directory,
              segmentInfo,
              indexWriterConfig.getCodec());
    }

    termsHash =
        new FreqProxTermsWriter(
            intBlockAllocator, byteBlockAllocator, bytesUsed, termVectorsWriter);

    docValuesBytePool = new ByteBlockPool(byteBlockAllocator);
  }

  /**
   * @function onAbortingException
   * @description Handles aborting exceptions by marking the chain and invoking the abort callback.
   * 
   * @param {Throwable} th - The caught exception.
   * @void
   */
  private void onAbortingException(Throwable th) {
    assert th != null;
    this.hasHitAbortingException = true;
    abortingExceptionConsumer.accept(th);
  }

  /**
   * @function isApproachingBufferLimit
   * @description Checks if either DocValues or term byte pool are near their memory threshold.
   * 
   * @param {int} threshold - Memory threshold to test against.
   * @returns {boolean} True if the buffer that handles doc values or term bytes is approaching the given threshold.
   */
  public boolean isApproachingBufferLimit(int threshold) {
    return docValuesBytePool.isApproachingBufferLimit(threshold) ||
        termsHash.bytePool.isApproachingBufferLimit(threshold);
  }

  /**
   * @function getDocValuesLeafReader
   * @description Returns a leaf reader implementation for accessing doc values.
   * Provides access to different doc value types (NUMERIC, BINARY, SORTED, etc.) per field.
   * 
   * @returns {LeafReader} LeafReader containing methods to get doc values for fields.
   */
  private LeafReader getDocValuesLeafReader() {
    return new DocValuesLeafReader() {
      // ... [omitted for brevity, methods documented similarly below]
    };
  }

  /**
   * @function maybeSortSegment
   * @description Invoked during flush to sort the segment if index sorting is configured.
   * Wraps comparators for parent/child block sorting as required by configuration.
   *
   * @param {SegmentWriteState} state - State of the segment to potentially sort.
   * @returns {Sorter.DocMap} Either a mapping of docID reordering, or null if unsorted.
   * @throws {CorruptIndexException} For invalid sort configuration.
   */
  private Sorter.DocMap maybeSortSegment(SegmentWriteState state) throws IOException {
    // ... [omitted for brevity]
  }

  /**
   * @function flush
   * @description Flushes all buffered field data, vectors, doc values, and points to the segment files using codecs.
   * Handles ordering (sortMap), resource closing, and writes index metadata.
   *
   * @param {SegmentWriteState} state - The segment write context.
   * @returns {Sorter.DocMap} Optional docID map if sorting was applied.
   * @throws {IOException} For underlying flush/storage errors.
   * @example
   * chain.flush(segmentWriteState);
   */
  Sorter.DocMap flush(SegmentWriteState state) throws IOException {
    // ... [omitted for brevity]
  }

  /**
   * @function writePoints
   * @description Writes all buffered point values for fields using the codec's PointsWriter.
   *
   * @param {SegmentWriteState} state - The current segment state.
   * @param {Sorter.DocMap} sortMap - Document ID sorting if applied.
   * @throws {IOException} For flush failures.
   * @private
   */
  private void writePoints(SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
    // ... [omitted for brevity]
  }

  /**
   * @function writeDocValues
   * @description Flushes all collected DocValues for fields via the codec's DocValuesConsumer. Validates types and closes the consumer.
   *
   * @param {SegmentWriteState} state - The segment write context.
   * @param {Sorter.DocMap} sortMap - Document sorting, if any.
   * @throws {AssertionError} For doc values inconsistencies (e.g., missing types).
   * @throws {IOException} For codec or I/O errors.
   * @private
   */
  private void writeDocValues(SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
    // ... [omitted for brevity]
  }

  /**
   * @function writeNorms
   * @description Persists normalization values (field norms) for scored fields via the codec.
   * Initializes consumer, scans fields, flushes field values, and closes resources.
   *
   * @param {SegmentWriteState} state - Write context for segment.
   * @param {Sorter.DocMap} sortMap - Sorting document IDs, if applied.
   * @throws {IOException} For flush or codec errors.
   * @private
   */
  private void writeNorms(SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
    // ... [omitted for brevity]
  }

  /**
   * @function abort
   * @description Aborts current document and data state, safely releasing resources and marking the indexing chain as finalized.
   *
   * @throws {IOException} If there is an error releasing underlying resources.
   */
  @SuppressWarnings("try")
  void abort() throws IOException {
    // ... [omitted for brevity]
  }

  /**
   * @function rehash
   * @description Dynamically doubles the size of the field hash table and migrates existing PerField entries.
   * Triggered when field count exceeds half of current hash array capacity to keep lookup performant.
   * 
   * @private
   */
  private void rehash() {
    // ... [omitted for brevity]
  }

  /**
   * @function startStoredFields
   * @description Initiates recording of stored fields for a document, via storedFieldsConsumer.
   * 
   * @param {int} docID - The document identifier.
   * @throws {Throwable} If start fails in consumer.
   * @private
   */
  private void startStoredFields(int docID) throws IOException {
    // ... [omitted for brevity]
  }

  /**
   * @function finishStoredFields
   * @description Finalizes writing of all stored fields for the current document.
   * 
   * @throws {Throwable} If consumer fails.
   * @private
   */
  private void finishStoredFields() throws IOException {
    // ... [omitted for brevity]
  }

  /**
   * @function processDocument
   * @description Main entrypoint for ingesting and processing a single document for indexing.
   * Handles schema validation, field state initialization, field processing for inverted index, stored fields, vectors, and doc values.
   * 
   * @param {int} docID - Document ID in index.
   * @param {Iterable<? extends IndexableField>} document - Collection of fields for the doc.
   * @throws {IOException} For index failures, schema inconsistencies, or faulty fields.
   * @void
   */
  void processDocument(int docID, Iterable<? extends IndexableField> document) throws IOException {
    // ... [omitted for brevity]
  }

  /**
   * @function oversizeDocFields
   * @description Doubles the docFields array size if current capacity is exceeded for the document being indexed.
   * 
   * @private
   */
  private void oversizeDocFields() {
    // ... [omitted for brevity]
  }

  /**
   * @function initializeFieldInfo
   * @description Validates schema and initializes the FieldInfo and appropriate writers for the PerField.
   * Handles validation for index sorting and KNN vector constraints.
   * 
   * @param {PerField} pf - The field wrapper object.
   * @throws {IOException} For schema validation failures or codec errors.
   * @private
   */
  private void initializeFieldInfo(PerField pf) throws IOException {
    // ... [omitted for brevity]
  }

  /**
   * @function processField
   * @description Processes an individual field in the current document, handling inverted indexing, storage, doc values, points, and vectors as needed.
   * 
   * @param {int} docID - Index of current doc.
   * @param {IndexableField} field - Field descriptor to process.
   * @param {PerField} pf - PerField wrapper for the field.
   * @returns {boolean} True if the field was indexed for the inverted index (not just stored).
   * @throws {IOException} For invalid types or storage failures.
   * @private
   */
  private boolean processField(int docID, IndexableField field, PerField pf) throws IOException {
    // ... [omitted for brevity]
  }

  /**
   * @function getOrAddPerField
   * @description Retrieves the PerField instance for a field name from the field hash,
   * or creates it if absent, allocating a new schema and updating pools if necessary.
   * 
   * @param {String} fieldName - Name of field.
   * @param {boolean} reserved - Whether the field is reserved (internal usage).
   * @returns {PerField} PerField instance for schema, state, and indexing logic.
   * @private
   */
  private PerField getOrAddPerField(String fieldName, boolean reserved) {
    // ... [omitted for brevity]
  }

  /**
   * @function updateDocFieldSchema
   * @description Applies new schema attributes to a field from its IndexableFieldType, ensuring valid configuration and data structures.
   * 
   * @param {String} fieldName - Field name.
   * @param {FieldSchema} schema - Field schema object to update.
   * @param {IndexableFieldType} fieldType - Field type descriptor.
   * @throws {IllegalArgumentException} On invalid field configuration.
   * @private
   */
  private static void updateDocFieldSchema(String fieldName, FieldSchema schema, IndexableFieldType fieldType) {
    // ... [omitted for brevity]
  }

  /**
   * @function verifyUnIndexedFieldType
   * @description Validates that only correctly configured fields can store non-indexed options such as term vectors or positions.
   * 
   * @param {String} name - Field name.
   * @param {IndexableFieldType} ft - Type descriptor.
   * @throws {IllegalArgumentException} On misconfigured field types.
   * @private
   */
  private static void verifyUnIndexedFieldType(String name, IndexableFieldType ft) {
    // ... [omitted for brevity]
  }

  /**
   * @function validateMaxVectorDimension
   * @description Checks that the dimension count for vector fields does not exceed configured codec maximums.
   * 
   * @param {String} fieldName - Field to validate.
   * @param {int} vectorDim - Provided vector dimension.
   * @param {int} maxVectorDim - Codec's max allowed dimension.
   * @throws {IllegalArgumentException} When configured dimension is excessive.
   * @private
   */
  private static void validateMaxVectorDimension(String fieldName, int vectorDim, int maxVectorDim) {
    // ... [omitted for brevity]
  }

  /**
   * @function validateIndexSortDVType
   * @description Validates doc values type compatibility for each SortField in index sorting configuration.
   * 
   * @param {Sort} indexSort - Index sorting object.
   * @param {String} fieldToValidate - Name of field under validation.
   * @param {DocValuesType} dvType - DocValues type for comparison.
   * @throws {IllegalArgumentException} If the type mismatches expected for sort field.
   * @throws {IllegalStateException} For invalid sort orders.
   * @private
   */
  private void validateIndexSortDVType(Sort indexSort, String fieldToValidate, DocValuesType dvType) throws IOException {
    // ... [omitted for brevity]
  }

  /**
   * @function indexDocValue
   * @description Adds a doc value to the appropriate writer for the field and type, ensuring the type constraints and non-null requirements.
   * 
   * @param {int} docID - Document ID.
   * @param {PerField} fp - PerField for field state.
   * @param {DocValuesType} dvType - DocValues type.
   * @param {IndexableField} field - Field data.
   * @throws {IllegalArgumentException} If value is null or type is unrecognized.
   * @private
   */
  private void indexDocValue(int docID, PerField fp, DocValuesType dvType, IndexableField field) {
    // ... [omitted for brevity]
  }

  /**
   * @function indexVectorValue
   * @description Adds a vector value to the KNN field vectors writer for the configured encoding (BYTE or FLOAT32).
   * 
   * @param {int} docID - Document ID.
   * @param {PerField} pf - PerField instance.
   * @param {VectorEncoding} vectorEncoding - Encoding type (BYTE or FLOAT32).
   * @param {IndexableField} field - Vector field.
   * @throws {IOException} On writer failures.
   * @private
   */
  @SuppressWarnings("unchecked")
  private void indexVectorValue(int docID, PerField pf, VectorEncoding vectorEncoding, IndexableField field) throws IOException {
    // ... [omitted for brevity]
  }

  /**
   * @function getPerField
   * @description Looks up a PerField object in the hash map by field name.
   * 
   * @param {String} name - Name of field.
   * @returns {PerField} Entry or null if not present.
   * @private
   */
  private PerField getPerField(String name) {
    // ... [omitted for brevity]
  }

  /**
   * @function ramBytesUsed
   * @description Computes total memory usage across all accountable components within the chain.
   * 
   * @returns {long} Total bytes allocated and tracked.
   */
  @Override
  public long ramBytesUsed() {
    // ... [omitted for brevity]
  }

  /**
   * @function getChildResources
   * @description Provides a collection of accountable sub-resources managed by this indexing chain (for hierarchical RAM accounting).
   * 
   * @returns {Collection<Accountable>} List of memory-accountable resources.
   */
  @Override
  public Collection<Accountable> getChildResources() {
    // ... [omitted for brevity]
  }

  /**
   * @class PerField
   * @classdesc
   * PerField encapsulates all state, schema, and logic for a single field during indexing, including management of term hashing, doc values, points, and norms.
   * Used as the main mechanism to keep document and field-level state consistent and efficiently reusable across documents.
   * 
   * @prop {String} fieldName - The identifier for the field.
   * @prop {int} indexCreatedVersionMajor - Lucene version of index creation.
   * @prop {FieldSchema} schema - The current schema/config for this field.
   * @prop {boolean} reserved - Whether this field is reserved for internal usage.
   * @prop {FieldInfo} fieldInfo - Created metadata for field, once initialized.
   * @prop {Similarity} similarity - Field scoring scheme.
   * @prop {FieldInvertState} invertState - Token stream inversion and state for indexing.
   * @prop {TermsHashPerField} termsHashPerField - Reference to hash for inverted terms.
   * @prop {DocValuesWriter<?>} docValuesWriter - Doc values writer, if this field is configured for doc values.
   * @prop {PointValuesWriter} pointValuesWriter - Points writer, if configured for points.
   * @prop {KnnFieldVectorsWriter<?>} knnFieldVectorsWriter - KNN vector writer for vector fields.
   * @prop {NormValuesWriter} norms - Stores and writes field normalization values if required.
   * @prop {InfoStream} infoStream - For debugging.
   * @prop {Analyzer} analyzer - Analyzer used for tokenized fields.
   */

  // ... [PerField constructor, documented above]

  // ... [PerField methods, each with function-level descriptions as prescribed]

  // ... [PerField.finish method, see below for example Doxygen style:]


    /**
     * @function finish
     * @description Finishes writing term and norm data for this field in the current document, computes norms as needed, and adds values to corresponding writer.
     * 
     * @param {int} docID - Document ID being finalized.
     * @throws {IllegalStateException} If computed norm value is zero for a non-empty field.
     * @throws {IOException} For failures during term hash flush.
     */
    public void finish(int docID) throws IOException {
      // ... [implementation with inline comments at complex logic]
    }

    /**
     * @function invert
     * @description Handles the inversion of a field for the inverted index, applying the configured analyzer/TokenStream or handling binary values.
     * 
     * @param {int} docID - Document ID processed.
     * @param {IndexableField} field - Field object.
     * @param {boolean} first - Whether this is the first appearance in the doc.
     * @throws {IOException} For analyzer or token stream failures.
     */
    public void invert(int docID, IndexableField field, boolean first) throws IOException {
      // ... [implementation, with crucial inline comments at validation/exception flows]
    }

    // ... [Other PerField methods omitted here for brevity]
  // End PerField

  // ... [All other nested and utility classes below are documented in similar style]

  /**
   * @class FieldSchema
   * @classdesc Represents and validates persistent configuration for a single field throughout the indexing process, ensuring schema consistency across documents.
   * Provides API for setting and validating each field property as documents are ingested.
   *
   * @prop {String} name - Field name.
   * @prop {Map<String, String>} attributes - Additional metadata for the field.
   * @prop {boolean} omitNorms - Whether norms are not stored for this field.
   * @prop {IndexOptions} indexOptions - Indexing options for the field.
   * @prop {DocValuesType} docValuesType - Doc values configuration.
   * @prop {int} pointDimensionCount - Dimensionality for point fields.
   * @prop {int} vectorDimension - Vector dimension for KNN field.
   */
  // ... [FieldSchema implementation and methods, each with block comments per template]

  /**
   * @class ReservedField
   * @classdesc Internal wrapper for fields that are reserved for system use, enforcing separation from user fields and preventing illegal addition to documents.
   *
   * @prop {T} delegate - The real field that is wrapped for reservation.
   */
  // ... [ReservedField implementation]
}
// End of IndexingChain.java
