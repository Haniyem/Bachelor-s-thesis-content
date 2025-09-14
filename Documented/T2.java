```java
/**
 * @file DocumentsWriterDeleteQueue.java
 * @module org.apache.lucene.index
 * 
 * @description
 * Provides a sophisticated, concurrent-safe, non-blocking delete queue implementation for Lucene indexing. This class maintains and applies buffered document, term, and query deletes, as well as DocValues updates, required for atomic and efficient index modifications. It supports both thread-private and global delete management for per-thread and overall pool state, including coordination of sequence numbers for transactional update visibility and commit consistency.
 * 
 * @dependencies
 * - org.apache.lucene.index.BufferedUpdates: Manages application of term/query deletes and doc values updates.
 * - org.apache.lucene.store.AlreadyClosedException: Exception handling for closed queue state.
 * - java.util.concurrent.locks.ReentrantLock: Ensures concurrency safety for the global update operations.
 * - java.util.concurrent.atomic.AtomicLong: Provides monotonic sequence number generation across threads.
 * - org.apache.lucene.util.InfoStream: Logging and debugging.
 * 
 * @author Apache Software Foundation
 * @version 1.0
 * @license Apache-2.0
 * @lastmodified 2025-09-14: Heavily documented for clarity and codebase onboarding.
 */

/**
 * @class DocumentsWriterDeleteQueue
 * @classdesc Maintains a non-blocking, concurrent deletion queue to buffer and apply deletes and updates during Lucene indexing.
 * 
 * @example
 * InfoStream info = ...;
 * DocumentsWriterDeleteQueue queue = new DocumentsWriterDeleteQueue(info);
 * queue.addDelete(new Term("field", "value"));
 *
 * // For per-thread usage:
 * DocumentsWriterDeleteQueue.DeleteSlice slice = queue.newSlice();
 * queue.addDelete(..., slice);
 * slice.apply(...);
 * 
 * @prop {Node<?>} tail - Current last node (latest delete operation) in the queue.
 * @prop {boolean} closed - Indicates whether the queue is closed to further modifications.
 * @prop {DeleteSlice} globalSlice - Reference slice for the global delete pool.
 * @prop {BufferedUpdates} globalBufferedUpdates - All pending global deletes/updates for committed segments.
 * @prop {ReentrantLock} globalBufferLock - Lock protecting global buffer state during mutations.
 * @prop {long} generation - Generation number for logical queue progression (e.g., after flush).
 * @prop {AtomicLong} nextSeqNo - Generator for monotonically increasing sequence numbers.
 * @prop {InfoStream} infoStream - Logging utility for state changes.
 * @prop {long} maxSeqNo - Maximum permitted sequence number at queue advancement.
 * @prop {long} startSeqNo - Starting sequence number used for this queue instance.
 * @prop {LongSupplier} previousMaxSeqId - Supplies the maximum sequence number of the prior queue for fallback.
 * @prop {boolean} advanced - Indicates if this queue has been superseded via advanceQueue().
 */
final class DocumentsWriterDeleteQueue implements Accountable, Closeable {

  // Latest operation in the delete queue (sentinel/real node)
  private volatile Node<?> tail;

  // Queue closed to further changes
  private volatile boolean closed = false;

  // Slice for global pool of deletes; shared among threads
  private final DeleteSlice globalSlice;

  // Consolidated buffer for all global deletes
  private final BufferedUpdates globalBufferedUpdates;

  // Lock for safe mutation and flushing of the global buffer
  final ReentrantLock globalBufferLock = new ReentrantLock();

  final long generation;

  // Generator for unique, total-order operation sequence numbers
  private final AtomicLong nextSeqNo;

  private final InfoStream infoStream;

  // Sequence number boundary after this instance is advanced/flushed
  private volatile long maxSeqNo = Long.MAX_VALUE;

  private final long startSeqNo;
  private final LongSupplier previousMaxSeqId;
  private boolean advanced;

  /**
   * Constructs a new delete queue with a sentinel tail node and initial sequence number.
   * 
   * @constructor
   * @param {InfoStream} infoStream - Logging and diagnostic stream for index writer events.
   */
  DocumentsWriterDeleteQueue(InfoStream infoStream) {
    // seqNo must start at 1 for API historical contract
    this(infoStream, 0, 1, () -> 0);
  }

  /**
   * Primary constructor for queue advancement and sequence management.
   * 
   * @param {InfoStream} infoStream - Info logging.
   * @param {long} generation - Logical generation of the queue.
   * @param {long} startSeqNo - Initial sequence number for this queue.
   * @param {LongSupplier} previousMaxSeqId - Supplier for previous queue's max sequence number.
   */
  private DocumentsWriterDeleteQueue(
      InfoStream infoStream, long generation, long startSeqNo, LongSupplier previousMaxSeqId) {
    this.infoStream = infoStream;
    this.globalBufferedUpdates = new BufferedUpdates("global");
    this.generation = generation;
    this.nextSeqNo = new AtomicLong(startSeqNo);
    this.startSeqNo = startSeqNo;
    this.previousMaxSeqId = previousMaxSeqId;
    long value = previousMaxSeqId.getAsLong();
    assert value <= startSeqNo : "illegal max sequence ID: " + value + " start was: " + startSeqNo;
    // Sentinel node ensures clean-one-way linking; no head required.
    tail = new Node<>(null);
    globalSlice = new DeleteSlice(tail);
  }

  /**
   * Adds delete queries to the queue and applies the global slice if possible.
   * 
   * @function addDelete
   * @description Appends one or more Query deletes to the queue for all DWPTs, immediately attempting to advance and apply the global slice to the update buffer.
   * 
   * @param {Query[]} queries - One or more queries to be marked for deletion.
   * @returns {long} The sequence number assigned to the added operation.
   */
  long addDelete(Query... queries) {
    long seqNo = add(new QueryArrayNode(queries));
    tryApplyGlobalSlice();
    return seqNo;
  }

  /**
   * Adds term deletes to the queue and applies the global slice.
   * 
   * @function addDelete
   * @param {Term[]} terms - Terms to be marked for deletion.
   * @returns {long} The sequence number for the operation.
   */
  long addDelete(Term... terms) {
    long seqNo = add(new TermArrayNode(terms));
    tryApplyGlobalSlice();
    return seqNo;
  }

  /**
   * Adds DocValues updates to the delete queue.
   * 
   * @function addDocValuesUpdates
   * @param {DocValuesUpdate[]} updates - Array of DocValues updates to apply for matching terms.
   * @returns {long} Sequence number assigned to the updates.
   */
  long addDocValuesUpdates(DocValuesUpdate... updates) {
    long seqNo = add(new DocValuesUpdatesNode(updates));
    tryApplyGlobalSlice();
    return seqNo;
  }

  // Utility constructor for term node
  static Node<Term> newNode(Term term) {
    return new TermNode(term);
  }

  // Utility constructor for single query node
  static Node<Query> newNode(Query query) {
    return new QueryNode(query);
  }

  // Utility for doc values update node construction
  static Node<DocValuesUpdate[]> newNode(DocValuesUpdate... updates) {
    return new DocValuesUpdatesNode(updates);
  }

  /**
   * Adds a delete node with atomicity guarantees for thread slice.
   * 
   * @function add
   * @description Atomically adds a delete node (e.g., term, query, or update) and updates the provided thread-private DeleteSlice so that future reads see this node. Guarantees atomic application with respect to concurrent updates on the same key (e.g., docID, term).
   * 
   * @param {Node<?>} deleteNode - Delete node to add.
   * @param {DeleteSlice} slice - Per-thread DeleteSlice tracking its "own" head/tail state.
   * @returns {long} Sequence number assigned to this delete node.
   */
  long add(Node<?> deleteNode, DeleteSlice slice) {
    long seqNo = add(deleteNode);
    // Ensures per-thread atomicity across slice and global buffer for update visibility
    slice.sliceTail = deleteNode;
    assert slice.sliceHead != slice.sliceTail : "slice head and tail must differ after add";
    tryApplyGlobalSlice();
    return seqNo;
  }

  /**
   * Appends a new node to the global queue tail.
   * 
   * @function add
   * @param {Node<?>} newNode - New node to be appended.
   * @returns {long} Sequence number for the node.
   * @throws {AlreadyClosedException} If the queue has been closed to changes.
   */
  synchronized long add(Node<?> newNode) {
    ensureOpen(); // Throws if closed
    tail.next = newNode;
    this.tail = newNode;
    return getNextSequenceNumber();
  }

  /**
   * Checks if any changes (pending deletes/updates) are buffered 
   * globally or in slices.
   * 
   * @function anyChanges
   * @returns {boolean} True if there are pending deletes or updates.
   */
  boolean anyChanges() {
    globalBufferLock.lock();
    try {
      // Checks for unapplied global deletes, non-empty slices, or unlinked tail.
      return globalBufferedUpdates.any()
          || !globalSlice.isEmpty()
          || globalSlice.sliceTail != tail
          || tail.next != null;
    } finally {
      globalBufferLock.unlock();
    }
  }

  /**
   * Attempts to apply the global slice to the buffered deletes, if no update is already in progress.
   * 
   * @function tryApplyGlobalSlice
   */
  void tryApplyGlobalSlice() {
    if (globalBufferLock.tryLock()) {
      ensureOpen();
      try {
        if (updateSliceNoSeqNo(globalSlice)) {
          globalSlice.apply(globalBufferedUpdates, BufferedUpdates.MAX_INT);
        }
      } finally {
        globalBufferLock.unlock();
      }
    }
  }

  /**
   * Freezes the global buffer, applying all pending deletes, and prepares a snapshot for flush/commit.
   * 
   * @function freezeGlobalBuffer
   * @param {DeleteSlice} callerSlice - Caller thread’s DeleteSlice (if present); null for global freeze.
   * @returns {FrozenBufferedUpdates} Snapshot of all buffered updates at this moment, or null if already closed.
   */
  FrozenBufferedUpdates freezeGlobalBuffer(DeleteSlice callerSlice) {
    globalBufferLock.lock();
    try {
      ensureOpen();
      final Node<?> currentTail = tail;
      if (callerSlice != null) {
        callerSlice.sliceTail = currentTail;
      }
      return freezeGlobalBufferInternal(currentTail);
    } finally {
      globalBufferLock.unlock();
    }
  }

  /**
   * Freezes the global buffer and clears references for GC on close or flush.
   * 
   * @function maybeFreezeGlobalBuffer
   * @returns {FrozenBufferedUpdates} Snapshot, or null if already closed and all changes applied.
   */
  FrozenBufferedUpdates maybeFreezeGlobalBuffer() {
    globalBufferLock.lock();
    try {
      if (!closed) {
        return freezeGlobalBufferInternal(tail);
      } else {
        assert anyChanges() == false : "we are closed but have changes";
        return null;
      }
    } finally {
      globalBufferLock.unlock();
    }
  }

  /**
   * Internal method; flushes and snapshots all pending deletes and clears state.
   * 
   * @function freezeGlobalBufferInternal
   * @param {Node<?>} currentTail - Local tail snapshot boundary for this freeze.
   * @returns {FrozenBufferedUpdates} Buffered updates packet, or null if no changes.
   */
  private FrozenBufferedUpdates freezeGlobalBufferInternal(final Node<?> currentTail) {
    assert globalBufferLock.isHeldByCurrentThread();
    if (globalSlice.sliceTail != currentTail) {
      globalSlice.sliceTail = currentTail;
      globalSlice.apply(globalBufferedUpdates, BufferedUpdates.MAX_INT);
    }
    if (globalBufferedUpdates.any()) {
      final FrozenBufferedUpdates packet =
          new FrozenBufferedUpdates(infoStream, globalBufferedUpdates, null);
      globalBufferedUpdates.clear();
      return packet;
    } else {
      return null;
    }
  }

  /**
   * Creates a new thread-private DeleteSlice instance anchored at current tail.
   * 
   * @function newSlice
   * @returns {DeleteSlice} A new per-thread DeleteSlice instance.
   */
  DeleteSlice newSlice() {
    return new DeleteSlice(tail);
  }

  /**
   * Updates the given DeleteSlice and returns a sequence number, or its negative if new deletes have arrived.
   * 
   * @function updateSlice
   * @param {DeleteSlice} slice - The slice to update to the tail.
   * @returns {long} Sequence number or negative of sequence if updated.
   */
  synchronized long updateSlice(DeleteSlice slice) {
    ensureOpen();
    long seqNo = getNextSequenceNumber();
    if (slice.sliceTail != tail) {
      slice.sliceTail = tail;
      seqNo = -seqNo;
    }
    return seqNo;
  }

  /**
   * Updates the slice's local tail without producing a sequence number.
   * 
   * @function updateSliceNoSeqNo
   * @param {DeleteSlice} slice - The slice to update.
   * @returns {boolean} True if the slice was advanced.
   */
  boolean updateSliceNoSeqNo(DeleteSlice slice) {
    if (slice.sliceTail != tail) {
      slice.sliceTail = tail;
      return true;
    }
    return false;
  }

  /**
   * Throws AlreadyClosedException if this queue is not open.
   * 
   * @function ensureOpen
   * @throws {AlreadyClosedException} If the queue is closed.
   */
  private void ensureOpen() {
    if (closed) {
      throw new AlreadyClosedException(
          "This " + DocumentsWriterDeleteQueue.class.getSimpleName() + " is already closed");
    }
  }

  /**
   * Checks if the queue is still open for modifications.
   * 
   * @function isOpen
   * @returns {boolean} True if the queue is open.
   */
  public boolean isOpen() {
    return closed == false;
  }

  /**
   * Closes the queue, disallowing further changes, and performs state checks.
   * 
   * @function close
   * @throws {IllegalStateException} If changes remain unapplied at close.
   */
  @Override
  public synchronized void close() {
    globalBufferLock.lock();
    try {
      if (anyChanges()) {
        throw new IllegalStateException("Can't close queue unless all changes are applied");
      }
      this.closed = true;
      long seqNo = nextSeqNo.get();
      assert seqNo <= maxSeqNo
          : "maxSeqNo must be greater or equal to " + seqNo + " but was " + maxSeqNo;
      nextSeqNo.set(maxSeqNo + 1);
    } finally {
      globalBufferLock.unlock();
    }
  }

  /**
   * @class DeleteSlice
   * @classdesc Maintains the local [head, tail] window of visible deletes for a specific consumer (DWPT or global pool).
   * 
   * @example
   * DeleteSlice slice = queue.newSlice();
   * // ... as new deletes arrive
   * slice.apply(bufferedUpdates, BufferedUpdates.MAX_INT);
   * 
   * @prop {Node<?>} sliceHead - Current "head" boundary (exclusive) of this slice.
   * @prop {Node<?>} sliceTail - Current "tail" boundary (inclusive) for deletes/updates to apply.
   */
  static class DeleteSlice {
    Node<?> sliceHead;
    Node<?> sliceTail;

    /**
     * Creates a new zero-length slice anchored at the given tail.
     * 
     * @constructor
     * @param {Node<?>} currentTail - Initial anchor for head/tail.
     */
    DeleteSlice(Node<?> currentTail) {
      assert currentTail != null;
      sliceHead = sliceTail = currentTail;
    }

    /**
     * Applies all deletes/updates in this slice range (exclusive head, inclusive tail) into the provided BufferedUpdates.
     * 
     * @function apply
     * @param {BufferedUpdates} del - Destination for buffered deletes.
     * @param {int} docIDUpto - Maximum doc ID to which deletes apply.
     */
    void apply(BufferedUpdates del, int docIDUpto) {
      if (sliceHead == sliceTail) {
        // Nothing to apply in zero-length slice.
        return;
      }
      Node<?> current = sliceHead;
      do {
        current = current.next;
        assert current != null
            : "slice property violated between the head on the tail must not be a null node";
        current.apply(del, docIDUpto);
      } while (current != sliceTail);
      reset();
    }

    /**
     * Resets the slice to a zero-length window, making it up-to-date.
     * @function reset
     */
    void reset() {
      sliceHead = sliceTail;
    }

    /**
     * Checks if the given node is the tail for this slice.
     * @function isTail
     * @param {Node<?>} node - Node to check.
     * @returns {boolean} True if node is tail.
     */
    boolean isTail(Node<?> node) {
      return sliceTail == node;
    }

    /**
     * Checks if the given object matches the item in the tail.
     * @function isTailItem
     * @param {Object} object - Item for comparison.
     * @returns {boolean} True if matches the tail's item.
     */
    boolean isTailItem(Object object) {
      return sliceTail.item == object;
    }

    /**
     * Checks if this slice is a no-op (zero length).
     * @function isEmpty
     * @returns {boolean} True if head and tail are the same.
     */
    boolean isEmpty() {
      return sliceHead == sliceTail;
    }
  }

  /**
   * Returns the number of global (not DWPT-local) term deletes buffered.
   * 
   * @function numGlobalTermDeletes
   * @returns {int} Count of term deletes.
   */
  int numGlobalTermDeletes() {
    return globalBufferedUpdates.deleteTerms.size();
  }

  /**
   * Clears the queue, discarding all pending updates and deletes.
   * 
   * @function clear
   */
  void clear() {
    globalBufferLock.lock();
    try {
      final Node<?> currentTail = tail;
      globalSlice.sliceHead = globalSlice.sliceTail = currentTail;
      globalBufferedUpdates.clear();
    } finally {
      globalBufferLock.unlock();
    }
  }

  /**
   * @class Node
   * @classdesc Abstract singly-linked list node for queuing delete elements (term, query, doc value update).
   * 
   * @param <T> Payload data type (Term, Query, etc).
   * @prop {Node<?>} next - Reference to the next node.
   * @prop {T} item - Payload for this node; null for sentinel.
   */
  static class Node<T> {
    volatile Node<?> next;
    final T item;
    Node(T item) {
      this.item = item;
    }

    /**
     * Applies the node's payload to the target BufferedUpdates.
     * Abstract except for sentinel (throw).
     * 
     * @function apply
     * @param {BufferedUpdates} bufferedDeletes - Target for delete/update.
     * @param {int} docIDUpto - Max doc ID.
     */
    void apply(BufferedUpdates bufferedDeletes, int docIDUpto) {
      throw new IllegalStateException("sentinel item must never be applied");
    }

    /**
     * Returns true if node is a delete operation (default).
     * @function isDelete
     * @returns {boolean} True for most Node types, false for DocValues update.
     */
    boolean isDelete() {
      return true;
    }
  }

  /**
   * Node type for buffered Term deletes.
   */
  private static final class TermNode extends Node<Term> {
    TermNode(Term term) {
      super(term);
    }

    /**
     * Applies term delete to the BufferedUpdates.
     */
    @Override
    void apply(BufferedUpdates bufferedDeletes, int docIDUpto) {
      bufferedDeletes.addTerm(item, docIDUpto);
    }

    @Override
    public String toString() {
      return "del=" + item;
    }
  }

  /**
   * Node type for buffered Query deletes.
   */
  private static final class QueryNode extends Node<Query> {
    QueryNode(Query query) {
      super(query);
    }

    @Override
    void apply(BufferedUpdates bufferedDeletes, int docIDUpto) {
      bufferedDeletes.addQuery(item, docIDUpto);
    }

    @Override
    public String toString() {
      return "del=" + item;
    }
  }

  /**
   * Node type for multiple Query deletes.
   */
  private static final class QueryArrayNode extends Node<Query[]> {
    QueryArrayNode(Query[] query) {
      super(query);
    }

    @Override
    void apply(BufferedUpdates bufferedUpdates, int docIDUpto) {
      for (Query query : item) {
        bufferedUpdates.addQuery(query, docIDUpto);
      }
    }
  }

  /**
   * Node type for multiple Term deletes.
   */
  private static final class TermArrayNode extends Node<Term[]> {
    TermArrayNode(Term[] term) {
      super(term);
    }

    @Override
    void apply(BufferedUpdates bufferedUpdates, int docIDUpto) {
      for (Term term : item) {
        bufferedUpdates.addTerm(term, docIDUpto);
      }
    }

    @Override
    public String toString() {
      return "dels=" + Arrays.toString(item);
    }
  }

  /**
   * Node type for DocValues updates (numeric/binary).
   */
  private static final class DocValuesUpdatesNode extends Node<DocValuesUpdate[]> {
    DocValuesUpdatesNode(DocValuesUpdate... updates) {
      super(updates);
    }

    /**
     * Applies each contained DocValues update to the buffer.
     */
    @Override
    void apply(BufferedUpdates bufferedUpdates, int docIDUpto) {
      for (DocValuesUpdate update : item) {
        switch (update.type) {
          case NUMERIC:
            bufferedUpdates.addNumericUpdate((NumericDocValuesUpdate) update, docIDUpto);
            break;
          case BINARY:
            bufferedUpdates.addBinaryUpdate((BinaryDocValuesUpdate) update, docIDUpto);
            break;
          // Following types currently unimplemented/unsupported
          case NONE:
          case SORTED:
          case SORTED_SET:
          case SORTED_NUMERIC:
          default:
            throw new IllegalArgumentException(
                update.type + " DocValues updates not supported yet!");
        }
      }
    }

    @Override
    boolean isDelete() {
      return false;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("docValuesUpdates: ");
      if (item.length > 0) {
        sb.append("term=").append(item[0].term).append("; updates: [");
        for (DocValuesUpdate update : item) {
          sb.append(update.field).append(':').append(update.valueToString()).append(',');
        }
        sb.setCharAt(sb.length() - 1, ']');
      }
      return sb.toString();
    }
  }

  /**
   * Returns the current size of buffered term deletes in the global buffer.
   * 
   * @function getBufferedUpdatesTermsSize
   * @returns {int} BufferedUpdates.deleteTerms.size()
   */
  public int getBufferedUpdatesTermsSize() {
    final ReentrantLock lock = globalBufferLock;
    lock.lock();
    try {
      final Node<?> currentTail = tail;
      if (globalSlice.sliceTail != currentTail) {
        globalSlice.sliceTail = currentTail;
        globalSlice.apply(globalBufferedUpdates, BufferedUpdates.MAX_INT);
      }
      return globalBufferedUpdates.deleteTerms.size();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Estimates the RAM usage for buffered deletes and updates.
   * 
   * @function ramBytesUsed
   * @returns {long} Approximate memory usage in bytes.
   */
  @Override
  public long ramBytesUsed() {
    return globalBufferedUpdates.ramBytesUsed();
  }

  @Override
  public String toString() {
    return "DWDQ: [ generation: " + generation + " ]";
  }

  /**
   * Generates and returns the next available sequence number.
   * 
   * @function getNextSequenceNumber
   * @returns {long} Incremented, unique operation sequence number.
   */
  public long getNextSequenceNumber() {
    long seqNo = nextSeqNo.getAndIncrement();
    assert seqNo <= maxSeqNo : "seqNo=" + seqNo + " vs maxSeqNo=" + maxSeqNo;
    return seqNo;
  }

  /**
   * Returns the most recently issued sequence number.
   * 
   * @function getLastSequenceNumber
   * @returns {long} Last value issued; always >= startSeqNo.
   */
  long getLastSequenceNumber() {
    return nextSeqNo.get() - 1;
  }

  /**
   * Skips ahead by the given number of sequence numbers (used on flush/commit).
   * 
   * @function skipSequenceNumbers
   * @param {long} jump - How many values to jump.
   */
  void skipSequenceNumbers(long jump) {
    nextSeqNo.addAndGet(jump);
  }

  /**
   * Returns the maximum completed sequence number for this queue.
   * Falls back to previous max if not yet advanced.
   * 
   * @function getMaxCompletedSeqNo
   * @returns {long} See above.
   */
  long getMaxCompletedSeqNo() {
    if (startSeqNo < nextSeqNo.get()) {
      return getLastSequenceNumber();
    } else {
      long value = previousMaxSeqId.getAsLong();
      assert value < startSeqNo : "illegal max sequence ID: " + value + " start was: " + startSeqNo;
      return value;
    }
  }

  /**
   * Returns a LongSupplier referencing the previous max sequence number,
   * preventing memory leaks due to queue chaining.
   * 
   * @function getPrevMaxSeqIdSupplier
   * @param {AtomicLong} nextSeqNo - The counter to watch.
   * @returns {LongSupplier} Lambda yielding previous sequence value.
   */
  private static LongSupplier getPrevMaxSeqIdSupplier(AtomicLong nextSeqNo) {
    return () -> nextSeqNo.get() - 1;
  }

  /**
   * Advances the queue, returning a fresh instance for the next index generation.
   * 
   * @function advanceQueue
   * @param {int} maxNumPendingOps - Number of operations (DWPTs) still in-flight for this queue.
   * @returns {DocumentsWriterDeleteQueue} Successor queue.
   * @throws {IllegalStateException} If already advanced.
   */
  synchronized DocumentsWriterDeleteQueue advanceQueue(int maxNumPendingOps) {
    if (advanced) {
      throw new IllegalStateException("queue was already advanced");
    }
    advanced = true;
    long seqNo = getLastSequenceNumber() + maxNumPendingOps + 1;
    maxSeqNo = seqNo;
    return new DocumentsWriterDeleteQueue(
        infoStream,
        generation + 1,
        seqNo + 1,
        getPrevMaxSeqIdSupplier(nextSeqNo));
  }

  /**
   * Returns the highest permitted sequence number for this queue instance.
   * 
   * @function getMaxSeqNo
   * @returns {long} Maximum sequence number (closes queue after advanced).
   */
  long getMaxSeqNo() {
    return maxSeqNo;
  }

  /**
   * Returns true if this queue has been advanced/replaced.
   * 
   * @function isAdvanced
   * @returns {boolean}
   */
  synchronized boolean isAdvanced() {
    return advanced;
  }
}
```
