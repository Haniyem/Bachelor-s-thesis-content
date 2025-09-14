```java
/**
 * @file FieldUpdatesBuffer.java
 * @module org.apache.lucene.index
 *
 * @description
 * This file defines the FieldUpdatesBuffer class, designed for efficiently buffering and managing batched updates to per-document field values in Apache Lucene's indexing subsystem.
 * The buffer enables temporary storage of numeric or binary DocValues updates prior to their eventual application in bulk to the underlying index. It meticulously tracks memory usage, handles heterogenous update types, optimizes storage for fields and document IDs, and supports efficient deduplication and iteration through its specialized iterator.
 *
 * @dependencies
 * - org.apache.lucene.util.ArrayUtil: Utilities for array growth and manipulation.
 * - org.apache.lucene.util.Bits: Bit set abstractions for tracking update presence.
 * - org.apache.lucene.util.BytesRef/BytesRefArray/BytesRefIterator/BytesRefComparator: Efficient handling, storage, and comparison of binary field values.
 * - org.apache.lucene.util.Counter: Reference-counted memory usage tracking.
 * - org.apache.lucene.util.FixedBitSet: Space-efficient tracking of which updates carry values.
 * - org.apache.lucene.util.RamUsageEstimator: Accurate RAM accounting for Java objects.
 *
 * @author Apache Lucene Project
 * @version 1.0
 * @license Apache License, Version 2.0
 * @lastmodified 2025-09-14: Documentation added and file-level comments improved.
 */

/**
 * @class FieldUpdatesBuffer
 * @classdesc
 * Buffers batched DocValues field updates (either numeric or binary) for efficient bulk-handling in Lucene's indexing flow.
 * It supports fast append, explicit memory accounting, field/document register growth, and iteration in stable or insertion order for deduplication purposes.
 *
 * @example
 * // Example: Buffering numeric update to a DocValue field
 * Counter counter = Counter.newCounter();
 * FieldUpdatesBuffer buffer = new FieldUpdatesBuffer(counter, initialNumericUpdate, docId);
 * buffer.addUpdate(term, newValue, docId);
 * buffer.finish();
 * for (FieldUpdatesBuffer.BufferedUpdateIterator it = buffer.iterator(); BufferedUpdate update; (update = it.next()) != null;) {
 *     // Process update.termField, update.numericValue, etc.
 * }
 *
 * @prop {Counter} bytesUsed - Tracks RAM usage for the buffer instance.
 * @prop {int} numUpdates - Total number of update operations buffered.
 * @prop {BytesRefArray} termValues - Holds term values marking the identity of updated fields.
 * @prop {BytesRefArray.SortState} termSortState - Optional in-memory sort state for deduplication scenarios.
 * @prop {BytesRefArray} byteValues - Stores binary update values if applicable.
 * @prop {int[]} docsUpTo - Highest document IDs affected by each update.
 * @prop {long[]} numericValues - Stores numeric update values if applicable.
 * @prop {FixedBitSet} hasValues - Indicates which updates actually contain a value (vs. a "no-op").
 * @prop {long} maxNumeric - Cached maximum of all numericValues for optimizations.
 * @prop {long} minNumeric - Cached minimum of all numericValues for optimizations.
 * @prop {String[]} fields - Tracks updated field names per operation.
 * @prop {boolean} isNumeric - True if all updates are numeric, false if binary.
 * @prop {boolean} finished - True when the buffer is closed/frozen for iteration.
 */
final class FieldUpdatesBuffer {
    private static final long SELF_SHALLOW_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(FieldUpdatesBuffer.class);
    private static final long STRING_SHALLOW_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(String.class);
    private final Counter bytesUsed;
    private int numUpdates = 1;

    private final BytesRefArray termValues;
    private BytesRefArray.SortState termSortState;
    private final BytesRefArray byteValues; 
    private int[] docsUpTo;
    private long[] numericValues; 
    private FixedBitSet hasValues;
    private long maxNumeric = Long.MIN_VALUE;
    private long minNumeric = Long.MAX_VALUE;
    private String[] fields;
    private final boolean isNumeric;
    private boolean finished = false;

    /**
     * @function FieldUpdatesBuffer
     * @description
     * Private core constructor. Initializes internal state and buffers for DocValues updates.
     * Only invoked by public, type-specific constructors.
     *
     * @param {Counter} bytesUsed - RAM usage tracker for enforced memory limits.
     * @param {DocValuesUpdate} initialValue - The first update data to seed the buffer.
     * @param {int} docUpTo - Document ID threshold this update applies to.
     * @param {boolean} isNumeric - True if numeric updates are buffered, otherwise false.
     */
    private FieldUpdatesBuffer(
        Counter bytesUsed, DocValuesUpdate initialValue, int docUpTo, boolean isNumeric) {
        this.bytesUsed = bytesUsed;
        // Account for buffer object shallow size
        this.bytesUsed.addAndGet(SELF_SHALLOW_SIZE);
        termValues = new BytesRefArray(bytesUsed);
        termValues.append(initialValue.term.bytes);
        fields = new String[] {initialValue.term.field};
        bytesUsed.addAndGet(sizeOfString(initialValue.term.field));
        docsUpTo = new int[] {docUpTo};
        if (initialValue.hasValue == false) {
            hasValues = new FixedBitSet(1);
            bytesUsed.addAndGet(hasValues.ramBytesUsed());
        }
        this.isNumeric = isNumeric;
        byteValues = isNumeric ? null : new BytesRefArray(bytesUsed);
    }

    /**
     * @function sizeOfString
     * @description
     * Computes (shallow) RAM cost in bytes of a given String instance, excluding references.
     *
     * @param {String} string - Target string.
     * @returns {long} Memory size in bytes; includes per-character cost.
     */
    private static long sizeOfString(String string) {
        return STRING_SHALLOW_SIZE + (string.length() * (long) Character.BYTES);
    }

    /**
     * @function FieldUpdatesBuffer (public Numeric)
     * @description
     * Instantiates a buffer seeded with an initial numeric DocValues update.
     * Initializes numeric values array and value range stats.
     *
     * @param {Counter} bytesUsed - RAM usage tracker.
     * @param {DocValuesUpdate.NumericDocValuesUpdate} initialValue - Initial numeric update.
     * @param {int} docUpTo - Document ID threshold.
     */
    FieldUpdatesBuffer(
        Counter bytesUsed, DocValuesUpdate.NumericDocValuesUpdate initialValue, int docUpTo) {
        this(bytesUsed, initialValue, docUpTo, true);
        if (initialValue.hasValue()) {
            numericValues = new long[] {initialValue.getValue()};
            maxNumeric = minNumeric = initialValue.getValue();
        } else {
            numericValues = new long[] {0};
        }
        bytesUsed.addAndGet(Long.BYTES);
    }

    /**
     * @function FieldUpdatesBuffer (public Binary)
     * @description
     * Instantiates a buffer for binary DocValues updates. Initializes buffer with initial binary value if present.
     *
     * @param {Counter} bytesUsed - RAM usage tracker.
     * @param {DocValuesUpdate.BinaryDocValuesUpdate} initialValue - Initial binary update.
     * @param {int} docUpTo - Document ID threshold.
     */
    FieldUpdatesBuffer(
        Counter bytesUsed, DocValuesUpdate.BinaryDocValuesUpdate initialValue, int docUpTo) {
        this(bytesUsed, initialValue, docUpTo, false);
        if (initialValue.hasValue()) {
            byteValues.append(initialValue.getValue());
        }
    }

    /**
     * @function getMaxNumeric
     * @description
     * Returns the maximum numeric value among all buffered updates, or 0 if none exist.
     *
     * @returns {long} Max numeric value buffered, or 0 if empty.
     */
    long getMaxNumeric() {
        assert isNumeric;
        if (minNumeric == Long.MAX_VALUE && maxNumeric == Long.MIN_VALUE) {
            return 0; // No values buffered
        }
        return maxNumeric;
    }

    /**
     * @function getMinNumeric
     * @description
     * Returns the minimum numeric value among all buffered updates, or 0 if none exist.
     *
     * @returns {long} Min numeric value buffered, or 0 if empty.
     */
    long getMinNumeric() {
        assert isNumeric;
        if (minNumeric == Long.MAX_VALUE && maxNumeric == Long.MIN_VALUE) {
            return 0; // No values buffered
        }
        return minNumeric;
    }

    /**
     * @function add
     * @description
     * Registers an update for a field, growing buffers as required and tracking value presence.
     * Handles deduplication of fields/docs and memory accounting.
     *
     * @param {String} field - Name of updated field.
     * @param {int} docUpTo - Max doc ID to which update applies.
     * @param {int} ord - Ordinal position in update array.
     * @param {boolean} hasValue - True if the update has a value; false for "no value" tombstones.
     */
    void add(String field, int docUpTo, int ord, boolean hasValue) {
        assert finished == false : "buffer was finished already";
        // Grow or set fields[] as needed, fill new entries
        if (fields[0].equals(field) == false || fields.length != 1) {
            if (fields.length <= ord) {
                String[] array = ArrayUtil.grow(fields, ord + 1);
                if (fields.length == 1) {
                    Arrays.fill(array, 1, ord, fields[0]);
                }
                bytesUsed.addAndGet(
                    (array.length - fields.length) * (long) RamUsageEstimator.NUM_BYTES_OBJECT_REF);
                fields = array;
            }
            if (field != fields[0]) {
                // Account memory for new unique field name
                bytesUsed.addAndGet(sizeOfString(field));
            }
            fields[ord] = field;
        }

        // Grow docsUpTo[] as needed
        if (docsUpTo[0] != docUpTo || docsUpTo.length != 1) {
            if (docsUpTo.length <= ord) {
                int[] array = ArrayUtil.grow(docsUpTo, ord + 1);
                if (docsUpTo.length == 1) {
                    Arrays.fill(array, 1, ord, docsUpTo[0]);
                }
                bytesUsed.addAndGet((array.length - docsUpTo.length) * (long) Integer.BYTES);
                docsUpTo = array;
            }
            docsUpTo[ord] = docUpTo;
        }

        // Track updates with/without value using bitset
        if (hasValue == false || hasValues != null) {
            if (hasValues == null) {
                hasValues = new FixedBitSet(ord + 1);
                hasValues.set(0, ord);
                bytesUsed.addAndGet(hasValues.ramBytesUsed());
            } else if (hasValues.length() <= ord) {
                FixedBitSet fixedBitSet =
                    FixedBitSet.ensureCapacity(hasValues, ArrayUtil.oversize(ord + 1, 1));
                bytesUsed.addAndGet(fixedBitSet.ramBytesUsed() - hasValues.ramBytesUsed());
                hasValues = fixedBitSet;
            }
            if (hasValue) {
                hasValues.set(ord);
            }
        }
    }

    /**
     * @function addUpdate (numeric)
     * @description
     * Adds a new numeric field update, updating range statistics and growing internal buffers.
     *
     * @param {Term} term - The term defining the field and its value.
     * @param {long} value - The numeric value to update.
     * @param {int} docUpTo - Max doc ID for update application.
     */
    void addUpdate(Term term, long value, int docUpTo) {
        assert isNumeric;
        final int ord = append(term);
        String field = term.field;
        add(field, docUpTo, ord, true);
        minNumeric = Math.min(minNumeric, value);
        maxNumeric = Math.max(maxNumeric, value);
        // Grow numericValues[] as needed
        if (numericValues[0] != value || numericValues.length != 1) {
            if (numericValues.length <= ord) {
                long[] array = ArrayUtil.grow(numericValues, ord + 1);
                if (numericValues.length == 1) {
                    Arrays.fill(array, 1, ord, numericValues[0]);
                }
                bytesUsed.addAndGet((array.length - numericValues.length) * (long) Long.BYTES);
                numericValues = array;
            }
            numericValues[ord] = value;
        }
    }

    /**
     * @function addNoValue
     * @description
     * Registers a "no value" (delete/tombstone) update for the specified term and document.
     *
     * @param {Term} term - The term defining the field to clear value for.
     * @param {int} docUpTo - Max doc ID for the update.
     */
    void addNoValue(Term term, int docUpTo) {
        final int ord = append(term);
        add(term.field, docUpTo, ord, false);
    }

    /**
     * @function addUpdate (binary)
     * @description
     * Appends a binary field value update for the given term/document.
     *
     * @param {Term} term - Term specifying field and update identity.
     * @param {BytesRef} value - The binary value.
     * @param {int} docUpTo - Max doc ID for update application.
     */
    void addUpdate(Term term, BytesRef value, int docUpTo) {
        assert isNumeric == false;
        final int ord = append(term);
        byteValues.append(value);
        add(term.field, docUpTo, ord, true);
    }

    /**
     * @function append
     * @description
     * Appends a term's bytes to the buffer, increments update ordinal counter, and returns the new position.
     *
     * @param {Term} term - The term to add.
     * @returns {int} Ordinal index assigned to this update.
     */
    private int append(Term term) {
        termValues.append(term.bytes);
        return numUpdates++;
    }

    /**
     * @function finish
     * @description
     * Freezes the buffer, performs optional sorting/optimization, and prepares for stable iteration.
     *
     * @throws {IllegalStateException} If already finished.
     */
    void finish() {
        if (finished) {
            throw new IllegalStateException("buffer was finished already");
        }
        finished = true;
        final boolean sortedTerms = hasSingleValue() && hasValues == null && fields.length == 1;
        if (sortedTerms) {
            termSortState = termValues.sort(BytesRefComparator.NATURAL, true);
            assert assertTermAndDocInOrder();
            bytesUsed.addAndGet(termSortState.ramBytesUsed());
        }
    }

    /**
     * @function assertTermAndDocInOrder
     * @description
     * Verifies that, after sorting, the updates are ordered by term and docId.
     * Used for assertions when optimization is applied.
     *
     * @returns {boolean} True if update order is valid.
     */
    private boolean assertTermAndDocInOrder() {
        try {
            BytesRefArray.IndexedBytesRefIterator iterator = termValues.iterator(termSortState);
            BytesRef last = null;
            int lastOrd = -1;
            BytesRef current;
            while ((current = iterator.next()) != null) {
                if (last != null) {
                    int cmp = current.compareTo(last);
                    assert cmp >= 0 : "term in reverse order";
                    assert cmp != 0
                            || docsUpTo[getArrayIndex(docsUpTo.length, lastOrd)]
                                <= docsUpTo[getArrayIndex(docsUpTo.length, iterator.ord())]
                        : "doc id in reverse order";
                }
                last = BytesRef.deepCopyOf(current);
                lastOrd = iterator.ord();
            }
        } catch (IOException e) {
            assert false : e.getMessage();
        }
        return true;
    }

    /**
     * @function iterator
     * @description
     * Returns a new iterator to traverse buffered updates in stable or insertion order.
     *
     * @returns {BufferedUpdateIterator} The iterator for all buffered updates.
     * @throws {IllegalStateException} If buffer is not finalized.
     */
    BufferedUpdateIterator iterator() {
        if (finished == false) {
            throw new IllegalStateException("buffer is not finished yet");
        }
        return new BufferedUpdateIterator();
    }

    /**
     * @function isNumeric
     * @description
     * Reports if buffer contains numeric (vs. binary) updates.
     *
     * @returns {boolean} True if updates are numeric.
     */
    boolean isNumeric() {
        assert isNumeric || byteValues != null;
        return isNumeric;
    }

    /**
     * @function hasSingleValue
     * @description
     * Indicates if the buffer contains a single update value (enables optimization).
     *
     * @returns {boolean} True if only one numeric value exists.
     */
    boolean hasSingleValue() {
        // Optimization applies for single numeric value updates only
        return isNumeric && numericValues.length == 1;
    }

    /**
     * @function getNumericValue
     * @description
     * Retrieves the numeric value at the given update index. Returns 0 if update has no value.
     *
     * @param {int} idx - Index into buffered updates.
     * @returns {long} Numeric value or 0.
     */
    long getNumericValue(int idx) {
        if (hasValues != null && hasValues.get(idx) == false) {
            return 0;
        }
        return numericValues[getArrayIndex(numericValues.length, idx)];
    }

    /**
     * @function getArrayIndex
     * @description
     * Maps logical index to physical array index, handling degenerate/single-value case.
     *
     * @param {int} arrayLength - Length of array.
     * @param {int} index - Requested index.
     * @returns {int} Safe, resolved index.
     */
    private static int getArrayIndex(int arrayLength, int index) {
        assert arrayLength == 1 || arrayLength > index
            : "illegal array index length: " + arrayLength + " index: " + index;
        return Math.min(arrayLength - 1, index);
    }

    /**
     * @class FieldUpdatesBuffer.BufferedUpdate
     * @classdesc
     * Plain structure used to return details of a buffered field update, including all relevant pointers.
     * Not designed for use in sets/maps (hashCode and equals throw).
     *
     * @prop {int} docUpTo - Max doc ID affected.
     * @prop {long} numericValue - Numeric value (if present, zero if binary).
     * @prop {BytesRef} binaryValue - Binary value (if present, null if numeric).
     * @prop {boolean} hasValue - Whether this update has a value or is a delete.
     * @prop {String} termField - Name of updated field.
     * @prop {BytesRef} termValue - Term bytes that identify this update.
     */
    static class BufferedUpdate {

        private BufferedUpdate() {}

        int docUpTo;
        long numericValue;
        BytesRef binaryValue;
        boolean hasValue;
        String termField;
        BytesRef termValue;

        /**
         * @function hashCode
         * @description
         * Not supported for this struct. Throws on invocation as updates should not be used in hash containers.
         * @throws {UnsupportedOperationException} Always.
         */
        @Override
        public int hashCode() {
            throw new UnsupportedOperationException(
                "this struct should not be use in map or other data-structures that use hashCode / equals");
        }

        /**
         * @function equals
         * @description
         * Not supported for this struct. Throws on invocation as updates should not be used in hash containers.
         * @throws {UnsupportedOperationException} Always.
         */
        @Override
        public boolean equals(Object obj) {
            throw new UnsupportedOperationException(
                "this struct should not be use in map or other data-structures that use hashCode / equals");
        }
    }

    /**
     * @class FieldUpdatesBuffer.BufferedUpdateIterator
     * @classdesc
     * Iterates over all buffered updates in insertion (or sorted) order, efficiently returning update details.
     * Shared BufferedUpdate instance reused for performance.
     *
     * @example
     * FieldUpdatesBuffer buf = ...;
     * for (BufferedUpdateIterator it = buf.iterator(); BufferedUpdate next; (next = it.next()) != null;) {
     *     // Consume update properties
     * }
     *
     * @prop {BytesRefArray.IndexedBytesRefIterator} termValuesIterator - Iterates update terms.
     * @prop {BytesRefArray.IndexedBytesRefIterator} lookAheadTermIterator - Auxiliary iterator for deduplication.
     * @prop {BytesRefIterator} byteValuesIterator - Iterates binary update values.
     * @prop {BufferedUpdate} bufferedUpdate - Shared return update instance.
     * @prop {Bits} updatesWithValue - Exposes which updates have associated values.
     */
    class BufferedUpdateIterator {
        private final BytesRefArray.IndexedBytesRefIterator termValuesIterator;
        private final BytesRefArray.IndexedBytesRefIterator lookAheadTermIterator;
        private final BytesRefIterator byteValuesIterator;
        private final BufferedUpdate bufferedUpdate = new BufferedUpdate();
        private final Bits updatesWithValue;

        /**
         * @function BufferedUpdateIterator
         * @description
         * Constructs the iterator, pre-initializing iterators and value presence bits.
         * Chooses optimized lookup order if buffer is sorted.
         */
        BufferedUpdateIterator() {
            this.termValuesIterator = termValues.iterator(termSortState);
            this.lookAheadTermIterator =
                termSortState != null ? termValues.iterator(termSortState) : null;
            this.byteValuesIterator = isNumeric ? null : byteValues.iterator();
            updatesWithValue = hasValues == null ? new Bits.MatchAllBits(numUpdates) : hasValues;
        }

        /**
         * @function isSortedTerms
         * @description
         * Reports whether the buffer's updates are organized in term-order (enables de-duplication optimization).
         *
         * @returns {boolean} True if sorted order is in effect.
         */
        boolean isSortedTerms() {
            return termSortState != null;
        }

        /**
         * @function next
         * @description
         * Advances iterator to the next buffered update, populating the shared return instance.
         * Returns null when all updates are consumed. Caller must fully process/persevere the instance before next call.
         *
         * @returns {BufferedUpdate} Next update, or null if done.
         * @throws {IOException} On low-level iteration error.
         */
        BufferedUpdate next() throws IOException {
            BytesRef next = nextTerm();
            if (next != null) {
                final int idx = termValuesIterator.ord();
                bufferedUpdate.termValue = next;
                bufferedUpdate.hasValue = updatesWithValue.get(idx);
                bufferedUpdate.termField = fields[getArrayIndex(fields.length, idx)];
                bufferedUpdate.docUpTo = docsUpTo[getArrayIndex(docsUpTo.length, idx)];
                if (bufferedUpdate.hasValue) {
                    if (isNumeric) {
                        bufferedUpdate.numericValue = numericValues[getArrayIndex(numericValues.length, idx)];
                        bufferedUpdate.binaryValue = null;
                    } else {
                        bufferedUpdate.binaryValue = byteValuesIterator.next();
                    }
                } else {
                    bufferedUpdate.binaryValue = null;
                    bufferedUpdate.numericValue = 0;
                }
                return bufferedUpdate;
            } else {
                return null;
            }
        }

        /**
         * @function nextTerm
         * @description
         * Yields the next term for update according to buffer ordering policy.
         * De-duplicates if sorted.
         *
         * @returns {BytesRef} The term, or null if done.
         * @throws {IOException} On data access error.
         */
        private BytesRef nextTerm() throws IOException {
            if (lookAheadTermIterator != null) {
                if (bufferedUpdate.termValue == null) {
                    lookAheadTermIterator.next();
                }
                BytesRef lastTerm, aheadTerm;
                do {
                    aheadTerm = lookAheadTermIterator.next();
                    lastTerm = termValuesIterator.next();
                } while (aheadTerm != null
                    // Optimization: aheadTerm equals lastTerm only possible with higher ord due to stable sort
                    && lookAheadTermIterator.ord() > termValuesIterator.ord()
                    && aheadTerm.equals(lastTerm));
                return lastTerm;
            } else {
                return termValuesIterator.next();
            }
        }
    }
}
