/**
 * @file Utility methods for safely providing empty or singleton DocValues implementations and accessors for field-level DocValues.
 * @module org.apache.lucene.index.DocValues
 * 
 * @description
 * This file supplies static utility methods and inner classes allowing safe and uniform handling of DocValues for Lucene indices,
 * including empty DocValues implementations (for missing fields), singleton adapters, type validation utilities,
 * and canonical retrieval patterns. Its core responsibility within Lucene is to minimize null-checking and error-prone 
 * handling of missing or unexpected DocValues by returning no-op or default objects that follow the Lucene contract, 
 * facilitating robust index reading and extension.
 * 
 * @dependencies
 * - org.apache.lucene.index.LeafReader: Provides access to per-segment index data and DocValues access.
 * - org.apache.lucene.index.DocValuesType: Used for validating field DocValues types.
 * - org.apache.lucene.util.BytesRef: Lucene’s reference wrapper for (potentially shared) byte arrays.
 * 
 * @author Apache Software Foundation
 * @version 1.0
 * @license Apache License 2.0
 * @lastmodified 2025-09-15: Initial comprehensive documentation added for all methods and purpose.
 */

package org.apache.lucene.index;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.util.BytesRef;

/**
 * @class DocValues
 * @classdesc A utility class offering static methods to access, create, and validate DocValues implementations (empty, singleton, or concrete) for Lucene index readers.
 * 
 * @example
 * // Access NumericDocValues for a field, always returns a non-null instance
 * NumericDocValues dv = DocValues.getNumeric(reader, "someField");
 * long v = dv.longValue();
 * 
 * // Wrap a SortedDocValues as a SingletonSortedSetDocValues
 * SortedSetDocValues ssdv = DocValues.singleton(sortedDocValues);
 */
public final class DocValues {

  // Private constructor prevents instantiation.
  private DocValues() {}

  /**
   * @function emptyBinary
   * @description Returns an implementation of BinaryDocValues which always reports no documents and no values.
   * 
   * @returns {BinaryDocValues} An always-empty BinaryDocValues instance; safe to use in place of nulls.
   * 
   * @example
   * BinaryDocValues binary = DocValues.emptyBinary();
   * assert binary.nextDoc() == DocIdSetIterator.NO_MORE_DOCS;
   */
  public static final BinaryDocValues emptyBinary() {
    return new BinaryDocValues() {
      private int doc = -1;

      @Override
      public int advance(int target) {
        return doc = NO_MORE_DOCS;
      }

      @Override
      public boolean advanceExact(int target) throws IOException {
        doc = target;
        return false;
      }

      @Override
      public int docID() {
        return doc;
      }

      @Override
      public int nextDoc() {
        return doc = NO_MORE_DOCS;
      }

      @Override
      public long cost() {
        return 0;
      }

      @Override
      public BytesRef binaryValue() {
        // This should never be called; return null by contract.
        assert false;
        return null;
      }
    };
  }

  /**
   * @function emptyNumeric
   * @description Returns an implementation of NumericDocValues which always reports no documents and no values.
   * 
   * @returns {NumericDocValues} An always-empty NumericDocValues instance; avoids nulls in client code.
   * 
   * @example
   * NumericDocValues numeric = DocValues.emptyNumeric();
   * assert numeric.nextDoc() == DocIdSetIterator.NO_MORE_DOCS;
   */
  public static final NumericDocValues emptyNumeric() {
    return new NumericDocValues() {
      private int doc = -1;

      @Override
      public int advance(int target) {
        return doc = NO_MORE_DOCS;
      }

      @Override
      public boolean advanceExact(int target) throws IOException {
        doc = target;
        return false;
      }

      @Override
      public int docID() {
        return doc;
      }

      @Override
      public int nextDoc() {
        return doc = NO_MORE_DOCS;
      }

      @Override
      public long cost() {
        return 0;
      }

      @Override
      public long longValue() {
        // This should never be called; default is zero.
        assert false;
        return 0;
      }
    };
  }

  /**
   * @function emptySorted
   * @description Returns an implementation of SortedDocValues which always reports no documents, zero values, and a single empty BytesRef.
   * 
   * @returns {SortedDocValues} An always-empty SortedDocValues instance.
   * 
   * @example
   * SortedDocValues sorted = DocValues.emptySorted();
   * assert sorted.getValueCount() == 0;
   */
  public static final SortedDocValues emptySorted() {
    final BytesRef empty = new BytesRef();
    return new SortedDocValues() {

      private int doc = -1;

      @Override
      public int advance(int target) {
        return doc = NO_MORE_DOCS;
      }

      @Override
      public boolean advanceExact(int target) throws IOException {
        doc = target;
        return false;
      }

      @Override
      public int docID() {
        return doc;
      }

      @Override
      public int nextDoc() {
        return doc = NO_MORE_DOCS;
      }

      @Override
      public long cost() {
        return 0;
      }

      @Override
      public int ordValue() {
        // This should never be called; return sentinel.
        assert false;
        return -1;
      }

      @Override
      public BytesRef lookupOrd(int ord) {
        return empty;
      }

      @Override
      public int getValueCount() {
        return 0;
      }
    };
  }

  /**
   * @function emptySortedNumeric
   * @description Returns an implementation of SortedNumericDocValues that always reports no documents and no values.
   * 
   * @returns {SortedNumericDocValues} An always-empty SortedNumericDocValues instance.
   * 
   * @example
   * SortedNumericDocValues sn = DocValues.emptySortedNumeric();
   * assert sn.nextDoc() == DocIdSetIterator.NO_MORE_DOCS;
   */
  public static final SortedNumericDocValues emptySortedNumeric() {
    return singleton(emptyNumeric());
  }

  /**
   * @function emptySortedSet
   * @description Returns an implementation of SortedSetDocValues which always reports no documents and no values.
   * 
   * @returns {SortedSetDocValues} An always-empty SortedSetDocValues instance.
   * 
   * @example
   * SortedSetDocValues ss = DocValues.emptySortedSet();
   * assert ss.nextDoc() == DocIdSetIterator.NO_MORE_DOCS;
   */
  public static final SortedSetDocValues emptySortedSet() {
    return singleton(emptySorted());
  }

  /**
   * @function singleton (SortedDocValues)
   * @description Wraps a SortedDocValues into a singleton SortedSetDocValues, for compatibility with APIs expecting the latter.
   * 
   * @param {SortedDocValues} dv - The SortedDocValues instance to wrap.
   * 
   * @returns {SortedSetDocValues} SingletonSortedSetDocValues delegating to the provided SortedDocValues.
   * 
   * @example
   * SortedSetDocValues ss = DocValues.singleton(sortedDocValues);
   */
  public static SortedSetDocValues singleton(SortedDocValues dv) {
    return new SingletonSortedSetDocValues(dv);
  }

  /**
   * @function unwrapSingleton (SortedSetDocValues)
   * @description Attempts to unwrap and return the underlying SortedDocValues from a singleton SortedSetDocValues.
   *       Returns null if the argument is not a SingletonSortedSetDocValues.
   * 
   * @param {SortedSetDocValues} dv - The SortedSetDocValues instance, possibly a singleton wrapper.
   * 
   * @returns {SortedDocValues|null} The wrapped SortedDocValues if applicable, otherwise null.
   * 
   * @example
   * SortedDocValues sdv = DocValues.unwrapSingleton(ssdv);
   */
  public static SortedDocValues unwrapSingleton(SortedSetDocValues dv) {
    if (dv instanceof SingletonSortedSetDocValues) {
      return ((SingletonSortedSetDocValues) dv).getSortedDocValues();
    } else {
      return null;
    }
  }

  /**
   * @function unwrapSingleton (SortedNumericDocValues)
   * @description Attempts to unwrap and return the underlying NumericDocValues from a singleton SortedNumericDocValues.
   *       Returns null if the argument is not a SingletonSortedNumericDocValues.
   * 
   * @param {SortedNumericDocValues} dv - The SortedNumericDocValues instance, possibly a singleton wrapper.
   * 
   * @returns {NumericDocValues|null} The wrapped NumericDocValues if applicable, otherwise null.
   * 
   * @example
   * NumericDocValues ndv = DocValues.unwrapSingleton(sortedNumeric);
   */
  public static NumericDocValues unwrapSingleton(SortedNumericDocValues dv) {
    if (dv instanceof SingletonSortedNumericDocValues) {
      return ((SingletonSortedNumericDocValues) dv).getNumericDocValues();
    } else {
      return null;
    }
  }

  /**
   * @function singleton (NumericDocValues)
   * @description Wraps a NumericDocValues instance in a singleton SortedNumericDocValues for compatibility.
   * 
   * @param {NumericDocValues} dv - The NumericDocValues instance to wrap.
   * 
   * @returns {SortedNumericDocValues} SingletonSortedNumericDocValues delegating to the provided NumericDocValues.
   * 
   * @example
   * SortedNumericDocValues snd = DocValues.singleton(numericDocValues);
   */
  public static SortedNumericDocValues singleton(NumericDocValues dv) {
    return new SingletonSortedNumericDocValues(dv);
  }

  /**
   * @function checkField
   * @description Throws an IllegalStateException if the field exists with an unexpected DocValuesType, otherwise does nothing.
   * 
   * @param {LeafReader} in - The index reader to examine.
   * @param {String} field - The field name to check.
   * @param {DocValuesType[]} expected - The expected DocValues type(s) for validation.
   * 
   * @throws {IllegalStateException} When field exists but the actual DocValuesType does not match the expected.
   * 
   * @example
   * // Throws if 'foo' exists but not with NUMERIC type
   * checkField(reader, "foo", DocValuesType.NUMERIC);
   */
  private static void checkField(LeafReader in, String field, DocValuesType... expected) {
    FieldInfo fi = in.getFieldInfos().fieldInfo(field);
    if (fi != null) {
      DocValuesType actual = fi.getDocValuesType();
      throw new IllegalStateException(
          "unexpected docvalues type "
              + actual
              + " for field '"
              + field
              + "' "
              + (expected.length == 1
                  ? "(expected=" + expected[0]
                  : "(expected one of " + Arrays.toString(expected))
              + "). "
              + "Re-index with correct docvalues type.");
    }
  }

  /**
   * @function getNumeric
   * @description Retrieves NumericDocValues for a given field from a LeafReader, or an empty implementation if missing.
   *       Throws if the field exists but is not of NUMERIC DocValuesType.
   * 
   * @param {LeafReader} reader - Per-segment Lucene reader.
   * @param {String} field - Name of the field to retrieve.
   * 
   * @returns {NumericDocValues} Valid NumericDocValues instance or empty if missing.
   * 
   * @throws {IllegalStateException} If field exists but has incompatible DocValuesType.
   * @throws {IOException} On I/O errors reading DocValues.
   * 
   * @example
   * NumericDocValues dv = DocValues.getNumeric(reader, "fieldA");
   */
  public static NumericDocValues getNumeric(LeafReader reader, String field) throws IOException {
    NumericDocValues dv = reader.getNumericDocValues(field);
    if (dv == null) {
      checkField(reader, field, DocValuesType.NUMERIC);
      return emptyNumeric();
    } else {
      return dv;
    }
  }

  /**
   * @function getBinary
   * @description Retrieves BinaryDocValues for the given field or returns an empty implementation if missing.
   *       Throws if the field exists but is not of BINARY DocValuesType.
   * 
   * @param {LeafReader} reader - Per-segment Lucene reader.
   * @param {String} field - Name of field.
   * 
   * @returns {BinaryDocValues} Valid BinaryDocValues instance, or always-empty object.
   * 
   * @throws {IllegalStateException} If field exists with incompatible DocValuesType.
   * @throws {IOException} On index access error.
   * 
   * @example
   * BinaryDocValues dv = DocValues.getBinary(reader, "binField");
   */
  public static BinaryDocValues getBinary(LeafReader reader, String field) throws IOException {
    BinaryDocValues dv = reader.getBinaryDocValues(field);
    if (dv == null) {
      checkField(reader, field, DocValuesType.BINARY);
      return emptyBinary();
    }
    return dv;
  }

  /**
   * @function getSorted
   * @description Retrieves SortedDocValues for a field, or returns an empty implementation if missing.
   *       Throws if the field exists but is not of SORTED DocValuesType.
   * 
   * @param {LeafReader} reader - Per-segment Lucene reader.
   * @param {String} field - Field name.
   * 
   * @returns {SortedDocValues} Valid SortedDocValues instance, or empty one.
   * 
   * @throws {IllegalStateException} If field exists but type is incompatible.
   * @throws {IOException} On index reading errors.
   * 
   * @example
   * SortedDocValues sorted = DocValues.getSorted(reader, "sortField");
   */
  public static SortedDocValues getSorted(LeafReader reader, String field) throws IOException {
    SortedDocValues dv = reader.getSortedDocValues(field);
    if (dv == null) {
      checkField(reader, field, DocValuesType.SORTED);
      return emptySorted();
    } else {
      return dv;
    }
  }

  /**
   * @function getSortedNumeric
   * @description Accesses SortedNumericDocValues for a field, or wraps a single-valued NumericDocValues as a singleton if only that exists.
   *       Returns empty implementation if neither is present. Throws if field type is incompatible.
   * 
   * @param {LeafReader} reader - Lucene per-segment reader.
   * @param {String} field - Field name.
   * 
   * @returns {SortedNumericDocValues} Valid SortedNumericDocValues or empty fallback.
   * 
   * @throws {IllegalStateException} If the field exists but is neither numeric nor sorted-numeric.
   * @throws {IOException} On index access error.
   * 
   * @example
   * SortedNumericDocValues snd = DocValues.getSortedNumeric(reader, "price");
   */
  public static SortedNumericDocValues getSortedNumeric(LeafReader reader, String field)
      throws IOException {
    SortedNumericDocValues dv = reader.getSortedNumericDocValues(field);
    if (dv == null) {
      NumericDocValues single = reader.getNumericDocValues(field);
      if (single == null) {
        checkField(reader, field, DocValuesType.SORTED_NUMERIC, DocValuesType.NUMERIC);
        return emptySortedNumeric();
      }
      return singleton(single);
    }
    return dv;
  }

  /**
   * @function getSortedSet
   * @description Accesses SortedSetDocValues, or wraps a SortedDocValues as singleton if only that exists.
   *       Returns empty implementation if neither is present. Throws if field exists but has an incompatible DocValuesType.
   * 
   * @param {LeafReader} reader - Per-segment Lucene reader.
   * @param {String} field - Field name.
   * 
   * @returns {SortedSetDocValues} SortedSetDocValues instance or empty fallback.
   * 
   * @throws {IllegalStateException} When present field has incorrect DocValuesType.
   * @throws {IOException} On index reading error.
   * 
   * @example
   * SortedSetDocValues ssdv = DocValues.getSortedSet(reader, "tags");
   */
  public static SortedSetDocValues getSortedSet(LeafReader reader, String field)
      throws IOException {
    SortedSetDocValues dv = reader.getSortedSetDocValues(field);
    if (dv == null) {
      SortedDocValues sorted = reader.getSortedDocValues(field);
      if (sorted == null) {
        checkField(reader, field, DocValuesType.SORTED, DocValuesType.SORTED_SET);
        return emptySortedSet();
      }
      dv = singleton(sorted);
    }
    return dv;
  }

  /**
   * @function isCacheable
   * @description Determines if all provided fields in a context are cacheable with respect to docValues generation.
   *       Returns false if any field has a docValuesGen > -1, indicating it is not cacheable.
   * 
   * @param {LeafReaderContext} ctx - Context referencing per-segment reader and field infos.
   * @param {String[]} fields - Fields to test for cacheability.
   * 
   * @returns {boolean} True if all fields are cacheable, false otherwise.
   * 
   * @example
   * boolean cache = DocValues.isCacheable(context, "foo", "bar");
   */
  public static boolean isCacheable(LeafReaderContext ctx, String... fields) {
    for (String field : fields) {
      FieldInfo fi = ctx.reader().getFieldInfos().fieldInfo(field);
      if (fi != null && fi.getDocValuesGen() > -1) return false;
    }
    return true;
  }
}
