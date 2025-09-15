/**
 * @file Utility methods and empty/singleton-safe DocValues implementations for Lucene index field values.
 * @module org.apache.lucene.index.DocValues
 * 
 * @description
 * Provides static utilities and abstract classes for managing DocValues in Lucene indices, enabling robust and unified handling
 * of binary, numeric, sorted, sorted set, and sorted numeric DocValues at the segment (LeafReader) level. This file supplies
 * empty implementations for missing fields, canonical singleton adapters to wrap non-collection DocValues with collection types,
 * and utility methods for retrieving, validating, and safely accessing DocValues by field name, all while enforcing Lucene contracts
 * and minimizing null-checking overhead for consumers.
 * 
 * @dependencies
 * - org.apache.lucene.index.LeafReader: Grants access to index segment (leaf) data and DocValues.
 * - org.apache.lucene.index.DocValuesType: Enumerates DocValues representations for validation and runtime type checks.
 * - org.apache.lucene.util.BytesRef: Provides an efficient, reference-counted byte array wrapper, commonly for field values.
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
 * @classdesc Static utility class providing empty-safe DocValues implementations, singleton adapters, canonical field DocValues retrieval, and DocValues type validation routines for Lucene index segments.
 * 
 * @example
 * // Usage example for safe DocValues retrieval in custom LeafReaderContext logic:
 * NumericDocValues ndv = DocValues.getNumeric(reader, "myField");
 * if (ndv.advanceExact(docId)) {
 *   long value = ndv.longValue();
 *   // ... use value
 * }
 */
public final class DocValues {

  // Prevent instantiation of static utility class
  private DocValues() {}

  /**
   * @function emptyBinary
   * @description Returns a BinaryDocValues implementation representing an empty value set; all lookups return no data.
   * Useful when a field is missing or explicitly empty, following the DocValues contract.
   * 
   * @returns {BinaryDocValues} A no-op, empty BinaryDocValues implementation.
   * 
   * @example
   * BinaryDocValues binaryDV = DocValues.emptyBinary();
   * assert binaryDV.advance(0) == DocIdSetIterator.NO_MORE_DOCS;
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
        // No value present for any doc
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
        // Should never be called according to DocValues contract (as advanceExact must return false)
        assert false;
        return null;
      }
    };
  }

  /**
   * @function emptyNumeric
   * @description Returns a NumericDocValues implementation representing an empty value set.
   * Used as a safe fallback for missing or empty numeric DocValues fields.
   * 
   * @returns {NumericDocValues} No-op, empty NumericDocValues implementation.
   * 
   * @example
   * NumericDocValues ndv = DocValues.emptyNumeric();
   * assert ndv.advance(0) == DocIdSetIterator.NO_MORE_DOCS;
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
        // Should never be called, as advanceExact always returns false
        assert false;
        return 0;
      }
    };
  }

  /**
   * @function emptySorted
   * @description Returns a SortedDocValues implementation for fields with no sorted terms.
   * Provides empty semantics and ensures all lookups yield no value.
   * 
   * @returns {SortedDocValues} Dummy SortedDocValues that contains zero ordinals/values.
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
        // Field has no value for any document
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
        // No ordinal is present; should not be called
        assert false;
        return -1;
      }

      @Override
      public BytesRef lookupOrd(int ord) {
        // Always return an empty BytesRef for any lookup
        return empty;
      }

      @Override
      public int getValueCount() {
        // No terms present
        return 0;
      }
    };
  }

  /**
   * @function emptySortedNumeric
   * @description Returns a SortedNumericDocValues implementation with no numeric values for all documents.
   * Leverages singleton adapter for the empty numeric DocValues.
   * 
   * @returns {SortedNumericDocValues} Sorted numeric values representing an empty set.
   */
  public static final SortedNumericDocValues emptySortedNumeric() {
    return singleton(emptyNumeric());
  }

  /**
   * @function emptySortedSet
   * @description Returns a SortedSetDocValues implementation with no ordinals/terms for all documents.
   * Uses singleton adapter to adapt empty SortedDocValues.
   * 
   * @returns {SortedSetDocValues} Sorted set doc values representing an empty set.
   */
  public static final SortedSetDocValues emptySortedSet() {
    return singleton(emptySorted());
  }

  /**
   * @function singleton
   * @description Wraps a single-valued SortedDocValues as a SortedSetDocValues adapter for compatibility with APIs expecting set-valued DocValues.
   * 
   * @param {SortedDocValues} dv - The SortedDocValues instance to adapt as a singleton set.
   * @returns {SortedSetDocValues} Singleton wrapper around the given SortedDocValues.
   */
  public static SortedSetDocValues singleton(SortedDocValues dv) {
    return new SingletonSortedSetDocValues(dv);
  }

  /**
   * @function unwrapSingleton
   * @description If the given SortedSetDocValues instance was wrapped as a singleton (i.e., adapted from SortedDocValues), returns the underlying SortedDocValues. Otherwise, returns null.
   * 
   * @param {SortedSetDocValues} dv - The possible singleton-adapted SortedSetDocValues instance.
   * @returns {SortedDocValues|null} The underlying single-valued SortedDocValues, or null if not adapted.
   */
  public static SortedDocValues unwrapSingleton(SortedSetDocValues dv) {
    if (dv instanceof SingletonSortedSetDocValues) {
      return ((SingletonSortedSetDocValues) dv).getSortedDocValues();
    } else {
      return null;
    }
  }

  /**
   * @function unwrapSingleton
   * @description Extracts the original NumericDocValues from a singleton-wrapped SortedNumericDocValues, or returns null if dv is not a singleton.
   *
   * @param {SortedNumericDocValues} dv - A SortedNumericDocValues instance, possibly a singleton adapter.
   * @returns {NumericDocValues|null} The inner NumericDocValues if dv is a singleton, or null otherwise.
   */
  public static NumericDocValues unwrapSingleton(SortedNumericDocValues dv) {
    if (dv instanceof SingletonSortedNumericDocValues) {
      return ((SingletonSortedNumericDocValues) dv).getNumericDocValues();
    } else {
      return null;
    }
  }

  /**
   * @function singleton
   * @description Wraps a single-valued NumericDocValues as a SortedNumericDocValues adapter, exposing it as a one-element collection.
   * 
   * @param {NumericDocValues} dv - Single-valued numeric doc values instance to adapt.
   * @returns {SortedNumericDocValues} Singleton wrapper around the given NumericDocValues.
   */
  public static SortedNumericDocValues singleton(NumericDocValues dv) {
    return new SingletonSortedNumericDocValues(dv);
  }


  /**
   * @function checkField
   * @description Internal utility to validate that a given field of a LeafReader matches one of the supplied DocValuesTypes.
   * Throws a detailed exception if the types do not align, aiding in runtime diagnostics.
   * 
   * @param {LeafReader} in - The reader providing field info.
   * @param {String} field - Name of the field to check.
   * @param {DocValuesType[]} expected - Expected valid types for the field.
   * 
   * @throws {IllegalStateException} If the actual field type is set and differs from all expected types.
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
   * @description Retrieves NumericDocValues for the given field from the specified LeafReader, returning a safe empty implementation if the field is missing.
   * Ensures that consumers always receive a non-null, contract-obeying NumericDocValues instance.
   * 
   * @param {LeafReader} reader - The segment reader supplying index field data.
   * @param {String} field - The name of the field whose numeric DocValues to retrieve.
   * 
   * @returns {NumericDocValues} A NumericDocValues for the field, or an empty one if not present.
   * 
   * @throws {IllegalStateException} If the field exists but is of an unexpected DocValues type.
   * @throws {IOException} For lower-level reader access exceptions.
   * 
   * @example
   * NumericDocValues ndv = DocValues.getNumeric(reader, "myField");
   * if (ndv.advanceExact(docId)) {
   *    long value = ndv.longValue();
   * }
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
   * @description Retrieves BinaryDocValues for the specified field, providing an empty implementation as fallback.
   * Automatic type validation protects against accidental field misuse.
   * 
   * @param {LeafReader} reader - The target index segment reader.
   * @param {String} field - The binary DocValues field to resolve.
   * 
   * @returns {BinaryDocValues} BinaryDocValues representing the field's data, or an empty instance.
   * 
   * @throws {IllegalStateException} For present fields with incompatible DocValues types.
   * @throws {IOException} For underlying reader I/O issues.
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
   * @description Safely retrieves SortedDocValues for a given field, returning an empty object for missing fields.
   * Type validation guards against misconfigured index fields.
   *
   * @param {LeafReader} reader - Source of segment/field data.
   * @param {String} field - Field whose sorted DocValues should be fetched.
   * 
   * @returns {SortedDocValues} Field's sorted doc values, or empty if absent.
   * 
   * @throws {IllegalStateException} If type is inconsistent with expected.
   * @throws {IOException} For reader-level failures.
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
   * @description Returns SortedNumericDocValues for a field, supporting both natively stored sorted-numeric fields and singleton-wrapped numeric fields.
   * Falls back to empty doc values if neither is present.
   * 
   * @param {LeafReader} reader - The segment reader supplying field info.
   * @param {String} field - Target field name.
   * 
   * @returns {SortedNumericDocValues} DocValues containing sorted numerics for this field, or empty set.
   * 
   * @throws {IllegalStateException} For mismatched types.
   * @throws {IOException} For I/O issues accessing DocValues.
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
   * @description Resolves a SortedSetDocValues for the specified field, accommodating both sorted-set and singleton-wrapped sorted field types.
   * Defaults to an empty set implementation for missing or invalid type fields.
   * 
   * @param {LeafReader} reader - Index segment reader.
   * @param {String} field - Name of the field.
   * 
   * @returns {SortedSetDocValues} Non-null SortedSetDocValues for the field.
   * 
   * @throws {IllegalStateException} If existing type is incompatible with sorted or sorted_set.
   * @throws {IOException} On failures at the reader or field level.
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
   * @description Determines whether all specified fields in the given segment context are cacheable, i.e., have non-volatile DocValues.
   * Returns false if any of the fields are set to be updated (docValuesGen > -1).
   * 
   * @param {LeafReaderContext} ctx - Current leaf segment context.
   * @param {String[]} fields - Array of field names to check for cacheability.
   * 
   * @returns {boolean} True if all fields are cacheable, false if any are updated at runtime.
   */
  public static boolean isCacheable(LeafReaderContext ctx, String... fields) {
    for (String field : fields) {
      FieldInfo fi = ctx.reader().getFieldInfos().fieldInfo(field);
      // Field is not cacheable if it is marked as updated
      if (fi != null && fi.getDocValuesGen() > -1) return false;
    }
    return true;
  }
}
