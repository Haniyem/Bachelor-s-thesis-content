```java
/**
 * @file IndexSorter.java
 * @module org.apache.lucene.index
 * 
 * @description
 * Defines the `IndexSorter` interface and several implementations for sorting Lucene search results based on document value types (int, long, float, double, and string). Provides value provider mechanisms, document comparators, and sorting implementations that plug into Lucene indexing and segmented search architecture. Handles missing values, supports ascending and descending order, and ensures optimal integration with Lucene's segment-leaf abstraction and DocValues system.
 * 
 * @dependencies
 * - org.apache.lucene.search.DocIdSetIterator: Sentinel value for iteration.
 * - org.apache.lucene.search.FieldComparator: For field sorting context.
 * - org.apache.lucene.search.SortField: For sort field constants (e.g., STRING_LAST).
 * - org.apache.lucene.util.LongValues: Utility for ordinal mapping.
 * - org.apache.lucene.util.NumericUtils: For converting sortable byte patterns.
 * - org.apache.lucene.util.packed.PackedInts: For ordinal remapping.
 * - org.apache.lucene.index.LeafReader, NumericDocValues, SortedDocValues: Access to Lucene’s per-segment storage and value loading.
 * 
 * @author Apache Software Foundation
 * @version 1.0
 * @license Apache-2.0
 * @lastmodified 2025-09-14: Comprehensive in-code documentation added.
 */

package org.apache.lucene.index;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.packed.PackedInts;

/**
 * @interface IndexSorter
 * @classdesc
 * Contract for Lucene implementations that wish to provide custom document sorting for an index, especially for segmented and compound document sets. Enables sorting on arbitrary numeric and string values via provider and comparator interfaces.
 * 
 * Implementations must provide a way to extract comparable values for each document, as well as provide comparators for within-segment sorting. Custom sorters are created by implementing this interface.
 * 
 * @example
 * // Instantiate an IntSorter for integer-based sorting
 * IndexSorter sorter = new IndexSorter.IntSorter(
 *     "myIntField", 0, false, reader -> reader.getNumericDocValues("myIntField"));
 */
public interface IndexSorter {

  /**
   * @interface ComparableProvider
   * @description
   * Abstraction for producing a long value per document. This value is used for segment-level or global sorting comparisons.
   * 
   * @method getAsComparableLong
   * @param {int} docID - The document ID within the segment, 0-based.
   * @returns {long} The long value comparably representative of the document’s sortable key.
   * @throws {IOException} If a lower-level value loading fails.
   */
  interface ComparableProvider {
    long getAsComparableLong(int docID) throws IOException;
  }

  /**
   * @interface DocComparator
   * @description
   * Binary comparison interface for two document IDs within a segment; used for in-place segment sorting.
   * 
   * @method compare
   * @param {int} docID1 - First document ID
   * @param {int} docID2 - Second document ID
   * @returns {int} Comparison result: negative = docID1 < docID2; zero = equal; positive = docID1 > docID2.
   */
  interface DocComparator {
    int compare(int docID1, int docID2);
  }

  /**
   * Returns per-segment providers that yield directly comparable values for global sorting/scoring.
   * 
   * @function getComparableProviders
   * @param {List<LeafReader>} readers - List of segment readers from the index.
   * @returns {ComparableProvider[]} Array of providers, one per reader, exposing doc value as a comparable long.
   * @throws {IOException} If there is an error in value loading or segment reading.
   */
  ComparableProvider[] getComparableProviders(List<? extends LeafReader> readers) throws IOException;

  /**
   * Provides a segment-local comparator for two document IDs for use in segment-level sort routines.
   * 
   * @function getDocComparator
   * @param {LeafReader} reader - Segment (leaf) reader context.
   * @param {int} maxDoc - Total docs in the segment.
   * @returns {DocComparator} Segment doc comparator, supporting all docID range.
   * @throws {IOException} If value access fails.
   */
  DocComparator getDocComparator(LeafReader reader, int maxDoc) throws IOException;

  /**
   * Returns a human-readable provider name suitable for diagnostics and serialization.
   * 
   * @function getProviderName
   * @returns {String} Unique name of the provider/sorter (typically field name).
   */
  String getProviderName();

  /**
   * @interface NumericDocValuesProvider
   * @description
   * Provider function for segment numeric document values.
   * @method get
   * @param {LeafReader} reader - Index segment reader.
   * @returns {NumericDocValues} Lucene's per-doc numeric value accessor.
   * @throws {IOException} On reader errors.
   */
  interface NumericDocValuesProvider {
    NumericDocValues get(LeafReader reader) throws IOException;
  }

  /**
   * @interface SortedDocValuesProvider
   * @description
   * Provider function for segment sorted document values.
   * @method get
   * @param {LeafReader} reader - Index segment reader.
   * @returns {SortedDocValues} Lucene's per-doc ordinal/string value accessor.
   * @throws {IOException} On reader errors.
   */
  interface SortedDocValuesProvider {
    SortedDocValues get(LeafReader reader) throws IOException;
  }

  /**
   * @class IntSorter
   * @classdesc
   * Sorts documents using integer values sourced from `NumericDocValues`. Handles missing values and supports both ascending and descending order.
   * 
   * @example
   * IndexSorter sorter = new IndexSorter.IntSorter("field", 0, false, reader -> reader.getNumericDocValues("field"));
   * 
   * @prop {Integer} missingValue - Value to use for documents lacking a value (nullable).
   * @prop {int} reverseMul - Multiplier for order: 1 = ascending, -1 = descending.
   * @prop {NumericDocValuesProvider} valuesProvider - Supplies doc values per-segment.
   * @prop {String} providerName - Human-readable provider/sorter name.
   */
  final class IntSorter implements IndexSorter {
    private final Integer missingValue;
    private final int reverseMul;
    private final NumericDocValuesProvider valuesProvider;
    private final String providerName;

    /**
     * @constructor
     * Initializes an integer sorter for the provided value source and sort configuration.
     * 
     * @param {String} providerName - Name/field identifying the sort.
     * @param {Integer} missingValue - Value assigned to docs missing a value; may be null.
     * @param {boolean} reverse - True = descending order; false = ascending.
     * @param {NumericDocValuesProvider} valuesProvider - Per-segment document value source.
     */
    public IntSorter(
        String providerName,
        Integer missingValue,
        boolean reverse,
        NumericDocValuesProvider valuesProvider
    ) {
      this.missingValue = missingValue;
      this.reverseMul = reverse ? -1 : 1;
      this.valuesProvider = valuesProvider;
      this.providerName = providerName;
    }

    /**
     * Creates segment-comparable providers. For each segment, yields a provider that returns either the doc's int value or the missing value.
     * 
     * @param {List<LeafReader>} readers - Segment readers.
     * @returns {ComparableProvider[]} Providers exposing doc value for sorting.
     * @throws {IOException} On value access errors.
     */
    @Override
    public ComparableProvider[] getComparableProviders(List<? extends LeafReader> readers) throws IOException {
      ComparableProvider[] providers = new ComparableProvider[readers.size()];
      final long missingValue = this.missingValue != null ? this.missingValue : 0L;
      for (int readerIndex = 0; readerIndex < readers.size(); readerIndex++) {
        final NumericDocValues values = valuesProvider.get(readers.get(readerIndex));
        providers[readerIndex] = docID -> {
          if (values.advanceExact(docID)) {
            return values.longValue();
          } else {
            return missingValue;
          }
        };
      }
      return providers;
    }

    /**
     * Provides a segment-specific integer doc comparator, utilizing an array for fast lookups.
     * 
     * @param {LeafReader} reader - Lucene segment reader.
     * @param {int} maxDoc - Number of documents in the segment.
     * @returns {DocComparator} Segment-local doc comparator.
     * @throws {IOException} On failures reading values.
     */
    @Override
    public DocComparator getDocComparator(LeafReader reader, int maxDoc) throws IOException {
      final NumericDocValues dvs = valuesProvider.get(reader);
      int[] values = new int[maxDoc];
      // Populate missing value for all documents as default.
      if (this.missingValue != null) {
        Arrays.fill(values, this.missingValue);
      }
      // Update present docs with their values.
      while (true) {
        int docID = dvs.nextDoc();
        if (docID == NO_MORE_DOCS) {
          break;
        }
        values[docID] = (int) dvs.longValue();
      }
      // Comparator scales direction by reverseMul.
      return (docID1, docID2) -> reverseMul * Integer.compare(values[docID1], values[docID2]);
    }

    /**
     * Gets the human-readable name of the sorter.
     * 
     * @returns {String}
     */
    @Override
    public String getProviderName() {
      return providerName;
    }
  }

  /**
   * @class LongSorter
   * @classdesc
   * Sorts documents by long-type values using `NumericDocValues`. Supports missing values and reverse order.
   * 
   * @example
   * IndexSorter sorter = new IndexSorter.LongSorter("field", null, true, reader -> reader.getNumericDocValues("field"));
   * 
   * @prop {String} providerName - Name/field for the sorter.
   * @prop {Long} missingValue - Value for missing docs.
   * @prop {int} reverseMul - Order multiplier.
   * @prop {NumericDocValuesProvider} valuesProvider - Per-segment value provider.
   */
  final class LongSorter implements IndexSorter {
    private final String providerName;
    private final Long missingValue;
    private final int reverseMul;
    private final NumericDocValuesProvider valuesProvider;

    /**
     * @constructor
     * Initializes a long-based sorter with optional missing value and order.
     * 
     * @param {String} providerName
     * @param {Long} missingValue
     * @param {boolean} reverse
     * @param {NumericDocValuesProvider} valuesProvider
     */
    public LongSorter(
        String providerName,
        Long missingValue,
        boolean reverse,
        NumericDocValuesProvider valuesProvider
    ) {
      this.providerName = providerName;
      this.missingValue = missingValue;
      this.reverseMul = reverse ? -1 : 1;
      this.valuesProvider = valuesProvider;
    }

    /**
     * Builds per-segment providers for retrieving long values for sorting.
     * 
     * @param {List<LeafReader>} readers
     * @returns {ComparableProvider[]}
     * @throws {IOException}
     */
    @Override
    public ComparableProvider[] getComparableProviders(List<? extends LeafReader> readers) throws IOException {
      ComparableProvider[] providers = new ComparableProvider[readers.size()];
      final long missingValue = this.missingValue != null ? this.missingValue : 0L;

      for (int readerIndex = 0; readerIndex < readers.size(); readerIndex++) {
        final NumericDocValues values = valuesProvider.get(readers.get(readerIndex));
        providers[readerIndex] = docID -> {
          if (values.advanceExact(docID)) {
            return values.longValue();
          } else {
            return missingValue;
          }
        };
      }
      return providers;
    }

    /**
     * Provides a doc comparator for long values within a segment.
     * 
     * @param {LeafReader} reader
     * @param {int} maxDoc
     * @returns {DocComparator}
     * @throws {IOException}
     */
    @Override
    public DocComparator getDocComparator(LeafReader reader, int maxDoc) throws IOException {
      final NumericDocValues dvs = valuesProvider.get(reader);
      long[] values = new long[maxDoc];
      // Fill with missing value.
      if (this.missingValue != null) {
        Arrays.fill(values, this.missingValue);
      }
      while (true) {
        int docID = dvs.nextDoc();
        if (docID == NO_MORE_DOCS) {
          break;
        }
        values[docID] = dvs.longValue();
      }
      return (docID1, docID2) -> reverseMul * Long.compare(values[docID1], values[docID2]);
    }

    /**
     * @returns {String} Provider name.
     */
    @Override
    public String getProviderName() {
      return providerName;
    }
  }

  /**
   * @class FloatSorter
   * @classdesc
   * Sorts documents by float values, using numeric doc values with missing value and reverse order support.
   * 
   * @example
   * IndexSorter sorter = new IndexSorter.FloatSorter("floatField", Float.NaN, false, reader -> reader.getNumericDocValues("floatField"));
   * 
   * @prop {String} providerName
   * @prop {Float} missingValue
   * @prop {int} reverseMul
   * @prop {NumericDocValuesProvider} valuesProvider
   */
  final class FloatSorter implements IndexSorter {
    private final String providerName;
    private final Float missingValue;
    private final int reverseMul;
    private final NumericDocValuesProvider valuesProvider;

    /**
     * @constructor
     * Constructs a sorter for float values.
     * 
     * @param {String} providerName
     * @param {Float} missingValue
     * @param {boolean} reverse
     * @param {NumericDocValuesProvider} valuesProvider
     */
    public FloatSorter(
        String providerName,
        Float missingValue,
        boolean reverse,
        NumericDocValuesProvider valuesProvider
    ) {
      this.providerName = providerName;
      this.missingValue = missingValue;
      this.reverseMul = reverse ? -1 : 1;
      this.valuesProvider = valuesProvider;
    }

    /**
     * Produces segment providers of sortable float bits for each document.
     * Values are mapped as sortable integer bits for fast comparison and ordering.
     * 
     * @param {List<LeafReader>} readers
     * @returns {ComparableProvider[]}
     * @throws {IOException}
     */
    @Override
    public ComparableProvider[] getComparableProviders(List<? extends LeafReader> readers) throws IOException {
      ComparableProvider[] providers = new ComparableProvider[readers.size()];
      final int missValueBits = Float.floatToIntBits(missingValue != null ? missingValue : 0.0f);

      for (int readerIndex = 0; readerIndex < readers.size(); readerIndex++) {
        final NumericDocValues values = valuesProvider.get(readers.get(readerIndex));
        providers[readerIndex] = docID -> {
          final int valueBits = values.advanceExact(docID) ? (int) values.longValue() : missValueBits;
          // Use Lucene sortable bits logic
          return NumericUtils.sortableFloatBits(valueBits);
        };
      }
      return providers;
    }

    /**
     * Segment-level doc comparator using float arrays, comparing raw float values.
     * 
     * @param {LeafReader} reader
     * @param {int} maxDoc
     * @returns {DocComparator}
     * @throws {IOException}
     */
    @Override
    public DocComparator getDocComparator(LeafReader reader, int maxDoc) throws IOException {
      final NumericDocValues dvs = valuesProvider.get(reader);
      float[] values = new float[maxDoc];
      if (this.missingValue != null) {
        Arrays.fill(values, this.missingValue);
      }
      while (true) {
        int docID = dvs.nextDoc();
        if (docID == NO_MORE_DOCS) {
          break;
        }
        // NumericDocValues is always stored as long; reinterpret bits as float.
        values[docID] = Float.intBitsToFloat((int) dvs.longValue());
      }
      return (docID1, docID2) -> reverseMul * Float.compare(values[docID1], values[docID2]);
    }

    /**
     * @returns {String} Provider name
     */
    @Override
    public String getProviderName() {
      return providerName;
    }
  }

  /**
   * @class DoubleSorter
   * @classdesc
   * Sorts documents based on double-precision values from NumericDocValues with support for missing value and order direction.
   * 
   * @example
   * IndexSorter sorter = new IndexSorter.DoubleSorter("doubleField", 0.0d, true, reader -> reader.getNumericDocValues("doubleField"));
   * 
   * @prop {String} providerName
   * @prop {Double} missingValue
   * @prop {int} reverseMul
   * @prop {NumericDocValuesProvider} valuesProvider
   */
  final class DoubleSorter implements IndexSorter {
    private final String providerName;
    private final Double missingValue;
    private final int reverseMul;
    private final NumericDocValuesProvider valuesProvider;

    /**
     * @constructor Adds configuration for double sorting.
     * 
     * @param {String} providerName
     * @param {Double} missingValue
     * @param {boolean} reverse
     * @param {NumericDocValuesProvider} valuesProvider
     */
    public DoubleSorter(
        String providerName,
        Double missingValue,
        boolean reverse,
        NumericDocValuesProvider valuesProvider
    ) {
      this.providerName = providerName;
      this.missingValue = missingValue;
      this.reverseMul = reverse ? -1 : 1;
      this.valuesProvider = valuesProvider;
    }

    /**
     * Yields per-segment providers that output a sortable long representing the double value.
     * 
     * @param {List<LeafReader>} readers
     * @returns {ComparableProvider[]}
     * @throws {IOException}
     */
    @Override
    public ComparableProvider[] getComparableProviders(List<? extends LeafReader> readers) throws IOException {
      ComparableProvider[] providers = new ComparableProvider[readers.size()];
      final long missingValueBits = Double.doubleToLongBits(missingValue != null ? missingValue : 0.0f);

      for (int readerIndex = 0; readerIndex < readers.size(); readerIndex++) {
        final NumericDocValues values = valuesProvider.get(readers.get(readerIndex));
        providers[readerIndex] = docID -> {
          final long valueBits = values.advanceExact(docID) ? values.longValue() : missingValueBits;
          // Convert to Lucene sortable bits.
          return NumericUtils.sortableDoubleBits(valueBits);
        };
      }
      return providers;
    }

    /**
     * Segment comparator for doubles, using a double array for direct value comparison.
     * 
     * @param {LeafReader} reader
     * @param {int} maxDoc
     * @returns {DocComparator}
     * @throws {IOException}
     */
    @Override
    public DocComparator getDocComparator(LeafReader reader, int maxDoc) throws IOException {
      final NumericDocValues dvs = valuesProvider.get(reader);
      double[] values = new double[maxDoc];
      if (missingValue != null) {
        Arrays.fill(values, missingValue);
      }
      while (true) {
        int docID = dvs.nextDoc();
        if (docID == NO_MORE_DOCS) {
          break;
        }
        // NumericDocValues stores bits as long; decode to double.
        values[docID] = Double.longBitsToDouble(dvs.longValue());
      }
      return (docID1, docID2) -> reverseMul * Double.compare(values[docID1], values[docID2]);
    }

    /**
     * @returns {String} Provider name.
     */
    @Override
    public String getProviderName() {
      return providerName;
    }
  }

  /**
   * @class StringSorter
   * @classdesc
   * Sorts documents based on sorted string values using `SortedDocValues`.
   * Handles global-to-segment ordinal remapping, missing values, and direction.
   * 
   * @example
   * IndexSorter sorter = new IndexSorter.StringSorter("stringField", SortField.STRING_LAST, false, reader -> reader.getSortedDocValues("stringField"));
   * 
   * @prop {String} providerName
   * @prop {Object} missingValue - SortField.STRING_LAST or other sentinel.
   * @prop {int} reverseMul
   * @prop {SortedDocValuesProvider} valuesProvider
   */
  final class StringSorter implements IndexSorter {
    private final String providerName;
    private final Object missingValue;
    private final int reverseMul;
    private final SortedDocValuesProvider valuesProvider;

    /**
     * @constructor
     * Sets up a string-based sorter with support for special missing value policies and direction.
     * 
     * @param {String} providerName
     * @param {Object} missingValue - SortField.STRING_LAST or similar.
     * @param {boolean} reverse
     * @param {SortedDocValuesProvider} valuesProvider
     */
    public StringSorter(
        String providerName,
        Object missingValue,
        boolean reverse,
        SortedDocValuesProvider valuesProvider
    ) {
      this.providerName = providerName;
      this.missingValue = missingValue;
      this.reverseMul = reverse ? -1 : 1;
      this.valuesProvider = valuesProvider;
    }

    /**
     * Produces segment providers that yield a sortable ordinal for each doc, using Lucene’s global OrdinalMap for merged segment-aware ordering.
     * 
     * @param {List<LeafReader>} readers
     * @returns {ComparableProvider[]}
     * @throws {IOException}
     */
    @Override
    public ComparableProvider[] getComparableProviders(List<? extends LeafReader> readers) throws IOException {
      final ComparableProvider[] providers = new ComparableProvider[readers.size()];
      final SortedDocValues[] values = new SortedDocValues[readers.size()];
      for (int i = 0; i < readers.size(); i++) {
        final SortedDocValues sorted = valuesProvider.get(readers.get(i));
        values[i] = sorted;
      }
      // Global ordinal mapping for merged comparability.
      OrdinalMap ordinalMap = OrdinalMap.build(null, values, PackedInts.DEFAULT);
      final int missingOrd = (missingValue == SortField.STRING_LAST) ? Integer.MAX_VALUE : Integer.MIN_VALUE;

      for (int readerIndex = 0; readerIndex < readers.size(); readerIndex++) {
        final SortedDocValues readerValues = values[readerIndex];
        final LongValues globalOrds = ordinalMap.getGlobalOrds(readerIndex);
        providers[readerIndex] = docID -> {
          if (readerValues.advanceExact(docID)) {
            // Convert to global ordinal for segment-agnostic comparison.
            return globalOrds.get(readerValues.ordValue());
          } else {
            return missingOrd;
          }
        };
      }
      return providers;
    }

    /**
     * Returns a segment-local comparator based on string ordinals. Accounts for missing values as configured.
     * 
     * @param {LeafReader} reader
     * @param {int} maxDoc
     * @returns {DocComparator}
     * @throws {IOException}
     */
    @Override
    public DocComparator getDocComparator(LeafReader reader, int maxDoc) throws IOException {
      final SortedDocValues sorted = valuesProvider.get(reader);
      final int missingOrd = (missingValue == SortField.STRING_LAST) ? Integer.MAX_VALUE : Integer.MIN_VALUE;

      final int[] ords = new int[maxDoc];
      Arrays.fill(ords, missingOrd);
      int docID;
      // Fill ordinals for all present docs.
      while ((docID = sorted.nextDoc()) != NO_MORE_DOCS) {
        ords[docID] = sorted.ordValue();
      }
      // Comparator multiplies by reverseMul for order.
      return (docID1, docID2) -> reverseMul * Integer.compare(ords[docID1], ords[docID2]);
    }

    /**
     * @returns {String} Provider name.
     */
    @Override
    public String getProviderName() {
      return providerName;
    }
  }
}
