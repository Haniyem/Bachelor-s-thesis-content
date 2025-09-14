```java
/**
 * @file DocValuesSkipper.java
 * @module org.apache.lucene.index
 * 
 * @description
 * Provides the definition for the abstract DocValuesSkipper class used in Apache Lucene for efficiently skipping through document values during search operations. 
 * It defines a multi-level skipping algorithm and abstractions to allow subclasses to implement navigation and range-based skipping across indexed documents. Includes static utility methods to aggregate minimum, maximum, and count values for a field across index segments, enhancing search and analysis performance.
 * 
 * @dependencies
 * - org.apache.lucene.search.DocIdSetIterator: Provides the constant for identifying when no more document IDs exist.
 * - org.apache.lucene.search.IndexSearcher: Used for traversing per-segment readers for aggregation methods.
 * - org.apache.lucene.index.LeafReaderContext: (implicit) For iterating leaf segment readers.
 * 
 * @author Apache Software Foundation
 * @version 1.0
 * @license Apache License 2.0
 * @lastmodified 2025-09-14: Documented, updated header and comments for clarity and maintainability.
 */

/**
 * @class DocValuesSkipper
 * @classdesc 
 * Abstract class providing the interface and core algorithms for skipping over document value ranges in multi-level structured postings for Lucene indexes. 
 * Designed as a base for concrete implementations that facilitate efficient navigation and aggregation on field value distributions.
 * 
 * @example
 * // Example usage (concrete implementation required)
 * DocValuesSkipper skipper = ...; // obtain from reader
 * skipper.advance(1000); // advance skipping logic to docID 1000
 * long min = skipper.minValue();
 * long max = skipper.maxValue();
 * 
 * @prop {int} Level - Represents a hierarchy of skip levels (e.g., blocks) for efficient navigation.
 * @prop {long} Value - Denotes the minimum/maximum value encountered at a particular level.
 */
public abstract class DocValuesSkipper {

  /**
   * Advance the internal pointer to at least the given target document ID using multi-level skipping.
   *
   * @function advance
   * @description
   * Abstract method to move the skipper forward to the nearest document whose ID is greater than or equal to the specified target.
   * Typical implementations use skip lists or block-level acceleration.
   * 
   * @param {int} target - Target docID to advance to (must be >= current).
   * @throws {IOException} If an I/O error occurs during index access.
   */
  public abstract void advance(int target) throws IOException;

  /**
   * Returns the total number of skip levels in the structure.
   *
   * @function numLevels
   * @description
   * Abstract method returning the number of discrete skip levels utilized in the skipper, dictating hierarchical depth.
   * 
   * @returns {int} Number of levels.
   */
  public abstract int numLevels();

  /**
   * Returns the minimum document ID present at the specified skip level.
   *
   * @function minDocID
   * @description
   * Provides the minimal docID at the specified level; may be -1 if that block contains no documents.
   * 
   * @param {int} level - Skip level index (0-based).
   * @returns {int} Minimum docID at this level; -1 if no docs available.
   */
  public abstract int minDocID(int level);

  /**
   * Returns the maximum document ID present at the specified skip level.
   *
   * @function maxDocID
   * @description
   * Provides the maximal docID at the specified level, representing the upper bound of this level's range.
   * 
   * @param {int} level - Skip level index (0-based).
   * @returns {int} Maximum docID at this level.
   */
  public abstract int maxDocID(int level);

  /**
   * Returns the minimum value referenced at the specified skip level.
   *
   * @function minValue
   * @description
   * Provides the minimum field value observed at the given level; useful for range queries or block aggregation.
   * 
   * @param {int} level - Skip level index (0-based).
   * @returns {long} Minimum field value at this level.
   */
  public abstract long minValue(int level);

  /**
   * Returns the maximum value referenced at the specified skip level.
   *
   * @function maxValue
   * @description
   * Provides the maximum field value observed at the given level.
   * 
   * @param {int} level - Skip level index (0-based).
   * @returns {long} Maximum field value at this level.
   */
  public abstract long maxValue(int level);

  /**
   * Returns the number of documents present at the specified skip level.
   *
   * @function docCount
   * @description
   * Returns the number of documents present in the current block or level.
   * 
   * @param {int} level - Skip level index (0-based).
   * @returns {int} Document count at this level.
   */
  public abstract int docCount(int level);

  /**
   * Returns the overall minimum value across all levels in this skipper instance.
   *
   * @function minValue
   * @description
   * Concrete overload; aggregate minimum field value tracked by this skipper instance.
   * 
   * @returns {long} Overall minimum field value.
   */
  public abstract long minValue();

  /**
   * Returns the overall maximum value across all levels in this skipper instance.
   *
   * @function maxValue
   * @description
   * Concrete overload; aggregate maximum field value tracked by this skipper instance.
   * 
   * @returns {long} Overall maximum field value.
   */
  public abstract long maxValue();

  /**
   * Returns the overall document count managed by this skipper instance.
   *
   * @function docCount
   * @description
   * Concrete overload; total number of documents tracked by this skipper instance.
   * 
   * @returns {int} Number of documents.
   */
  public abstract int docCount();

  /**
   * Efficiently advances the skipper to the first block or document whose value range intersects the specified [minValue, maxValue] range.
   *
   * @function advance
   * @description
   * Multi-level skip-loop that advances the skipper to the next relevant block, skipping all blocks whose value intervals do not intersect the [minValue, maxValue] range.
   * Implements block skipping with support for hierarchical levels to accelerate range search performance.
   *
   * @param {long} minValue - Lower bound of the desired value range.
   * @param {long} maxValue - Upper bound of the desired value range.
   * @throws {IOException} If an I/O error occurs.
   * 
   * @example
   * skipper.advance(10L, 100L); // Advances to the next document/block in value range [10, 100]
   */
  public final void advance(long minValue, long maxValue) throws IOException {
    // If the base-level block is empty, initialize by advancing to docID 0.
    if (minDocID(0) == -1) {
      advance(0);
    }

    // Skip blocks that do not overlap with the given value range at any hierarchy level.
    while (minDocID(0) != DocIdSetIterator.NO_MORE_DOCS
        && ((minValue(0) > maxValue || maxValue(0) < minValue))) {
      int maxDocID = maxDocID(0);
      int nextLevel = 1;

      // Drill down the skip levels, searching for the next block to skip to.
      while (nextLevel < numLevels()
          && (minValue(nextLevel) > maxValue || maxValue(nextLevel) < minValue)) {
        maxDocID = maxDocID(nextLevel);
        nextLevel++;
      }
      // Advance to the document just after the maxDocID of the found level.
      advance(maxDocID + 1);
    }
  }

  /**
   * Computes the global minimum value for the given field across all index segments.
   *
   * @function globalMinValue
   * @description
   * Utility static method that iterates all leaf segments of the provided IndexSearcher and finds the smallest field value using the segment's DocValuesSkipper.
   * If any segment does not implement a skipper for the given field, returns Long.MIN_VALUE as conservative default.
   *
   * @param {IndexSearcher} searcher - Active searcher providing leaf contexts.
   * @param {String} field - Name of the field for value aggregation.
   * @returns {long} The smallest value encountered for the field globally, or Long.MIN_VALUE if not available everywhere.
   * @throws {IOException} If underlying index I/O fails.
   * 
   * @example
   * long minVal = DocValuesSkipper.globalMinValue(searcher, "age");
   */
  public static long globalMinValue(IndexSearcher searcher, String field) throws IOException {
    long minValue = Long.MAX_VALUE;
    for (LeafReaderContext ctx : searcher.getLeafContexts()) {
      if (ctx.reader().getFieldInfos().fieldInfo(field) == null) {
        continue; // skip leaf segments without this field
      }
      DocValuesSkipper skipper = ctx.reader().getDocValuesSkipper(field);
      if (skipper == null) {
        // If any segment can't provide a skipper, propagate sentinel value.
        return Long.MIN_VALUE;
      } else {
        minValue = Math.min(minValue, skipper.minValue());
      }
    }
    return minValue;
  }

  /**
   * Computes the global maximum value for the given field across all index segments.
   *
   * @function globalMaxValue
   * @description
   * Utility static method that iterates all leaf segments, aggregating the largest value across DocValuesSkipper instances for the specified field.
   * If any segment lacks a skipper, returns Long.MAX_VALUE.
   *
   * @param {IndexSearcher} searcher - Active searcher with segment readers.
   * @param {String} field - Field name to aggregate.
   * @returns {long} The largest value for the field globally, or Long.MAX_VALUE if not available everywhere.
   * @throws {IOException} On underlying index I/O failure.
   * 
   * @example
   * long maxVal = DocValuesSkipper.globalMaxValue(searcher, "price");
   */
  public static long globalMaxValue(IndexSearcher searcher, String field) throws IOException {
    long maxValue = Long.MIN_VALUE;
    for (LeafReaderContext ctx : searcher.getLeafContexts()) {
      if (ctx.reader().getFieldInfos().fieldInfo(field) == null) {
        continue; // skip segments without the required field
      }
      DocValuesSkipper skipper = ctx.reader().getDocValuesSkipper(field);
      if (skipper == null) {
        // Unavailable skipper indicates inconsistent field mapping.
        return Long.MAX_VALUE;
      } else {
        maxValue = Math.max(maxValue, skipper.maxValue());
      }
    }
    return maxValue;
  }

  /**
   * Computes the global document count for the specified field across all segments.
   *
   * @function globalDocCount
   * @description
   * Sums document counts from all DocValuesSkipper instances for the specified field across all index segments.
   * Segments lacking a skipper for the field are excluded from the total.
   *
   * @param {IndexSearcher} searcher - The active searcher with segments.
   * @param {String} field - Field name for which to count documents.
   * @returns {int} The total document count across all relevant segments.
   * @throws {IOException} If any segment cannot be accessed.
   * 
   * @example
   * int totalDocs = DocValuesSkipper.globalDocCount(searcher, "timestamp");
   */
  public static int globalDocCount(IndexSearcher searcher, String field) throws IOException {
    int docCount = 0;
    for (LeafReaderContext ctx : searcher.getLeafContexts()) {
      DocValuesSkipper skipper = ctx.reader().getDocValuesSkipper(field);
      if (skipper != null) {
        docCount += skipper.docCount();
      }
    }
    return docCount;
  }
}
```

[1](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/6341464/7db4b6f9-1f7c-4e0f-8587-1451033425a2/T5.txt)
