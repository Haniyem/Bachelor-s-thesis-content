```java
/**
 * @file Implements a custom Lucene MergePolicy for retaining documents marked as soft-deleted using retention queries.
 * @module org.apache.lucene.index.SoftDeletesRetentionMergePolicy
 * 
 * @description
 * This file defines the SoftDeletesRetentionMergePolicy, a specialized Lucene MergePolicy that applies configurable retention logic during segment merges.
 * Its principal function is to preserve documents marked with "soft deletes" based on user-defined retention queries. When merging segments, 
 * this policy leverages the supplied query to determine which logically deleted documents should remain accessible, thus supporting features 
 * like data retention, compliance, and advanced deletion strategies beyond simple hard deletes. The class is designed as a final extension 
 * over OneMergeWrappingMergePolicy, composing both core Lucene and custom retention policies.
 * 
 * @dependencies
 * - org.apache.lucene.index.MergePolicy: Abstract Lucene API for merge policy support.
 * - org.apache.lucene.index.CodecReader: Provides read access to a segment’s raw postings and stored fields.
 * - org.apache.lucene.search.Query: Framework for composable document search queries.
 * - org.apache.lucene.util.Bits, FixedBitSet: Efficient bitmask structures for tracking live/deleted documents.
 * 
 * @author Apache Software Foundation
 * @version 1.0
 * @license Apache-2.0
 * @lastmodified 2024-06-01: Initial implementation of soft-deletes retention logic for Lucene merge policies.
 */

package org.apache.lucene.index;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOSupplier;

/**
 * @class SoftDeletesRetentionMergePolicy
 * @classdesc 
 * A MergePolicy that retains soft-deleted documents matching a retention query during segment merges.
 * Integrates configurable logic to preserve a subset of soft-deleted documents, extending OneMergeWrappingMergePolicy to allow custom merge-time wrapping.
 *
 * @example
 * // Example instantiation:
 * MergePolicy basePolicy = new TieredMergePolicy();
 * Supplier<Query> retentionQuery = () -> new TermQuery(new Term("user_id", "123"));
 * MergePolicy policy = new SoftDeletesRetentionMergePolicy("soft_delete_field", retentionQuery, basePolicy);
 *
 * @prop {String} field - The name of the soft-deletes marker field.
 * @prop {Supplier<Query>} retentionQuerySupplier - Supplier returning the Query to identify documents to retain.
 */
public final class SoftDeletesRetentionMergePolicy extends OneMergeWrappingMergePolicy {
  private final String field;
  private final Supplier<Query> retentionQuerySupplier;

  /**
   * @constructor
   * @description 
   * Constructs a SoftDeletesRetentionMergePolicy.
   * Wraps a base MergePolicy and adds retention logic for soft-deleted documents based on a field and query supplier.
   *
   * @param {String} field - Name of the document field marking soft deletes. Must not be null.
   * @param {Supplier<Query>} retentionQuerySupplier - Supplies retention queries for each merge operation. Must not be null.
   * @param {MergePolicy} in - The underlying MergePolicy to wrap.
   *
   * @throws {NullPointerException} If any argument is null.
   * 
   * @example
   * MergePolicy policy = new SoftDeletesRetentionMergePolicy("is_deleted", () -> retentionQuery, basePolicy);
   */
  public SoftDeletesRetentionMergePolicy(
      String field, Supplier<Query> retentionQuerySupplier, MergePolicy in) {
    super(
        in,
        toWrap ->
            new MergePolicy.OneMerge(toWrap.segments) {
              @Override
              public CodecReader wrapForMerge(CodecReader reader) throws IOException {
                // Apply additional soft deletes retention logic only if there are live docs (i.e., any deletions).
                CodecReader wrapped = toWrap.wrapForMerge(reader);
                Bits liveDocs = reader.getLiveDocs();
                if (liveDocs == null) { // No deletes in this segment; no need to wrap.
                  return wrapped;
                }
                return applyRetentionQuery(field, retentionQuerySupplier.get(), wrapped);
              }
            });
    Objects.requireNonNull(field, "field must not be null");
    Objects.requireNonNull(retentionQuerySupplier, "retentionQuerySupplier must not be null");
    this.field = field;
    this.retentionQuerySupplier = retentionQuerySupplier;
  }

  /**
   * @method keepFullyDeletedSegment
   * @description 
   * Determines whether to retain a segment that is fully deleted according to hard deletes, based on retention query hits.
   * Uses the retention query to check for logically retained docs and keeps the segment 
   * if at least one document matches.
   *
   * @param {IOSupplier<CodecReader>} readerIOSupplier - Supplies the segment reader; may perform I/O.
   *
   * @returns {boolean} True if the segment should be retained; otherwise, delegates to the parent.
   *
   * @throws {IOException} If I/O operations fail on segment access.
   * 
   * @example
   * boolean retain = policy.keepFullyDeletedSegment(readerSupplier);
   */
  @Override
  public boolean keepFullyDeletedSegment(IOSupplier<CodecReader> readerIOSupplier)
      throws IOException {
    CodecReader reader = readerIOSupplier.get();
    // Only a single hit is sufficient to keep the segment; soft deletes handling is embedded in scorer logic.
    Scorer scorer =
        getScorer(
            retentionQuerySupplier.get(),
            FilterCodecReader.wrapLiveDocs(reader, null, reader.maxDoc()));
    if (scorer != null) {
      DocIdSetIterator iterator = scorer.iterator();
      boolean atLeastOneHit = iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS;
      return atLeastOneHit;
    }
    return super.keepFullyDeletedSegment(readerIOSupplier);
  }

  /**
   * @function applyRetentionQuery
   * @description
   * Applies the retention query to dead documents in the provided segment. Any soft-deleted document that matches
   * the retention query will be resurrected (marked live in the new live docs bitset).
   * 
   * Iterates only over documents that are already soft-deleted, wrapping the reader accordingly. Updates the live docs mask
   * if matches are found, otherwise returns the original reader.
   *
   * @param {String} softDeleteField - Name of the field indicating soft deletes.
   * @param {Query} retentionQuery - A Lucene query identifying documents to retain.
   * @param {CodecReader} reader - The codec reader representing the merge segment.
   *
   * @returns {CodecReader} A possibly wrapped CodecReader with updated live docs tracking retention hits.
   *
   * @throws {IOException} On failure to score or access segment data.
   *
   * @example
   * CodecReader retainedReader = SoftDeletesRetentionMergePolicy.applyRetentionQuery("deleted", retentionQuery, segmentReader);
   */
  static CodecReader applyRetentionQuery(
      String softDeleteField, Query retentionQuery, CodecReader reader) throws IOException {
    Bits liveDocs = reader.getLiveDocs();
    if (liveDocs == null) { // Segment contains no deletes; return unchanged.
      return reader;
    }
    // Wrap the reader so that only deleted docs appear as 'live', for search by the scorer below.
    CodecReader wrappedReader =
        FilterCodecReader.wrapLiveDocs(
            reader,
            new Bits() { // Only treat deleted docs as visible (live) for the scorer.
              @Override
              public boolean get(int index) {
                return liveDocs.get(index) == false;
              }
              @Override
              public int length() {
                return liveDocs.length();
              }
            },
            reader.maxDoc() - reader.numDocs());
    // Compose a query that requires the soft delete field to exist and match the retention query.
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.add(new FieldExistsQuery(softDeleteField), BooleanClause.Occur.FILTER);
    builder.add(retentionQuery, BooleanClause.Occur.FILTER);
    Scorer scorer = getScorer(builder.build(), wrappedReader);
    if (scorer != null) {
      // Clone the original live docs bitset for mutation.
      FixedBitSet cloneLiveDocs = FixedBitSet.copyOf(liveDocs);
      DocIdSetIterator iterator = scorer.iterator();
      int numExtraLiveDocs = 0;
      // Iterate through matching, previously-dead docs and "resurrect" them.
      while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        if (cloneLiveDocs.getAndSet(iterator.docID()) == false) {
          // Only count newly reinstated docs.
          numExtraLiveDocs++;
        }
      }
      assert reader.numDocs() + numExtraLiveDocs <= reader.maxDoc()
          : "numDocs: "
              + reader.numDocs()
              + " numExtraLiveDocs: "
              + numExtraLiveDocs
              + " maxDoc: "
              + reader.maxDoc();
      return FilterCodecReader.wrapLiveDocs(
          reader, cloneLiveDocs, reader.numDocs() + numExtraLiveDocs);
    } else {
      // No doc was retained; return the reader as-is.
      return reader;
    }
  }

  /**
   * @function getScorer
   * @description
   * Utility to get a scorer for a query against a given segment reader. Disables query caching for precise, uncached reads.
   *
   * @param {Query} query - Query to execute.
   * @param {CodecReader} reader - The segment reader for context.
   * 
   * @returns {Scorer|null} A scorer over matching document IDs, or null if no matches.
   * 
   * @throws {IOException} If query rewriting or scorer creation fails.
   * 
   * @example
   * Scorer s = getScorer(query, codecReader);
   */
  private static Scorer getScorer(Query query, CodecReader reader) throws IOException {
    IndexSearcher s = new IndexSearcher(reader);
    s.setQueryCache(null);
    Weight weight = s.createWeight(s.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
    return weight.scorer(reader.getContext());
  }

  /**
   * @method numDeletesToMerge
   * @description
   * Computes the number of deleted documents to merge from a segment, adjusting for soft deletes that match the retention query.
   * Ensures that documents matching both soft delete and retention are not counted as deleted for merging purposes.
   *
   * @param {SegmentCommitInfo} info - Metadata about the segment.
   * @param {int} delCount - Number of deletes reported by the base policy.
   * @param {IOSupplier<CodecReader>} readerSupplier - Supplies the underlying CodecReader.
   *
   * @returns {int} Adjusted number of deletes for merge logic.
   *
   * @throws {IOException} If reading the segment data fails.
   */
  @Override
  public int numDeletesToMerge(
      SegmentCommitInfo info, int delCount, IOSupplier<CodecReader> readerSupplier)
      throws IOException {
    final int numDeletesToMerge = super.numDeletesToMerge(info, delCount, readerSupplier);
    if (numDeletesToMerge != 0 && info.getSoftDelCount() > 0) {
      final CodecReader reader = readerSupplier.get();
      if (reader.getLiveDocs() != null) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new FieldExistsQuery(field), BooleanClause.Occur.FILTER);
        builder.add(retentionQuerySupplier.get(), BooleanClause.Occur.FILTER);
        Scorer scorer =
            getScorer(
                builder.build(), FilterCodecReader.wrapLiveDocs(reader, null, reader.maxDoc()));
        if (scorer != null) {
          DocIdSetIterator iterator = scorer.iterator();
          Bits liveDocs = reader.getLiveDocs();
          int numDeletedDocs = reader.numDeletedDocs();
          // Adjust count: If a deleted doc matches retention, subtract it from deleted count.
          while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            if (liveDocs.get(iterator.docID()) == false) {
              numDeletedDocs--;
            }
          }
          return numDeletedDocs;
        }
      }
    }
    assert numDeletesToMerge >= 0 : "numDeletesToMerge: " + numDeletesToMerge;
    assert numDeletesToMerge <= info.info.maxDoc()
        : "numDeletesToMerge: " + numDeletesToMerge + " maxDoc:" + info.info.maxDoc();
    return numDeletesToMerge;
  }
}
