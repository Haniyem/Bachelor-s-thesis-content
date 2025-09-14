```java
/**
 * @file Provides high-performance, memory-segment backed vector scoring suppliers for the Lucene 9.9+ ANN (Approximate Nearest Neighbor) framework.
 * @module org.apache.lucene.internal.vectorization
 *
 * @description
 * This file defines and implements a set of specialized suppliers for random vector scorers using Java's MemorySegment API.
 * Each supplier supports a specific vector similarity function (Cosine, Dot Product, Euclidean, Maximum Inner Product) with efficient, bulk-compatible operations
 * over memory segments rather than classical arrays, facilitating vectorized computations required in ANN indexing and search.
 * Used internally within Lucene for high-performance similarity computations on vector fields, this module aims to maximize throughput, minimize GC pressure, and
 * leverage foreign memory APIs for data-intensive operations tied to Lucene's HNSW (Hierarchical Navigable Small World) graph-based KNN search.
 *
 * @dependencies
 * - org.apache.lucene.index.FloatVectorValues: Abstraction for accessing vector values by ordinal.
 * - org.apache.lucene.index.VectorSimilarityFunction: Enum specifying supported similarity functions.
 * - org.apache.lucene.store.FilterIndexInput, MemorySegmentAccessInput: For foreign memory interaction and index abstraction.
 * - org.apache.lucene.util.VectorUtil: Utilities for normalizing vector scores/distances.
 * - org.apache.lucene.util.hnsw.RandomVectorScorerSupplier, UpdateableRandomVectorScorer: Abstract scorer contracts for random-access vector scoring.
 * - MemorySegmentBulkVectorOps: Provides low-level, SIMD-exploiting vector operations on memory segments.
 *
 * @author Apache Software Foundation contributors
 * @version 9.9+
 * @license Apache License 2.0
 * @lastmodified 2025-09-14: Documented all internal classes and scoring logic for maintainability.
 */

package org.apache.lucene.internal.vectorization;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Optional;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;

/**
 * @class Lucene99MemorySegmentFloatVectorScorerSupplier
 * @classdesc Abstract supplier for random vector scorers leveraging memory segments for float vector storage and computation.
 *            Serves as a base for vector similarity implementations, supporting various scoring backends with direct memory access.
 *
 * @example
 * Optional<RandomVectorScorerSupplier> supplierOpt = Lucene99MemorySegmentFloatVectorScorerSupplier.create(
 *     VectorSimilarityFunction.COSINE, idxInput, vectorValues);
 * RandomVectorScorer scorer = supplierOpt.get().scorer();
 * float score = scorer.score(nodeOrdinal);
 *
 * @prop {int} vectorByteSize - Size in bytes of a single vector element.
 * @prop {int} maxOrd - Number of vectors represented.
 * @prop {int} dims - Dimensionality of vectors.
 * @prop {MemorySegment} seg - Backing memory segment, referencing vector storage.
 * @prop {FloatVectorValues} values - Reference to vector value accessor.
 */
public abstract sealed class Lucene99MemorySegmentFloatVectorScorerSupplier
    implements RandomVectorScorerSupplier {

  final int vectorByteSize;
  final int maxOrd;
  final int dims;
  final MemorySegment seg;
  final FloatVectorValues values;

  /**
   * @function create
   * @description Factory method that creates a scorer supplier for the specified similarity function,
   *              if the IndexInput supports memory-segment backed access.
   *
   * @param {VectorSimilarityFunction} type - The similarity function to be used (COSINE, DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT).
   * @param {IndexInput} input - The input source for backing vectors, expected to support MemorySegment access.
   * @param {FloatVectorValues} values - Abstraction for random-access vector content.
   *
   * @returns {Optional<RandomVectorScorerSupplier>} The matching scorer supplier, or Optional.empty() if incompatible.
   *
   * @throws {IllegalArgumentException} If the input's length is inconsistent with the expected vector data size.
   *
   * @example
   * Optional<RandomVectorScorerSupplier> s = Lucene99MemorySegmentFloatVectorScorerSupplier.create(
   *     VectorSimilarityFunction.COSINE, indexInput, fvValues);
   * if (s.isPresent()) { ... }
   */
  static Optional<RandomVectorScorerSupplier> create(
      VectorSimilarityFunction type, IndexInput input, FloatVectorValues values)
      throws IOException {
    // Unwrap for testability if under a test harness
    input = FilterIndexInput.unwrapOnlyTest(input);
    MemorySegment seg;
    // Only proceed if IndexInput supports memory segment slices; otherwise return empty
    if (!(input instanceof MemorySegmentAccessInput msInput
        && (seg = msInput.segmentSliceOrNull(0L, msInput.length())) != null)) {
      return Optional.empty();
    }
    checkInvariants(values.size(), values.getVectorByteLength(), input);
    // Return specialized scorer supplier for the selected similarity function
    return switch (type) {
      case COSINE -> Optional.of(new CosineSupplier(seg, values));
      case DOT_PRODUCT -> Optional.of(new DotProductSupplier(seg, values));
      case EUCLIDEAN -> Optional.of(new EuclideanSupplier(seg, values));
      case MAXIMUM_INNER_PRODUCT -> Optional.of(new MaxInnerProductSupplier(seg, values));
    };
  }

  /**
   * @function constructor
   * @description Initializes a new supplier with a memory segment and backing vector values.
   *
   * @param {MemorySegment} seg - Reference to vector data.
   * @param {FloatVectorValues} values - Vector access abstraction.
   */
  Lucene99MemorySegmentFloatVectorScorerSupplier(MemorySegment seg, FloatVectorValues values) {
    this.seg = seg;
    this.values = values;
    this.vectorByteSize = values.getVectorByteLength();
    this.maxOrd = values.size();
    this.dims = values.dimension();
  }

  /**
   * @function checkInvariants
   * @description Validates that the IndexInput contains enough data for all expected vectors.
   *
   * @param {int} maxOrd - Expected number of vectors.
   * @param {int} vectorByteLength - Number of bytes per vector.
   * @param {IndexInput} input - Input source for vectors.
   *
   * @throws {IllegalArgumentException} If the input is too short for the declared vectors.
   */
  static void checkInvariants(int maxOrd, int vectorByteLength, IndexInput input) {
    if (input.length() < (long) vectorByteLength * maxOrd) {
      throw new IllegalArgumentException("input length is less than expected vector data");
    }
  }

  /**
   * @class CosineSupplier
   * @classdesc Scorer supplier specializing in cosine similarity, operating on vectors via memory segments for optimal bulk and single-score operations.
   *
   * @example
   * RandomVectorScorer s = new CosineSupplier(seg, values).scorer();
   * float sim = s.score(nodeOrdinal);
   */
  static final class CosineSupplier extends Lucene99MemorySegmentFloatVectorScorerSupplier {

    static final MemorySegmentBulkVectorOps.Cosine COS_OPS =
        MemorySegmentBulkVectorOps.COS_INSTANCE;

    /**
     * @function constructor
     * @description Initializes the cosine similarity supplier.
     *
     * @param {MemorySegment} seg - Reference to vector data.
     * @param {FloatVectorValues} values - Vector value accessor.
     */
    CosineSupplier(MemorySegment seg, FloatVectorValues values) {
      super(seg, values);
    }

    /**
     * @function scorer
     * @description Returns an updatable scorer that computes cosine similarity between query and candidate vectors.
     *
     * @returns {UpdateableRandomVectorScorer} Scorer instance.
     */
    @Override
    public UpdateableRandomVectorScorer scorer() {
      return new AbstractBulkScorer(values) {
        /**
         * @function vectorOp
         * @description Computes cosine similarity between query and data vector segments.
         *
         * @param {MemorySegment} seg - Data segment.
         * @param {long} q - Offset of query vector.
         * @param {long} d - Offset of data vector.
         * @param {int} elementCount - Dimensionality of vectors.
         *
         * @returns {float} Cosine similarity score.
         */
        @Override
        float vectorOp(MemorySegment seg, long q, long d, int elementCount) {
          return COS_OPS.cosine(seg, q, d, dims);
        }

        /**
         * @function vectorOp (bulk)
         * @description Populates bulk scores for four candidate nodes using SIMD.
         */
        @Override
        void vectorOp(
            MemorySegment seg,
            float[] scores,
            long queryOffset,
            long node1Offset,
            long node2Offset,
            long node3Offset,
            long node4Offset,
            int elementCount) {
          COS_OPS.cosineBulk(
              seg, scores, queryOffset, node1Offset, node2Offset, node3Offset, node4Offset, dims);
        }

        /**
         * @function normalizeRawScore
         * @description Normalizes cosine scores to a unit interval.
         * @param {float} rawScore - Raw similarity value.
         * @returns {float} Normalized score.
         */
        @Override
        float normalizeRawScore(float rawScore) {
          return VectorUtil.normalizeToUnitInterval(rawScore);
        }
      };
    }

    /**
     * @function copy
     * @description Creates a copy of the supplier (deep-copying vector values).
     * @returns {CosineSupplier} New instance.
     */
    @Override
    public CosineSupplier copy() throws IOException {
      return new CosineSupplier(seg, values.copy()); // Defensive copy; ensures thread safety for vector values.
    }
  }

  /**
   * @class DotProductSupplier
   * @classdesc Scorer supplier for dot product similarity, supporting both single and bulk scoring on memory segment vectors.
   *
   * @example
   * RandomVectorScorer s = new DotProductSupplier(seg, values).scorer();
   * float sim = s.score(nodeOrdinal);
   */
  static final class DotProductSupplier extends Lucene99MemorySegmentFloatVectorScorerSupplier {

    static final MemorySegmentBulkVectorOps.DotProduct DOT_OPS =
        MemorySegmentBulkVectorOps.DOT_INSTANCE;

    /**
     * @function constructor
     * @description Initializes the dot product similarity supplier.
     *
     * @param {MemorySegment} seg - Data segment.
     * @param {FloatVectorValues} values - Vector accessor.
     */
    DotProductSupplier(MemorySegment seg, FloatVectorValues values) {
      super(seg, values);
    }

    /**
     * @function scorer
     * @description Returns a scorer for dot product similarity.
     * @returns {UpdateableRandomVectorScorer} Instance.
     */
    @Override
    public UpdateableRandomVectorScorer scorer() {
      return new AbstractBulkScorer(values) {
        @Override
        float vectorOp(MemorySegment seg, long q, long d, int elementCount) {
          return DOT_OPS.dotProduct(seg, q, d, dims);
        }

        @Override
        void vectorOp(
            MemorySegment seg,
            float[] scores,
            long queryOffset,
            long node1Offset,
            long node2Offset,
            long node3Offset,
            long node4Offset,
            int elementCount) {
          DOT_OPS.dotProductBulk(
              seg, scores, queryOffset, node1Offset, node2Offset, node3Offset, node4Offset, dims);
        }

        @Override
        float normalizeRawScore(float rawScore) {
          return VectorUtil.normalizeToUnitInterval(rawScore);
        }
      };
    }

    /**
     * @function copy
     * @description Returns a shallow copy of the supplier instance.
     * @returns {DotProductSupplier}
     */
    @Override
    public DotProductSupplier copy() throws IOException {
      return new DotProductSupplier(seg, values);
    }
  }

  /**
   * @class EuclideanSupplier
   * @classdesc Scorer supplier for squared Euclidean distance, with normalization and SIMD-optimized bulk computations.
   *
   * @example
   * RandomVectorScorer s = new EuclideanSupplier(seg, values).scorer();
   * float dist = s.score(nodeOrdinal);
   */
  static final class EuclideanSupplier extends Lucene99MemorySegmentFloatVectorScorerSupplier {

    static final MemorySegmentBulkVectorOps.SqrDistance SQR_OPS =
        MemorySegmentBulkVectorOps.SQR_INSTANCE;

    /**
     * @function constructor
     * @description Initializes the squared Euclidean distance supplier.
     *
     * @param {MemorySegment} seg - Data segment.
     * @param {FloatVectorValues} values - Vector accessor.
     */
    EuclideanSupplier(MemorySegment seg, FloatVectorValues values) {
      super(seg, values);
    }

    /**
     * @function scorer
     * @description Returns a scorer for squared Euclidean distance with normalization.
     * @returns {UpdateableRandomVectorScorer}
     */
    @Override
    public UpdateableRandomVectorScorer scorer() {
      return new AbstractBulkScorer(values) {
        @Override
        float vectorOp(MemorySegment seg, long q, long d, int elementCount) {
          return SQR_OPS.sqrDistance(seg, q, d, dims);
        }

        @Override
        void vectorOp(
            MemorySegment seg,
            float[] scores,
            long queryOffset,
            long node1Offset,
            long node2Offset,
            long node3Offset,
            long node4Offset,
            int elementCount) {
          SQR_OPS.sqrDistanceBulk(
              seg, scores, queryOffset, node1Offset, node2Offset, node3Offset, node4Offset, dims);
        }

        @Override
        float normalizeRawScore(float rawScore) {
          return VectorUtil.normalizeDistanceToUnitInterval(rawScore);
        }
      };
    }

    /**
     * @function copy
     * @description Returns a shallow copy of the supplier instance. (TODO: Consider deep copy for safety.)
     * @returns {EuclideanSupplier}
     */
    @Override
    public EuclideanSupplier copy() throws IOException {
      return new EuclideanSupplier(seg, values); // Potential issue: check if vector values need deep copy.
    }
  }

  /**
   * @class MaxInnerProductSupplier
   * @classdesc Scorer supplier for maximum inner product similarity (MIPS), directly reusing dot product implementation and normalizing as per MIPS protocol.
   *
   * @example
   * RandomVectorScorer s = new MaxInnerProductSupplier(seg, values).scorer();
   * float score = s.score(nodeOrdinal);
   */
  static final class MaxInnerProductSupplier
      extends Lucene99MemorySegmentFloatVectorScorerSupplier {

    static final MemorySegmentBulkVectorOps.DotProduct DOT_OPS =
        MemorySegmentBulkVectorOps.DOT_INSTANCE;

    /**
     * @function constructor
     * @description Initializes the max inner product similarity supplier.
     *
     * @param {MemorySegment} seg - Memory segment reference.
     * @param {FloatVectorValues} values - Vector values accessor.
     */
    MaxInnerProductSupplier(MemorySegment seg, FloatVectorValues values) {
      super(seg, values);
    }

    /**
     * @function scorer
     * @description Returns a scorer that computes maximum inner products and normalizes the score for ranking.
     * @returns {UpdateableRandomVectorScorer}
     */
    @Override
    public UpdateableRandomVectorScorer scorer() {
      return new AbstractBulkScorer(values) {
        @Override
        float vectorOp(MemorySegment seg, long q, long d, int elementCount) {
          return DOT_OPS.dotProduct(seg, q, d, dims);
        }

        @Override
        void vectorOp(
            MemorySegment seg,
            float[] scores,
            long queryOffset,
            long node1Offset,
            long node2Offset,
            long node3Offset,
            long node4Offset,
            int elementCount) {
          DOT_OPS.dotProductBulk(
              seg, scores, queryOffset, node1Offset, node2Offset, node3Offset, node4Offset, dims);
        }

        @Override
        float normalizeRawScore(float rawScore) {
          return VectorUtil.scaleMaxInnerProductScore(rawScore);
        }
      };
    }

    /**
     * @function copy
     * @description Returns a shallow copy. Note that deep copy may be required if vector values are mutable.
     * @returns {MaxInnerProductSupplier}
     */
    @Override
    public MaxInnerProductSupplier copy() throws IOException {
      return new MaxInnerProductSupplier(seg, values);
    }
  }

  /**
   * @class AbstractBulkScorer
   * @classdesc Abstract scorer implementing UpdateableRandomVectorScorer logic for both single and bulk vector scoring with memory segment offsets. 
   *            Handles ordinal checks and score normalization for subtypes.
   *
   * @example
   * AbstractBulkScorer scorer = ...;
   * scorer.setScoringOrdinal(queryNode);
   * float score = scorer.score(candidateNode);
   * int[] nodes = ...;
   * float[] scores = new float[nodes.length];
   * scorer.bulkScore(nodes, scores, nodes.length);
   *
   * @prop {int} queryOrd - Ordinal index of the current query.
   * @prop {float[]} scratchScores - Temporary array for four-way SIMD bulk vector results.
   */
  abstract class AbstractBulkScorer
      extends UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer {
    private int queryOrd;
    final float[] scratchScores = new float[4];

    /**
     * @function constructor
     * @description Initializes scorer with reference to FloatVectorValues.
     *
     * @param {FloatVectorValues} values - Vector value accessor.
     */
    AbstractBulkScorer(FloatVectorValues values) {
      super(values);
    }

    /**
     * @function checkOrdinal
     * @description Validates that the provided ordinal is within allowed vector range.
     *
     * @param {int} ord - Ordinal.
     * @throws {IllegalArgumentException} For out-of-bounds.
     */
    final void checkOrdinal(int ord) {
      if (ord < 0 || ord >= maxOrd) {
        throw new IllegalArgumentException("illegal ordinal: " + ord);
      }
    }

    /**
     * @function vectorOp
     * @description Abstract method for per-vector, memory segment-based similarity/distance calculations.
     *
     * @param {MemorySegment} seg - Memory segment.
     * @param {long} q - Query vector offset.
     * @param {long} d - Candidate node offset.
     * @param {int} elementCount - Number of dimensions.
     * @returns {float} Raw similarity/distance score.
     */
    abstract float vectorOp(MemorySegment seg, long q, long d, int elementCount);

    /**
     * @function vectorOp (bulk)
     * @description Abstract method for bulk population of up to four scores leveraging SIMD-style operations.
     */
    abstract void vectorOp(
        MemorySegment seg,
        float[] scores,
        long queryOffset,
        long node1Offset,
        long node2Offset,
        long node3Offset,
        long node4Offset,
        int elementCount);

    /**
     * @function normalizeRawScore
     * @description Abstract normalization function to convert raw scores to [0,1].
     * @param {float} rawScore
     * @returns {float}
     */
    abstract float normalizeRawScore(float rawScore);

    /**
     * @function score
     * @description Computes and normalizes the similarity/distance score between the stored query vector and a candidate node.
     *
     * @param {int} node - Ordinal of candidate node.
     * @returns {float} Normalized score.
     * @throws {IllegalArgumentException} For invalid ordinals.
     */
    @Override
    public float score(int node) {
      checkOrdinal(node);
      long queryAddr = (long) queryOrd * vectorByteSize;
      long addr = (long) node * vectorByteSize;
      var raw = vectorOp(seg, queryAddr, addr, dims);
      return normalizeRawScore(raw);
    }

    /**
     * @function bulkScore
     * @description Efficiently computes normalized similarity/distance for up to four nodes at a time.
     *
     * @param {int[]} nodes - Array of candidate node ordinals.
     * @param {float[]} scores - Output buffer for scores.
     * @param {int} numNodes - Number of candidates.
     */
    @Override
    public void bulkScore(int[] nodes, float[] scores, int numNodes) {
      int i = 0;
      long queryAddr = (long) queryOrd * vectorByteSize;
      final int limit = numNodes & ~3; // SIMD block processing
      for (; i < limit; i += 4) {
        long offset1 = (long) nodes[i] * vectorByteSize;
        long offset2 = (long) nodes[i + 1] * vectorByteSize;
        long offset3 = (long) nodes[i + 2] * vectorByteSize;
        long offset4 = (long) nodes[i + 3] * vectorByteSize;
        // Perform SIMD-style scoring
        vectorOp(seg, scratchScores, queryAddr, offset1, offset2, offset3, offset4, dims);
        scores[i + 0] = normalizeRawScore(scratchScores[0]);
        scores[i + 1] = normalizeRawScore(scratchScores[1]);
        scores[i + 2] = normalizeRawScore(scratchScores[2]);
        scores[i + 3] = normalizeRawScore(scratchScores[3]);
      }

      int remaining = numNodes - i;
      if (remaining > 0) {
        long addr1 = (long) nodes[i] * vectorByteSize;
        long addr2 = (remaining > 1) ? (long) nodes[i + 1] * vectorByteSize : addr1;
        long addr3 = (remaining > 2) ? (long) nodes[i + 2] * vectorByteSize : addr1;
        // Process last 1-3 leftovers using re-used slots in scratchScores
        vectorOp(seg, scratchScores, queryAddr, addr1, addr2, addr3, addr1, dims);
        scores[i] = normalizeRawScore(scratchScores[0]);
        if (remaining > 1) scores[i + 1] = normalizeRawScore(scratchScores[1]);
        if (remaining > 2) scores[i + 2] = normalizeRawScore(scratchScores[2]);
      }
    }

    /**
     * @function setScoringOrdinal
     * @description Sets the current query vector ordinal for subsequent score computations.
     *
     * @param {int} node - Query vector index.
     * @throws {IllegalArgumentException} For invalid input.
     */
    @Override
    public void setScoringOrdinal(int node) {
      checkOrdinal(node);
      queryOrd = node;
    }
  }
}


