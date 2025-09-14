```java
/**
 * @file FilterLeafReader.java
 * @module org.apache.lucene.index
 * 
 * @description
 * This file defines the abstract base class `FilterLeafReader` and its associated filter classes for the Lucene indexing library. 
 * `FilterLeafReader` is designed as a wrapper around another `LeafReader` instance, providing an extensible foundation for modifying, transforming, or enhancing reader behavior at the segment (leaf) level without altering the underlying data source. 
 * This design pattern enables advanced features such as runtime document masking, dynamic field transformation, or custom scoring, all while maintaining compatibility with Lucene's reader interface contracts.
 * 
 * @dependencies
 * - org.apache.lucene.index.LeafReader:  Lucene's lowest-level API for per-segment read access.
 * - org.apache.lucene.index.Fields, Terms, TermsEnum, PostingsEnum: Low-level APIs for field and term enumeration.
 * - org.apache.lucene.util.AttributeSource, Bits, BytesRef, IOBooleanSupplier: Utilities for efficient data manipulation and control.
 * - org.apache.lucene.search.AcceptDocs, KnnCollector: Interfaces for advanced search features and filtering.
 * 
 * @author Apache Lucene Project
 * @version 9.x
 * @license Apache License, Version 2.0
 * @lastmodified 2025-09-14: Documentation overhaul and code clarification for maintainability and extensibility.
 */

package org.apache.lucene.index;

import java.io.IOException;
import java.util.Iterator;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOBooleanSupplier;
import org.apache.lucene.util.Unwrappable;

/**
 * @class FilterLeafReader
 * @classdesc
 * Abstract base class that wraps another `LeafReader`, enabling selective transformation or extension of underlying segment reader behavior.
 * All method calls are delegated to the wrapped reader unless explicitly overridden by subclasses, making it well-suited for composable reader decoration patterns.
 * 
 * @example
 * // Example of extending FilterLeafReader for custom filtering
 * class MaskedLeafReader extends FilterLeafReader {
 *   public MaskedLeafReader(LeafReader in) {
 *     super(in);
 *   }
 *   // Override methods to mask certain documents here
 * }
 * 
 * @prop {LeafReader} in - The wrapped, underlying `LeafReader` instance providing actual data access.
 */
public abstract class FilterLeafReader extends LeafReader {

  /**
   * @function unwrap
   * @description
   * Unwraps recursively through any nested `FilterLeafReader` layers to obtain the innermost delegate `LeafReader`.
   * 
   * @param {LeafReader} reader - The possibly wrapped reader instance.
   * @returns {LeafReader} The base (unwrapped) `LeafReader` instance.
   * 
   * @example
   * LeafReader base = FilterLeafReader.unwrap(compositeReader);
   */
  public static LeafReader unwrap(LeafReader reader) {
    while (reader instanceof FilterLeafReader) {
      reader = ((FilterLeafReader) reader).getDelegate();
    }
    return reader;
  }

  /**
   * @class FilterFields
   * @classdesc
   * Abstract base class for wrapping and filtering `Fields` implementations, which enumerate fields for a segment.
   * 
   * @example
   * class LoggingFields extends FilterLeafReader.FilterFields {
   *   LoggingFields(Fields in) { super(in); }
   *   // Override methods to log field accesses
   * }
   * 
   * @prop {Fields} in - The underlying `Fields` instance to be decorated.
   */
  public abstract static class FilterFields extends Fields {
    /** The underlying Fields instance. */
    protected final Fields in;

    /**
     * @constructor
     * @description Constructs a filtering fields wrapper.
     * @param {Fields} in - The fields delegate to wrap; must not be null.
     * @throws {NullPointerException} If `in` is null.
     */
    protected FilterFields(Fields in) {
      if (in == null) {
        throw new NullPointerException("incoming Fields must not be null");
      }
      this.in = in;
    }

    @Override
    public Iterator<String> iterator() {
      // Delegate iteration to wrapped Fields instance
      return in.iterator();
    }

    @Override
    public Terms terms(String field) throws IOException {
      // Delegate field terms enumeration
      return in.terms(field);
    }

    @Override
    public int size() {
      // Delegate field count
      return in.size();
    }
  }

  /**
   * @class FilterTerms
   * @classdesc
   * Abstract base class for wrapping and filtering `Terms` implementations.
   * Provides delegation point for term-level operations, such as boosting or masking.
   * 
   * @example
   * class LoggingTerms extends FilterLeafReader.FilterTerms {
   *   LoggingTerms(Terms in) { super(in); }
   *   // Override relevant methods for logging or filtering
   * }
   * 
   * @prop {Terms} in - The underlying `Terms` instance being wrapped.
   */
  public abstract static class FilterTerms extends Terms {
    /** The underlying Terms instance. */
    protected final Terms in;

    /**
     * @constructor
     * @description Constructs a terms filter wrapper.
     * @param {Terms} in - The base Terms to filter; must not be null.
     * @throws {NullPointerException} If `in` is null.
     */
    protected FilterTerms(Terms in) {
      if (in == null) {
        throw new NullPointerException("incoming Terms must not be null");
      }
      this.in = in;
    }

    @Override
    public TermsEnum iterator() throws IOException {
      // Delegate term enumeration
      return in.iterator();
    }

    @Override
    public long size() throws IOException {
      return in.size();
    }

    @Override
    public long getSumTotalTermFreq() throws IOException {
      return in.getSumTotalTermFreq();
    }

    @Override
    public long getSumDocFreq() throws IOException {
      return in.getSumDocFreq();
    }

    @Override
    public int getDocCount() throws IOException {
      return in.getDocCount();
    }

    @Override
    public boolean hasFreqs() {
      return in.hasFreqs();
    }

    @Override
    public boolean hasOffsets() {
      return in.hasOffsets();
    }

    @Override
    public boolean hasPositions() {
      return in.hasPositions();
    }

    @Override
    public boolean hasPayloads() {
      return in.hasPayloads();
    }

    @Override
    public Object getStats() throws IOException {
      return in.getStats();
    }
  }

  /**
   * @class FilterTermsEnum
   * @classdesc
   * Abstract base class for filtering `TermsEnum` implementations, which enumerate posting lists for specific terms.
   * 
   * @example
   * class LoggingTermsEnum extends FilterLeafReader.FilterTermsEnum {
   *   LoggingTermsEnum(TermsEnum in) { super(in); }
   *   // Override key methods for monitoring term access
   * }
   * 
   * @prop {TermsEnum} in - The enumerator instance being wrapped.
   */
  public abstract static class FilterTermsEnum extends TermsEnum {

    /** The underlying TermsEnum instance. */
    protected final TermsEnum in;

    /**
     * @constructor
     * @description Initializes a filter enum over another TermsEnum.
     * @param {TermsEnum} in - The underlying enumerator; must not be null.
     * @throws {NullPointerException} If `in` is null.
     */
    protected FilterTermsEnum(TermsEnum in) {
      if (in == null) {
        throw new NullPointerException("incoming TermsEnum must not be null");
      }
      this.in = in;
    }

    @Override
    public AttributeSource attributes() {
      return in.attributes();
    }

    @Override
    public SeekStatus seekCeil(BytesRef text) throws IOException {
      return in.seekCeil(text);
    }

    @Override
    public boolean seekExact(BytesRef text) throws IOException {
      return in.seekExact(text);
    }

    @Override
    public void seekExact(long ord) throws IOException {
      in.seekExact(ord);
    }

    @Override
    public BytesRef next() throws IOException {
      return in.next();
    }

    @Override
    public BytesRef term() throws IOException {
      return in.term();
    }

    @Override
    public long ord() throws IOException {
      return in.ord();
    }

    @Override
    public int docFreq() throws IOException {
      return in.docFreq();
    }

    @Override
    public long totalTermFreq() throws IOException {
      return in.totalTermFreq();
    }

    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
      return in.postings(reuse, flags);
    }

    @Override
    public ImpactsEnum impacts(int flags) throws IOException {
      return in.impacts(flags);
    }

    @Override
    public void seekExact(BytesRef term, TermState state) throws IOException {
      in.seekExact(term, state);
    }

    @Override
    public IOBooleanSupplier prepareSeekExact(BytesRef text) throws IOException {
      return in.prepareSeekExact(text);
    }

    @Override
    public TermState termState() throws IOException {
      return in.termState();
    }
  }

  /**
   * @class FilterPostingsEnum
   * @classdesc
   * Abstract class for filtering and wrapping a `PostingsEnum`, which enumerates the document and position postings for a term.
   * Also implements the `Unwrappable` interface for extraction of the underlying postings enumerator.
   * 
   * @example
   * class MaskedPostingsEnum extends FilterLeafReader.FilterPostingsEnum {
   *   MaskedPostingsEnum(PostingsEnum in) { super(in); }
   *   // Override methods to exclude masked docs
   * }
   * 
   * @prop {PostingsEnum} in - The wrapped postings enumerator.
   */
  public abstract static class FilterPostingsEnum extends PostingsEnum
      implements Unwrappable<PostingsEnum> {
    /** The underlying PostingsEnum instance. */
    protected final PostingsEnum in;

    /**
     * @constructor
     * @description Wraps a postings enumerator for additional filtering or transformation.
     * @param {PostingsEnum} in - The enumerator instance; must not be null.
     * @throws {NullPointerException} If `in` is null.
     */
    protected FilterPostingsEnum(PostingsEnum in) {
      if (in == null) {
        throw new NullPointerException("incoming PostingsEnum must not be null");
      }
      this.in = in;
    }

    @Override
    public int docID() {
      return in.docID();
    }

    @Override
    public int freq() throws IOException {
      return in.freq();
    }

    @Override
    public int nextDoc() throws IOException {
      return in.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
      return in.advance(target);
    }

    @Override
    public int nextPosition() throws IOException {
      return in.nextPosition();
    }

    @Override
    public int startOffset() throws IOException {
      return in.startOffset();
    }

    @Override
    public int endOffset() throws IOException {
      return in.endOffset();
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return in.getPayload();
    }

    @Override
    public long cost() {
      return in.cost();
    }

    /**
     * @function unwrap
     * @description Returns the innermost, non-decorated PostingsEnum.
     * @returns {PostingsEnum} the unwrapped postings enumerator.
     */
    @Override
    public PostingsEnum unwrap() {
      return in;
    }
  }

  /** The underlying LeafReader. */
  protected final LeafReader in;

  /**
   * @constructor
   * @description Initializes a `FilterLeafReader` with a delegate base reader.
   * Note that closing this instance will also close the wrapped reader.
   * 
   * @param {LeafReader} in - The input delegate reader to be wrapped; must not be null.
   * @throws {NullPointerException} If `in` is null.
   */
  protected FilterLeafReader(LeafReader in) {
    super();
    if (in == null) {
      throw new NullPointerException("incoming LeafReader must not be null");
    }
    this.in = in;
  }

  @Override
  public Bits getLiveDocs() {
    ensureOpen();
    return in.getLiveDocs();
  }

  @Override
  public FieldInfos getFieldInfos() {
    return in.getFieldInfos();
  }

  @Override
  public PointValues getPointValues(String field) throws IOException {
    return in.getPointValues(field);
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String field) throws IOException {
    return in.getFloatVectorValues(field);
  }

  @Override
  public ByteVectorValues getByteVectorValues(String field) throws IOException {
    return in.getByteVectorValues(field);
  }

  @Override
  public void searchNearestVectors(
      String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
      throws IOException {
    in.searchNearestVectors(field, target, knnCollector, acceptDocs);
  }

  @Override
  public void searchNearestVectors(
      String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
      throws IOException {
    in.searchNearestVectors(field, target, knnCollector, acceptDocs);
  }

  @Override
  public TermVectors termVectors() throws IOException {
    ensureOpen();
    return in.termVectors();
  }

  @Override
  public int numDocs() {
    // Intentionally omitting ensureOpen() for performance, as doc count is immutable for a segment.
    return in.numDocs();
  }

  @Override
  public int maxDoc() {
    // Intentionally omitting ensureOpen() for performance.
    return in.maxDoc();
  }

  @Override
  public StoredFields storedFields() throws IOException {
    ensureOpen();
    return in.storedFields();
  }

  /**
   * @function doClose
   * @description Closes the wrapped delegate reader; subclasses may extend for additional teardown.
   * @throws {IOException} If an I/O error occurs during underlying resource cleanup.
   */
  @Override
  protected void doClose() throws IOException {
    in.close();
  }

  @Override
  public Terms terms(String field) throws IOException {
    ensureOpen();
    return in.terms(field);
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder("FilterLeafReader(");
    buffer.append(in);
    buffer.append(')');
    return buffer.toString();
  }

  @Override
  public NumericDocValues getNumericDocValues(String field) throws IOException {
    ensureOpen();
    return in.getNumericDocValues(field);
  }

  @Override
  public BinaryDocValues getBinaryDocValues(String field) throws IOException {
    ensureOpen();
    return in.getBinaryDocValues(field);
  }

  @Override
  public SortedDocValues getSortedDocValues(String field) throws IOException {
    ensureOpen();
    return in.getSortedDocValues(field);
  }

  @Override
  public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
    ensureOpen();
    return in.getSortedNumericDocValues(field);
  }

  @Override
  public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
    ensureOpen();
    return in.getSortedSetDocValues(field);
  }

  @Override
  public DocValuesSkipper getDocValuesSkipper(String field) throws IOException {
    ensureOpen();
    return in.getDocValuesSkipper(field);
  }

  @Override
  public NumericDocValues getNormValues(String field) throws IOException {
    ensureOpen();
    return in.getNormValues(field);
  }

  @Override
  public LeafMetaData getMetaData() {
    ensureOpen();
    return in.getMetaData();
  }

  @Override
  public void checkIntegrity() throws IOException {
    ensureOpen();
    in.checkIntegrity();
  }

  /**
   * @function getDelegate
   * @description
   * Returns the wrapped (delegate) `LeafReader` for subclasses needing direct access.
   * 
   * @returns {LeafReader} The underlying delegate reader instance.
   * 
   * @example
   * LeafReader inner = filterLeafReader.getDelegate();
   */
  public LeafReader getDelegate() {
    return in;
  }
}
