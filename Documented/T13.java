```java
/**
 * @file SegmentInfo.java
 * @module org.apache.lucene.index
 * 
 * @description
 * Provides the definition and management of the SegmentInfo class, which encapsulates metadata, file associations, and configuration for a Lucene index segment.
 * This file is integral to the lifecycle of Lucene segments, including creation, diagnostics recording, file tracking, codec association, and metadata retrieval.
 * It enables optimized access, storage, and recovery of vital information about Lucene index segments, supporting both internal management 
 * and plug-in extensibility via code injection, diagnostics, and codec attributes.
 * 
 * @dependencies
 * - org.apache.lucene.store.Directory: Abstracts persistent storage for segments.
 * - org.apache.lucene.search.Sort: Encapsulates index sorting strategy/metadata.
 * - org.apache.lucene.codecs.Codec: Handles codec-specific segment read/write logic.
 * - org.apache.lucene.util.Version: Stores versioning metadata for compatibility and upgrade scenarios.
 * - org.apache.lucene.util.StringHelper: Supports segment ID integrity.
 * - java.util (Collections, Map, Set, Objects): Provides collection and utility support.
 * 
 * @author Apache Software Foundation
 * @version 1.0
 * @license Apache License, Version 2.0
 * @lastmodified 2024-06-12: Enhanced diagnostics aggregation and thread-safety of codec attributes
 */

/**
 * @class SegmentInfo
 * @classdesc
 * Represents metadata and configuration for a Lucene segment, holding segment identity, versioning information, file associations,
 * sort order, custom diagnostics data, codec configuration, and attribute maps.
 *
 * Designed for robust segment tracking, validation, and association with codec infrastructure. Supports both direct query and attribute extension by codecs.
 *
 * @example
 * Directory dir = ...;
 * Version version = ...;
 * Version minVersion = ...;
 * Codec codec = ...;
 * Map<String, String> diagnostics = Map.of("creation", "2024-06-12");
 * byte[] id = ...;
 * Map<String, String> attributes = Map.of("custom", "value");
 * Sort indexSort = ...;
 * SegmentInfo si = new SegmentInfo(dir, version, minVersion, "segment_01", 1000, true, false, codec, diagnostics, id, attributes, indexSort);
 *
 * @prop {String} name - Unique identifier for the segment in its directory.
 * @prop {Directory} dir - Abstraction for persistent storage of segment files.
 * @prop {int} maxDoc - Number of documents contained in the segment (ignoring deletions).
 * @prop {boolean} isCompoundFile - Indicates if this segment is stored in a compound file format.
 * @prop {byte[]} id - Unique segment identifier (typically hash-based).
 * @prop {Codec} codec - Codec instance responsible for reading/writing this segment.
 * @prop {Map<String, String>} diagnostics - Immutable map containing metadata describing how/why this segment was created.
 * @prop {Map<String, String>} attributes - Key/value map for custom codec or system metadata.
 * @prop {Sort} indexSort - Encodes segment sort order or null if unsorted.
 * @prop {Version} version - Lucene version that created this segment.
 * @prop {Version} minVersion - Minimum Lucene version that contributed documents to this segment.
 * @prop {boolean} hasBlocks - Indicates if any blocks were written to segment; cannot be reverted.
 */
public final class SegmentInfo {

  /** Constant to represent an entity as 'not present', typically used for norms or deletions. */
  public static final int NO = -1;

  /** Constant to represent an entity as 'present', applicable to norms or deletion markers. */
  public static final int YES = 1;

  /** Unique segment name within the directory. */
  public final String name;

  /** Number of documents in this segment; set via constructor or setter. */
  private int maxDoc;

  /** Pointer to the directory abstraction for segment files. */
  public final Directory dir;

  /** Whether this segment is stored as a compound file. */
  private boolean isCompoundFile;

  /** Unique identifier for this segment. */
  private final byte[] id;

  /** Codec used for encoding/decoding this segment. */
  private Codec codec;

  /** Immutable map holding diagnostics metadata at time of segment creation. */
  private Map<String, String> diagnostics;

  /** Immutable key/value map for extended attributes, typically for codecs. */
  private Map<String, String> attributes;

  /** Index sort specification, or null if this segment is unsorted. */
  private final Sort indexSort;

  /** Lucene version used to create this segment. See o.a.l.util.Version. */
  private final Version version;

  /** Minimum Lucene version for any contributing document; allows merge tracking. */
  Version minVersion;

  /** Indicates whether blocks (atomic doc groups) have been written. */
  private boolean hasBlocks;

  /**
   * Sets the diagnostics map to the specified immutable copy.
   * Only for internal use during segment info update.
   *
   * @param diagnostics {Map<String, String>} The new diagnostics map (will be copied and wrapped).
   */
  void setDiagnostics(Map<String, String> diagnostics) {
    this.diagnostics = Map.copyOf(Objects.requireNonNull(diagnostics));
  }

  /**
   * @function addDiagnostics
   * @description Adds or updates diagnostics metadata for this segment. Existing keys will be overwritten.
   *
   * Utilizes a copy-on-write pattern to maintain immutability of the diagnostics map, ensuring thread-safety and deterministic segment state.
   *
   * @param {Map<String, String>} diagnostics - Map of diagnostics to add or update. Null keys/values not permitted.
   *
   * @returns {void}
   *
   * @throws {NullPointerException} If the parameter is null.
   *
   * @example
   * Map<String, String> ext = Map.of("merge", "auto");
   * segmentInfo.addDiagnostics(ext);
   */
  public void addDiagnostics(Map<String, String> diagnostics) {
    Objects.requireNonNull(diagnostics);
    Map<String, String> copy = new HashMap<>(this.diagnostics);
    copy.putAll(diagnostics);
    setDiagnostics(copy);
  }

  /**
   * @function getDiagnostics
   * @description Retrieves the immutable diagnostics metadata for this segment.
   *
   * @returns {Map<String, String>} Immutable diagnostics map (may be empty).
   *
   * @example
   * Map<String, String> diag = segmentInfo.getDiagnostics();
   */
  public Map<String, String> getDiagnostics() {
    return diagnostics;
  }

  /**
   * @constructor SegmentInfo
   * @description
   * Constructs a SegmentInfo object with complete specification of directory, naming, versioning, codec, and metadata.
   *
   * Performs ID integrity, null-checks, and verifies codec/attribute/directory requirements. Designed to be called by internal Lucene components and codec packages.
   *
   * @param {Directory} dir - Directory where segment resides; must not be a TrackingDirectoryWrapper.
   * @param {Version} version - Lucene version that originally wrote this segment.
   * @param {Version} minVersion - Minimum version that contributed any document. Can be null for legacy segments.
   * @param {String} name - Unique name for this segment; must not be null.
   * @param {int} maxDoc - Number of documents contained.
   * @param {boolean} isCompoundFile - True if segment is a compound file.
   * @param {boolean} hasBlocks - True if segment contains block-encoded docs.
   * @param {Codec} codec - Codec responsible for persistence format.
   * @param {Map<String, String>} diagnostics - Creation diagnostics, non-null.
   * @param {byte[]} id - Unique segment identifier, must match StringHelper.ID_LENGTH.
   * @param {Map<String, String>} attributes - Extended attribute key-value map, non-null.
   * @param {Sort} indexSort - Sort metadata, or null if unsorted.
   *
   * @throws {IllegalArgumentException} If id length is invalid.
   *
   * @example
   * // See class-level example
   */
  public SegmentInfo(
      Directory dir,
      Version version,
      Version minVersion,
      String name,
      int maxDoc,
      boolean isCompoundFile,
      boolean hasBlocks,
      Codec codec,
      Map<String, String> diagnostics,
      byte[] id,
      Map<String, String> attributes,
      Sort indexSort) {
    assert !(dir instanceof TrackingDirectoryWrapper);
    this.dir = Objects.requireNonNull(dir);
    this.version = Objects.requireNonNull(version);
    this.minVersion = minVersion;
    this.name = Objects.requireNonNull(name);
    this.maxDoc = maxDoc;
    this.isCompoundFile = isCompoundFile;
    this.hasBlocks = hasBlocks;
    this.codec = codec;
    this.diagnostics = Map.copyOf(Objects.requireNonNull(diagnostics));
    this.id = id;
    if (id.length != StringHelper.ID_LENGTH) {
      throw new IllegalArgumentException("invalid id: " + Arrays.toString(id));
    }
    this.attributes = Map.copyOf(Objects.requireNonNull(attributes));
    this.indexSort = indexSort;
  }

  /**
   * @function setUseCompoundFile
   * @description Sets whether this segment uses a compound file for storage.
   * Package-private; not for external API use.
   *
   * @param {boolean} isCompoundFile - True to mark segment as compound file; otherwise false.
   */
  void setUseCompoundFile(boolean isCompoundFile) {
    this.isCompoundFile = isCompoundFile;
  }

  /**
   * @function getUseCompoundFile
   * @description Returns whether this segment utilizes compound file storage.
   *
   * @returns {boolean} True if segment is stored as compound file; otherwise false.
   *
   * @example
   * boolean isCompound = segmentInfo.getUseCompoundFile();
   */
  public boolean getUseCompoundFile() {
    return isCompoundFile;
  }

  /**
   * @function getHasBlocks
   * @description Returns whether this segment contains block-encoded documents.
   *
   * @returns {boolean} True if any blocks present; otherwise false.
   *
   * @see LeafMetaData#hasBlocks()
   */
  public boolean getHasBlocks() {
    return hasBlocks;
  }

  /**
   * @function setHasBlocks
   * @description Marks the segment as containing blocks. This action is irreversible.
   */
  void setHasBlocks() {
    hasBlocks = true;
  }

  /**
   * @function setCodec
   * @description Assigns a codec to this segment. Can only be set once.
   * Ensures the codec cannot be null and cannot overwrite an existing value.
   *
   * @param {Codec} codec - The codec to assign. Must not be null.
   *
   * @throws {IllegalArgumentException} If codec is null.
   * @throws {AssertionError} If codec is already set.
   */
  public void setCodec(Codec codec) {
    assert this.codec == null;
    if (codec == null) {
      throw new IllegalArgumentException("codec must be non-null");
    }
    this.codec = codec;
  }

  /**
   * @function getCodec
   * @description Retrieves the assigned codec responsible for reading/writing this segment.
   *
   * @returns {Codec} The codec associated with this segment.
   */
  public Codec getCodec() {
    return codec;
  }

  /**
   * @function maxDoc
   * @description Returns the document count in this segment, NOT accounting for deletions.
   *
   * @returns {int} The max document count.
   *
   * @throws {IllegalStateException} If maxDoc is unset (-1).
   */
  public int maxDoc() {
    if (this.maxDoc == -1) {
      throw new IllegalStateException("maxDoc isn't set yet");
    }
    return maxDoc;
  }

  /**
   * @function setMaxDoc
   * @description Package-private. Sets the maxDoc property for this segment. Can only be set once.
   *
   * @param {int} maxDoc - The total document count.
   *
   * @throws {IllegalStateException} If maxDoc was already set.
   */
  void setMaxDoc(int maxDoc) {
    if (this.maxDoc != -1) {
      throw new IllegalStateException(
          "maxDoc was already set: this.maxDoc=" + this.maxDoc + " vs maxDoc=" + maxDoc);
    }
    this.maxDoc = maxDoc;
  }

  /** Tracks referenced files in this segment. Mutable only via setFiles/addFiles/addFile. */
  private Set<String> setFiles;

  /**
   * @function files
   * @description Returns all files referenced by this SegmentInfo. The returned Set is immutable.
   *
   * @returns {Set<String>} Immutable set of file names.
   *
   * @throws {IllegalStateException} If files have not yet been set.
   */
  public Set<String> files() {
    if (setFiles == null) {
      throw new IllegalStateException(
          "files were not computed yet; segment=" + name + " maxDoc=" + maxDoc);
    }
    return Collections.unmodifiableSet(setFiles);
  }

  /**
   * @function setFiles
   * @description Sets the collection of files written for this segment. Replaces any existing set.
   *
   * @param {Collection<String>} files - Names of files associated with this segment.
   *
   * @throws {IllegalArgumentException} If filenames are invalid or have forbidden suffixes.
   */
  public void setFiles(Collection<String> files) {
    setFiles = new HashSet<>();
    addFiles(files);
  }

  /**
   * @function addFiles
   * @description Adds a collection of files to the tracked file set, validating names.
   *
   * Performs strict name and extension checks, and ensures segment name scoping.
   *
   * @param {Collection<String>} files - File names to add.
   *
   * @throws {IllegalArgumentException} If file names are invalid.
   */
  public void addFiles(Collection<String> files) {
    checkFileNames(files);
    for (String f : files) {
      setFiles.add(namedForThisSegment(f));
    }
  }

  /**
   * @function addFile
   * @description Adds a single file to the tracked file set for this segment.
   *
   * @param {String} file - File name to add.
   *
   * @throws {IllegalArgumentException} If the file name is invalid.
   */
  public void addFile(String file) {
    checkFileNames(Collections.singleton(file));
    setFiles.add(namedForThisSegment(file));
  }

  /**
   * @function checkFileNames
   * @description Validates each file name in the provided collection. File names must match the codec pattern and must not have ".tmp" extensions.
   *
   * Throws IllegalArgumentException for any violation.
   *
   * @param {Collection<String>} files - Files to validate.
   *
   * @throws {IllegalArgumentException} If a file name is invalid or forbidden.
   */
  private void checkFileNames(Collection<String> files) {
    Matcher m = IndexFileNames.CODEC_FILE_PATTERN.matcher("");
    for (String file : files) {
      m.reset(file);
      if (!m.matches()) {
        throw new IllegalArgumentException(
            "invalid codec filename '"
                + file
                + "', must match: "
                + IndexFileNames.CODEC_FILE_PATTERN.pattern());
      }
      if (file.toLowerCase(Locale.ROOT).endsWith(".tmp")) {
        throw new IllegalArgumentException(
            "invalid codec filename '" + file + "', cannot end with .tmp extension");
      }
    }
  }

  /**
   * @function namedForThisSegment
   * @description Returns the file name with the segment name prefixed, after stripping any other segment association.
   *
   * @param {String} file - The raw file name.
   * @returns {String} The normalized, segment-specific file name.
   */
  String namedForThisSegment(String file) {
    return name + IndexFileNames.stripSegmentName(file);
  }

  /**
   * @function getAttribute
   * @description Retrieves a codec attribute value as recorded in this segment.
   *
   * Uses synchronization for thread-safety, supporting concurrent reader and writer access.
   *
   * @param {String} key - Attribute key to retrieve.
   * @returns {String} Value associated with key, or null if none present.
   *
   * @example
   * String value = segmentInfo.getAttribute("checksum");
   */
  public synchronized String getAttribute(String key) {
    return attributes.get(key);
  }

  /**
   * @function putAttribute
   * @description Thread-safe insertion or update of a codec attribute.
   *
   * Creates an unmodifiable copy for concurrent access safety. Returns any replaced value.
   *
   * @param {String} key - The attribute key to update or insert.
   * @param {String} value - The attribute value to store.
   *
   * @returns {String} Previous value associated with key, or null if new.
   *
   * @example
   * segmentInfo.putAttribute("key", "value");
   */
  public synchronized String putAttribute(String key, String value) {
    HashMap<String, String> newMap = new HashMap<>(attributes);
    String oldValue = newMap.put(key, value);
    attributes = Collections.unmodifiableMap(newMap);
    return oldValue;
  }

  /**
   * @function getAttributes
   * @description Provides access to the internal codec attribute map.
   *
   * Synchronized for thread-safety.
   *
   * @returns {Map<String, String>} Current unmodifiable attributes map.
   *
   * @example
   * Map<String, String> attrs = segmentInfo.getAttributes();
   */
  public synchronized Map<String, String> getAttributes() {
    return attributes;
  }

  /**
   * @function getIndexSort
   * @description Returns the sort order metadata for this segment, or null if segment is unsorted.
   *
   * @returns {Sort} The index sort, or null.
   */
  public Sort getIndexSort() {
    return indexSort;
  }

  /**
   * @function getVersion
   * @description Retrieves the Lucene version that originally wrote this segment.
   *
   * @returns {Version} Lucene version.
   */
  public Version getVersion() {
    return version;
  }

  /**
   * @function getMinVersion
   * @description Returns the minimum Lucene version that contributed documents to this segment.
   * Null if unknown.
   * 
   * @returns {Version} Minimum Lucene version or null.
   */
  public Version getMinVersion() {
    return minVersion;
  }

  /**
   * @function getId
   * @description Returns a clone of the unique identifier for this segment.
   *
   * @returns {byte[]} Copy of the segment ID.
   */
  public byte[] getId() {
    return id.clone();
  }

  /**
   * @function toString
   * @description Returns a human-readable string representation of this segment, including version, file format, doc counts, and optionally sort and diagnostic information.
   * 
   * Note: Format is for debugging only and is subject to change.
   *
   * @returns {String} String representation of segment state.
   */
  @Override
  public String toString() {
    return toString(0);
  }

  /**
   * @function toString
   * @description Provides a formatted display of segment state, including deletion count, used for debugging or logging purposes.
   *
   * @param {int} delCount - Number of document deletions (display only).
   * @returns {String} String summary with keys and values of various properties.
   */
  public String toString(int delCount) {
    StringBuilder s = new StringBuilder();
    s.append(name).append('(').append(version == null ? "?" : version).append(')').append(':');
    char cfs = getUseCompoundFile() ? 'c' : 'C';
    s.append(cfs);

    s.append(maxDoc);

    if (delCount != 0) {
      s.append('/').append(delCount);
    }

    if (indexSort != null) {
      s.append(":[indexSort=");
      s.append(indexSort);
      s.append(']');
    }

    if (!diagnostics.isEmpty()) {
      s.append(":[diagnostics=");
      s.append(diagnostics.toString());
      s.append(']');
    }

    Map<String, String> attributes = getAttributes();
    if (!attributes.isEmpty()) {
      s.append(":[attributes=");
      s.append(attributes.toString());
      s.append(']');
    }

    return s.toString();
  }

  /**
   * @function equals
   * @description Compares this SegmentInfo for equality. Two instances are equal if they have the same directory and name.
   *
   * @param {Object} obj - Object to compare.
   * @returns {boolean} True if equal; otherwise false.
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj instanceof SegmentInfo other) {
      return other.dir == dir && other.name.equals(name);
    } else {
      return false;
    }
  }

  /**
   * @function hashCode
   * @description Computes a hash code based on directory and segment name for use in collections.
   *
   * @returns {int} Hash code.
   */
  @Override
  public int hashCode() {
    return dir.hashCode() + name.hashCode();
  }
}
