```java
/**
 * @file StandardDirectoryReader.java
 * @module org.apache.lucene.index
 * 
 * @description
 * Provides an implementation of a Lucene DirectoryReader that opens and manages low-level access to segments in an index. The core responsibility of this file is to construct, open, and manage lifecycle operations (such as closing, refreshing, and versioning) on segment readers for a collection of index segments, often in coordination with an IndexWriter for near-real-time searching. The implementation handles intricate details such as segment deletion visibility, compound/non-compound segment awareness, and reader cache management. This file is a fundamental building block for Lucene's index reading mechanism, enabling efficient and consistent reading as files on disk evolve.
 * 
 * @dependencies
 * - org.apache.lucene.store.Directory: Abstracts physical index storage and file manipulation.
 * - org.apache.lucene.store.IOContext: Manages the I/O context for read/write actions with performance hints.
 * - org.apache.lucene.util.Bits: Provides a generic interface for bitmap-like objects, such as live docs.
 * - org.apache.lucene.util.IOFunction: Functional interface for IO operations.
 * - org.apache.lucene.index.IndexWriter: Coordinates edits and visibility into the index for NRT search.
 * - java.util.concurrent.CopyOnWriteArraySet: Thread-safe set implementation used for listener management.
 * - org.apache.lucene.util.Version: Manages Lucene version compatibility and code branching.
 * 
 * @author Apache Software Foundation
 * @version 9.0.0 (Example version; update as needed)
 * @license Apache License 2.0
 * @lastmodified 2023-12-01: Refined NRT (near-real-time) support and cache notification logic.
 */

/**
 * @class StandardDirectoryReader
 * @classdesc 
 * Implements a DirectoryReader backed by a set of SegmentReader instances, managing their opening, reuse, reference counting, and lifecycle, including in the presence of an IndexWriter for near-real-time search.
 * 
 * @example
 * Directory directory = FSDirectory.open(Paths.get("/index"));
 * DirectoryReader reader = StandardDirectoryReader.open(directory, null, null);
 * // Use reader to query index data, then close when done
 * reader.close();
 * 
 * @prop {IndexWriter} writer - If non-null, the associated IndexWriter for NRT reader invalidation/refresh logic.
 * @prop {SegmentInfos} segmentInfos - Metadata about the segments this reader is covering.
 * @prop {boolean} applyAllDeletes - Determines whether all deletions should be applied when opening a reader.
 * @prop {boolean} writeAllDeletes - Determines if deletions should be written back on closing.
 */

public final class StandardDirectoryReader extends DirectoryReader {

  final IndexWriter writer;
  final SegmentInfos segmentInfos;
  private final boolean applyAllDeletes;
  private final boolean writeAllDeletes;

  /**
   * @constructor
   * @description Instantiates a StandardDirectoryReader with the provided segment readers and context. Used internally by static open methods.
   * 
   * @param {Directory} directory - The directory holding the index files.
   * @param {LeafReader[]} readers - Array of segment readers representing the index state.
   * @param {IndexWriter} writer - (Optional) Associated writer for NRT support; null if read-only.
   * @param {SegmentInfos} sis - Segment metadata describing all index segments.
   * @param {Comparator<LeafReader>} leafSorter - Optional comparator for segment sorting.
   * @param {boolean} applyAllDeletes - Whether to apply all deletions at open time.
   * @param {boolean} writeAllDeletes - Whether to persist deletions on close.
   * 
   * @throws {IOException} If the Directory cannot be read or a segment fails to open.
   */
  StandardDirectoryReader(
      Directory directory,
      LeafReader[] readers,
      IndexWriter writer,
      SegmentInfos sis,
      Comparator<LeafReader> leafSorter,
      boolean applyAllDeletes,
      boolean writeAllDeletes)
      throws IOException {
    super(directory, readers, leafSorter);
    this.writer = writer;
    this.segmentInfos = sis;
    this.applyAllDeletes = applyAllDeletes;
    this.writeAllDeletes = writeAllDeletes;
  }
  
  /**
   * @function open
   * @description Opens a new DirectoryReader for the provided directory and optional commit, reading segment listings and instantiating segment readers accordingly.
   * 
   * @param {Directory} directory - The directory containing the index.
   * @param {IndexCommit} commit - The commit point to open, or null for latest.
   * @param {Comparator<LeafReader>} leafSorter - Optional comparator for segment ordering.
   * @returns {DirectoryReader} A reader representing the index state at the specified commit.
   * @throws {IOException} If reading the segment files or instantiating SegmentReaders fails.
   * 
   * @example
   * DirectoryReader reader = StandardDirectoryReader.open(directory, null, null);
   */
  static DirectoryReader open(
      final Directory directory, final IndexCommit commit, Comparator<LeafReader> leafSorter)
      throws IOException {
    return open(directory, Version.MIN_SUPPORTED_MAJOR, commit, leafSorter);
  }

  /**
   * @function open (overloaded)
   * @description Opens a DirectoryReader with explicit minimum supported major version. Handles segment version validation and compatibility.
   * 
   * @param {Directory} directory - The directory containing the index.
   * @param {int} minSupportedMajorVersion - Minimum supported segment version.
   * @param {IndexCommit} commit - The commit point or null for latest.
   * @param {Comparator<LeafReader>} leafSorter - Comparator for leaves.
   * @returns {DirectoryReader} The new DirectoryReader instance.
   * @throws {IOException} If the minSupportedMajorVersion is invalid or reading fails.
   */
  static DirectoryReader open(
      final Directory directory,
      int minSupportedMajorVersion,
      final IndexCommit commit,
      Comparator<LeafReader> leafSorter)
      throws IOException {
    // Anonymous class used to encapsulate segment read logic and fallback
    return new SegmentInfos.FindSegmentsFile<DirectoryReader>(directory) {
      @Override
      protected DirectoryReader doBody(String segmentFileName) throws IOException {
        if (minSupportedMajorVersion > Version.LATEST.major || minSupportedMajorVersion < 0) {
          throw new IllegalArgumentException(
              "minSupportedMajorVersion must be positive and <= "
                  + Version.LATEST.major
                  + " but was: "
                  + minSupportedMajorVersion);
        }
        SegmentInfos sis =
            SegmentInfos.readCommit(directory, segmentFileName, minSupportedMajorVersion);

        final SegmentReader[] readers = new SegmentReader[sis.size()];
        try {
          // Iterate backwards for optimal reuse/cache locality
          for (int i = sis.size() - 1; i >= 0; i--) {
            readers[i] =
                new SegmentReader(
                    sis.info(i), sis.getIndexCreatedVersionMajor(), IOContext.DEFAULT);
          }
          // Might throw if too many docs; proper resource cleanup below
          return new StandardDirectoryReader(
              directory, readers, null, sis, leafSorter, false, false);
        } catch (Throwable t) {
          IOUtils.closeWhileSuppressingExceptions(t, readers);
          throw t;
        }
      }
    }.run(commit);
  }

  /**
   * @function open (writer-backed)
   * @description Constructs a StandardDirectoryReader using an IndexWriter, allowing for near-real-time readers that can observe but not commit unflushed changes.
   * 
   * @param {IndexWriter} writer - Associated writer.
   * @param {IOFunction<SegmentCommitInfo, SegmentReader>} readerFunction - Factory for SegmentReaders.
   * @param {SegmentInfos} infos - The segment information metadata.
   * @param {boolean} applyAllDeletes - Whether to reflect all deletions.
   * @param {boolean} writeAllDeletes - Whether to persist deletes to disk.
   * 
   * @returns {StandardDirectoryReader} The newly constructed reader covering the writer's segments.
   * 
   * @throws {IOException} If any segment cannot be opened.
   */
  static StandardDirectoryReader open(
      IndexWriter writer,
      IOFunction<SegmentCommitInfo, SegmentReader> readerFunction,
      SegmentInfos infos,
      boolean applyAllDeletes,
      boolean writeAllDeletes)
      throws IOException {

    final int numSegments = infos.size();
    final List<SegmentReader> readers = new ArrayList<>(numSegments);
    final Directory dir = writer.getDirectory();
    final SegmentInfos segmentInfos = infos.clone();
    int infosUpto = 0;
    try {
      for (int i = 0; i < numSegments; i++) {
        final SegmentCommitInfo info = infos.info(i);
        assert info.info.dir == dir;
        final SegmentReader reader = readerFunction.apply(info);
        // Only retain readers that are not fully deleted
        if (reader.numDocs() > 0
            || writer.getConfig().mergePolicy.keepFullyDeletedSegment(() -> reader)) {
          readers.add(reader);
          infosUpto++;
        } else {
          reader.decRef();
          segmentInfos.remove(infosUpto);
        }
      }
      writer.incRefDeleter(segmentInfos);

      StandardDirectoryReader result =
          new StandardDirectoryReader(
              dir,
              readers.toArray(new SegmentReader[readers.size()]),
              writer,
              segmentInfos,
              writer.getConfig().getLeafSorter(),
              applyAllDeletes,
              writeAllDeletes);
      return result;
    } catch (Throwable t) {
      // Clean-up already-opened readers if error occurs
      try {
        IOUtils.applyToAll(readers, SegmentReader::decRef);
      } catch (Throwable t1) {
        t.addSuppressed(t1);
      }
      throw t;
    }
  }

  /**
   * @function open (segment-based)
   * @description Opens a reader across the given segment infos and optionally reuses matching old readers for efficiency.
   * 
   * @param {Directory} directory - The target directory.
   * @param {SegmentInfos} infos - The segment metadata.
   * @param {List<? extends LeafReader>} oldReaders - Readers from previous snapshot for potential reuse.
   * @param {Comparator<LeafReader>} leafSorter - Segment sorting comparator.
   * 
   * @returns {DirectoryReader} New StandardDirectoryReader reflecting the specified segments.
   * 
   * @throws {IOException} If construction or validation fails.
   */
  public static DirectoryReader open(
      Directory directory,
      SegmentInfos infos,
      List<? extends LeafReader> oldReaders,
      Comparator<LeafReader> leafSorter)
      throws IOException {

    Map<String, Integer> segmentReaders = Collections.emptyMap();

    if (oldReaders != null) {
      segmentReaders = CollectionUtil.newHashMap(oldReaders.size());
      for (int i = 0, c = oldReaders.size(); i < c; i++) {
        final SegmentReader sr = (SegmentReader) oldReaders.get(i);
        segmentReaders.put(sr.getSegmentName(), Integer.valueOf(i));
      }
    }

    SegmentReader[] newReaders = new SegmentReader[infos.size()];
    for (int i = infos.size() - 1; i >= 0; i--) {
      SegmentCommitInfo commitInfo = infos.info(i);

      Integer oldReaderIndex = segmentReaders.get(commitInfo.info.name);
      SegmentReader oldReader = (oldReaderIndex == null) ? null : (SegmentReader) oldReaders.get(oldReaderIndex.intValue());

      // Ensure index segment identity matches; aids against illegal file system state changes
      if (oldReader != null
          && !Arrays.equals(commitInfo.info.getId(), oldReader.getSegmentInfo().info.getId())) {
        throw new IllegalStateException(
            "same segment "
                + commitInfo.info.name
                + " has invalid doc count change; likely you are re-opening a reader after illegally removing index files yourself and building a new index in their place.  Use IndexWriter.deleteAll or open a new IndexWriter using OpenMode.CREATE instead");
      }

      try {
        SegmentReader newReader;
        if (oldReader == null
            || commitInfo.info.getUseCompoundFile() != oldReader.getSegmentInfo().info.getUseCompoundFile()) {
          // Always create a new reader if compound file state has changed
          newReader =
              new SegmentReader(commitInfo, infos.getIndexCreatedVersionMajor(), IOContext.DEFAULT);
          newReaders[i] = newReader;
        } else {
          if (oldReader.isNRT) {
            // NRT: must reload liveDocs and doc values updates from disk
            Bits liveDocs =
                commitInfo.hasDeletions()
                    ? commitInfo
                        .info
                        .getCodec()
                        .liveDocsFormat()
                        .readLiveDocs(commitInfo.info.dir, commitInfo, IOContext.READONCE)
                    : null;
            newReaders[i] =
                new SegmentReader(
                    commitInfo,
                    oldReader,
                    liveDocs,
                    liveDocs,
                    commitInfo.info.maxDoc() - commitInfo.getDelCount(),
                    false);
          } else {
            // Reuse if delGen and fieldInfosGen are equal (i.e., segment content didn't change)
            if (oldReader.getSegmentInfo().getDelGen() == commitInfo.getDelGen()
                && oldReader.getSegmentInfo().getFieldInfosGen() == commitInfo.getFieldInfosGen()) {
              oldReader.incRef();
              newReaders[i] = oldReader;
            } else {
              assert commitInfo.info.dir == oldReader.getSegmentInfo().info.dir;
              if (oldReader.getSegmentInfo().getDelGen() == commitInfo.getDelGen()) {
                // Only field infos changed, so reuse liveDocs
                newReaders[i] =
                    new SegmentReader(
                        commitInfo,
                        oldReader,
                        oldReader.getLiveDocs(),
                        oldReader.getHardLiveDocs(),
                        oldReader.numDocs(),
                        false);
              } else {
                // Deletions changed, reload liveDocs from disk
                Bits liveDocs =
                    commitInfo.hasDeletions()
                        ? commitInfo
                            .info
                            .getCodec()
                            .liveDocsFormat()
                            .readLiveDocs(commitInfo.info.dir, commitInfo, IOContext.READONCE)
                        : null;
                newReaders[i] =
                    new SegmentReader(
                        commitInfo,
                        oldReader,
                        liveDocs,
                        liveDocs,
                        commitInfo.info.maxDoc() - commitInfo.getDelCount(),
                        false);
              }
            }
          }
        }
      } catch (Throwable t) {
        decRefWhileSuppressingException(t, newReaders);
        throw t;
      }
    }
    return new StandardDirectoryReader(
        directory, newReaders, null, infos, leafSorter, false, false);
  }

  /**
   * @function decRefWhileSuppressingException
   * @description Utility method to decrement the reference count of readers, suppressing any exceptions and adding them to the provided throwable for error diagnostics.
   * 
   * @param {Throwable} t - The original throwable being handled.
   * @param {SegmentReader[]} readers - Array of readers whose reference counts are to be decremented if non-null.
   */
  private static void decRefWhileSuppressingException(Throwable t, SegmentReader[] readers) {
    for (SegmentReader reader : readers) {
      if (reader != null) {
        try {
          reader.decRef();
        } catch (Throwable rt) {
          t.addSuppressed(rt);
        }
      }
    }
  }

  /**
   * @function toString
   * @description Returns a human-readable summary of the StandardDirectoryReader, including its class name, segments file, version, index writer status, and all sub-readers.
   * 
   * @returns {String} String representation for debugging.
   */
  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder();
    buffer.append(getClass().getSimpleName());
    buffer.append('(');
    final String segmentsFile = segmentInfos.getSegmentsFileName();
    if (segmentsFile != null) {
      buffer.append(segmentsFile).append(":").append(segmentInfos.getVersion());
    }
    if (writer != null) {
      buffer.append(":nrt");
    }
    for (final LeafReader r : getSequentialSubReaders()) {
      buffer.append(' ');
      buffer.append(r);
    }
    buffer.append(')');
    return buffer.toString();
  }

  /**
   * @function doOpenIfChanged
   * @description Checks if the index has changed and opens a new reader reflecting the changes if so. Returns null if no changes detected.
   * 
   * @returns {DirectoryReader} New DirectoryReader reflecting the updated index, or null if unchanged.
   * @throws {IOException} On failure to check or reopen segments.
   */
  @Override
  protected DirectoryReader doOpenIfChanged() throws IOException {
    return doOpenIfChanged((IndexCommit) null);
  }

  /**
   * @function doOpenIfChanged (commit-aware)
   * @description Checks for index changes at a specific commit point, or at the latest if commit is null, and returns a new DirectoryReader if changes are detected.
   * 
   * @param {IndexCommit} commit - The commit point to check against, or null for latest.
   * @returns {DirectoryReader} New reader instance if index changed; null otherwise.
   * @throws {IOException} If index state cannot be determined.
   */
  @Override
  protected DirectoryReader doOpenIfChanged(final IndexCommit commit) throws IOException {
    ensureOpen();
    if (writer != null) {
      return doOpenFromWriter(commit);
    } else {
      return doOpenNoWriter(commit);
    }
  }

  /**
   * @function doOpenIfChanged (writer-aware)
   * @description Checks for changes in the index reflecting the given writer; creates a new reader if changes or deletion policies warrant it.
   * 
   * @param {IndexWriter} writer - IndexWriter to synchronize against.
   * @param {boolean} applyAllDeletes - Whether to apply all deletes during open.
   * @returns {DirectoryReader} New DirectoryReader or null if unchanged.
   * @throws {IOException} On error during index state inspection.
   */
  @Override
  protected DirectoryReader doOpenIfChanged(IndexWriter writer, boolean applyAllDeletes)
      throws IOException {
    ensureOpen();
    if (writer == this.writer && applyAllDeletes == this.applyAllDeletes) {
      return doOpenFromWriter(null);
    } else {
      return writer.getReader(applyAllDeletes, writeAllDeletes);
    }
  }

  /**
   * @function doOpenFromWriter
   * @description Helper for refreshing from a live IndexWriter, handling NRT logic and version consistency checks.
   * 
   * @param {IndexCommit} commit - Commit point, or null for latest.
   * @returns {DirectoryReader} New DirectoryReader, or null if index unchanged.
   * @throws {IOException} On segment info access or state validation failure.
   */
  private DirectoryReader doOpenFromWriter(IndexCommit commit) throws IOException {
    if (commit != null) {
      return doOpenFromCommit(commit);
    }
    if (writer.nrtIsCurrent(segmentInfos)) {
      return null;
    }
    DirectoryReader reader = writer.getReader(applyAllDeletes, writeAllDeletes);
    if (reader.getVersion() == segmentInfos.getVersion()) {
      reader.decRef();
      return null;
    }
    return reader;
  }

  /**
   * @function doOpenNoWriter
   * @description Helper for refreshing a reader in read-only/non-writer scenarios, based on commit or latest index state.
   * 
   * @param {IndexCommit} commit - The target commit point or null.
   * @returns {DirectoryReader} Refreshed reader if index changed; null otherwise.
   * @throws {IOException} For directory mismatch or segment validation errors.
   */
  private DirectoryReader doOpenNoWriter(IndexCommit commit) throws IOException {
    if (commit == null) {
      if (isCurrent()) {
        return null;
      }
    } else {
      if (directory != commit.getDirectory()) {
        throw new IOException("the specified commit does not match the specified Directory");
      }
      if (segmentInfos != null
          && commit.getSegmentsFileName().equals(segmentInfos.getSegmentsFileName())) {
        return null;
      }
    }
    return doOpenFromCommit(commit);
  }

  /**
   * @function doOpenFromCommit
   * @description Opens a new DirectoryReader instance from the provided index commit by reading its segment files.
   * 
   * @param {IndexCommit} commit - Target commit point.
   * @returns {DirectoryReader} Fresh DirectoryReader for the commit's state.
   * @throws {IOException} If the commit cannot be read.
   */
  private DirectoryReader doOpenFromCommit(IndexCommit commit) throws IOException {
    return new SegmentInfos.FindSegmentsFile<DirectoryReader>(directory) {
      @Override
      protected DirectoryReader doBody(String segmentFileName) throws IOException {
        final SegmentInfos infos = SegmentInfos.readCommit(directory, segmentFileName);
        return doOpenIfChanged(infos);
      }
    }.run(commit);
  }

  /**
   * @function doOpenIfChanged (SegmentInfos)
   * @description Constructs a new StandardDirectoryReader from the provided SegmentInfos and existing subreaders, reusing readers whenever possible.
   * 
   * @param {SegmentInfos} infos - Segment metadata for the new reader.
   * @returns {DirectoryReader} Reader for new segment state.
   * @throws {IOException} If construction fails.
   */
  DirectoryReader doOpenIfChanged(SegmentInfos infos) throws IOException {
    return StandardDirectoryReader.open(
        directory, infos, getSequentialSubReaders(), subReadersSorter);
  }

  /**
   * @function getVersion
   * @description Returns the version number associated with the current segment infos.
   * @returns {long} Segment version.
   */
  @Override
  public long getVersion() {
    ensureOpen();
    return segmentInfos.getVersion();
  }

  /**
   * @function getSegmentInfos
   * @description Returns the SegmentInfos object holding metadata for current index segments.
   * @returns {SegmentInfos} Active segment info state.
   */
  public SegmentInfos getSegmentInfos() {
    return segmentInfos;
  }

  /**
   * @function isCurrent
   * @description Checks whether this reader's view of the index is the latest, considering segment and deletion state.
   * 
   * @returns {boolean} True if the reader is current; false otherwise.
   * @throws {IOException} If state inspection fails.
   */
  @Override
  public boolean isCurrent() throws IOException {
    ensureOpen();
    if (writer == null || writer.isClosed()) {
      // Compare current on-disk segment version to reader's segment version
      SegmentInfos sis = SegmentInfos.readLatestCommit(directory);
      return sis.getVersion() == segmentInfos.getVersion();
    } else {
      return writer.nrtIsCurrent(segmentInfos);
    }
  }

  /**
   * @function doClose
   * @description Safely decrements reference counts of all subreaders and updates internal IndexWriter state if needed. Handles AlreadyClosedException from IndexWriter.
   * 
   * @throws {IOException} On IO/resource cleanup errors.
   */
  @Override
  @SuppressWarnings("try")
  protected void doClose() throws IOException {
    Closeable decRefDeleter =
        () -> {
          if (writer != null) {
            try {
              writer.decRefDeleter(segmentInfos);
            } catch (AlreadyClosedException _) {
              // Ignore if writer is already closed
            }
          }
        };
    try (var _ = decRefDeleter) {
      final List<? extends LeafReader> sequentialSubReaders = getSequentialSubReaders();
      IOUtils.applyToAll(sequentialSubReaders, LeafReader::decRef);
    }
  }

  /**
   * @function getIndexCommit
   * @description Returns an IndexCommit reflecting the reader's segment state for external synchronization and recovery.
   * 
   * @returns {IndexCommit} Metadata object for commit operations.
   * @throws {IOException} On segment info access error.
   */
  @Override
  public IndexCommit getIndexCommit() throws IOException {
    ensureOpen();
    return new ReaderCommit(this, segmentInfos, directory);
  }

  /**
   * @class ReaderCommit
   * @classdesc 
   * IndexCommit implementation holding state about a specific index commit as viewed by this reader instance.
   * 
   * @example
   * IndexCommit commit = directoryReader.getIndexCommit();
   * String segmentFilename = commit.getSegmentsFileName();
   */
  static final class ReaderCommit extends IndexCommit {
    private String segmentsFileName;
    Collection<String> files;
    Directory dir;
    long generation;
    final Map<String, String> userData;
    private final int segmentCount;
    private final StandardDirectoryReader reader;

    /**
     * @constructor
     * @description Constructs a ReaderCommit for the provided reader, segment infos, and directory.
     * 
     * @param {StandardDirectoryReader} reader - Associated DirectoryReader.
     * @param {SegmentInfos} infos - Segment metadata.
     * @param {Directory} dir - Directory containing the index.
     * @throws {IOException} On segment file enumeration errors.
     */
    ReaderCommit(StandardDirectoryReader reader, SegmentInfos infos, Directory dir)
        throws IOException {
      segmentsFileName = infos.getSegmentsFileName();
      this.dir = dir;
      userData = infos.getUserData();
      files = Collections.unmodifiableCollection(infos.files(true));
      generation = infos.getGeneration();
      segmentCount = infos.size();
      this.reader = reader;
    }

    /**
     * @function toString
     * @description Returns string representation describing the commit.
     * @returns {String}
     */
    @Override
    public String toString() {
      return "StandardDirectoryReader.ReaderCommit(" + segmentsFileName + " files=" + files + ")";
    }

    /**
     * @function getSegmentCount
     * @description Returns number of segments at this commit point.
     * @returns {int}
     */
    @Override
    public int getSegmentCount() {
      return segmentCount;
    }

    /**
     * @function getSegmentsFileName
     * @description Returns the filename of the segments_N file at this commit.
     * @returns {String}
     */
    @Override
    public String getSegmentsFileName() {
      return segmentsFileName;
    }

    /**
     * @function getFileNames
     * @description Returns an unmodifiable collection of all files referenced by this commit.
     * @returns {Collection<String>}
     */
    @Override
    public Collection<String> getFileNames() {
      return files;
    }

    /**
     * @function getDirectory
     * @description Returns the directory containing the commit.
     * @returns {Directory}
     */
    @Override
    public Directory getDirectory() {
      return dir;
    }

    /**
     * @function getGeneration
     * @description Returns the unique generation number for this commit.
     * @returns {long}
     */
    @Override
    public long getGeneration() {
      return generation;
    }

    /**
     * @function isDeleted
     * @description Reader commits are always non-deletable; returns false.
     * @returns {boolean}
     */
    @Override
    public boolean isDeleted() {
      return false;
    }

    /**
     * @function getUserData
     * @description Returns user data (custom commit metadata) as stored with this commit.
     * @returns {Map<String, String>}
     */
    @Override
    public Map<String, String> getUserData() {
      return userData;
    }

    /**
     * @function delete
     * @description Unsupported operation; attempting deletion will throw.
     * @throws {UnsupportedOperationException} Always thrown.
     */
    @Override
    public void delete() {
      throw new UnsupportedOperationException("This IndexCommit does not support deletions");
    }

    /**
     * @function getReader
     * @description Returns the StandardDirectoryReader associated with this commit.
     * @returns {StandardDirectoryReader}
     */
    @Override
    StandardDirectoryReader getReader() {
      return reader;
    }
  }

  /**
   * Set of listeners to notify when this reader closes.
   * @type {Set<ClosedListener>}
   */
  private final Set<ClosedListener> readerClosedListeners = new CopyOnWriteArraySet<>();

  /**
   * Cache helper instance that provides listener notification when this reader is closed.
   * @type {CacheHelper}
   */
  private final CacheHelper cacheHelper =
      new CacheHelper() {
        private final CacheKey cacheKey = new CacheKey();

        /**
         * @function getKey
         * @description Returns unique CacheKey for this reader instance.
         * @returns {CacheHelper.CacheKey}
         */
        @Override
        public CacheKey getKey() {
          return cacheKey;
        }
        /**
         * @function addClosedListener
         * @description Registers a ClosedListener to receive a callback when the reader closes.
         * @param {ClosedListener} listener - Listener to register.
         */
        @Override
        public void addClosedListener(ClosedListener listener) {
          ensureOpen();
          readerClosedListeners.add(listener);
        }
      };

  /**
   * @function notifyReaderClosedListeners
   * @description Internal: Notifies all registered ClosedListener instances that the reader is now closed. Catches and logs all exceptions from listeners.
   * @throws {IOException} On listener notification failures.
   */
  @Override
  protected void notifyReaderClosedListeners() throws IOException {
    synchronized (readerClosedListeners) {
      IOUtils.applyToAll(readerClosedListeners, l -> l.onClose(cacheHelper.getKey()));
    }
  }

  /**
   * @function getReaderCacheHelper
   * @description Returns the cache helper associated with this DirectoryReader, supporting cache invalidation and closed listener notification.
   * @returns {CacheHelper}
   */
  @Override
  public CacheHelper getReaderCacheHelper() {
    return cacheHelper;
  }
}
