```java
/**
 * @file Provides a snapshot-based index deletion policy within the Lucene index management system.
 * @module org.apache.lucene.index.SnapshotDeletionPolicy
 * 
 * @description
 * Implements an index deletion policy that enables taking and managing point-in-time snapshots of Lucene index commits. 
 * The purpose is to prevent the deletion of specific commits (index snapshots) until they are explicitly released, 
 * thus facilitating consistent backups, rollbacks, and disaster recovery scenarios. The class wraps a primary 
 * IndexDeletionPolicy and interposes additional logic for reference-counting and tracking snapshot lifecycles.
 * 
 * @dependencies
 * - IndexDeletionPolicy: Base Lucene policy to delegate standard index life cycle events (onCommit, onInit).
 * - IndexCommit: Represents a single commit point in the index.
 * - Directory: Lucene interface for directory abstraction.
 * - Java Collections (Map, List, ArrayList, HashMap, Collection): Used for tracking snapshot states and references.
 * 
 * @author Apache Software Foundation
 * @version 1.0
 * @license Apache License 2.0
 * @lastmodified [Unspecified: Sourced from the Apache Lucene codebase]
 */


/**
 * @class SnapshotDeletionPolicy
 * @classdesc
 * A snapshot-aware index deletion policy for Lucene, allowing atomic snapshots of index commits to be retained 
 * and released independently of the primary deletion policy’s lifecycle. 
 * This enables clients to safely preserve index states across operations until snapshots are explicitly released.
 * 
 * @example
 * // Wrap a standard deletion policy:
 * IndexDeletionPolicy primaryPolicy = new KeepOnlyLastCommitDeletionPolicy();
 * SnapshotDeletionPolicy snapshotPolicy = new SnapshotDeletionPolicy(primaryPolicy);
 * 
 * // Pass the snapshotPolicy to the IndexWriter configuration:
 * IndexWriterConfig config = new IndexWriterConfig(...).setIndexDeletionPolicy(snapshotPolicy);
 * IndexWriter writer = new IndexWriter(directory, config);
 * 
 * // Take a snapshot of the latest commit:
 * IndexCommit commit = snapshotPolicy.snapshot();
 * 
 * // List all active snapshots:
 * List<IndexCommit> activeSnapshots = snapshotPolicy.getSnapshots();
 * 
 * // Release the snapshot when done:
 * snapshotPolicy.release(commit);
 * 
 * @prop {Map<Long, Integer>} refCounts - Reference counts for each snapshotted commit generation.
 * @prop {Map<Long, IndexCommit>} indexCommits - Maps generation numbers to retained IndexCommit objects.
 * @prop {IndexDeletionPolicy} primary - The wrapped primary deletion policy.
 * @prop {IndexCommit} lastCommit - Reference to the most recent commit, for efficient snapshotting.
 * @prop {boolean} initCalled - Indicates whether onInit hook was invoked; guards proper policy usage.
 */
public class SnapshotDeletionPolicy extends IndexDeletionPolicy {

    /**
     * Reference count mapping per commit generation; controls snapshot lifespans.
     */
    protected final Map<Long, Integer> refCounts = new HashMap<>();

    /**
     * Maps a commit's generation number to its corresponding IndexCommit instance while snapshotted.
     */
    protected final Map<Long, IndexCommit> indexCommits = new HashMap<>();

    /**
     * The primary (wrapped) deletion policy receiving all delegated events.
     */
    private final IndexDeletionPolicy primary;

    /**
     * Reference to the most recently seen IndexCommit; used for new snapshots.
     */
    protected IndexCommit lastCommit;

    /**
     * Indicates whether onInit has been called by IndexWriter.
     */
    private boolean initCalled;

    /**
     * @constructor
     * @function SnapshotDeletionPolicy
     * @description Constructs a SnapshotDeletionPolicy that defers its lifecycle hooks to a primary policy, 
     * while independently retaining references to snapshotted commits.
     * 
     * @param {IndexDeletionPolicy} primary - The underlying policy to control basic commit deletion.
     * 
     * @returns {SnapshotDeletionPolicy} New policy ready for snapshot handling.
     */
    public SnapshotDeletionPolicy(IndexDeletionPolicy primary) {
        this.primary = primary;
    }

    /**
     * @function onCommit
     * @description Invoked on each commit operation to update deletion policy state. Delegates processing to 
     * primary, wraps visible commits, and updates the record of the latest commit.
     * 
     * @param {List<? extends IndexCommit>} commits - Ordered list of all current commits in the index.
     * 
     * @returns {void}
     * 
     * @throws {IOException} If an error occurs in the underlying policy's onCommit.
     */
    @Override
    public synchronized void onCommit(List<? extends IndexCommit> commits) throws IOException {
        // Wrap all commits to interpose custom deletion semantics
        primary.onCommit(wrapCommits(commits));
        lastCommit = commits.get(commits.size() - 1);
    }

    /**
     * @function onInit
     * @description Initialization hook invoked when the IndexWriter attaches this policy. 
     * Prepares the initial state, including reference tracking based on existing commits.
     * 
     * @param {List<? extends IndexCommit>} commits - Ordered list of available commits at startup.
     * 
     * @returns {void}
     * 
     * @throws {IOException} If an error occurs in the underlying policy's onInit.
     */
    @Override
    public synchronized void onInit(List<? extends IndexCommit> commits) throws IOException {
        initCalled = true;
        // Ensure primary policy processes wrapped commits
        primary.onInit(wrapCommits(commits));
        for (IndexCommit commit : commits) {
            if (refCounts.containsKey(commit.getGeneration())) {
                indexCommits.put(commit.getGeneration(), commit);
            }
        }
        if (!commits.isEmpty()) {
            lastCommit = commits.get(commits.size() - 1);
        }
    }

    /**
     * @function release
     * @description Releases a previously snapshotted commit. Decrements the reference count associated 
     * with a commit generation. If the count reaches zero, frees the commit for deletion by the underlying policy.
     * 
     * @param {IndexCommit} commit - The commit to be released. Must have been obtained via snapshot().
     * 
     * @returns {void}
     * 
     * @throws {IllegalStateException} If onInit was never called (misuse of the policy outside IndexWriter).
     * @throws {IllegalArgumentException} If attempting to release a commit not currently snapshotted.
     */
    public synchronized void release(IndexCommit commit) throws IOException {
        long gen = commit.getGeneration();
        releaseGen(gen);
    }

    /**
     * @function releaseGen
     * @description Internal logic for decrementing and potentially removing a reference to a snapshotted generation.
     * 
     * @param {long} gen - The index commit generation number to release.
     * 
     * @returns {void}
     * 
     * @throws {IllegalStateException} If invoked before proper initialization (onInit).
     * @throws {IllegalArgumentException} If releasing a non-snapshotted generation.
     */
    protected void releaseGen(long gen) throws IOException {
        if (!initCalled) {
            throw new IllegalStateException(
                "this instance is not being used by IndexWriter; be sure to use the instance returned from writer.getConfig().getIndexDeletionPolicy()");
        }
        Integer refCount = refCounts.get(gen);
        if (refCount == null) {
            throw new IllegalArgumentException("commit gen=" + gen + " is not currently snapshotted");
        }
        int refCountInt = refCount.intValue();
        assert refCountInt > 0;
        refCountInt--;
        if (refCountInt == 0) {
            // No more references; safe to remove
            refCounts.remove(gen);
            indexCommits.remove(gen);
        } else {
            refCounts.put(gen, refCountInt);
        }
    }

    /**
     * @function incRef
     * @description Increments the reference count for a commit, extending its retention as a snapshot.
     * 
     * @param {IndexCommit} ic - The commit to reference.
     * 
     * @returns {void}
     */
    protected synchronized void incRef(IndexCommit ic) {
        long gen = ic.getGeneration();
        Integer refCount = refCounts.get(gen);
        int refCountInt;
        if (refCount == null) {
            // First reference for this commit, associate with the latest commit known
            indexCommits.put(gen, lastCommit);
            refCountInt = 0;
        } else {
            refCountInt = refCount.intValue();
        }
        refCounts.put(gen, refCountInt + 1);
    }

    /**
     * @function snapshot
     * @description Captures an atomic snapshot of the current (most recent) commit, increments its reference count, 
     * and prevents its deletion until released. Only callable after proper initialization.
     * 
     * @returns {IndexCommit} The snapshotted latest commit object.
     * 
     * @throws {IllegalStateException} If the policy was not properly initialized, or if there is no commit to snapshot.
     * 
     * @example
     * IndexCommit commit = snapshotDeletionPolicy.snapshot();
     */
    public synchronized IndexCommit snapshot() throws IOException {
        if (!initCalled) {
            throw new IllegalStateException(
                "this instance is not being used by IndexWriter; be sure to use the instance returned from writer.getConfig().getIndexDeletionPolicy()");
        }
        if (lastCommit == null) {
            // Cannot snapshot before any commit exists
            throw new IllegalStateException("No index commit to snapshot");
        }

        incRef(lastCommit);

        return lastCommit;
    }

    /**
     * @function getSnapshots
     * @description Returns an immutable list of all currently retained (snapshotted) commits.
     * 
     * @returns {List<IndexCommit>} All IndexCommit instances managed as active snapshots.
     * 
     * @example
     * List<IndexCommit> snaps = snapshotDeletionPolicy.getSnapshots();
     */
    public synchronized List<IndexCommit> getSnapshots() {
        return new ArrayList<>(indexCommits.values());
    }

    /**
     * @function getSnapshotCount
     * @description Calculates the total number of active snapshot references.
     * 
     * @returns {int} The count of all retained snapshot references across generations.
     * 
     * @example
     * int count = snapshotDeletionPolicy.getSnapshotCount();
     */
    public synchronized int getSnapshotCount() {
        int total = 0;
        for (Integer refCount : refCounts.values()) {
            total += refCount.intValue();
        }
        return total;
    }

    /**
     * @function getIndexCommit
     * @description Fetches a retained IndexCommit by its generation number, if it is actively snapshotted.
     * 
     * @param {long} gen - The generation number for the commit.
     * 
     * @returns {IndexCommit|null} The commit instance if present, otherwise null.
     * 
     * @example
     * IndexCommit commit = snapshotDeletionPolicy.getIndexCommit(12345L);
     */
    public synchronized IndexCommit getIndexCommit(long gen) {
        return indexCommits.get(gen);
    }

    /**
     * @function wrapCommits
     * @description Wraps the provided commits with SnapshotCommitPoint decorators, allowing for custom deletion logic.
     * 
     * @param {List<? extends IndexCommit>} commits - List of delegate commit points.
     * 
     * @returns {List<IndexCommit>} New list of wrapped IndexCommit objects.
     */
    private List<IndexCommit> wrapCommits(List<? extends IndexCommit> commits) {
        List<IndexCommit> wrappedCommits = new ArrayList<>(commits.size());
        for (IndexCommit ic : commits) {
            wrappedCommits.add(new SnapshotCommitPoint(ic));
        }
        return wrappedCommits;
    }


    /**
     * @class SnapshotCommitPoint
     * @classdesc
     * Decorator for IndexCommit that intercepts deletion requests. Ensures that commits 
     * with active snapshots are not erroneously deleted while retained.
     * 
     * @prop {IndexCommit} cp - Underlying commit object.
     */
    private class SnapshotCommitPoint extends IndexCommit {

        /**
         * The delegate IndexCommit being wrapped.
         */
        protected IndexCommit cp;

        /**
         * @constructor
         * @function SnapshotCommitPoint
         * @description Constructs a snapshot-aware commit proxy for use in deletion policy logic.
         * 
         * @param {IndexCommit} cp - The commit to wrap.
         * 
         * @returns {SnapshotCommitPoint} New commit wrapper.
         */
        protected SnapshotCommitPoint(IndexCommit cp) {
            this.cp = cp;
        }

        /**
         * @function toString
         * @description String representation for debugging or logging purposes.
         * 
         * @returns {String} Human-readable identifier.
         */
        @Override
        public String toString() {
            return "SnapshotDeletionPolicy.SnapshotCommitPoint(" + cp + ")";
        }

        /**
         * @function delete
         * @description Attempts to delete the commit point. Suppresses deletion if the commit is currently 
         * referenced by an active snapshot.
         * 
         * @returns {void}
         */
        @Override
        public void delete() {
            synchronized (SnapshotDeletionPolicy.this) {
                // Only delete if there are no active references to this generation
                if (!refCounts.containsKey(cp.getGeneration())) {
                    cp.delete();
                }
            }
        }

        /**
         * @function getDirectory
         * @description Returns the directory for the wrapped commit.
         * 
         * @returns {Directory} Directory containing the commit files.
         */
        @Override
        public Directory getDirectory() {
            return cp.getDirectory();
        }

        /**
         * @function getFileNames
         * @description Returns the set of filenames referenced by the commit.
         * 
         * @returns {Collection<String>} Filenames in the commit.
         * @throws {IOException} If an error occurs accessing file metadata.
         */
        @Override
        public Collection<String> getFileNames() throws IOException {
            return cp.getFileNames();
        }

        /**
         * @function getGeneration
         * @description Returns the unique generation number for the commit.
         * 
         * @returns {long} Commit generation identifier.
         */
        @Override
        public long getGeneration() {
            return cp.getGeneration();
        }

        /**
         * @function getSegmentsFileName
         * @description Retrieves the primary segments file name for this commit.
         * 
         * @returns {String} Primary segments file.
         */
        @Override
        public String getSegmentsFileName() {
            return cp.getSegmentsFileName();
        }

        /**
         * @function getUserData
         * @description Provides the user data map associated with the commit.
         * 
         * @returns {Map<String, String>} Custom metadata.
         * @throws {IOException} If user data cannot be retrieved.
         */
        @Override
        public Map<String, String> getUserData() throws IOException {
            return cp.getUserData();
        }

        /**
         * @function isDeleted
         * @description Indicates whether the commit has been deleted.
         * 
         * @returns {boolean} True if deleted, false otherwise.
         */
        @Override
        public boolean isDeleted() {
            return cp.isDeleted();
        }

        /**
         * @function getSegmentCount
         * @description Returns the number of segments in this commit.
         * 
         * @returns {int} Number of index segments.
         */
        @Override
        public int getSegmentCount() {
            return cp.getSegmentCount();
        }
    }
}
