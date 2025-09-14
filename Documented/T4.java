```java
/**
 * @file Provides the definition for the abstract IndexCommit class in Apache Lucene. 
 * @module org.apache.lucene.index.IndexCommit
 * 
 * @description
 * This file declares the IndexCommit abstract class, a key API component in Apache Lucene's index management subsystem. 
 * IndexCommit models a point-in-time immutable view of a committed index snapshot in a particular Directory instance, including vital metadata such as segment file names, generation number, read-only state, and user-provided commit data. 
 * This abstraction is fundamental for advanced index lifecycle management (e.g., implementing snapshotting, rollback, and index replication strategies) where access to past and present commit states is required, but direct modification is prohibited (the deletion operation is provided for removing entire commits, not files).
 * Concrete implementations are provided within Lucene's internals and expose provider- or storage-specific behavior while obeying this contract.
 * 
 * @dependencies
 * - Directory: Represents the storage abstraction for index files. Essential for correctly associating commits with storage.
 * - java.util.Collection, java.util.Map: Standard Java utilities for managing file names and user data in commit metadata.
 * 
 * @author Apache Software Foundation
 * @version Lucene 9.x (see project version for details)
 * @license Apache License 2.0
 * @lastmodified 2025-09-14: Documentation added and class-level structure reviewed.
 */

package org.apache.lucene.index;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import org.apache.lucene.store.Directory;

/**
 * @class IndexCommit
 * @classdesc
 * Abstract representation of a single Lucene index commit—an immutable snapshot of all files and metadata associated with a specific commit point in a Directory.
 * Provides methods for accessing commit metadata (generation, file names, directory, user data), lifecycle management (delete), and comparability for commit ordering.
 *
 * @example
 * // Usage example (with concrete subclass, e.g., from DirectoryReader.listCommits):
 * IndexCommit commit = ...; // obtained from DirectoryReader or IndexWriter
 * String segmentsFile = commit.getSegmentsFileName();
 * Map<String, String> userData = commit.getUserData();
 * commit.delete(); // Marks commit as deleted in the Directory
 *
 * @prop {Directory} directory - The Directory storing the index files for this commit.
 * @prop {long} generation - Unique generation number identifying this commit instance.
 * @prop {Collection<String>} fileNames - Set of all files associated with this commit.
 * @prop {Map<String, String>} userData - Arbitrary commit metadata, typically used for versioning or app-specific tags.
 * @prop {boolean} deleted - Indicates whether this commit has been deleted from the Directory.
 */
public abstract class IndexCommit implements Comparable<IndexCommit> {

  /**
   * @function getSegmentsFileName
   * @description
   * Returns the name of the segments_N file that uniquely identifies this commit point.
   *
   * @returns {String} Name of the segments file for the commit (e.g., 'segments_5').
   */
  public abstract String getSegmentsFileName();

  /**
   * @function getFileNames
   * @description
   * Lists all file names referenced by this commit, representing the state of the index at the commit point.
   * This typically includes all segment and auxiliary files.
   *
   * @returns {Collection<String>} Names of all files in this commit snapshot.
   * @throws {IOException} If directory access fails or files cannot be enumerated.
   */
  public abstract Collection<String> getFileNames() throws IOException;

  /**
   * @function getDirectory
   * @description
   * Retrieves the Directory instance containing the files associated with this commit.
   *
   * @returns {Directory} The associated storage Directory.
   */
  public abstract Directory getDirectory();

  /**
   * @function delete
   * @description
   * Deletes this commit point, marking all associated files as eligible for removal once unreferenced.
   * Delete operations are typically only necessary when custom retention policies are in place.
   * <p>
   * Note: This does not physically remove files if other commits reference them.
   */
  public abstract void delete();

  /**
   * @function isDeleted
   * @description
   * Returns whether this commit instance has been marked as deleted within the index storage.
   *
   * @returns {boolean} True if commit is deleted; otherwise, false.
   */
  public abstract boolean isDeleted();

  /**
   * @function getSegmentCount
   * @description
   * Reports the number of segments present in this commit's snapshot.
   *
   * @returns {int} Number of segments in the commit.
   */
  public abstract int getSegmentCount();

  /**
   * @constructor IndexCommit
   * @description
   * Protected default constructor to enforce abstract base class semantics.
   * Only instantiable by subclasses within the Lucene package.
   */
  protected IndexCommit() {}

  /**
   * @function equals
   * @description
   * Tests equality with another object: returns true only if both are IndexCommit instances
   * from the same Directory and with the same generation number.
   *
   * @param {Object} other - Object reference for equality comparison.
   * @returns {boolean} True if object is a corresponding IndexCommit, otherwise false.
   *
   * @example
   * boolean same = commit1.equals(commit2);
   */
  @Override
  public boolean equals(Object other) {
    if (other instanceof IndexCommit otherCommit) {
      // Equality: must reference same Directory instance and have the same generation
      return otherCommit.getDirectory() == getDirectory()
          && otherCommit.getGeneration() == getGeneration();
    } else {
      return false;
    }
  }

  /**
   * @function hashCode
   * @description
   * Computes hash code for the IndexCommit, using a combination of Directory and generation.
   * Ensures stable map/set membership across commit lifecycles.
   *
   * @returns {int} Hash code for use in hash-based collections.
   */
  @Override
  public int hashCode() {
    // Combine Directory and generation for uniqueness
    return getDirectory().hashCode() + Long.valueOf(getGeneration()).hashCode();
  }

  /**
   * @function getGeneration
   * @description
   * Fetches the unique generation number for this commit.
   * The generation is strictly monotonically increasing across index commits in the Directory.
   *
   * @returns {long} Generation identifier (usually derived from the segments file).
   */
  public abstract long getGeneration();

  /**
   * @function getUserData
   * @description
   * Retrieves user-supplied commit metadata, which can be used for application-level tagging, versioning, or tracing.
   *
   * @returns {Map<String, String>} Unmodifiable map of user data.
   * @throws {IOException} If underlying storage access fails.
   *
   * @example
   * Map<String, String> metadata = commit.getUserData();
   */
  public abstract Map<String, String> getUserData() throws IOException;

  /**
   * @function compareTo
   * @description
   * Implements total ordering for IndexCommit instances, allowing for chronological comparisons
   * of commit points within the same Directory.
   * Throws an exception if compared to a commit from a different Directory.
   *
   * @param {IndexCommit} commit - Another commit for comparison.
   * @returns {int} Negative if this comes before, positive if after, zero if same generation.
   * @throws {UnsupportedOperationException} If directories differ.
   *
   * @example
   * commits.sort(); // Commits will be sorted by generation within Directory
   */
  @Override
  public int compareTo(IndexCommit commit) {
    if (getDirectory() != commit.getDirectory()) {
      // Prevent cross-directory ordering, which would be semantically meaningless
      throw new UnsupportedOperationException(
          "cannot compare IndexCommits from different Directory instances");
    }

    long gen = getGeneration();
    long comgen = commit.getGeneration();
    return Long.compare(gen, comgen);
  }

  /**
   * @function getReader
   * @description
   * Retrieves a StandardDirectoryReader instance for this commit.
   * Default implementation returns null; overridden by internal concrete subclasses.
   * Package-private—used by Lucene's reader infrastructure.
   *
   * @returns {StandardDirectoryReader|null} Reader for this commit or null if not available.
   */
  StandardDirectoryReader getReader() {
    // Default: not implemented in base (to be overridden internally)
    return null;
  }
}
