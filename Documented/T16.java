/**
 * @file ApproximatePriorityQueue.java
 * @module org.apache.lucene.index
 * 
 * @description
 * This file defines an internal, non-thread-safe generic priority queue implementation with fast approximate slot-based storage for objects, designed for performance-sensitive scenarios within the Lucene indexing module. Elements can be added with an associated weight (used to determine a slot by analyzing the number of leading zeros in the weight), and are retrieved according to a flexible predicate, optimizing both memory and retrieval efficiency for cases where exact ordering is relaxed. The design is tailored to limit heap fragmentation and improve access by maintaining a compact, fixed-size structure for the common case, with overflow handled in a secondary array region.
 * 
 * @dependencies
 * - java.util.ArrayList: Provides dynamic-size array storage for slots.
 * - java.util.List: Interface for the slots container.
 * - java.util.ListIterator: Enables traversal and removal for overflown slots.
 * - java.util.function.Predicate: Allows custom logic for element selection/removal.
 * 
 * @author Apache Lucene Project
 * @version 1.0
 * @license Apache License 2.0
 * @lastmodified 2024-06-01: Refactored for improved documentation and clarity.
 */

package org.apache.lucene.index;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.function.Predicate;

/**
 * @class ApproximatePriorityQueue<T>
 * @classdesc
 * Implements a fixed-capacity (plus overflow) slot-based approximate priority queue where elements are assigned to slots based on the number of leading zeros in their associated weight. Provides quick insertion and polling under predicate filtering, with efficient space management using bitwise tracking of occupied slots.
 * 
 * @example
 * // Example usage:
 * ApproximatePriorityQueue<String> queue = new ApproximatePriorityQueue<>();
 * queue.add("high", 2L);
 * queue.add("low", 1024L);
 * String item = queue.poll(s -> s.startsWith("h")); // retrieves "high" if present
 * 
 * @prop {List<T>} slots - Backing storage for slot-assigned and overflowed elements.
 * @prop {long} usedSlots - Bitmask indicates presence of occupied slots (first Long.SIZE slots).
 */
final class ApproximatePriorityQueue<T> {

  /** Backing storage for entries using slot-based allocation; may overflow past Long.SIZE entries. */
  private final List<T> slots = new ArrayList<>(Long.SIZE);

  /** Bitmask of slot occupancy for the first Long.SIZE entries (64 bits; one per slot). */
  private long usedSlots = 0L;

  /**
   * @constructor
   * @description
   * Initializes the queue with Long.SIZE (typically 64) preallocated null slots, prepared for fixed-slot insertion. No items are present initially.
   * 
   * @example
   * ApproximatePriorityQueue<Integer> q = new ApproximatePriorityQueue<>();
   */
  ApproximatePriorityQueue() {
    // Preallocate Long.SIZE slots to cover fast-path insertion; overflow handled by dynamic growth.
    for (int i = 0; i < Long.SIZE; ++i) {
      slots.add(null);
    }
  }

  /**
   * @method add
   * @description
   * Inserts an entry into the queue, assigning it to a suitable slot based on the number of leading zeros in the provided weight. If the ideal or next available slot is occupied, uses bit twiddling to select the next available within the fixed-size slot region; otherwise, falls back to overflow (grows the array beyond Long.SIZE with regular linear addition).
   * 
   * @param {T} entry - The value to add. Must be non-null.
   * @param {long} weight - Determines preferred slot; typically, smaller weights result in higher priority (slot 0).
   * 
   * @returns {void}
   * 
   * @throws {AssertionError} If entry is null or an unexpected slot over-write is detected (in debug configurations).
   * 
   * @example
   * queue.add("task", 15L);
   */
  void add(T entry, long weight) {
    assert entry != null;

    // Compute target slot index from weight: leading zeros determine priority.
    final int expectedSlot = Long.numberOfLeadingZeros(weight);

    // Find the first free slot at or after expectedSlot using a bit twiddling technique.
    final long freeSlots = ~usedSlots;
    final int destinationSlot =
        expectedSlot + Long.numberOfTrailingZeros(freeSlots >>> expectedSlot);
    assert destinationSlot >= expectedSlot;
    if (destinationSlot < Long.SIZE) {
      // Occupied region: mark slot as used and set entry.
      usedSlots |= 1L << destinationSlot;
      T previous = slots.set(destinationSlot, entry);
      // Ensure we're not overwriting a live entry (should not occur in this path).
      assert previous == null;
    } else {
      // Overflow region: grow dynamic list for entries beyond core slots.
      slots.add(entry);
    }
  }

  /**
   * @method poll
   * @description
   * Removes and returns the first element that matches the supplied predicate, searching first through the fixed-size slots in increasing slot order, then scanning overflow slots in reverse insertion order. Deallocates the slot or removes the overflowed element from the array upon successful match.
   * 
   * @param {Predicate<T>} predicate - The condition to identify a matching element for retrieval/removal.
   * 
   * @returns {T} The first element satisfying the predicate or null if none exist.
   * 
   * @example
   * String polled = queue.poll(s -> s.equals("urgent"));
   */
  T poll(Predicate<T> predicate) {
    // Fast path: search fixed-length slots for matching entry.
    int nextSlot = 0;
    do {
      // Find next used slot at or after nextSlot.
      final int nextUsedSlot = nextSlot + Long.numberOfTrailingZeros(usedSlots >>> nextSlot);
      if (nextUsedSlot >= Long.SIZE) {
        break; // No more marked slots in fixed region.
      }
      final T entry = slots.get(nextUsedSlot);
      if (predicate.test(entry)) {
        // Clear occupancy bit and remove entry.
        usedSlots &= ~(1L << nextUsedSlot);
        slots.set(nextUsedSlot, null);
        return entry;
      } else {
        // Continue searching from next slot.
        nextSlot = nextUsedSlot + 1;
      }
    } while (nextSlot < Long.SIZE);

    // Slow path: search overflow slots (those beyond fixed region) in reverse order for LIFO semantics.
    for (ListIterator<T> lit = slots.listIterator(slots.size());
         lit.previousIndex() >= Long.SIZE;) {
      final T entry = lit.previous();
      if (predicate.test(entry)) {
        lit.remove(); // Remove only the first match found.
        return entry;
      }
    }

    // No matching element found.
    return null;
  }

  /**
   * @method contains
   * @description
   * Checks if the queue currently contains a reference-equal entry to the given object.
   * 
   * @param {Object} o - The object to check for presence (must not be null).
   * 
   * @returns {boolean} True if present in any slot or overflow; false otherwise.
   * 
   * @throws {NullPointerException} If the argument is null.
   * 
   * @example
   * boolean present = queue.contains(taskObj);
   */
  boolean contains(Object o) {
    if (o == null) {
      throw new NullPointerException();
    }
    return slots.contains(o);
  }

  /**
   * @method isEmpty
   * @description
   * Determines if the queue is empty (contains no live entries in either core or overflow slots).
   * 
   * @returns {boolean} True if empty, otherwise false.
   * 
   * @example
   * if (queue.isEmpty()) { ... }
   */
  boolean isEmpty() {
    // No slots used and no overflow beyond core capacity.
    return usedSlots == 0 && slots.size() == Long.SIZE;
  }

  /**
   * @method remove
   * @description
   * Removes the first occurrence of the specified object, if present, either from assigned slots or from the overflow region.
   * 
   * @param {Object} o - The object to remove (must not be null).
   * 
   * @returns {boolean} True if removal succeeded; false if not found.
   * 
   * @throws {NullPointerException} If the argument is null.
   * 
   * @example
   * boolean wasRemoved = queue.remove(taskObj);
   */
  boolean remove(Object o) {
    if (o == null) {
      throw new NullPointerException();
    }
    int index = slots.indexOf(o);
    if (index == -1) {
      return false;
    }
    if (index >= Long.SIZE) {
      // Entry resides in overflow region; remove directly.
      slots.remove(index);
    } else {
      // Clear occupancy and slot reference.
      usedSlots &= ~(1L << index);
      slots.set(index, null);
    }
    return true;
  }
}
