/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.index;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.function.Predicate;


final class ApproximatePriorityQueue<T> {

 
  private final List<T> slots = new ArrayList<>(Long.SIZE);

 
  private long usedSlots = 0L;

  ApproximatePriorityQueue() {
    for (int i = 0; i < Long.SIZE; ++i) {
      slots.add(null);
    }
  }

  
  void add(T entry, long weight) {
    assert entry != null;

   
    final int expectedSlot = Long.numberOfLeadingZeros(weight);

    
    final long freeSlots = ~usedSlots;
    final int destinationSlot =
        expectedSlot + Long.numberOfTrailingZeros(freeSlots >>> expectedSlot);
    assert destinationSlot >= expectedSlot;
    if (destinationSlot < Long.SIZE) {
      usedSlots |= 1L << destinationSlot;
      T previous = slots.set(destinationSlot, entry);
      assert previous == null;
    } else {
      slots.add(entry);
    }
  }

  
  T poll(Predicate<T> predicate) {
   
    int nextSlot = 0;
    do {
      final int nextUsedSlot = nextSlot + Long.numberOfTrailingZeros(usedSlots >>> nextSlot);
      if (nextUsedSlot >= Long.SIZE) {
        break;
      }
      final T entry = slots.get(nextUsedSlot);
      if (predicate.test(entry)) {
        usedSlots &= ~(1L << nextUsedSlot);
        slots.set(nextUsedSlot, null);
        return entry;
      } else {
        nextSlot = nextUsedSlot + 1;
      }
    } while (nextSlot < Long.SIZE);

    
    for (ListIterator<T> lit = slots.listIterator(slots.size());
        lit.previousIndex() >= Long.SIZE; ) {
      final T entry = lit.previous();
      if (predicate.test(entry)) {
        lit.remove();
        return entry;
      }
    }

   
    return null;
  }


  boolean contains(Object o) {
    if (o == null) {
      throw new NullPointerException();
    }
    return slots.contains(o);
  }

  boolean isEmpty() {
    return usedSlots == 0 && slots.size() == Long.SIZE;
  }

  boolean remove(Object o) {
    if (o == null) {
      throw new NullPointerException();
    }
    int index = slots.indexOf(o);
    if (index == -1) {
      return false;
    }
    if (index >= Long.SIZE) {
      slots.remove(index);
    } else {
      usedSlots &= ~(1L << index);
      slots.set(index, null);
    }
    return true;
  }
}
