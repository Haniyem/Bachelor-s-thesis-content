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

import java.io.IOException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;


public abstract class DocValuesSkipper {

 
  public abstract void advance(int target) throws IOException;

  public abstract int numLevels();

  
  public abstract int minDocID(int level);

  
  public abstract int maxDocID(int level);

  
  public abstract long minValue(int level);

  
  public abstract long maxValue(int level);

  public abstract int docCount(int level);

  public abstract long minValue();

 
  public abstract long maxValue();

  
  public abstract int docCount();

 
  public final void advance(long minValue, long maxValue) throws IOException {
    if (minDocID(0) == -1) {
      
      advance(0);
    }
    
    while (minDocID(0) != DocIdSetIterator.NO_MORE_DOCS
        && ((minValue(0) > maxValue || maxValue(0) < minValue))) {
      int maxDocID = maxDocID(0);
      int nextLevel = 1;
     
      while (nextLevel < numLevels()
          && (minValue(nextLevel) > maxValue || maxValue(nextLevel) < minValue)) {
        maxDocID = maxDocID(nextLevel);
        nextLevel++;
      }
      advance(maxDocID + 1);
    }
  }

  
  public static long globalMinValue(IndexSearcher searcher, String field) throws IOException {
    long minValue = Long.MAX_VALUE;
    for (LeafReaderContext ctx : searcher.getLeafContexts()) {
      if (ctx.reader().getFieldInfos().fieldInfo(field) == null) {
        continue; // no field values in this segment, so we can ignore it
      }
      DocValuesSkipper skipper = ctx.reader().getDocValuesSkipper(field);
      if (skipper == null) {
        
        return Long.MIN_VALUE;
      } else {
        minValue = Math.min(minValue, skipper.minValue());
      }
    }
    return minValue;
  }

  
  public static long globalMaxValue(IndexSearcher searcher, String field) throws IOException {
    long maxValue = Long.MIN_VALUE;
    for (LeafReaderContext ctx : searcher.getLeafContexts()) {
      if (ctx.reader().getFieldInfos().fieldInfo(field) == null) {
        continue; 
      }
      DocValuesSkipper skipper = ctx.reader().getDocValuesSkipper(field);
      if (skipper == null) {
        
        return Long.MAX_VALUE;
      } else {
        maxValue = Math.max(maxValue, skipper.maxValue());
      }
    }
    return maxValue;
  }

  
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
