/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.upsert.merger.columnar;

import org.apache.pinot.spi.config.table.UpsertConfig;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class PartialUpsertColumnMergerTest {

  @Test
  public void testAppendMergers() {
    AppendMerger appendMerger = (AppendMerger) PartialUpsertColumnMergerFactory.getMerger(UpsertConfig.Strategy.APPEND);

    Integer[] array1 = {1, 2, 3};
    Integer[] array2 = {3, 4, 6};

    assertEquals(new Integer[]{1, 2, 3, 3, 4, 6}, appendMerger.merge(array1, array2));
  }

  @Test
  public void testIncrementMergers() {
    IncrementMerger incrementMerger =
        (IncrementMerger) PartialUpsertColumnMergerFactory.getMerger(UpsertConfig.Strategy.INCREMENT);
    assertEquals(3, incrementMerger.merge(1, 2));
  }

  @Test
  public void testIgnoreMergers() {
    IgnoreMerger ignoreMerger = (IgnoreMerger) PartialUpsertColumnMergerFactory.getMerger(UpsertConfig.Strategy.IGNORE);
    assertEquals(null, ignoreMerger.merge(null, 3));
    assertEquals(3, ignoreMerger.merge(3, null));
  }

  @Test
  public void testMaxMinMergers() {
    MaxMerger maxMerger = (MaxMerger) PartialUpsertColumnMergerFactory.getMerger(UpsertConfig.Strategy.MAX);
    MinMerger minMerger = (MinMerger) PartialUpsertColumnMergerFactory.getMerger(UpsertConfig.Strategy.MIN);
    assertEquals(1, maxMerger.merge(0, 1));
    assertEquals(0, minMerger.merge(0, 1));
    assertEquals(1, maxMerger.merge(1, 0));
    assertEquals(0, minMerger.merge(1, 0));
  }

  @Test
  public void testOverwriteMergers() {
    OverwriteMerger overwriteMerger =
        (OverwriteMerger) PartialUpsertColumnMergerFactory.getMerger(UpsertConfig.Strategy.OVERWRITE);
    assertEquals("newValue", overwriteMerger.merge("oldValue", "newValue"));
  }

  @Test
  public void testUnionMergers() {
    UnionMerger unionMerger = (UnionMerger) PartialUpsertColumnMergerFactory.getMerger(UpsertConfig.Strategy.UNION);

    String[] array1 = {"a", "b", "c"};
    String[] array2 = {"c", "d", "e"};

    assertEquals(new String[]{"a", "b", "c", "d", "e"}, unionMerger.merge(array1, array2));
  }
}
