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
package org.apache.pinot.core.common;

/**
 * The DataBlock an extension of {@link Block} that is composed of multiple blocks.
 * Functions and arguments can now take multiple arguments, so we need a block abstraction
 * that spans across multiple columns/expressions (arguments of function).
 *
 */
public interface DataBlock extends Block {

  /**
   * Returns the blockValSet for the given expression/column.
   *
   * @param expression Expression/Column for which to get the data block.
   * @return Returns the block for a given expression/column
   */
  BlockValSet getBlockValueSet(String expression);

  /**
   * Returns the number of docs in the DataBlock.
   *
   * @return Number of docs in the DataBlock
   */
  int getNumDocs();

  // TODO: Add getSchema() in future. Not adding now as no callers, as well as would need to define Schema class.
}
