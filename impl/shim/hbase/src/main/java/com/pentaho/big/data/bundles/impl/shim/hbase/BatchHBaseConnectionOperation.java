/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package com.pentaho.big.data.bundles.impl.shim.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by bryan on 1/26/16.
 */
public class BatchHBaseConnectionOperation implements HBaseConnectionOperation {
  private final List<HBaseConnectionOperation> hBaseConnectionOperations;

  public BatchHBaseConnectionOperation() {
    hBaseConnectionOperations = new ArrayList<>();
  }

  public void addOperation( HBaseConnectionOperation hBaseConnectionOperation ) {
    hBaseConnectionOperations.add( hBaseConnectionOperation );
  }

  @Override public void perform( HBaseConnectionWrapper hBaseConnectionWrapper ) throws IOException {
    for ( HBaseConnectionOperation hBaseConnectionOperation : hBaseConnectionOperations ) {
      hBaseConnectionOperation.perform( hBaseConnectionWrapper );
    }
  }
}
