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

package org.pentaho.bigdata.api.hbase.table;

import org.pentaho.bigdata.api.hbase.mapping.ColumnFilter;
import org.pentaho.bigdata.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;

import java.io.IOException;

/**
 * Created by bryan on 1/19/16.
 */
public interface ResultScannerBuilder {
  void addColumnToScan( String colFamilyName, String colName, boolean colNameIsBinary ) throws IOException;

  void addColumnFilterToScan( ColumnFilter cf, HBaseValueMetaInterface columnMeta, VariableSpace vars,
                              boolean matchAny )
    throws IOException;

  void setCaching( int cacheSize );

  ResultScanner build() throws IOException;
}
