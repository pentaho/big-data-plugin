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

import java.io.IOException;

/**
 * Created by bryan on 1/20/16.
 */
public interface HBasePut {
  void setWriteToWAL( boolean writeToWAL );

  void addColumn( String columnFamily, String columnName, boolean colNameIsBinary, byte[] colValue ) throws
    IOException;

  String createColumnName( String... parts );

  void execute() throws IOException;
}
