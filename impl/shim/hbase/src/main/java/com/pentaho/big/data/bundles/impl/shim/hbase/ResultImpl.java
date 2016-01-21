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

import org.pentaho.bigdata.api.hbase.Result;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;

import java.util.NavigableMap;

/**
 * Created by bryan on 1/22/16.
 */
public class ResultImpl implements Result {
  private final org.apache.hadoop.hbase.client.Result result;
  private final HBaseBytesUtilShim hBaseBytesUtilShim;

  public ResultImpl( org.apache.hadoop.hbase.client.Result result, HBaseBytesUtilShim hBaseBytesUtilShim ) {
    this.result = result;
    this.hBaseBytesUtilShim = hBaseBytesUtilShim;
  }

  @Override public byte[] getRow() {
    return result.getRow();
  }

  @Override public NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> getMap() {
    return result.getMap();
  }

  @Override public NavigableMap<byte[], byte[]> getFamilyMap( String familyName ) {
    return result.getFamilyMap( hBaseBytesUtilShim.toBytes( familyName ) );
  }

  @Override public byte[] getValue( String colFamilyName, String colName, boolean colNameIsBinary ) {
    return result.getValue( hBaseBytesUtilShim.toBytes( colFamilyName ),
      colNameIsBinary ? hBaseBytesUtilShim.toBytesBinary( colName ) : hBaseBytesUtilShim.toBytes( colName ) );
  }

  @Override public boolean isEmpty() {
    return result.isEmpty();
  }
}
