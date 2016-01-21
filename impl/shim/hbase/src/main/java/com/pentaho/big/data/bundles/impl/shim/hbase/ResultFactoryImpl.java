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
import org.pentaho.bigdata.api.hbase.ResultFactory;
import org.pentaho.bigdata.api.hbase.ResultFactoryException;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;

/**
 * Created by bryan on 1/29/16.
 */
public class ResultFactoryImpl implements ResultFactory {
  private final HBaseBytesUtilShim hBaseBytesUtilShim;

  public ResultFactoryImpl( HBaseBytesUtilShim hBaseBytesUtilShim ) {
    this.hBaseBytesUtilShim = hBaseBytesUtilShim;
  }

  @Override public boolean canHandle( Object object ) {
    return object == null || object instanceof org.apache.hadoop.hbase.client.Result;
  }

  @Override public Result create( Object object ) throws ResultFactoryException {
    if ( object == null ) {
      return null;
    }
    try {
      return new ResultImpl( (org.apache.hadoop.hbase.client.Result) object, hBaseBytesUtilShim );
    } catch ( ClassCastException e ) {
      throw new ResultFactoryException( e );
    }
  }
}
