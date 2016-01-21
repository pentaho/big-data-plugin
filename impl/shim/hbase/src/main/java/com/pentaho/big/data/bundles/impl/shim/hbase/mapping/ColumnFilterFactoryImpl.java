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

package com.pentaho.big.data.bundles.impl.shim.hbase.mapping;

import org.pentaho.bigdata.api.hbase.mapping.ColumnFilter;
import org.pentaho.bigdata.api.hbase.mapping.ColumnFilterFactory;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.w3c.dom.Node;

/**
 * Created by bryan on 1/21/16.
 */
public class ColumnFilterFactoryImpl implements ColumnFilterFactory {
  @Override public ColumnFilter createFilter( Node filterNode ) {
    return new ColumnFilterImpl( org.pentaho.hbase.shim.api.ColumnFilter.getFilter( filterNode ) );
  }

  @Override public ColumnFilter createFilter( Repository rep, int nodeNum, ObjectId id_step ) throws KettleException {
    return new ColumnFilterImpl( org.pentaho.hbase.shim.api.ColumnFilter.getFilter( rep, nodeNum, id_step ) );
  }

  @Override public ColumnFilter createFilter( String alias ) {
    return new ColumnFilterImpl( new org.pentaho.hbase.shim.api.ColumnFilter( alias ) );
  }
}
