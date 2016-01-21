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
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;

/**
 * Created by bryan on 1/21/16.
 */
public class ColumnFilterImpl implements ColumnFilter {
  private final org.pentaho.hbase.shim.api.ColumnFilter delegate;

  public ColumnFilterImpl( org.pentaho.hbase.shim.api.ColumnFilter delegate ) {
    this.delegate = delegate;
  }

  @Override public String getFieldAlias() {
    return delegate.getFieldAlias();
  }

  @Override public void setFieldAlias( String alias ) {
    delegate.setFieldAlias( alias );
  }

  @Override public String getFieldType() {
    return delegate.getFieldType();
  }

  @Override public void setFieldType( String type ) {
    delegate.setFieldType( type );
  }

  @Override public ComparisonType getComparisonOperator() {
    org.pentaho.hbase.shim.api.ColumnFilter.ComparisonType comparisonOperator = delegate.getComparisonOperator();
    if ( comparisonOperator == null ) {
      return null;
    }
    return ComparisonType.valueOf( comparisonOperator.name() );
  }

  @Override public void setComparisonOperator( ComparisonType c ) {
    if ( c == null ) {
      delegate.setComparisonOperator( null );
    } else {
      delegate.setComparisonOperator( org.pentaho.hbase.shim.api.ColumnFilter.ComparisonType.valueOf( c.name() ) );
    }
  }

  @Override public boolean getSignedComparison() {
    return delegate.getSignedComparison();
  }

  @Override public void setSignedComparison( boolean signed ) {
    delegate.setSignedComparison( signed );
  }

  @Override public String getConstant() {
    return delegate.getConstant();
  }

  @Override public void setConstant( String constant ) {
    delegate.setConstant( constant );
  }

  @Override public String getFormat() {
    return delegate.getFormat();
  }

  @Override public void setFormat( String format ) {
    delegate.setFormat( format );
  }

  @Override public void appendXML( StringBuilder buff ) {
    StringBuffer stringBuffer = new StringBuffer();
    delegate.appendXML( stringBuffer );
    buff.append( stringBuffer );
  }

  @Override public void saveRep( Repository rep, ObjectId id_transformation, ObjectId id_step, int filterNum )
    throws KettleException {
    delegate.saveRep( rep, id_transformation, id_step, filterNum );
  }
}
