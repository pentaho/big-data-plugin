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

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.hbase.shim.api.ColumnFilter;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Created by bryan on 2/9/16.
 */
public class ColumnFilterImplTest {
  private ColumnFilter delegate;
  private ColumnFilterImpl columnFilter;

  @Before
  public void setup() {
    delegate = mock( ColumnFilter.class );
    columnFilter = new ColumnFilterImpl( delegate );
  }

  @Test
  public void testGetFieldAlias() {
    String alias = "alias";
    when( delegate.getFieldAlias() ).thenReturn( alias );
    assertEquals( alias, columnFilter.getFieldAlias() );
  }

  @Test
  public void testSetFieldAlias() {
    String alias = "alias";
    columnFilter.setFieldAlias( alias );
    verify( delegate ).setFieldAlias( alias );
  }

  @Test
  public void testGetFieldType() {
    String type = "type";
    when( delegate.getFieldType() ).thenReturn( type );
    assertEquals( type, columnFilter.getFieldType() );
  }

  @Test
  public void testSetFieldType() {
    String type = "type";
    columnFilter.setFieldType( type );
    verify( delegate ).setFieldType( type );
  }

  @Test
  public void testGetComparisonOperator() {
    for ( org.pentaho.bigdata.api.hbase.mapping.ColumnFilter.ComparisonType comparisonType : org.pentaho.bigdata.api
      .hbase.mapping.ColumnFilter.ComparisonType.values() ) {
      when( delegate.getComparisonOperator() )
        .thenReturn( ColumnFilter.ComparisonType.valueOf( comparisonType.name() ) );
      assertEquals( comparisonType, columnFilter.getComparisonOperator() );
    }
    when( delegate.getComparisonOperator() ).thenReturn( null );
    assertNull( columnFilter.getComparisonOperator() );
  }

  @Test
  public void testSetComparisonOperator() {
    for ( org.pentaho.bigdata.api.hbase.mapping.ColumnFilter.ComparisonType comparisonType : org.pentaho.bigdata.api
      .hbase.mapping.ColumnFilter.ComparisonType.values() ) {
      columnFilter.setComparisonOperator( comparisonType );
      verify( delegate ).setComparisonOperator( ColumnFilter.ComparisonType.valueOf( comparisonType.name() ) );
    }
    columnFilter.setComparisonOperator( null );
    verify( delegate ).setComparisonOperator( null );
  }

  @Test
  public void testGetSignedComparison() {
    when( delegate.getSignedComparison() ).thenReturn( true ).thenReturn( false );
    assertTrue( columnFilter.getSignedComparison() );
    assertFalse( columnFilter.getSignedComparison() );
  }

  @Test
  public void testSetSignedComparison() {
    columnFilter.setSignedComparison( true );
    verify( delegate ).setSignedComparison( true );
    columnFilter.setSignedComparison( false );
    verify( delegate ).setSignedComparison( false );
  }

  @Test
  public void testGetConstant() {
    String constant = "constant";
    when( delegate.getConstant() ).thenReturn( constant );
    assertEquals( constant, columnFilter.getConstant() );
  }

  @Test
  public void testSetConstant() {
    String constant = "constant";
    columnFilter.setConstant( constant );
    verify( delegate ).setConstant( constant );
  }

  @Test
  public void testGetFormat() {
    String format = "format";
    when( delegate.getFormat() ).thenReturn( format );
    assertEquals( format, columnFilter.getFormat() );
  }

  @Test
  public void testSetFormat() {
    String format = "format";
    columnFilter.setFormat( format );
    verify( delegate ).setFormat( format );
  }

  @Test
  public void testAppendXml() {
    final String xml = "xml";
    doAnswer( new Answer<Void>() {
      @Override public Void answer( InvocationOnMock invocation ) throws Throwable {
        ( (StringBuffer) invocation.getArguments()[ 0 ] ).append( xml );
        return null;
      }
    } ).when( delegate ).appendXML( any( StringBuffer.class ) );
    StringBuilder stringBuilder = new StringBuilder();
    columnFilter.appendXML( stringBuilder );
    assertEquals( xml, stringBuilder.toString() );
  }

  @Test
  public void testSaveRep() throws KettleException {
    Repository repository = mock( Repository.class );
    ObjectId id_transformation = mock( ObjectId.class );
    ObjectId id_step = mock( ObjectId.class );
    int filterNum = 42;

    columnFilter.saveRep( repository, id_transformation, id_step, filterNum );
    verify( delegate ).saveRep( repository, id_transformation, id_step, filterNum );
  }
}
