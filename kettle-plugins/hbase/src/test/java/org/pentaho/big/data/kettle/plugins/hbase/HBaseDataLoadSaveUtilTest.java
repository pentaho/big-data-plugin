/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
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
package org.pentaho.big.data.kettle.plugins.hbase;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.bigdata.api.hbase.mapping.ColumnFilter;
import org.pentaho.bigdata.api.hbase.mapping.ColumnFilterFactory;
import org.pentaho.bigdata.api.hbase.mapping.Mapping;
import org.pentaho.bigdata.api.hbase.mapping.MappingFactory;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.w3c.dom.Node;

/**
 * @author Tatsiana_Kasiankova
 *
 */
public class HBaseDataLoadSaveUtilTest {
  private MappingFactory mappingFactoryMock = mock( MappingFactory.class );
  private ColumnFilterFactory cfFactoryMock = mock( ColumnFilterFactory.class );
  private Mapping mappingMock = mock( Mapping.class );
  private Repository repMock = mock( Repository.class );
  private ObjectId objIdMock = mock( ObjectId.class );
  private final static String STEP_NODE_STRING = "<step_node/>";
  private Node stepNode;

  @Before
  public void setUp() throws KettleXMLException {
    stepNode = XMLHandler.loadXMLString( STEP_NODE_STRING );

    when( mappingFactoryMock.createMapping() ).thenReturn( mappingMock );

  }

  @Test
  public void testLoadMappingFromNodeSuccessfully() throws KettleException {
    // Mapping loaded successfully from Node
    when( mappingMock.loadXML( stepNode ) ).thenReturn( true );

    verify( mappingMock, times( 0 ) ).loadXML( stepNode );
    assertSame( mappingMock, HBaseDataLoadSaveUtil.loadMapping( stepNode, mappingFactoryMock ) );
    verify( mappingMock, times( 1 ) ).loadXML( stepNode );
  }

  @Test
  public void testLoadMappingFromNodeReturnsNull() throws KettleException {
    // Mapping NOT loaded successfully from Node
    when( mappingMock.loadXML( stepNode ) ).thenReturn( false );

    verify( mappingMock, times( 0 ) ).loadXML( stepNode );
    assertNull( HBaseDataLoadSaveUtil.loadMapping( stepNode, mappingFactoryMock ) );
    verify( mappingMock, times( 1 ) ).loadXML( stepNode );
  }

  @Test
  public void testLoadMappingFromRepoSuccessfully() throws KettleException {
    // Mapping loaded successfully from repo
    when( mappingMock.readRep( repMock, objIdMock ) ).thenReturn( true );

    verify( mappingMock, times( 0 ) ).readRep( repMock, objIdMock );
    assertSame( mappingMock, HBaseDataLoadSaveUtil.loadMapping( repMock, objIdMock, mappingFactoryMock ) );
    verify( mappingMock, times( 1 ) ).readRep( repMock, objIdMock );
  }

  @Test
  public void testLoadMappingFromRepoReturnsNull() throws KettleException {
    // Mapping NOT loaded successfully from repo
    when( mappingMock.readRep( repMock, objIdMock ) ).thenReturn( false );

    verify( mappingMock, times( 0 ) ).readRep( repMock, objIdMock );
    assertNull( HBaseDataLoadSaveUtil.loadMapping( repMock, objIdMock, mappingFactoryMock ) );
    verify( mappingMock, times( 1 ) ).readRep( repMock, objIdMock );
  }

  @Test
  public void testLoadFiltersFromNodeSuccessfully() throws KettleException {
    stepNode = XMLHandler.loadXMLFile( "src/test/resources/Filters.xml" );
    int expectedFiltersNb = 2;
    List<ColumnFilter> clFilterList = getExpectedFilterMocks( expectedFiltersNb );
    verify( cfFactoryMock, times( 0 ) ).createFilter( any( Node.class ) );
    when( cfFactoryMock.createFilter( any( Node.class ) ) ).thenAnswer( i -> getColumnFilterMock( clFilterList, (Node) i.getArguments()[0] ) );
    List<ColumnFilter> actualFilters = HBaseDataLoadSaveUtil.loadFilters( stepNode, cfFactoryMock );
    assertNotNull( actualFilters );
    assertEquals( expectedFiltersNb, actualFilters.size() );
    verify( cfFactoryMock, times( 2 ) ).createFilter( any( Node.class ) );
    for ( int i = 0; i < expectedFiltersNb; i++ ) {
      assertSame( clFilterList.get( i ), actualFilters.get( i ) );
    }
  }

  @Test
  public void testLoadFiltersFromNodeReturnsNull() throws KettleException {
    List<ColumnFilter> actualFilters = HBaseDataLoadSaveUtil.loadFilters( stepNode, cfFactoryMock );
    assertNull( actualFilters );
  }

  @Test
  public void testLoadFiltersFromRepoSuccessfully() throws KettleException {
    when( repMock.countNrStepAttributes( objIdMock, "cf_comparison_opp" ) ).thenReturn( 2 );
    int expectedFiltersNb = 2;
    List<ColumnFilter> clFilterList = getExpectedFilterMocks( expectedFiltersNb );
    verify( cfFactoryMock, times( 0 ) ).createFilter( eq( repMock ), anyInt(), eq( objIdMock ) );
    when( cfFactoryMock.createFilter( eq( repMock ), anyInt(), eq( objIdMock ) ) ).thenAnswer( i -> getColumnFilterMock( clFilterList, (Integer) i.getArguments()[1] ) );
    List<ColumnFilter> actualFilters = HBaseDataLoadSaveUtil.loadFilters( repMock, objIdMock, cfFactoryMock );
    assertNotNull( actualFilters );
    assertEquals( expectedFiltersNb, actualFilters.size() );
    for ( int i = 0; i < expectedFiltersNb; i++ ) {
      assertSame( clFilterList.get( i ), actualFilters.get( i ) );
    }
  }

  @Test
  public void testLoadFiltersFromRepoReturnsNull() throws KettleException {
    when( repMock.countNrStepAttributes( objIdMock, "cf_comparison_opp" ) ).thenReturn( 0 );
    List<ColumnFilter> actualFilters = HBaseDataLoadSaveUtil.loadFilters( repMock, objIdMock, cfFactoryMock );
    assertNull( actualFilters );
  }

  private List<ColumnFilter> getExpectedFilterMocks( int filterNb ) {
    List<ColumnFilter> colFilters = new ArrayList<ColumnFilter>( filterNb );
    for ( int i = 0; i < filterNb; i++ ) {
      colFilters.add( mock( ColumnFilter.class ) );
    }
    return colFilters;
  }

  private ColumnFilter getColumnFilterMock( List<ColumnFilter> filters, Node node ) {
    int nb = Integer.parseInt( XMLHandler.getTagValue( node, "alias" ) );
    return filters.get( nb );
  }

  private ColumnFilter getColumnFilterMock( List<ColumnFilter> filters, int i ) {
    return filters.get( i );
  }

}
