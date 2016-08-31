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

package com.pentaho.big.data.bundles.impl.shim.hbase.meta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.Charset;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.AbstractRepository;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

public class HBaseValueMetaInterfaceImplTest {

  /**
   *
   */
  private static final int NUMBER = 1;
  private static final String[] INDEXES = new String[] { "oneValue", "twoValue " };
  private static final String EXPECTED_INDEXES_ROW = buildIndexesRow( INDEXES );
  private static final String XML_TAG_FIELD = "field";

  private Repository repMock = mock( Repository.class );
  private ObjectId idTransfIdMock = mock( ObjectId.class );
  private ObjectId idStepIdMock = mock( ObjectId.class );
  private HBaseBytesUtilShim hBaseBytesUtilShim;
  private String name;
  private int type;
  private int length;
  private int precision;
  private HBaseValueMetaInterfaceImpl hBaseValueMetaInterface;

  abstract class TestRepo extends AbstractRepository {
    public TestRepo() {
    }
  }

  @Before
  public void setup() {
    hBaseBytesUtilShim = mock( HBaseBytesUtilShim.class );
    name = "columnFamily,column,alias";
    type = 2;
    length = 100;
    precision = 100;
    hBaseValueMetaInterface = new HBaseValueMetaInterfaceImpl( name, type, length, precision, hBaseBytesUtilShim );
  }

  @Test
  public void testDecodeColumnValue() throws KettleException {
    String result = "result";
    byte[] rawVal = result.getBytes( Charset.forName( "UTF-8" ) );
    when( hBaseBytesUtilShim.toString( rawVal ) ).thenReturn( result );
    assertEquals( result, hBaseValueMetaInterface.decodeColumnValue( rawVal ) );
  }

  @Test
  public void testEncodeColumnValue() throws KettleException {
    String rawVal = "result";
    byte[] result = rawVal.getBytes( Charset.forName( "UTF-8" ) );
    when( hBaseBytesUtilShim.toBytes( rawVal ) ).thenReturn( result );
    assertEquals( result, hBaseValueMetaInterface.encodeColumnValue( rawVal, new ValueMetaString() ) );
  }

  @Test
  public void testGetXml_StandartCase() throws KettleException, ParserConfigurationException {
    StringBuilder result = new StringBuilder();
    assertEquals( 0, result.length() );
    hBaseValueMetaInterface.getXml( result );
    verifyXml( hBaseValueMetaInterface, result );
  }

  @Test
  public void testGetXml_IndexedStorageType() throws KettleException, ParserConfigurationException {
    hBaseValueMetaInterface.setStorageType( ValueMetaInterface.STORAGE_TYPE_INDEXED );
    hBaseValueMetaInterface.setIndex( INDEXES );
    StringBuilder result = new StringBuilder();
    assertEquals( 0, result.length() );
    hBaseValueMetaInterface.getXml( result );
    verifyXml( hBaseValueMetaInterface, result, true );
  }

  @Test
  public void testSaveRep_StandartCase() throws KettleException {
    hBaseValueMetaInterface.saveRep( repMock, idTransfIdMock, idStepIdMock, NUMBER );
    verifyRepo( idTransfIdMock, idStepIdMock, NUMBER );
  }

  @Test
  public void testSaveRep_IndexedStorageType() throws KettleException {
    hBaseValueMetaInterface.setStorageType( ValueMetaInterface.STORAGE_TYPE_INDEXED );
    hBaseValueMetaInterface.setIndex( INDEXES );
    hBaseValueMetaInterface.saveRep( repMock, idTransfIdMock, idStepIdMock, NUMBER );
    verifyRepo( idTransfIdMock, idStepIdMock, NUMBER, true );
  }

  private void verifyRepo( ObjectId arg0, ObjectId arg1, int arg2 ) throws KettleException {
    verifyRepo( arg0, arg1, arg2, false );
  }

  private void verifyRepo( ObjectId arg0, ObjectId arg1, int arg2, boolean isIndexedStorageType ) throws KettleException {
    verify( repMock, times( 1 ) ).saveStepAttribute( arg0, arg1, NUMBER, "table_name", hBaseValueMetaInterface.getTableName() );
    verify( repMock ).saveStepAttribute( arg0, arg1, NUMBER, "mapping_name", hBaseValueMetaInterface.getMappingName() );
    verify( repMock ).saveStepAttribute( arg0, arg1, NUMBER, "alias", hBaseValueMetaInterface.getAlias() );
    verify( repMock ).saveStepAttribute( arg0, arg1, NUMBER, "family", hBaseValueMetaInterface.getColumnFamily() );
    verify( repMock ).saveStepAttribute( arg0, arg1, NUMBER, "column", hBaseValueMetaInterface.getColumnName() );
    verify( repMock ).saveStepAttribute( arg0, arg1, NUMBER, "key", hBaseValueMetaInterface.isKey() );
    verify( repMock ).saveStepAttribute( arg0, arg1, NUMBER, "type", ValueMeta.getTypeDesc( hBaseValueMetaInterface.getType() ) );
    verify( repMock ).saveStepAttribute( arg0, arg1, NUMBER, "format", hBaseValueMetaInterface.getConversionMask() );
    int i = isIndexedStorageType ? 1 : 0;
    verify( repMock, times( i ) ).saveStepAttribute( arg0, arg1, NUMBER, "index_values", isIndexedStorageType ? EXPECTED_INDEXES_ROW : null );
  }

  private void verifyXml( HBaseValueMetaInterfaceImpl toVerify, StringBuilder result ) throws KettleXMLException, ParserConfigurationException {
    verifyXml( toVerify, result, false );
  }

  private void verifyXml( HBaseValueMetaInterfaceImpl toVerify, StringBuilder result, boolean isIndexedStorageType ) throws KettleXMLException, ParserConfigurationException {
    assertNotEquals( 0, result.length() );

    Document document = loadDocumentFromString( result );

    Node node = XMLHandler.getSubNode( document, XML_TAG_FIELD );
    assertEquals( hBaseValueMetaInterface.getTableName(), XMLHandler.getTagValue( node, "table_name" ) );
    assertEquals( hBaseValueMetaInterface.getMappingName(), XMLHandler.getTagValue( node, "mapping_name" ) );
    assertEquals( hBaseValueMetaInterface.getAlias(), XMLHandler.getTagValue( node, "alias" ) );
    assertEquals( hBaseValueMetaInterface.getColumnFamily(), XMLHandler.getTagValue( node, "family" ) );
    assertEquals( hBaseValueMetaInterface.getColumnName(), XMLHandler.getTagValue( node, "column" ) );
    assertEquals( hBaseValueMetaInterface.isKey() ? "Y" : "N", XMLHandler.getTagValue( node, "key" ) );
    assertEquals( ValueMeta.getTypeDesc( hBaseValueMetaInterface.getType() ), XMLHandler.getTagValue( node, "type" ) );
    assertEquals( hBaseValueMetaInterface.getConversionMask(), XMLHandler.getTagValue( node, "format" ) );
    assertEquals( isIndexedStorageType ? EXPECTED_INDEXES_ROW : null, XMLHandler.getTagValue( node, "index_values" ) );
  }

  private Document loadDocumentFromString( StringBuilder result ) throws KettleXMLException, ParserConfigurationException {
    DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
    Document document = XMLHandler.loadXMLString( docBuilderFactory.newDocumentBuilder(), result.toString() );
    return document;
  }

  private static String buildIndexesRow( String[] indexes ) {
    StringBuffer vals = new StringBuffer();
    vals.append( "{" );
    for ( int i = 0; i < indexes.length; i++ ) {
      if ( i != indexes.length - 1 ) {
        vals.append( indexes[i].toString().trim() ).append( "," );
      } else {
        vals.append( indexes[i].toString().trim() ).append( "}" );
      }
    }
    return vals.toString();
  };

}
