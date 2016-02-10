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

import com.pentaho.big.data.bundles.impl.shim.hbase.meta.HBaseValueMetaInterfaceFactoryImpl;
import com.pentaho.big.data.bundles.impl.shim.hbase.meta.HBaseValueMetaInterfaceImpl;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.pentaho.bigdata.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.hbase.shim.api.HBaseValueMeta;
import org.pentaho.hbase.shim.api.Mapping;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;
import org.w3c.dom.Node;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Created by bryan on 2/9/16.
 */
public class MappingImplTest {
  private Mapping delegate;
  private HBaseBytesUtilShim hBaseBytesUtilShim;
  private HBaseValueMetaInterfaceFactoryImpl
    hBaseValueMetaInterfaceFactory;
  private MappingImpl mapping;

  @Before
  public void setup() {
    delegate = mock( Mapping.class );
    hBaseBytesUtilShim = mock( HBaseBytesUtilShim.class );
    hBaseValueMetaInterfaceFactory = mock( HBaseValueMetaInterfaceFactoryImpl.class );
    mapping = new MappingImpl( delegate, hBaseBytesUtilShim, hBaseValueMetaInterfaceFactory );
  }

  @Test
  public void testAddMappedColumnHBaseValueMeta() throws Exception {
    String value = "value";
    String value2 = "value2";
    HBaseValueMetaInterfaceImpl column = mock( HBaseValueMetaInterfaceImpl.class );
    when( delegate.addMappedColumn( column, true ) ).thenReturn( value );
    when( delegate.addMappedColumn( column, false ) ).thenReturn( value2 );
    assertEquals( value, mapping.addMappedColumn( column, true ) );
    assertEquals( value2, mapping.addMappedColumn( column, false ) );
  }

  @Test
  public void testAddMappedColumnHBaseValueMetaInterface() throws Exception {
    String value = "value";
    String value2 = "value2";
    HBaseValueMetaInterface hBaseValueMetaInterface = mock( HBaseValueMetaInterface.class );
    HBaseValueMetaInterfaceImpl column = mock( HBaseValueMetaInterfaceImpl.class );
    when( hBaseValueMetaInterfaceFactory.copy( hBaseValueMetaInterface ) ).thenReturn( column );
    when( delegate.addMappedColumn( column, true ) ).thenReturn( value );
    when( delegate.addMappedColumn( column, false ) ).thenReturn( value2 );
    assertEquals( value, mapping.addMappedColumn( hBaseValueMetaInterface, true ) );
    assertEquals( value2, mapping.addMappedColumn( hBaseValueMetaInterface, false ) );
  }

  @Test
  public void testGetTableName() {
    String name = "name";
    when( delegate.getTableName() ).thenReturn( name );
    assertEquals( name, mapping.getTableName() );
  }

  @Test
  public void testSetTableName() {
    String name = "name";
    mapping.setTableName( name );
    verify( delegate ).setTableName( name );
  }

  @Test
  public void testGetMappingName() {
    String name = "name";
    when( delegate.getMappingName() ).thenReturn( name );
    assertEquals( name, mapping.getMappingName() );
  }

  @Test
  public void testSetMappingName() {
    String name = "name";
    mapping.setMappingName( name );
    verify( delegate ).setMappingName( name );
  }

  @Test
  public void testGetKeyName() {
    String name = "name";
    when( delegate.getKeyName() ).thenReturn( name );
    assertEquals( name, mapping.getKeyName() );
  }

  @Test
  public void testSetKeyName() {
    String name = "name";
    mapping.setKeyName( name );
    verify( delegate ).setKeyName( name );
  }

  @Test
  public void testSetKeyTypeAsString() throws Exception {
    String type = "type";
    mapping.setKeyTypeAsString( type );
    verify( delegate ).setKeyTypeAsString( type );
  }

  @Test
  public void testGetKeyType() {
    for ( Mapping.KeyType keyType : Mapping.KeyType.values() ) {
      when( delegate.getKeyType() ).thenReturn( keyType );
      org.pentaho.bigdata.api.hbase.mapping.Mapping.KeyType type = mapping.getKeyType();
      assertNotNull( type );
      assertEquals( keyType.name(), type.name() );
    }
    when( delegate.getKeyType() ).thenReturn( null );
    assertNull( mapping.getKeyType() );
  }

  @Test
  public void testSetKeyType() {
    for ( org.pentaho.bigdata.api.hbase.mapping.Mapping.KeyType keyType : org.pentaho.bigdata.api.hbase.mapping
      .Mapping.KeyType.values() ) {
      mapping.setKeyType( keyType );
      Mapping.KeyType type = Mapping.KeyType.valueOf( keyType.name() );
      verify( delegate ).setKeyType( type );
      assertNotNull( type );
      assertEquals( keyType.name(), type.name() );
    }
    mapping.setKeyType( null );
    verify( delegate ).setKeyType( null );
  }

  @Test
  public void testIsTupleMapping() {
    when( delegate.isTupleMapping() ).thenReturn( true ).thenReturn( false );
    assertTrue( mapping.isTupleMapping() );
    assertFalse( mapping.isTupleMapping() );
  }

  @Test
  public void testSetTupleMapping() {
    mapping.setTupleMapping( true );
    verify( delegate ).setTupleMapping( true );
    mapping.setTupleMapping( false );
    verify( delegate ).setTupleMapping( false );
  }

  @Test
  public void testGetTupleFamilies() {
    String families = "families";
    when( delegate.getTupleFamilies() ).thenReturn( families );
    assertEquals( families, mapping.getTupleFamilies() );
  }

  @Test
  public void testSetTupleFamilies() {
    String families = "families";
    mapping.setTupleFamilies( families );
    verify( delegate ).setTupleFamilies( families );
  }

  @Test
  public void testNumMappedColumns() {
    int num = 42;
    Map map = mock( Map.class );
    when( map.size() ).thenReturn( num );
    when( delegate.getMappedColumns() ).thenReturn( map );
    assertEquals( num, mapping.numMappedColumns() );
  }

  @Test
  public void testGetTupleFamiliesSplit() {
    String family1 = "family1";
    String family2 = "family2";
    String families = family1 + HBaseValueMeta.SEPARATOR + family2;
    when( delegate.getTupleFamilies() ).thenReturn( families );
    assertArrayEquals( new String[] { family1, family2 }, mapping.getTupleFamiliesSplit() );
  }

  @Test
  public void testGetMappedColumns() {
    String cast = "cast";
    HBaseValueMetaInterfaceImpl hBaseValueMetaInterface = mock( HBaseValueMetaInterfaceImpl.class );
    String copy = "copy";
    HBaseValueMeta hBaseValueMeta = mock( HBaseValueMeta.class );
    HBaseValueMetaInterfaceImpl hBaseValueMetaInterface2 = mock( HBaseValueMetaInterfaceImpl.class );
    when( hBaseValueMetaInterfaceFactory.copy( hBaseValueMeta ) ).thenReturn( hBaseValueMetaInterface2 );

    Map<String, HBaseValueMeta> hashMap = new HashMap<>();
    hashMap.put( cast, hBaseValueMetaInterface );
    hashMap.put( copy, hBaseValueMeta );
    when( delegate.getMappedColumns() ).thenReturn( hashMap );

    Map<String, HBaseValueMetaInterface> mappedColumns = mapping.getMappedColumns();
    assertEquals( 2, mappedColumns.size() );
    assertEquals( hBaseValueMetaInterface, mappedColumns.get( cast ) );
    assertEquals( hBaseValueMetaInterface2, mappedColumns.get( copy ) );
  }

  @Test
  public void testSetMappedColumns() {
    String cast = "cast";
    HBaseValueMetaInterfaceImpl hBaseValueMetaInterfaceImpl = mock( HBaseValueMetaInterfaceImpl.class );
    String copy = "copy";
    HBaseValueMetaInterface hBaseValueMetaInterface = mock( HBaseValueMetaInterface.class );
    HBaseValueMetaInterfaceImpl hBaseValueMetaInterfaceImpl2 = mock( HBaseValueMetaInterfaceImpl.class );
    when( hBaseValueMetaInterfaceFactory.copy( hBaseValueMetaInterface ) ).thenReturn( hBaseValueMetaInterfaceImpl2 );

    Map<String, HBaseValueMetaInterface> map = new HashMap<>();
    map.put( cast, hBaseValueMetaInterfaceImpl );
    map.put( copy, hBaseValueMetaInterface );

    mapping.setMappedColumns( map );
    ArgumentCaptor<Map> mapArgumentCaptor = ArgumentCaptor.forClass( Map.class );
    verify( delegate ).setMappedColumns( mapArgumentCaptor.capture() );

    Map<String, HBaseValueMeta> value = mapArgumentCaptor.getValue();
    assertEquals( 2, value.size() );
    assertEquals( hBaseValueMetaInterfaceImpl, value.get( cast ) );
    assertEquals( hBaseValueMetaInterfaceImpl2, value.get( copy ) );
  }

  @Test
  public void testSaveRep() throws KettleException {
    Repository repository = mock( Repository.class );
    ObjectId id_transformation = mock( ObjectId.class );
    ObjectId id_step = mock( ObjectId.class );

    mapping.saveRep( repository, id_transformation, id_step );
    verify( delegate ).saveRep( repository, id_transformation, id_step );
  }

  @Test
  public void testGetXml() {
    String xml = "xml";
    when( delegate.getXML() ).thenReturn( xml );
    assertEquals( xml, mapping.getXML() );
  }

  @Test
  public void testLoadXml() throws KettleXMLException {
    Node node = mock( Node.class );
    when( delegate.loadXML( node ) ).thenReturn( true ).thenReturn( false);

    assertTrue( mapping.loadXML( node ) );
    assertFalse( mapping.loadXML( node ) );
  }

  @Test
  public void testReadRep() throws KettleException {
    Repository repository = mock( Repository.class );
    ObjectId id_step = mock( ObjectId.class );
    when( delegate.readRep( repository, id_step ) ).thenReturn( true ).thenReturn( false );

    assertTrue( mapping.readRep( repository, id_step ) );
    assertFalse( mapping.readRep( repository, id_step ) );
  }

  @Test
  public void testGetFriendlyName() {
    String mappingName = "mapping";
    String table = "table";
    String friendly = mappingName + HBaseValueMeta.SEPARATOR + table;
    when( delegate.getMappingName() ).thenReturn( mappingName );
    when( delegate.getTableName() ).thenReturn( table );

    assertEquals( friendly, mapping.getFriendlyName() );
  }

  @Test
  public void testDecodeKeyValue() throws KettleException {
    String result = "result";
    byte[] rawVal = result.getBytes( Charset.forName( "UTF-8" ) );

    when( delegate.getKeyType() ).thenReturn( Mapping.KeyType.STRING );
    when( hBaseBytesUtilShim.toString( rawVal ) ).thenReturn( result );

    assertEquals( result, mapping.decodeKeyValue( rawVal ) );
  }

  @Test
  public void testToString() {
    String string = "string";
    when( delegate.toString() ).thenReturn( string );
    assertEquals( string, mapping.toString() );
  }
}
