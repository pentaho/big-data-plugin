/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.big.data.kettle.plugins.hive;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBigNumber;
import org.pentaho.di.core.row.value.ValueMetaBoolean;
import org.pentaho.di.core.row.value.ValueMetaDate;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaInternetAddress;
import org.pentaho.di.core.row.value.ValueMetaNumber;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.row.value.ValueMetaTimestamp;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.jdbc.DriverLocator;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.locator.api.MetastoreLocator;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Driver;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 4/14/16.
 */
@RunWith( MockitoJUnitRunner.class )
public class Hive2DatabaseMetaTest {
  public static final String LOCALHOST = "localhost";
  public static final String PORT = "10000";
  public static final String DEFAULT = "default";
  @Mock DriverLocator driverLocator;
  @Mock Driver driver;
  @Mock NamedClusterService namedClusterService;
  @Mock MetastoreLocator metastoreLocator;
  @Mock IMetaStore iMetaStore;
  Hive2DatabaseMeta hive2DatabaseMeta;
  private String hive2DatabaseMetaURL;
  private List<String> namedClusterList = Arrays.asList( new String[]{ "cluster1", "cluster2" } );
  ArgumentCaptor<IMetaStore> iMetaStoreCaptor = ArgumentCaptor.forClass( IMetaStore.class );
  private static String CLUSTER = "cluster1";
  private static String PLUGIN_ID = "hive2";

  @Before
  public void setup() throws Throwable {
    hive2DatabaseMeta = new Hive2DatabaseMeta( driverLocator, namedClusterService, metastoreLocator );
    hive2DatabaseMetaURL = hive2DatabaseMeta.getURL( LOCALHOST, PORT, DEFAULT );
    when( driverLocator.getDriver( hive2DatabaseMetaURL ) ).thenReturn( driver );
    when( metastoreLocator.getMetastore() ).thenReturn( iMetaStore );
    when( namedClusterService.listNames( any() ) ).thenReturn( namedClusterList );
  }

  @Test
  public void testGetAccessTypeList() {
    assertArrayEquals( Hive2DatabaseMeta.ACCESS_TYPE_LIST, hive2DatabaseMeta.getAccessTypeList() );
  }

  @Test
  public void testGetUsedLibraries() {
    assertArrayEquals( new String[] { Hive2DatabaseMeta.JAR_FILE }, hive2DatabaseMeta.getUsedLibraries() );
  }

  @Test
  public void testGetDriverClass() {
    assertEquals( Hive2DatabaseMeta.DRIVER_CLASS_NAME, hive2DatabaseMeta.getDriverClass() );
  }

  @Test
  public void testGetAddColumnStatement() {
    String testTable = "testTable";
    String booleanCol = "booleanCol";
    ValueMetaInterface valueMetaInterface = new ValueMetaBoolean();
    valueMetaInterface.setName( booleanCol );
    String addColumnStatement =
      hive2DatabaseMeta.getAddColumnStatement( testTable, valueMetaInterface, null, false, null, false );
    assertTrue( addColumnStatement.contains( "BOOLEAN" ) );
    assertTrue( addColumnStatement.contains( testTable ) );
    assertTrue( addColumnStatement.contains( booleanCol ) );
  }

  @Test
  public void testGetFieldDefinitionBoolean() {
    assertGetFieldDefinition( new ValueMetaBoolean(), "boolName", "BOOLEAN" );
  }

  @Test
  public void testGetFieldDefinitionDate() {
    when( driver.getMajorVersion() ).thenReturn( 0 );
    when( driver.getMinorVersion() ).thenReturn( 12 );
    assertGetFieldDefinition( new ValueMetaDate(), "dateName", "DATE" );
  }

  @Test
  public void testGetFieldDefinitionDateUnsupported() {
    when( driver.getMajorVersion() ).thenReturn( 0 );
    when( driver.getMinorVersion() ).thenReturn( 11 );
    assertGetFieldDefinition( new ValueMetaDate(), "dateName", "DATE" );
  }

  @Test
  public void testGetFieldDefinitionTimestamp() {
    when( driver.getMajorVersion() ).thenReturn( 0 );
    when( driver.getMinorVersion() ).thenReturn( 8 );
    assertGetFieldDefinition( new ValueMetaTimestamp(), "timestampName", "TIMESTAMP" );
  }

  @Test
  public void testGetFieldDefinitionUnsupported() {
    when( driver.getMajorVersion() ).thenReturn( 0 );
    when( driver.getMinorVersion() ).thenReturn( 7 );
    assertGetFieldDefinition( new ValueMetaTimestamp(), "timestampName", "TIMESTAMP" );
  }

  @Test
  public void testGetFieldDefinitionString() {
    assertGetFieldDefinition( new ValueMetaString(), "stringName", "STRING" );
  }

  @Test
  public void testGetFieldDefinitionNumber() {
    String numberName = "numberName";
    ValueMetaInterface valueMetaInterface = new ValueMetaNumber();
    valueMetaInterface.setName( numberName );
    valueMetaInterface.setPrecision( 0 );
    valueMetaInterface.setLength( 9 );
    assertGetFieldDefinition( valueMetaInterface, "INT" );

    valueMetaInterface.setLength( 18 );
    assertGetFieldDefinition( valueMetaInterface, "BIGINT" );

    valueMetaInterface.setLength( 19 );
    assertGetFieldDefinition( valueMetaInterface, "FLOAT" );

    valueMetaInterface.setPrecision( 10 );
    valueMetaInterface.setLength( 16 );
    assertGetFieldDefinition( valueMetaInterface, "FLOAT" );

    valueMetaInterface.setLength( 15 );
    assertGetFieldDefinition( valueMetaInterface, "DOUBLE" );
  }

  @Test
  public void testGetFieldDefinitionInteger() {
    String integerName = "integerName";
    ValueMetaInterface valueMetaInterface = new ValueMetaInteger();
    valueMetaInterface.setName( integerName );
    valueMetaInterface.setPrecision( 0 );
    valueMetaInterface.setLength( 9 );
    assertGetFieldDefinition( valueMetaInterface, "INT" );

    valueMetaInterface.setLength( 18 );
    assertGetFieldDefinition( valueMetaInterface, "BIGINT" );

    valueMetaInterface.setLength( 19 );
    assertGetFieldDefinition( valueMetaInterface, "FLOAT" );
  }

  @Test
  public void testGetFieldDefinitionBigNumber() {
    String bigNumberName = "bigNumberName";
    ValueMetaInterface valueMetaInterface = new ValueMetaBigNumber();
    valueMetaInterface.setName( bigNumberName );
    valueMetaInterface.setPrecision( 0 );
    valueMetaInterface.setLength( 9 );
    assertGetFieldDefinition( valueMetaInterface, "INT" );

    valueMetaInterface.setLength( 18 );
    assertGetFieldDefinition( valueMetaInterface, "BIGINT" );

    valueMetaInterface.setLength( 19 );
    assertGetFieldDefinition( valueMetaInterface, "FLOAT" );

    valueMetaInterface.setPrecision( 10 );
    valueMetaInterface.setLength( 16 );
    assertGetFieldDefinition( valueMetaInterface, "FLOAT" );

    valueMetaInterface.setLength( 15 );
    assertGetFieldDefinition( valueMetaInterface, "DOUBLE" );
  }

  @Test
  public void testGetFieldDefinition() {
    assertGetFieldDefinition( new ValueMetaInternetAddress(), "internetAddressName", "" );
  }

  @Test
  public void testGetModifyColumnStatement() {
    String testTable = "testTable";
    String booleanCol = "booleanCol";
    ValueMetaInterface valueMetaInterface = new ValueMetaBoolean();
    valueMetaInterface.setName( booleanCol );
    String addColumnStatement = hive2DatabaseMeta.getModifyColumnStatement( testTable, valueMetaInterface, null, false,
      null, false );
    assertTrue( addColumnStatement.contains( "BOOLEAN" ) );
    assertTrue( addColumnStatement.contains( testTable ) );
    assertTrue( addColumnStatement.contains( booleanCol ) );
  }

  @Test
  public void testGetURL() throws KettleDatabaseException, MalformedURLException {
    String testHostname = "testHostname";
    int port = 9429;
    String testDbName = "testDbName";
    String urlString = hive2DatabaseMeta.getURL( testHostname, "" + port, testDbName );
    assertTrue( urlString.startsWith( Hive2DatabaseMeta.URL_PREFIX ) );
    // Use known prefix
    urlString = "http://" + urlString.substring( Hive2DatabaseMeta.URL_PREFIX.length() );
    URL url = new URL( urlString );
    assertEquals( testHostname, url.getHost() );
    assertEquals( port, url.getPort() );
    assertEquals( "/" + testDbName, url.getPath() );
  }

  @Test
  public void testGetSelectCountStatement() {
    String testTable = "testTable";
    assertEquals( Hive2DatabaseMeta.SELECT_COUNT_1_FROM + testTable,
      hive2DatabaseMeta.getSelectCountStatement( testTable ) );
  }

  @Test
  public void testGenerateColumnAlias5AndPrior() {
    when( driver.getMajorVersion() ).thenReturn( 0 );
    when( driver.getMinorVersion() ).thenReturn( 5 );
    String suggestedName = "suggestedName";
    int columnIndex = 12;
    assertEquals( suggestedName, hive2DatabaseMeta.generateColumnAlias( columnIndex, suggestedName ) );
  }

  @Test
  public void testGenerateColumnAlias6AndLater() {
    when( driver.getMajorVersion() ).thenReturn( 0 );
    when( driver.getMinorVersion() ).thenReturn( 6 );
    String suggestedName = "suggestedName";
    int columnIndex = 12;
    assertEquals( suggestedName, hive2DatabaseMeta.generateColumnAlias( columnIndex, suggestedName ) );
  }

  @Test
  public void testIsDriverVersionNull() {
    assertTrue( hive2DatabaseMeta.isDriverVersion( -1, -1 ) );
  }

  @Test
  public void testIsDriverVersionMajorGreater() {
    when( driver.getMajorVersion() ).thenReturn( 6 );
    when( driver.getMinorVersion() ).thenReturn( 0 );
    assertTrue( hive2DatabaseMeta.isDriverVersion( 5, 5 ) );
  }

  @Test
  public void testIsDriverVersionMajorSameMinorEqual() {
    when( driver.getMajorVersion() ).thenReturn( 5 );
    when( driver.getMinorVersion() ).thenReturn( 5 );
    assertTrue( hive2DatabaseMeta.isDriverVersion( 5, 5 ) );
  }

  @Test
  public void testIsDriverVersionMajorSameMinorLess() {
    when( driver.getMajorVersion() ).thenReturn( 5 );
    when( driver.getMinorVersion() ).thenReturn( 4 );
    assertFalse( hive2DatabaseMeta.isDriverVersion( 5, 5 ) );
  }

  @Test
  public void testIsDriverVersionMajorLess() {
    when( driver.getMajorVersion() ).thenReturn( 4 );
    when( driver.getMinorVersion() ).thenReturn( 6 );
    assertFalse( hive2DatabaseMeta.isDriverVersion( 5, 5 ) );
  }

  @Test
  public void testGetStartQuote() {
    assertEquals( 0, hive2DatabaseMeta.getStartQuote().length() );
  }

  @Test
  public void testGetEndQuote() {
    assertEquals( 0, hive2DatabaseMeta.getEndQuote().length() );
  }

  @Test
  public void testGetTableTypesReturnsNull() {
    assertNull( hive2DatabaseMeta.getTableTypes() );
  }

  @Test
  public void testGetViewTypes() {
    assertArrayEquals( new String[] { Hive2DatabaseMeta.VIEW, Hive2DatabaseMeta.VIRTUAL_VIEW },
      hive2DatabaseMeta.getViewTypes() );
  }

  @Test
  public void testGetTruncateTableStatement10OrPrior() {
    when( driver.getMajorVersion() ).thenReturn( 0 );
    when( driver.getMinorVersion() ).thenReturn( 10 );
    String testTableName = "testTableName";
    assertEquals( Hive2DatabaseMeta.TRUNCATE_TABLE + testTableName,
      hive2DatabaseMeta.getTruncateTableStatement( testTableName ) );
  }

  @Test
  public void testGetTruncateTableStatement11AndLater() {
    when( driver.getMajorVersion() ).thenReturn( 0 );
    when( driver.getMinorVersion() ).thenReturn( 11 );
    String testTableName = "testTableName";
    assertEquals( Hive2DatabaseMeta.TRUNCATE_TABLE + testTableName,
      hive2DatabaseMeta.getTruncateTableStatement( testTableName ) );
  }

  @Test
  public void testSupportsSetCharacterStream() {
    assertFalse( hive2DatabaseMeta.supportsSetCharacterStream() );
  }

  @Test
  public void testSupportsBatchUpdates() {
    assertFalse( hive2DatabaseMeta.supportsBatchUpdates() );
  }

  @Test
  public void testSupportsTimeStampToDateConversion() {
    assertFalse( hive2DatabaseMeta.supportsTimeStampToDateConversion() );
  }

  @Test
  public void testGetNamedClusterList() throws Exception {
    assertEquals( namedClusterList, hive2DatabaseMeta.getNamedClusterList() );
    verify( namedClusterService ).listNames( iMetaStoreCaptor.capture() );
  }

  @Test
  public void testPutOptionalOptions() {
    hive2DatabaseMeta.setNamedCluster( CLUSTER );
    hive2DatabaseMeta.setPluginId( PLUGIN_ID );
    Map<String, String> extraOptions = new HashMap<String, String>();
    hive2DatabaseMeta.putOptionalOptions( extraOptions );
    String value = extraOptions.get( PLUGIN_ID + ".pentahoNamedCluster" );
    assertEquals( CLUSTER, value );
  }

  private void assertGetFieldDefinition( ValueMetaInterface valueMetaInterface, String name, String expectedType ) {
    valueMetaInterface = valueMetaInterface.clone();
    valueMetaInterface.setName( name );
    assertGetFieldDefinition( valueMetaInterface, expectedType );
  }

  private void assertGetFieldDefinition( ValueMetaInterface valueMetaInterface, String expectedType ) {
    assertEquals( expectedType, hive2DatabaseMeta.getFieldDefinition( valueMetaInterface, null, null, false, false,
      false ) );
    assertEquals( valueMetaInterface.getName() + " " + expectedType,
      hive2DatabaseMeta.getFieldDefinition( valueMetaInterface, null, null, false, true, false ) );
  }
}
