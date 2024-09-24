/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.hive;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.hadoop.shim.api.jdbc.DriverLocator;
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

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Driver;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class HiveDatabaseMetaTest {
  public static final String LOCALHOST = "localhost";
  public static final String PORT = "10000";
  public static final String DEFAULT = "default";
  @Mock DriverLocator driverLocator;
  @Mock Driver driver;
  private HiveDatabaseMeta hiveDatabaseMeta;
  private String hiveDatabaseMetaURL;

  @Before
  public void setup() throws Throwable {
    hiveDatabaseMeta = new HiveDatabaseMeta( driverLocator );
    hiveDatabaseMetaURL = hiveDatabaseMeta.getURL( LOCALHOST, PORT, DEFAULT );
    when( driverLocator.getDriver( hiveDatabaseMetaURL ) ).thenReturn( driver );
  }

  @Test
  public void testColumnAlias_060_And_Later() throws Throwable {
    when( driver.getMajorVersion() ).thenReturn( 0 );
    when( driver.getMinorVersion() ).thenReturn( 6 );

    String alias = hiveDatabaseMeta.generateColumnAlias( 0, "alias" );
    assertEquals( "alias", alias );

    alias = hiveDatabaseMeta.generateColumnAlias( 1, "alias1" );
    assertEquals( "alias1", alias );

    alias = hiveDatabaseMeta.generateColumnAlias( 2, "alias2" );
    assertEquals( "alias2", alias );
  }

  @Test
  public void testColumnAlias_050() throws Throwable {
    when( driver.getMajorVersion() ).thenReturn( 0 );
    when( driver.getMinorVersion() ).thenReturn( 5 );

    String alias = hiveDatabaseMeta.generateColumnAlias( 0, "alias" );
    assertEquals( "_col0", alias );

    alias = hiveDatabaseMeta.generateColumnAlias( 1, "alias1" );
    assertEquals( "_col1", alias );

    alias = hiveDatabaseMeta.generateColumnAlias( 2, "alias2" );
    assertEquals( "_col2", alias );
  }

  @Test
  public void testGetAddColumnStatement() {
    String testTable = "testTable";
    String booleanCol = "booleanCol";
    ValueMetaInterface valueMetaInterface = new ValueMetaBoolean();
    valueMetaInterface.setName( booleanCol );
    String addColumnStatement =
      hiveDatabaseMeta.getAddColumnStatement( testTable, valueMetaInterface, null, false, null, false );
    assertTrue( addColumnStatement.contains( "BOOLEAN" ) );
    assertTrue( addColumnStatement.contains( testTable ) );
    assertTrue( addColumnStatement.contains( booleanCol ) );
  }

  @Test
  public void testGetDriverClass() {
    assertEquals( HiveDatabaseMeta.DRIVER_CLASS_NAME, hiveDatabaseMeta.getDriverClass() );
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

  @Test( expected = IllegalArgumentException.class )
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

  @Test( expected = IllegalArgumentException.class )
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
    String addColumnStatement = hiveDatabaseMeta.getModifyColumnStatement( testTable, valueMetaInterface, null, false,
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
    String urlString = hiveDatabaseMeta.getURL( testHostname, "" + port, testDbName );
    assertTrue( urlString.startsWith( HiveDatabaseMeta.URL_PREFIX ) );
    // Use known prefix
    urlString = "http://" + urlString.substring( HiveDatabaseMeta.URL_PREFIX.length() );
    URL url = new URL( urlString );
    assertEquals( testHostname, url.getHost() );
    assertEquals( port, url.getPort() );
    assertEquals( "/" + testDbName, url.getPath() );
  }

  @Test
  public void testGetURLEmptyPort() throws KettleDatabaseException, MalformedURLException {
    String testHostname = "testHostname";
    String testDbName = "testDbName";
    String urlString = hiveDatabaseMeta.getURL( testHostname, "", testDbName );
    assertTrue( urlString.startsWith( HiveDatabaseMeta.URL_PREFIX ) );
    // Use known prefix
    urlString = "http://" + urlString.substring( HiveDatabaseMeta.URL_PREFIX.length() );
    URL url = new URL( urlString );
    assertEquals( testHostname, url.getHost() );
    assertEquals( hiveDatabaseMeta.getDefaultDatabasePort(), url.getPort() );
    assertEquals( "/" + testDbName, url.getPath() );
  }

  @Test
  public void testGetUsedLibraries() {
    assertArrayEquals( new String[] { HiveDatabaseMeta.JAR_FILE }, hiveDatabaseMeta.getUsedLibraries() );
  }

  @Test
  public void testGetSelectCountStatement() {
    String tableName = "tableName";
    assertEquals( HiveDatabaseMeta.SELECT_COUNT_1_FROM + tableName,
      hiveDatabaseMeta.getSelectCountStatement( tableName ) );
  }

  @Test
  public void testIsDriverVersionNull() {
    assertTrue( hiveDatabaseMeta.isDriverVersion( -1, -1 ) );
  }

  @Test
  public void testIsDriverVersionMajorGreater() {
    when( driver.getMajorVersion() ).thenReturn( 6 );
    when( driver.getMinorVersion() ).thenReturn( 0 );
    assertTrue( hiveDatabaseMeta.isDriverVersion( 5, 5 ) );
  }

  @Test
  public void testIsDriverVersionMajorSameMinorEqual() {
    when( driver.getMajorVersion() ).thenReturn( 5 );
    when( driver.getMinorVersion() ).thenReturn( 5 );
    assertTrue( hiveDatabaseMeta.isDriverVersion( 5, 5 ) );
  }

  @Test
  public void testIsDriverVersionMajorSameMinorLess() {
    when( driver.getMajorVersion() ).thenReturn( 5 );
    when( driver.getMinorVersion() ).thenReturn( 4 );
    assertFalse( hiveDatabaseMeta.isDriverVersion( 5, 5 ) );
  }

  @Test
  public void testIsDriverVersionMajorLess() {
    when( driver.getMajorVersion() ).thenReturn( 4 );
    when( driver.getMinorVersion() ).thenReturn( 6 );
    assertFalse( hiveDatabaseMeta.isDriverVersion( 5, 5 ) );
  }

  @Test
  public void testGetStartQuote() {
    assertEquals( 0, hiveDatabaseMeta.getStartQuote().length() );
  }

  @Test
  public void testGetEndQuote() {
    assertEquals( 0, hiveDatabaseMeta.getEndQuote().length() );
  }

  @Test
  public void testGetDefaultDatabasePort() {
    assertEquals( HiveDatabaseMeta.DEFAULT_PORT, hiveDatabaseMeta.getDefaultDatabasePort() );
  }

  @Test
  public void testGetTableTypes() {
    assertNull( hiveDatabaseMeta.getTableTypes() );
  }

  @Test
  public void testGetViewTypes() {
    assertArrayEquals( new String[] { HiveDatabaseMeta.VIEW, HiveDatabaseMeta.VIRTUAL_VIEW },
      hiveDatabaseMeta.getViewTypes() );
  }

  @Test
  public void testGetTruncateTableStatement10OrPrior() {
    when( driver.getMajorVersion() ).thenReturn( 0 );
    when( driver.getMinorVersion() ).thenReturn( 10 );
    assertNull( hiveDatabaseMeta.getTruncateTableStatement( "testTableName" ) );
  }

  @Test
  public void testGetTruncateTableStatement11AndLater() {
    when( driver.getMajorVersion() ).thenReturn( 0 );
    when( driver.getMinorVersion() ).thenReturn( 11 );
    String testTableName = "testTableName";
    assertEquals( HiveDatabaseMeta.TRUNCATE_TABLE + testTableName,
      hiveDatabaseMeta.getTruncateTableStatement( testTableName ) );
  }

  @Test
  public void testSupportsSetCharacterStream() {
    assertFalse( hiveDatabaseMeta.supportsSetCharacterStream() );
  }

  @Test
  public void testSupportsBatchUpdates() {
    assertFalse( hiveDatabaseMeta.supportsBatchUpdates() );
  }

  @Test
  public void testSupportsTimeStampToDateConversion() {
    assertFalse( hiveDatabaseMeta.supportsTimeStampToDateConversion() );
  }

  private void assertGetFieldDefinition( ValueMetaInterface valueMetaInterface, String name, String expectedType ) {
    valueMetaInterface = valueMetaInterface.clone();
    valueMetaInterface.setName( name );
    assertGetFieldDefinition( valueMetaInterface, expectedType );
  }

  private void assertGetFieldDefinition( ValueMetaInterface valueMetaInterface, String expectedType ) {
    assertEquals( expectedType, hiveDatabaseMeta.getFieldDefinition( valueMetaInterface, null, null, false, false,
      false ) );
    assertEquals( valueMetaInterface.getName() + " " + expectedType,
      hiveDatabaseMeta.getFieldDefinition( valueMetaInterface, null, null, false, true, false ) );
  }
}
