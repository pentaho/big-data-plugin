/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.core.database;

import org.junit.Before;
import org.junit.Test;
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.pentaho.di.core.database.Hive2DatabaseMeta.URL_PREFIX;

/**
 * Created by bryan on 10/19/15.
 */
public class Hive2DatabaseMetaTest {
  private Hive2DatabaseMeta hive2DatabaseMeta;

  @Before
  public void setup() throws Throwable {
    hive2DatabaseMeta = new Hive2DatabaseMeta();
  }

  @Test
  public void testVersionConstructor() throws Throwable {
    int majorVersion = 10;
    int minorVersion = 11;
    Hive2DatabaseMeta hive2DatabaseMeta = new Hive2DatabaseMeta( majorVersion, minorVersion );
    assertEquals( Integer.valueOf( majorVersion ), hive2DatabaseMeta.driverMajorVersion );
    assertEquals( Integer.valueOf( minorVersion ), hive2DatabaseMeta.driverMinorVersion );
  }

  @Test
  public void testGetAccessTypeList() {
    assertArrayEquals( new int[] { DatabaseMeta.TYPE_ACCESS_NATIVE }, hive2DatabaseMeta.getAccessTypeList() );
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
    hive2DatabaseMeta.driverMajorVersion = 0;
    hive2DatabaseMeta.driverMinorVersion = 12;
    assertGetFieldDefinition( new ValueMetaDate(), "dateName", "DATE" );
  }

  @Test( expected = IllegalArgumentException.class )
  public void testGetFieldDefinitionDateUnsupported() {
    hive2DatabaseMeta.driverMajorVersion = 0;
    hive2DatabaseMeta.driverMinorVersion = 11;
    assertGetFieldDefinition( new ValueMetaDate(), "dateName", "DATE" );
  }

  @Test
  public void testGetFieldDefinitionTimestamp() {
    hive2DatabaseMeta.driverMajorVersion = 0;
    hive2DatabaseMeta.driverMinorVersion = 8;
    assertGetFieldDefinition( new ValueMetaTimestamp(), "timestampName", "TIMESTAMP" );
  }

  @Test( expected = IllegalArgumentException.class )
  public void testGetFieldDefinitionUnsupported() {
    hive2DatabaseMeta.driverMajorVersion = 0;
    hive2DatabaseMeta.driverMinorVersion = 7;
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
    assertTrue( urlString.startsWith( URL_PREFIX ) );
    // Use known prefix
    urlString = "http://" + urlString.substring( URL_PREFIX.length() );
    URL url = new URL( urlString );
    assertEquals( testHostname, url.getHost() );
    assertEquals( port, url.getPort() );
    assertEquals( "/" + testDbName, url.getPath() );
  }

  @Test
  public void testGetUsedLibraries() {
    assertArrayEquals( new String[] { Hive2DatabaseMeta.JAR_FILE }, hive2DatabaseMeta.getUsedLibraries() );
  }

  @Test
  public void testGetSelectCountStatement() {
    String testTable = "testTable";
    assertEquals( Hive2DatabaseMeta.SELECT_COUNT_1_FROM + testTable,
      hive2DatabaseMeta.getSelectCountStatement( testTable ) );
  }

  @Test
  public void testGenerateColumnAlias5AndPrior() {
    hive2DatabaseMeta.driverMajorVersion = 0;
    hive2DatabaseMeta.driverMinorVersion = 5;
    String suggestedName = "suggestedName";
    int columnIndex = 12;
    assertEquals( Hive2DatabaseMeta.ALIAS_SUFFIX + columnIndex,
      hive2DatabaseMeta.generateColumnAlias( columnIndex, suggestedName ) );
  }

  @Test
  public void testGenerateColumnAlias6AndLater() {
    hive2DatabaseMeta.driverMajorVersion = 0;
    hive2DatabaseMeta.driverMinorVersion = 6;
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
    hive2DatabaseMeta.driverMajorVersion = 6;
    hive2DatabaseMeta.driverMinorVersion = 0;
    assertTrue( hive2DatabaseMeta.isDriverVersion( 5, 5 ) );
  }

  @Test
  public void testIsDriverVersionMajorSameMinorEqual() {
    hive2DatabaseMeta.driverMajorVersion = 5;
    hive2DatabaseMeta.driverMinorVersion = 5;
    assertTrue( hive2DatabaseMeta.isDriverVersion( 5, 5 ) );
  }

  @Test
  public void testIsDriverVersionMajorSameMinorLess() {
    hive2DatabaseMeta.driverMajorVersion = 5;
    hive2DatabaseMeta.driverMinorVersion = 4;
    assertFalse( hive2DatabaseMeta.isDriverVersion( 5, 5 ) );
  }

  @Test
  public void testIsDriverVersionMajorLess() {
    hive2DatabaseMeta.driverMajorVersion = 4;
    hive2DatabaseMeta.driverMinorVersion = 6;
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
    hive2DatabaseMeta.driverMajorVersion = 0;
    hive2DatabaseMeta.driverMinorVersion = 10;
    assertNull( hive2DatabaseMeta.getTruncateTableStatement( "testTableName" ) );
  }

  @Test
  public void testGetTruncateTableStatement11AndLater() {
    hive2DatabaseMeta.driverMajorVersion = 0;
    hive2DatabaseMeta.driverMinorVersion = 11;
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
