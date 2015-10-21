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
import static org.junit.Assert.assertTrue;
import static org.pentaho.di.core.database.Hive2DatabaseMeta.URL_PREFIX;

/**
 * Created by bryan on 10/20/15.
 */
public class ImpalaDatabaseMetaTest {
  private ImpalaDatabaseMeta impalaDatabaseMeta;

  @Before
  public void setup() throws Throwable {
    impalaDatabaseMeta = new ImpalaDatabaseMeta();
  }

  @Test
  public void testVersionConstructor() throws Throwable {
    int majorVersion = 22;
    int minorVersion = 33;
    impalaDatabaseMeta = new ImpalaDatabaseMeta( majorVersion, minorVersion );
    assertEquals( Integer.valueOf( majorVersion ), impalaDatabaseMeta.driverMajorVersion );
    assertEquals( Integer.valueOf( minorVersion ), impalaDatabaseMeta.driverMinorVersion );
  }

  @Test
  public void testGetAccessTypeList() {
    assertArrayEquals( new int[] { DatabaseMeta.TYPE_ACCESS_NATIVE }, impalaDatabaseMeta.getAccessTypeList() );
  }

  @Test
  public void testGetDriverClass() {
    assertEquals( ImpalaDatabaseMeta.DRIVER_CLASS_NAME, impalaDatabaseMeta.getDriverClass() );
  }

  @Test
  public void testGetFieldDefinitionBoolean() {
    assertGetFieldDefinition( new ValueMetaBoolean(), "boolName", "BOOLEAN" );
  }

  @Test
  public void testGetFieldDefinitionDate() {
    impalaDatabaseMeta.driverMajorVersion = 0;
    impalaDatabaseMeta.driverMinorVersion = 8;
    assertGetFieldDefinition( new ValueMetaDate(), "dateName", "TIMESTAMP" );
  }

  @Test( expected = IllegalArgumentException.class )
  public void testGetFieldDefinitionDateUnsupported() {
    impalaDatabaseMeta.driverMajorVersion = 0;
    impalaDatabaseMeta.driverMinorVersion = 7;
    assertGetFieldDefinition( new ValueMetaDate(), "dateName", "TIMESTAMP" );
  }

  @Test
  public void testGetFieldDefinitionTimestamp() {
    impalaDatabaseMeta.driverMajorVersion = 0;
    impalaDatabaseMeta.driverMinorVersion = 8;
    assertGetFieldDefinition( new ValueMetaTimestamp(), "timestampName", "TIMESTAMP" );
  }

  @Test( expected = IllegalArgumentException.class )
  public void testGetFieldDefinitionUnsupported() {
    impalaDatabaseMeta.driverMajorVersion = 0;
    impalaDatabaseMeta.driverMinorVersion = 7;
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
  public void testGetURL() throws KettleDatabaseException, MalformedURLException {
    String testHostname = "testHostname";
    int port = 9429;
    String testDbName = "testDbName";
    String urlString = impalaDatabaseMeta.getURL( testHostname, "" + port, testDbName );
    assertTrue( urlString.startsWith( URL_PREFIX ) );
    // Use known prefix
    urlString = "http://" + urlString.substring( URL_PREFIX.length() );
    URL url = new URL( urlString );
    assertEquals( testHostname, url.getHost() );
    assertEquals( port, url.getPort() );
    assertEquals( "/" + testDbName + ImpalaDatabaseMeta.AUTH_NO_SASL, url.getPath() );
  }

  @Test
  public void testGetURLPrincipal() throws KettleDatabaseException, MalformedURLException {
    String testHostname = "testHostname";
    int port = 9429;
    String testDbName = "testDbName";
    impalaDatabaseMeta.getAttributes().put( "principal", "testP" );
    String urlString = impalaDatabaseMeta.getURL( testHostname, "" + port, testDbName );
    assertTrue( urlString.startsWith( URL_PREFIX ) );
    // Use known prefix
    urlString = "http://" + urlString.substring( URL_PREFIX.length() );
    URL url = new URL( urlString );
    assertEquals( testHostname, url.getHost() );
    assertEquals( port, url.getPort() );
    assertEquals( "/" + testDbName, url.getPath() );

    impalaDatabaseMeta.getAttributes().remove( "principal" );
    impalaDatabaseMeta.getAttributes()
      .put( ImpalaDatabaseMeta.ATTRIBUTE_PREFIX_EXTRA_OPTION + impalaDatabaseMeta.getPluginId() + ".principal",
        "testP" );
    urlString = impalaDatabaseMeta.getURL( testHostname, "" + port, testDbName );
    assertTrue( urlString.startsWith( URL_PREFIX ) );
    // Use known prefix
    urlString = "http://" + urlString.substring( URL_PREFIX.length() );
    url = new URL( urlString );
    assertEquals( testHostname, url.getHost() );
    assertEquals( port, url.getPort() );
    assertEquals( "/" + testDbName, url.getPath() );
  }

  @Test
  public void testGetURLEmptyPort() throws KettleDatabaseException, MalformedURLException {
    String testHostname = "testHostname";
    String testDbName = "testDbName";
    String urlString = impalaDatabaseMeta.getURL( testHostname, "", testDbName );
    assertTrue( urlString.startsWith( URL_PREFIX ) );
    // Use known prefix
    urlString = "http://" + urlString.substring( URL_PREFIX.length() );
    URL url = new URL( urlString );
    assertEquals( testHostname, url.getHost() );
    assertEquals( impalaDatabaseMeta.getDefaultDatabasePort(), url.getPort() );
    assertEquals( "/" + testDbName + ImpalaDatabaseMeta.AUTH_NO_SASL, url.getPath() );
  }

  @Test
  public void testGetUsedLibraries() {
    assertArrayEquals( new String[] { ImpalaDatabaseMeta.JAR_FILE }, impalaDatabaseMeta.getUsedLibraries() );
  }

  @Test
  public void testGetDefaultDatabasePort() {
    assertEquals( ImpalaDatabaseMeta.DEFAULT_PORT, impalaDatabaseMeta.getDefaultDatabasePort() );
  }

  private void assertGetFieldDefinition( ValueMetaInterface valueMetaInterface, String name, String expectedType ) {
    valueMetaInterface = valueMetaInterface.clone();
    valueMetaInterface.setName( name );
    assertGetFieldDefinition( valueMetaInterface, expectedType );
  }

  private void assertGetFieldDefinition( ValueMetaInterface valueMetaInterface, String expectedType ) {
    assertEquals( expectedType, impalaDatabaseMeta.getFieldDefinition( valueMetaInterface, null, null, false, false,
      false ) );
    assertEquals( valueMetaInterface.getName() + " " + expectedType,
      impalaDatabaseMeta.getFieldDefinition( valueMetaInterface, null, null, false, true, false ) );
  }
}
