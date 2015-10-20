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
import static org.pentaho.di.core.database.Hive2SimbaDatabaseMeta.JDBC_URL_PREFIX;

/**
 * Created by bryan on 10/20/15.
 */
public class Hive2SimbaDatabaseMetaTest {
  private Hive2SimbaDatabaseMeta hive2SimbaDatabaseMeta;

  @Before
  public void setup() throws Throwable {
    hive2SimbaDatabaseMeta = new Hive2SimbaDatabaseMeta();
  }

  @Test
  public void testVersionConstructor() throws Throwable {
    int majorVersion = 22;
    int minorVersion = 33;
    hive2SimbaDatabaseMeta = new Hive2SimbaDatabaseMeta( majorVersion, minorVersion );
    assertEquals( Integer.valueOf( majorVersion ), hive2SimbaDatabaseMeta.driverMajorVersion );
    assertEquals( Integer.valueOf( minorVersion ), hive2SimbaDatabaseMeta.driverMinorVersion );
  }

  @Test
  public void testGetAccessTypeList() {
    assertArrayEquals(
      new int[] { DatabaseMeta.TYPE_ACCESS_NATIVE, DatabaseMeta.TYPE_ACCESS_ODBC, DatabaseMeta.TYPE_ACCESS_JNDI },
      hive2SimbaDatabaseMeta.getAccessTypeList() );
  }

  @Test
  public void testGetDriverClassODBC() {
    hive2SimbaDatabaseMeta.setAccessType( DatabaseMeta.TYPE_ACCESS_ODBC );
    assertEquals( Hive2SimbaDatabaseMeta.ODBC_DRIVER_CLASS_NAME, hive2SimbaDatabaseMeta.getDriverClass() );
  }

  @Test
  public void testGetDriverClassOther() {
    assertEquals( Hive2SimbaDatabaseMeta.DRIVER_CLASS_NAME, hive2SimbaDatabaseMeta.getDriverClass() );
  }

  @Test
  public void testGetUrlDefaults() throws KettleDatabaseException, MalformedURLException {
    String testHost = "testHost";
    String urlString = hive2SimbaDatabaseMeta.getURL( testHost, "", "" );
    assertTrue( urlString.startsWith( JDBC_URL_PREFIX ) );
    URL url = new URL( "http://" + urlString.substring( JDBC_URL_PREFIX.length() ) );
    assertEquals( testHost, url.getHost() );
    assertEquals( hive2SimbaDatabaseMeta.getDefaultDatabasePort(), url.getPort() );
    assertEquals( "/default;" + Hive2SimbaDatabaseMeta.AUTH_MECH + "=0", url.getPath() );
  }

  @Test
  public void testGetUrlOdbc() throws KettleDatabaseException {
    String testDbName = "testDbName";
    hive2SimbaDatabaseMeta.setAccessType( DatabaseMeta.TYPE_ACCESS_ODBC );
    assertEquals( String.format( Hive2SimbaDatabaseMeta.JDBC_ODBC_S, testDbName ),
      hive2SimbaDatabaseMeta.getURL( "", "", testDbName ) );
  }

  @Test
  public void testGetUrlJndi() throws KettleDatabaseException {
    hive2SimbaDatabaseMeta.setAccessType( DatabaseMeta.TYPE_ACCESS_JNDI );
    assertEquals( Hive2SimbaDatabaseMeta.URL_IS_CONFIGURED_THROUGH_JNDI, hive2SimbaDatabaseMeta.getURL( "", "", "" ) );
  }

  @Test
  public void testGetUrlKerb() throws Throwable {
    String testHost = "testHost";
    String testPort = "1111";
    String testDb = "testDb";
    // Regular properties
    hive2SimbaDatabaseMeta.getAttributes().put( Hive2SimbaDatabaseMeta.KRB_HOST_FQDN, "fqdn" );
    hive2SimbaDatabaseMeta.getAttributes().put( Hive2SimbaDatabaseMeta.KRB_SERVICE_NAME, "service" );
    String urlString = hive2SimbaDatabaseMeta.getURL( testHost, testPort, testDb );
    assertTrue( urlString.startsWith( JDBC_URL_PREFIX ) );
    URL url = new URL( "http://" + urlString.substring( JDBC_URL_PREFIX.length() ) );
    assertEquals( testHost, url.getHost() );
    assertEquals( Integer.valueOf( testPort ).intValue(), url.getPort() );
    assertEquals( "/" + testDb + ";" + Hive2SimbaDatabaseMeta.AUTH_MECH + "=1", url.getPath() );

    // Extra properties
    hive2SimbaDatabaseMeta = new Hive2SimbaDatabaseMeta();
    hive2SimbaDatabaseMeta.getAttributes().put(
      Hive2SimbaDatabaseMeta.ATTRIBUTE_PREFIX_EXTRA_OPTION + hive2SimbaDatabaseMeta.getPluginId() + "."
        + Hive2SimbaDatabaseMeta.KRB_HOST_FQDN, "fqdn" );
    hive2SimbaDatabaseMeta.getAttributes()
      .put( Hive2SimbaDatabaseMeta.ATTRIBUTE_PREFIX_EXTRA_OPTION + hive2SimbaDatabaseMeta.getPluginId() + "."
        + Hive2SimbaDatabaseMeta.KRB_SERVICE_NAME, "service" );
    urlString = hive2SimbaDatabaseMeta.getURL( testHost, testPort, testDb );
    assertTrue( urlString.startsWith( JDBC_URL_PREFIX ) );
    url = new URL( "http://" + urlString.substring( JDBC_URL_PREFIX.length() ) );
    assertEquals( testHost, url.getHost() );
    assertEquals( Integer.valueOf( testPort ).intValue(), url.getPort() );
    assertEquals( "/" + testDb + ";" + Hive2SimbaDatabaseMeta.AUTH_MECH + "=1", url.getPath() );
  }

  @Test
  public void testGetUrlUsername() throws KettleDatabaseException, MalformedURLException {
    String testUsername = "testUsername";
    hive2SimbaDatabaseMeta.setUsername( testUsername );

    String testHost = "testHost";
    String testPort = "1111";
    String testDb = "testDb";
    String urlString = hive2SimbaDatabaseMeta.getURL( testHost, testPort, testDb );
    assertTrue( urlString.startsWith( JDBC_URL_PREFIX ) );
    URL url = new URL( "http://" + urlString.substring( JDBC_URL_PREFIX.length() ) );
    assertEquals( testHost, url.getHost() );
    assertEquals( Integer.valueOf( testPort ).intValue(), url.getPort() );
    assertEquals( "/" + testDb + ";" + Hive2SimbaDatabaseMeta.AUTH_MECH + "=2;UID=" + testUsername, url.getPath() );
  }

  @Test
  public void testGetUrlPassword() throws KettleDatabaseException, MalformedURLException {
    String testUsername = "testUsername";
    String testPassword = "testPassword";
    hive2SimbaDatabaseMeta.setUsername( testUsername );
    hive2SimbaDatabaseMeta.setPassword( testPassword );

    String testHost = "testHost";
    String testPort = "1111";
    String testDb = "testDb";
    String urlString = hive2SimbaDatabaseMeta.getURL( testHost, testPort, testDb );
    assertTrue( urlString.startsWith( JDBC_URL_PREFIX ) );
    URL url = new URL( "http://" + urlString.substring( JDBC_URL_PREFIX.length() ) );
    assertEquals( testHost, url.getHost() );
    assertEquals( Integer.valueOf( testPort ).intValue(), url.getPort() );
    assertEquals(
      "/" + testDb + ";" + Hive2SimbaDatabaseMeta.AUTH_MECH + "=3;UID=" + testUsername + ";PWD=" + testPassword,
      url.getPath() );
  }

  @Test
  public void testGetFieldDefinitionBoolean() {
    assertGetFieldDefinition( new ValueMetaBoolean(), "boolName", "BOOLEAN" );
  }

  @Test
  public void testGetFieldDefinitionDate() {
    hive2SimbaDatabaseMeta.driverMajorVersion = 0;
    hive2SimbaDatabaseMeta.driverMinorVersion = 12;
    assertGetFieldDefinition( new ValueMetaDate(), "dateName", "DATE" );
  }

  @Test( expected = IllegalArgumentException.class )
  public void testGetFieldDefinitionDateUnsupported() {
    hive2SimbaDatabaseMeta.driverMajorVersion = 0;
    hive2SimbaDatabaseMeta.driverMinorVersion = 11;
    assertGetFieldDefinition( new ValueMetaDate(), "dateName", "DATE" );
  }

  @Test
  public void testGetFieldDefinitionTimestamp() {
    hive2SimbaDatabaseMeta.driverMajorVersion = 0;
    hive2SimbaDatabaseMeta.driverMinorVersion = 8;
    assertGetFieldDefinition( new ValueMetaTimestamp(), "timestampName", "TIMESTAMP" );
  }

  @Test( expected = IllegalArgumentException.class )
  public void testGetFieldDefinitionUnsupported() {
    hive2SimbaDatabaseMeta.driverMajorVersion = 0;
    hive2SimbaDatabaseMeta.driverMinorVersion = 7;
    assertGetFieldDefinition( new ValueMetaTimestamp(), "timestampName", "TIMESTAMP" );
  }

  @Test
  public void testGetFieldDefinitionString() {
    assertGetFieldDefinition( new ValueMetaString(), "stringName", "STRING" );
    ValueMetaString valueMetaInterface = new ValueMetaString();
    valueMetaInterface.setLength( 1 );
    assertGetFieldDefinition( valueMetaInterface, "stringName", "STRING" );
  }

  @Test
  public void testGetFieldDefinitionStringChar() {
    hive2SimbaDatabaseMeta.driverMajorVersion = 0;
    hive2SimbaDatabaseMeta.driverMinorVersion = 13;
    ValueMetaString valueMetaInterface = new ValueMetaString();
    valueMetaInterface.setLength( 1 );
    assertGetFieldDefinition( valueMetaInterface, "stringName", "CHAR" );
  }

  @Test
  public void testGetFieldDefinitionStringVarchar() {
    hive2SimbaDatabaseMeta.driverMajorVersion = 0;
    hive2SimbaDatabaseMeta.driverMinorVersion = 12;
    assertGetFieldDefinition( new ValueMetaString(), "stringName", "VARCHAR" );
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

  private void assertGetFieldDefinition( ValueMetaInterface valueMetaInterface, String name, String expectedType ) {
    valueMetaInterface = valueMetaInterface.clone();
    valueMetaInterface.setName( name );
    assertGetFieldDefinition( valueMetaInterface, expectedType );
  }

  private void assertGetFieldDefinition( ValueMetaInterface valueMetaInterface, String expectedType ) {
    assertEquals( expectedType, hive2SimbaDatabaseMeta.getFieldDefinition( valueMetaInterface, null, null, false, false,
      false ) );
    assertEquals( valueMetaInterface.getName() + " " + expectedType,
      hive2SimbaDatabaseMeta.getFieldDefinition( valueMetaInterface, null, null, false, true, false ) );
  }
}
