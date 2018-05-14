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
import org.pentaho.di.core.database.DatabaseMeta;
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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 10/20/15.
 */
@RunWith( MockitoJUnitRunner.class )
public class ImpalaDatabaseMetaTest {
  public static final String LOCALHOST = "localhost";
  public static final String PORT = "10000";
  public static final String DEFAULT = "default";
  @Mock DriverLocator driverLocator;
  @Mock Driver driver;
  private ImpalaDatabaseMeta impalaDatabaseMeta;
  private String impalaDatabaseMetaURL;

  @Before
  public void setup() throws Throwable {
    impalaDatabaseMeta = new ImpalaDatabaseMeta( driverLocator );
    impalaDatabaseMetaURL = impalaDatabaseMeta.getURL( LOCALHOST, PORT, DEFAULT );
    when( driverLocator.getDriver( impalaDatabaseMetaURL ) ).thenReturn( driver );
  }

  @Test
  public void testVersionConstructor() throws Throwable {
    int majorVersion = 22;
    int minorVersion = 33;
    when( driver.getMajorVersion() ).thenReturn( majorVersion );
    when( driver.getMinorVersion() ).thenReturn( minorVersion );
    assertTrue( impalaDatabaseMeta.isDriverVersion( majorVersion, minorVersion ) );
    assertFalse( impalaDatabaseMeta.isDriverVersion( majorVersion, minorVersion + 1 ) );
    assertFalse( impalaDatabaseMeta.isDriverVersion( majorVersion + 1, minorVersion ) );
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
    when( driver.getMajorVersion() ).thenReturn( 0 );
    when( driver.getMinorVersion() ).thenReturn( 8 );
    assertGetFieldDefinition( new ValueMetaDate(), "dateName", "TIMESTAMP" );
  }

  @Test( expected = IllegalArgumentException.class )
  public void testGetFieldDefinitionDateUnsupported() {
    when( driver.getMajorVersion() ).thenReturn( 0 );
    when( driver.getMinorVersion() ).thenReturn( 7 );
    assertGetFieldDefinition( new ValueMetaDate(), "dateName", "TIMESTAMP" );
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
  public void testGetURL() throws KettleDatabaseException, MalformedURLException {
    String testHostname = "testHostname";
    int port = 9429;
    String testDbName = "testDbName";
    String urlString = impalaDatabaseMeta.getURL( testHostname, "" + port, testDbName );
    assertTrue( urlString.startsWith( ImpalaDatabaseMeta.URL_PREFIX ) );
    // Use known prefix
    urlString = "http://" + urlString.substring( ImpalaDatabaseMeta.URL_PREFIX.length() );
    URL url = new URL( urlString );
    assertEquals( testHostname, url.getHost() );
    assertEquals( port, url.getPort() );
    assertEquals( "/" + testDbName + ImpalaDatabaseMeta.AUTH_NO_SASL + ";impala_db=true", url.getPath() );
  }

  @Test
  public void testGetURLPrincipal() throws KettleDatabaseException, MalformedURLException {
    String testHostname = "testHostname";
    int port = 9429;
    String testDbName = "testDbName";
    impalaDatabaseMeta.getAttributes().put( "principal", "testP" );
    String urlString = impalaDatabaseMeta.getURL( testHostname, "" + port, testDbName );
    assertTrue( urlString.startsWith( ImpalaDatabaseMeta.URL_PREFIX ) );
    // Use known prefix
    urlString = "http://" + urlString.substring( ImpalaDatabaseMeta.URL_PREFIX.length() );
    URL url = new URL( urlString );
    assertEquals( testHostname, url.getHost() );
    assertEquals( port, url.getPort() );
    assertEquals( "/" + testDbName + ";impala_db=true", url.getPath() );

    impalaDatabaseMeta.getAttributes().remove( "principal" );
    impalaDatabaseMeta.getAttributes()
      .put( ImpalaDatabaseMeta.ATTRIBUTE_PREFIX_EXTRA_OPTION + impalaDatabaseMeta.getPluginId() + ".principal",
        "testP" );
    urlString = impalaDatabaseMeta.getURL( testHostname, "" + port, testDbName );
    assertTrue( urlString.startsWith( ImpalaDatabaseMeta.URL_PREFIX ) );
    // Use known prefix
    urlString = "http://" + urlString.substring( ImpalaDatabaseMeta.URL_PREFIX.length() );
    url = new URL( urlString );
    assertEquals( testHostname, url.getHost() );
    assertEquals( port, url.getPort() );
    assertEquals( "/" + testDbName + ";impala_db=true", url.getPath() );
  }

  @Test
  public void testGetURLEmptyPort() throws KettleDatabaseException, MalformedURLException {
    String testHostname = "testHostname";
    String testDbName = "testDbName";
    String urlString = impalaDatabaseMeta.getURL( testHostname, "", testDbName );
    assertTrue( urlString.startsWith( ImpalaDatabaseMeta.URL_PREFIX ) );
    // Use known prefix
    urlString = "http://" + urlString.substring( ImpalaDatabaseMeta.URL_PREFIX.length() );
    URL url = new URL( urlString );
    assertEquals( testHostname, url.getHost() );
    assertEquals( impalaDatabaseMeta.getDefaultDatabasePort(), url.getPort() );
    assertEquals( "/" + testDbName + ImpalaDatabaseMeta.AUTH_NO_SASL + ";impala_db=true", url.getPath() );
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
