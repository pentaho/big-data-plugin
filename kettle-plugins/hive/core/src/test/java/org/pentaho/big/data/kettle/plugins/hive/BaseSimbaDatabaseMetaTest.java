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
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.core.logging.KettleLogStore;
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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Mockito.when;
import static org.pentaho.big.data.kettle.plugins.hive.SimbaUrl.KRB_HOST_FQDN;
import static org.pentaho.big.data.kettle.plugins.hive.SimbaUrl.KRB_SERVICE_NAME;

@RunWith( MockitoJUnitRunner.Silent.class )
public class BaseSimbaDatabaseMetaTest {
  private static final String LOCALHOST = "localhost";
  private static final String PORT = "10000";
  private static final String DEFAULT = "default";
  @Mock private DriverLocator driverLocator;
  @Mock private Driver driver;
  private BaseSimbaDatabaseMeta baseSimbaDatabaseMeta;

  private String driverClassname = "driverClassname";
  private String jdbcPrefix = "jdbc:prefix://";

  @BeforeClass
  public static void initLogs() {
    KettleLogStore.init();
  }

  @Before
  public void setup() throws Throwable {
    baseSimbaDatabaseMeta = new BaseSimbaDatabaseMeta( driverLocator, null, null ) {
      @Override protected String getJdbcPrefix() {
        return jdbcPrefix;
      }

      @Override public String getDriverClass() {
        return driverClassname;
      }
    };
    String baseSimbaDatabaseMetaURL = baseSimbaDatabaseMeta.getURL( LOCALHOST, PORT, DEFAULT );
    when( driverLocator.getDriver( baseSimbaDatabaseMetaURL ) ).thenReturn( driver );
  }

  @Test
  public void testVersionConstructor() throws Throwable {
    int majorVersion = 22;
    int minorVersion = 33;
    when( driver.getMajorVersion() ).thenReturn( majorVersion );
    when( driver.getMinorVersion() ).thenReturn( minorVersion );
    assertTrue( baseSimbaDatabaseMeta.isDriverVersion( majorVersion, minorVersion ) );
    assertFalse( baseSimbaDatabaseMeta.isDriverVersion( majorVersion, minorVersion + 1 ) );
    assertFalse( baseSimbaDatabaseMeta.isDriverVersion( majorVersion + 1, minorVersion ) );
  }

  @Test
  public void testGetAccessTypeList() {
    assertArrayEquals(
      new int[] { DatabaseMeta.TYPE_ACCESS_NATIVE, DatabaseMeta.TYPE_ACCESS_JNDI },
      baseSimbaDatabaseMeta.getAccessTypeList() );
  }


  @Test
  public void testGetDriverClassOther() {
    assertEquals( driverClassname, baseSimbaDatabaseMeta.getDriverClass() );
  }

  @Test
  public void testGetUrlDefaults() throws KettleDatabaseException, MalformedURLException {
    String testHost = "testHost";
    String urlString = baseSimbaDatabaseMeta.getURL( testHost, "", "" );
    assertTrue( urlString.startsWith( jdbcPrefix ) );
    URL url = new URL( "http://" + urlString.substring( ImpalaSimbaDatabaseMeta.JDBC_URL_PREFIX.length() ) );
    assertEquals( testHost, url.getHost() );
    assertEquals( baseSimbaDatabaseMeta.getDefaultDatabasePort(), url.getPort() );
    assertEquals( "/default;AuthMech=0", url.getPath() );
  }

  @Test
  public void testGetUrlJndi() throws KettleDatabaseException {
    baseSimbaDatabaseMeta.setAccessType( DatabaseMeta.TYPE_ACCESS_JNDI );
    assertEquals( Hive2SimbaDatabaseMeta.URL_IS_CONFIGURED_THROUGH_JNDI, baseSimbaDatabaseMeta.getURL( "", "", "" ) );
  }

  @Test
  public void testGetUrlKerb() throws Throwable {
    String testHost = "testHost";
    String testPort = "1111";
    String testDb = "testDb";
    // Regular properties
    baseSimbaDatabaseMeta.getAttributes().put( KRB_HOST_FQDN, "fqdn" );
    baseSimbaDatabaseMeta.getAttributes().put( KRB_SERVICE_NAME, "service" );
    String urlString = baseSimbaDatabaseMeta.getURL( testHost, testPort, testDb );
    assertTrue( urlString.startsWith( jdbcPrefix ) );
    URL url = new URL( "http://" + urlString.substring( ImpalaSimbaDatabaseMeta.JDBC_URL_PREFIX.length() ) );
    assertEquals( testHost, url.getHost() );
    assertEquals( Integer.valueOf( testPort ).intValue(), url.getPort() );
    assertEquals( "/" + testDb + ";AuthMech=1", url.getPath() );

    // Extra properties
    baseSimbaDatabaseMeta = new ImpalaSimbaDatabaseMeta( driverLocator, null );
    baseSimbaDatabaseMeta.getAttributes().put(
      Hive2SimbaDatabaseMeta.ATTRIBUTE_PREFIX_EXTRA_OPTION + baseSimbaDatabaseMeta.getPluginId() + "."
        + KRB_HOST_FQDN, "fqdn" );
    baseSimbaDatabaseMeta.getAttributes()
      .put( Hive2SimbaDatabaseMeta.ATTRIBUTE_PREFIX_EXTRA_OPTION + baseSimbaDatabaseMeta.getPluginId() + "."
        + KRB_SERVICE_NAME, "service" );
    urlString = baseSimbaDatabaseMeta.getURL( testHost, testPort, testDb );
    assertTrue( urlString.startsWith( ImpalaSimbaDatabaseMeta.JDBC_URL_PREFIX ) );
    url = new URL( "http://" + urlString.substring( ImpalaSimbaDatabaseMeta.JDBC_URL_PREFIX.length() ) );
    assertEquals( testHost, url.getHost() );
    assertEquals( Integer.valueOf( testPort ).intValue(), url.getPort() );
    assertEquals( "/" + testDb + ";AuthMech=1", url.getPath() );
  }

  @Test
  public void testGetUrlUsername() throws KettleDatabaseException, MalformedURLException {
    String testUsername = "testUsername";
    baseSimbaDatabaseMeta.setUsername( testUsername );

    String testHost = "testHost";
    String testPort = "1111";
    String testDb = "testDb";
    String urlString = baseSimbaDatabaseMeta.getURL( testHost, testPort, testDb );
    assertTrue( urlString.startsWith( jdbcPrefix ) );
    URL url = new URL( "http://" + urlString.substring( ImpalaSimbaDatabaseMeta.JDBC_URL_PREFIX.length() ) );
    assertEquals( testHost, url.getHost() );
    assertEquals( Integer.valueOf( testPort ).intValue(), url.getPort() );
    assertEquals( "/" + testDb + ";AuthMech=2;UID=" + testUsername, url.getPath() );
  }

  @Test
  public void testGetUrlPassword() throws KettleDatabaseException, MalformedURLException {
    String testUsername = "testUsername";
    String testPassword = "testPassword";
    baseSimbaDatabaseMeta.setUsername( testUsername );
    baseSimbaDatabaseMeta.setPassword( testPassword );

    String testHost = "testHost";
    String testPort = "1111";
    String testDb = "testDb";
    String urlString = baseSimbaDatabaseMeta.getURL( testHost, testPort, testDb );
    assertTrue( urlString.startsWith( jdbcPrefix ) );
    URL url = new URL( "http://" + urlString.substring( ImpalaSimbaDatabaseMeta.JDBC_URL_PREFIX.length() ) );
    assertEquals( testHost, url.getHost() );
    assertEquals( Integer.valueOf( testPort ).intValue(), url.getPort() );
    assertEquals(
      "/" + testDb + ";AuthMech=3;UID=" + testUsername + ";PWD=" + testPassword,
      url.getPath() );
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
  public void testGetFieldDefinitionTimestamp() {
    when( driver.getMajorVersion() ).thenReturn( 0 );
    when( driver.getMinorVersion() ).thenReturn( 8 );
    assertGetFieldDefinition( new ValueMetaTimestamp(), "timestampName", "TIMESTAMP" );
  }

  @Test
  public void testGetFieldDefinitionStringVarchar() {
    when( driver.getMajorVersion() ).thenReturn( 0 );
    when( driver.getMinorVersion() ).thenReturn( 12 );
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
    assertEquals( expectedType,
      baseSimbaDatabaseMeta.getFieldDefinition( valueMetaInterface, null, null, false, false,
        false ) );
    assertEquals( valueMetaInterface.getName() + " " + expectedType,
      baseSimbaDatabaseMeta.getFieldDefinition( valueMetaInterface, null, null, false, true, false ) );
  }
}
