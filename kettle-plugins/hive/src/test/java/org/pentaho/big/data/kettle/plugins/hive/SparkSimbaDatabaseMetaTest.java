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

package org.pentaho.big.data.kettle.plugins.hive;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.big.data.api.jdbc.DriverLocator;
import org.pentaho.di.core.database.DatabaseMeta;

import java.sql.Driver;
import java.util.Arrays;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class SparkSimbaDatabaseMetaTest {
  public static final String LOCALHOST = "localhost";
  public static final String PORT = "10000";
  public static final String DEFAULT = "default";
  @Mock DriverLocator driverLocator;
  @Mock Driver driver;
  @InjectMocks private SparkSimbaDatabaseMeta sparkSimbaDatabaseMeta;
  @Rule public final ExpectedException exception = ExpectedException.none();

  private String sparkSimbaDatabaseMetaURL;
  private static final String DB_NAME = "dbName";

  @Before
  public void setup() throws Throwable {
    sparkSimbaDatabaseMetaURL = sparkSimbaDatabaseMeta.getURL( LOCALHOST, PORT, DEFAULT );
    when( driverLocator.getDriver( sparkSimbaDatabaseMetaURL ) ).thenReturn( driver );
    sparkSimbaDatabaseMeta.setDatabaseName( DB_NAME );
  }

  @Test
  public void testGetAccessTypeList() {
    assertArrayEquals(
      new int[] { DatabaseMeta.TYPE_ACCESS_NATIVE, DatabaseMeta.TYPE_ACCESS_JNDI },
      sparkSimbaDatabaseMeta.getAccessTypeList() );
  }

  @Test
  public void testGetJdbcPrefix() {
    assertEquals( SparkSimbaDatabaseMeta.JDBC_URL_PREFIX,
      sparkSimbaDatabaseMeta.getJdbcPrefix() );
  }

  @Test
  public void testGetUsedLibraries() {
    assertTrue( Arrays.equals(
      sparkSimbaDatabaseMeta.getUsedLibraries(),
      new String[] { sparkSimbaDatabaseMeta.JAR_FILE } ) );
  }

  @Test
  public void testGetDefaultDatabasePort() {
    assertEquals( SparkSimbaDatabaseMeta.DEFAULT_PORT,
      sparkSimbaDatabaseMeta.getDefaultDatabasePort() );
  }

  @Test
  public void testQuoting() {
    assertEquals( "`", sparkSimbaDatabaseMeta.getStartQuote() );
    assertEquals( "`", sparkSimbaDatabaseMeta.getEndQuote() );
  }

  @Test
  public void testGeneratedSQLContainsSchemaReferenceWhenTableUnqualified() {
    verifyExpectedSql( null, "foo" );
  }

  @Test
  public void testGeneratedSQLContainsSchemaReferenceWhenTableQualified() {
    verifyExpectedSql( DB_NAME, "foo" );
  }

  private void verifyExpectedSql( String schemaName, String tableName ) {
    String expectedTableName = schemaName == null ? tableName
      : schemaName + "." + tableName;
    assertThat( sparkSimbaDatabaseMeta.getSQLTableExists( expectedTableName ),
      is( "SELECT 1 FROM " + expectedTableName + " LIMIT 1" ) );
    assertThat( sparkSimbaDatabaseMeta.getTruncateTableStatement( expectedTableName ),
      is( "TRUNCATE TABLE " + expectedTableName ) );
    assertThat( sparkSimbaDatabaseMeta.getSQLColumnExists( "column", expectedTableName ),
      is( "SELECT column FROM " + expectedTableName  + " LIMIT 1" ) );
    assertThat( sparkSimbaDatabaseMeta.getSQLQueryFields( expectedTableName ),
      is( "SELECT * FROM " + expectedTableName + " LIMIT 1" ) );
    assertThat( sparkSimbaDatabaseMeta.getSelectCountStatement( expectedTableName ),
      is( SparkSimbaDatabaseMeta.SELECT_COUNT_STATEMENT + " " + expectedTableName ) );
  }


  @Test
  public void testUnsupportedDrop() {
    exception.expect( UnsupportedOperationException.class );
    sparkSimbaDatabaseMeta.getDropColumnStatement( "tab", null, "tk", false, "pk", false );
  }

  @Test
  public void testUnsupportedAddCol() {
    exception.expect( UnsupportedOperationException.class );
    sparkSimbaDatabaseMeta.getAddColumnStatement( "tab", null, "tk", false, "pk", false );
  }

  @Test
  public void testUnsupportedModCol() {
    exception.expect( UnsupportedOperationException.class );
    sparkSimbaDatabaseMeta.getModifyColumnStatement( "tab", null, "tk", false, "pk", false );
  }

  @Test
  public void testGetDriverClass() {
    assertThat( sparkSimbaDatabaseMeta.getDriverClass(),
      is( SparkSimbaDatabaseMeta.DRIVER_CLASS_NAME ) );
  }
}
