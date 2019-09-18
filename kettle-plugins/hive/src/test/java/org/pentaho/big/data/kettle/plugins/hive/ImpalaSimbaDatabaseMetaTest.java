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

import java.sql.Driver;
import java.util.Arrays;
import java.util.Map;

import org.hamcrest.collection.IsMapContaining;
import org.hamcrest.collection.IsMapWithSize;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.hadoop.shim.api.jdbc.DriverLocator;
import org.pentaho.di.core.database.DatabaseMeta;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 10/21/15.
 */
@RunWith( MockitoJUnitRunner.class )
public class ImpalaSimbaDatabaseMetaTest {
  public static final String LOCALHOST = "localhost";
  public static final String PORT = "10000";
  public static final String DEFAULT = "default";
  @Mock DriverLocator driverLocator;
  @Mock Driver driver;
  @InjectMocks ImpalaSimbaDatabaseMeta impalaSimbaDatabaseMeta;
  private String impalaSimbaDatabaseMetaURL;

  @Before
  public void setup() throws Throwable {
    impalaSimbaDatabaseMetaURL = impalaSimbaDatabaseMeta.getURL( LOCALHOST, PORT, DEFAULT );
    when( driverLocator.getDriver( impalaSimbaDatabaseMetaURL ) ).thenReturn( driver );
  }

  @Test
  public void testGetAccessTypeList() {
    assertArrayEquals(
      new int[] { DatabaseMeta.TYPE_ACCESS_NATIVE, DatabaseMeta.TYPE_ACCESS_ODBC, DatabaseMeta.TYPE_ACCESS_JNDI },
      impalaSimbaDatabaseMeta.getAccessTypeList() );
  }

  @Test
  public void testGetDriverClassODBC() {
    impalaSimbaDatabaseMeta.setAccessType( DatabaseMeta.TYPE_ACCESS_ODBC );
    assertEquals( ImpalaSimbaDatabaseMeta.ODBC_DRIVER_CLASS_NAME, impalaSimbaDatabaseMeta.getDriverClass() );
  }

  @Test
  public void testGetDriverClassOther() {
    assertEquals( ImpalaSimbaDatabaseMeta.DRIVER_CLASS_NAME, impalaSimbaDatabaseMeta.getDriverClass() );
  }

  @Test
  public void testGetJdbcPrefix() {
    assertEquals( ImpalaSimbaDatabaseMeta.JDBC_URL_PREFIX,
      impalaSimbaDatabaseMeta.getJdbcPrefix() );
  }

  @Test
  public void testGetUsedLibraries() {
    assertTrue( Arrays.equals(
      impalaSimbaDatabaseMeta.getUsedLibraries(),
      new String[] { impalaSimbaDatabaseMeta.JAR_FILE } ) );
  }

  @Test
  public void testGetDefaultDatabasePort() {
    assertEquals( ImpalaSimbaDatabaseMeta.DEFAULT_PORT,
      impalaSimbaDatabaseMeta.getDefaultDatabasePort() );
  }

  @Test
  public void testGetDefaultOptions() {
    Map<String, String> options = impalaSimbaDatabaseMeta.getDefaultOptions();
    assertThat( options, IsMapWithSize.aMapWithSize( 1 ) );
    assertThat( options, IsMapContaining.hasEntry( impalaSimbaDatabaseMeta.getPluginId() + "."
      + SparkSimbaDatabaseMeta.SOCKET_TIMEOUT_OPTION, "10" ) );
  }
}
