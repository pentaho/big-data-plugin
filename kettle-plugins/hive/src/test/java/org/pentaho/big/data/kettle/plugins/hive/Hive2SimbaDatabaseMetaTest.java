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
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.hadoop.shim.api.jdbc.DriverLocator;
import org.pentaho.di.core.database.DatabaseMeta;

import java.sql.Driver;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class Hive2SimbaDatabaseMetaTest {
  public static final String LOCALHOST = "localhost";
  public static final String PORT = "10000";
  public static final String DEFAULT = "default";
  @Mock DriverLocator driverLocator;
  @Mock Driver driver;
  @InjectMocks Hive2SimbaDatabaseMeta hive2SimbaDatabaseMeta;
  private String hive2SimbaDatabaseMetaURL;

  @Before
  public void setup() throws Throwable {
    hive2SimbaDatabaseMetaURL = hive2SimbaDatabaseMeta.getURL( LOCALHOST, PORT, DEFAULT );
    when( driverLocator.getDriver( hive2SimbaDatabaseMetaURL ) ).thenReturn( driver );
  }

  @Test
  public void testGetDriverClassOther() {
    assertEquals( Hive2SimbaDatabaseMeta.DRIVER_CLASS_NAME, hive2SimbaDatabaseMeta.getDriverClass() );
  }

  @Test
  public void testGetDriverClassODBC() {
    hive2SimbaDatabaseMeta.setAccessType( DatabaseMeta.TYPE_ACCESS_ODBC );
    assertEquals( Hive2SimbaDatabaseMeta.ODBC_DRIVER_CLASS_NAME, hive2SimbaDatabaseMeta.getDriverClass() );
  }

  @Test
  public void testGetJdbcPrefix() {
    assertEquals( Hive2SimbaDatabaseMeta.JDBC_URL_PREFIX,
      hive2SimbaDatabaseMeta.getJdbcPrefix() );
  }

  @Test
  public void testGetUsedLibraries() {
    assertTrue( Arrays.equals(
      hive2SimbaDatabaseMeta.getUsedLibraries(),
      new String[] { hive2SimbaDatabaseMeta.JAR_FILE } ) );
  }

  @Test
  public void testGetDefaultDatabasePort() {
    assertEquals( Hive2SimbaDatabaseMeta.DEFAULT_PORT,
      hive2SimbaDatabaseMeta.getDefaultDatabasePort() );
  }
}
