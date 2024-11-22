/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package org.pentaho.big.data.kettle.plugins.hive;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.hadoop.shim.api.jdbc.DriverLocator;

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

  @BeforeClass
  public static void initLogs() {
    KettleLogStore.init();
  }

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
