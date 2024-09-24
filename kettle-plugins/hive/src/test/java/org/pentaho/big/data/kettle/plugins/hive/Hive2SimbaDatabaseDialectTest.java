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

import junit.framework.Assert;
import org.junit.Test;
import org.pentaho.database.DatabaseDialectException;
import org.pentaho.database.model.DatabaseAccessType;
import org.pentaho.database.model.DatabaseConnection;
import org.pentaho.database.model.IDatabaseType;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class Hive2SimbaDatabaseDialectTest {

  private Hive2SimbaDatabaseDialect dialect;

  public Hive2SimbaDatabaseDialectTest() {
    this.dialect = new Hive2SimbaDatabaseDialect();
  }

  @Test
  public void testGetNativeDriver() {
    Assert.assertEquals( dialect.getNativeDriver(), "org.apache.hive.jdbc.HiveSimbaDriver" );
  }

  @Test
  public void testGetURLNative() throws Exception {
    DatabaseConnection conn = new DatabaseConnection();
    conn.setAccessType( DatabaseAccessType.NATIVE );
    conn.setUsername( "joe" );
    assertThat( dialect.getURL( conn ), is( "jdbc:hive2://null:10000/default;AuthMech=2;UID=joe" ) );
  }

  @Test
  public void testGetURLJndi() throws DatabaseDialectException {
    DatabaseConnection conn = new DatabaseConnection();
    conn.setAccessType( DatabaseAccessType.JNDI );
    assertThat( dialect.getURL( conn ),
      is( SimbaUrl.URL_IS_CONFIGURED_THROUGH_JNDI ) );
  }

  @Test
  public void testGetUsedLibraries() {
    assertEquals( dialect.getUsedLibraries()[0], "HiveJDBC41.jar" );
  }

  @Test
  public void testGetNativeJdbcPre() {
    Assert.assertEquals( dialect.getNativeJdbcPre(), "jdbc:hive2://" );
  }

  @Test
  public void testGetDatabaseType() {
    IDatabaseType dbType = dialect.getDatabaseType();
    assertThat( dbType.getName(), is( "Hadoop Hive 2 (Simba)" ) );
  }

  @Test
  public void testGetReservedWords() {
    assertFalse( dialect.getReservedWords().length > 0 );
  }

  @Test
  public void testSupportsBitmapIndex() {
    assertTrue( dialect.supportsBitmapIndex() );
  }

  @Test
  public void testGetTruncateTableStatement() {
    String tableName = "table1";
    assertEquals( dialect.getTruncateTableStatement( tableName ), "TRUNCATE TABLE " + tableName );
  }
}
