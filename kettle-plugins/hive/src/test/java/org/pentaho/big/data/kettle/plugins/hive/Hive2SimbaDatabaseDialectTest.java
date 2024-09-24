/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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
