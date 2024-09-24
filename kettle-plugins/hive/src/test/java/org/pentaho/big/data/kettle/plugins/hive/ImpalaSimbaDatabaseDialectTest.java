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

import com.google.common.base.Joiner;
import java.util.Map;
import org.hamcrest.collection.IsMapContaining;
import org.hamcrest.collection.IsMapWithSize;
import org.junit.Test;
import org.pentaho.database.DatabaseDialectException;
import org.pentaho.database.model.DatabaseAccessType;
import org.pentaho.database.model.DatabaseConnection;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class ImpalaSimbaDatabaseDialectTest {
  private ImpalaSimbaDatabaseDialect dialect = new ImpalaSimbaDatabaseDialect();

  @Test
  public void testGetUrlNative() throws DatabaseDialectException {
    DatabaseConnection conn = new DatabaseConnection();
    conn.setAccessType( DatabaseAccessType.NATIVE );
    conn.setUsername( "jack" );
    conn.setHostname( "hostname" );
    assertThat( dialect.getURL( conn ), is( "jdbc:impala://hostname:21050/default;AuthMech=2;UID=jack" ) );
  }

  @Test
  public void testDefaultSocketTimeout() {
    Map<String, String> options = dialect.getDatabaseType().getDefaultOptions();
    assertThat( options, IsMapWithSize.aMapWithSize( 1 ) );
    assertThat( options, IsMapContaining.hasEntry( Joiner.on( "." ).join( ImpalaSimbaDatabaseDialect.DB_TYPE_NAME_SHORT,
        Hive2SimbaDatabaseDialect.SOCKET_TIMEOUT_OPTION ), Hive2SimbaDatabaseDialect.DEFAULT_SOCKET_TIMEOUT ) );
  }

}
