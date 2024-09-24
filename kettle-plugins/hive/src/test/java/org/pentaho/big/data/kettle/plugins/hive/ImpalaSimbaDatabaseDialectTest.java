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
