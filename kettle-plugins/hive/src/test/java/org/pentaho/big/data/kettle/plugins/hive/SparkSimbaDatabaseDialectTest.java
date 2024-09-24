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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SparkSimbaDatabaseDialectTest {

  SparkSimbaDatabaseDialect dialect = new SparkSimbaDatabaseDialect();

  @Test
  public void getDatabaseType() throws Exception {
    assertThat( dialect.getDatabaseType(), is( SparkSimbaDatabaseDialect.DBTYPE ) );
  }

  @Test
  public void getNativeDriver() throws Exception {
    assertThat( dialect.getNativeDriver(), is( SparkSimbaDatabaseMeta.DRIVER_CLASS_NAME ) );
  }

  @Test
  public void getNativeJdbcPre() throws Exception {
    assertThat( dialect.getNativeJdbcPre(), is( "jdbc:spark://" ) );
  }

  @Test
  public void getDefaultDatabasePort() throws Exception {
    assertThat( dialect.getDefaultDatabasePort(), is( 10015 ) );
  }

  @Test
  public void getUsedLibraries() throws Exception {
    assertThat( dialect.getUsedLibraries(), is( new String[] { SparkSimbaDatabaseMeta.JAR_FILE } ) );
  }

  @Test
  public void testDefaultSocketTimeout() {
    Map<String, String> options = dialect.getDatabaseType().getDefaultOptions();
    assertThat( options, IsMapWithSize.aMapWithSize( 1 ) );
    assertThat( options, IsMapContaining.hasEntry( Joiner.on( "." ).join( SparkSimbaDatabaseDialect.DB_TYPE_NAME_SHORT,
        Hive2SimbaDatabaseDialect.SOCKET_TIMEOUT_OPTION ), Hive2SimbaDatabaseDialect.DEFAULT_SOCKET_TIMEOUT ) );
  }
}
