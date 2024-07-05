/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 * <p>
 * ******************************************************************************
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
