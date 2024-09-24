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

import org.junit.Test;
import org.pentaho.database.model.DatabaseConnection;

import static org.junit.Assert.assertEquals;

/**
 * User: Dzmitry Stsiapanau Date: 10/4/14 Time: 10:55 PM
 */
public class ImpalaDatabaseDialectTest {

  @Test
  public void testGetURL() throws Exception {
    ImpalaDatabaseDialect impala = new ImpalaDatabaseDialect();
    DatabaseConnection dbconn = new DatabaseConnection();
    String url = impala.getURL( dbconn );
    assertEquals( "noauth url", "jdbc:hive2://null:null/null;impala_db=true;auth=noSasl", url );
    dbconn.addExtraOption( impala.getDatabaseType().getShortName(), "principal", "someValue" );
    url = impala.getURL( dbconn );
    assertEquals( "principal url", "jdbc:hive2://null:null/null;impala_db=true", url );
  }
}
