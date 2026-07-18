/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
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
