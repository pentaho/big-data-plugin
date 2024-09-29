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


package org.pentaho.database;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.pentaho.di.core.database.DatabaseMeta;

/**
 * Tests the value returned from org.pentaho.di.core.database.DatabaseInterface.getSelectCountStatement for the database
 * the interface is fronting.
 * 
 * As this release, Hive uses the following to select the number of rows:
 * 
 * SELECT COUNT(1) FROM ....
 * 
 * All other databases use:
 * 
 * SELECT COUNT(*) FROM ....
 */
public class TestSelectCount {

  private static final String HiveSelect = "select count(1) from ";
  private static final String TableName = "NON_EXISTANT";

  public static final String HiveDatabaseXML = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + "<connection>"
      + "<name>Hadoop Hive</name>" + "<server>127.0.0.1</server>" + "<type>Hadoop Hive</type>" + "<access></access>"
      + "<database>default</database>" + "<port>10000</port>" + "<username>sean</username>"
      + "<password>sean</password>" + "</connection>";

  @Test
  public void testHiveDatabase() throws Exception {
    try {
      String expectedSQL = HiveSelect + TableName;
      DatabaseMeta databaseMeta = new DatabaseMeta( HiveDatabaseXML );
      String sql = databaseMeta.getDatabaseInterface().getSelectCountStatement( TableName );
      assertTrue( sql.equalsIgnoreCase( expectedSQL ) );
    } catch ( Exception e ) {
      e.printStackTrace();
    }
  }

}
