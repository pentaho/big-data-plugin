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
package org.pentaho.big.data.kettle.plugins.hbase;

import org.junit.Test;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;

import static org.junit.Assert.*;

public class HbaseUtilTest {

  @Test
  public void testParseNamespaceFromTableName() {
    assertEquals( "namespace", HbaseUtil.parseNamespaceFromTableName( "namespace:qualifier" ) );
    assertEquals( "namespace", HbaseUtil.parseNamespaceFromTableName( "namespace:qualifier", "other" ) );
    assertEquals( "other", HbaseUtil.parseNamespaceFromTableName( "qualifier", "other" ) );
    assertEquals( null, HbaseUtil.parseNamespaceFromTableName( "qualifier", null ) );
  }

  @Test
  public void testParseQualifierFromTableName() {
    assertEquals( "qualifier", HbaseUtil.parseQualifierFromTableName( "namespace:qualifier" ) );
    assertEquals( "qualifier", HbaseUtil.parseQualifierFromTableName( ":qualifier" ) );
    assertEquals( "qualifier", HbaseUtil.parseQualifierFromTableName( "qualifier" ) );
    assertEquals( "", HbaseUtil.parseQualifierFromTableName( "namespace:" ) );
  }

  @Test
  public void testExpandTableName() {
    assertEquals( "default:", HbaseUtil.expandTableName( null ) );
    assertEquals( "default:qualifier", HbaseUtil.expandTableName( "qualifier" ) );
    assertEquals( "default:qualifier", HbaseUtil.expandTableName( ":qualifier" ) );
    assertEquals( "default:qualifier", HbaseUtil.expandTableName( "qualifier" ) );
    assertEquals( "namespace:qualifier", HbaseUtil.expandTableName( "namespace","qualifier" ) );
    assertEquals( "namespace:qualifier", HbaseUtil.expandTableName( "namespace","other:qualifier" ) );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIllegalArgsInExpandTableName() {
      HbaseUtil.expandTableName( "","" );
  }

  @Test
  public void expandLegacyTableNameOnLoad() {
    assertEquals("default:", HbaseUtil.expandLegacyTableNameOnLoad( null ) );
    assertEquals( "default:weblogs", HbaseUtil.expandLegacyTableNameOnLoad( "weblogs" ) );
    assertEquals( "ns:weblogs", HbaseUtil.expandLegacyTableNameOnLoad( "ns:weblogs" ) );
    assertEquals( "ns:${two}", HbaseUtil.expandLegacyTableNameOnLoad( "ns:${two}" ) );
    assertEquals( "default:${two}", HbaseUtil.expandLegacyTableNameOnLoad( ":${two}" ) );
    assertEquals( "${one}", HbaseUtil.expandLegacyTableNameOnLoad( "${one}" ) );
    assertEquals( "%%one%%", HbaseUtil.expandLegacyTableNameOnLoad( "%%one%%" ) );
    assertEquals( "${one}:${two}", HbaseUtil.expandLegacyTableNameOnLoad( "${one}:${two}" ) );
    assertEquals( "default:", HbaseUtil.expandLegacyTableNameOnLoad( "" ) );
    assertEquals( "${one}:two", HbaseUtil.expandLegacyTableNameOnLoad( "${one}:two" ) );
  }
}