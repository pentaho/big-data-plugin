/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2020 by Hitachi Vantara : http://www.pentaho.com
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