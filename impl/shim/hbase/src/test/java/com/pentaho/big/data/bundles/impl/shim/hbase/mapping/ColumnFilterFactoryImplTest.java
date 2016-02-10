/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

package com.pentaho.big.data.bundles.impl.shim.hbase.mapping;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.bigdata.api.hbase.mapping.ColumnFilter;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 2/9/16.
 */
public class ColumnFilterFactoryImplTest {
  private ColumnFilterFactoryImpl columnFilterFactory;

  @Before
  public void setup() {
    columnFilterFactory = new ColumnFilterFactoryImpl();
  }

  @Test
  public void testCreateFilterAlias() {
    String alias = "alias";
    assertEquals( alias, columnFilterFactory.createFilter( alias ).getFieldAlias() );
  }

  @Test
  public void testCreateFilterNode() throws ParserConfigurationException, IOException, SAXException {
    String alias = "alias";
    String fieldType = "type";
    ColumnFilter.ComparisonType comparisonType = ColumnFilter.ComparisonType.EQUAL;
    boolean signed = true;
    String constant = "constant";
    String format = "format";

    ColumnFilter filter = columnFilterFactory.createFilter( alias );
    filter.setFieldType( fieldType );
    filter.setComparisonOperator( comparisonType );
    filter.setSignedComparison( signed );
    filter.setConstant( constant );
    filter.setFormat( format );

    StringBuilder stringBuilder = new StringBuilder();
    filter.appendXML( stringBuilder );
    Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder()
      .parse( new ByteArrayInputStream( stringBuilder.toString().getBytes( Charset.forName( "UTF-8" ) ) ) );
    ColumnFilter result = columnFilterFactory.createFilter( document.getFirstChild() );
    assertEquals( alias, result.getFieldAlias() );
    assertEquals( fieldType, result.getFieldType() );
    assertEquals( comparisonType, result.getComparisonOperator() );
    assertEquals( signed, result.getSignedComparison() );
    assertEquals( constant, result.getConstant() );
    assertEquals( format, result.getFormat() );
  }

  @Test
  public void testCreateFilterRep() throws KettleException {
    String alias = "alias";
    String fieldType = "type";
    ColumnFilter.ComparisonType comparisonType = ColumnFilter.ComparisonType.EQUAL;
    boolean signed = true;
    String constant = "constant";
    String format = "format";

    Repository repository = mock( Repository.class );
    int nodeNum = 1;
    ObjectId id_step = mock( ObjectId.class );
    when( repository.getStepAttributeString( id_step, nodeNum, "cf_alias" ) ).thenReturn( alias );
    when( repository.getStepAttributeString( id_step, nodeNum, "cf_type" ) ).thenReturn( fieldType );
    when( repository.getStepAttributeString( id_step, nodeNum, "cf_comparison_opp" ) )
      .thenReturn( comparisonType.toString() );
    when( repository.getStepAttributeBoolean( id_step, nodeNum, "cf_signed_comp" ) ).thenReturn( signed );
    when( repository.getStepAttributeString( id_step, nodeNum, "cf_constant" ) ).thenReturn( constant );
    when( repository.getStepAttributeString( id_step, nodeNum, "cf_format" ) ).thenReturn( format );
    ColumnFilter result = columnFilterFactory.createFilter( repository, nodeNum, id_step );

    assertEquals( alias, result.getFieldAlias() );
    assertEquals( fieldType, result.getFieldType() );
    assertEquals( comparisonType, result.getComparisonOperator() );
    assertEquals( signed, result.getSignedComparison() );
    assertEquals( constant, result.getConstant() );
    assertEquals( format, result.getFormat() );
  }
}
