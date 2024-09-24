/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.hbase.meta;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;

import static org.junit.Assert.fail;

@RunWith( MockitoJUnitRunner.class )
public class AELHBaseMappingTest {
  private AELHBaseMappingImpl stubMapping;

  @Before
  public void setup() throws Exception {
    stubMapping = new AELHBaseMappingImpl();

    Node mappingNode = null;
    try {
      mappingNode = getMappingNode();
    } catch( Exception ex ) {
      fail();
    }

    stubMapping.loadXML( mappingNode );
  }

  @Test
  public void inflateFromXmlTest() {
    Assert.assertEquals( stubMapping.getTableName(), "iemployee" );
    Assert.assertEquals( stubMapping.getMappingName(), "simple input map" );
    Assert.assertEquals( stubMapping.getMappedColumns().size(), 3 );
  }

  @Test
  public void serializeToXmlTest() throws IOException {
    String serializedStub = stubMapping.getXML();

    Assert.assertTrue( serializedStub.contains( "iemployee" ) );
    Assert.assertTrue( serializedStub.contains( "simple input map" ) );
  }

  private Node getMappingNode() throws IOException, ParserConfigurationException, SAXException {
    String xml = IOUtils.toString( getClass().getClassLoader().getResourceAsStream( "StubMapping.xml" ) );

    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = factory.newDocumentBuilder();

    Document doc = builder.parse( new InputSource( new StringReader( xml ) ) );

    return doc.getDocumentElement();
  }
}

