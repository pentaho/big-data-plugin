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

