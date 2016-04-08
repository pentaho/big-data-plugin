/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
 * <p>
 * ******************************************************************************
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package org.pentaho.trans.steps.hadoopfileoutput;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.steps.hadoopfileoutput.HadoopFileOutputMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HadoopFileOutputTest {

  // for message resolution
  private static Class<?> MessagePKG = HadoopFileOutputMeta.class;
  private static final String FILE_NAME = "fileName";

  private HadoopFileOutputMetaWrapper hadoopFileOutputMeta = new HadoopFileOutputMetaWrapper();
  private Node nodeMock;
  private IMetaStore metaStoreMock;
  private Repository repositoryMock;
  private ObjectId objectIdMock;

  @Before
  public void init(){
    nodeMock = mock( Node.class );
    metaStoreMock = mock( IMetaStore.class );
    repositoryMock = mock( Repository.class );
    objectIdMock = mock( ObjectId.class );
  }

  /**
   * Tests HadoopFileOutputMeta methods: 1. isFileAsCommand returns false 2. setFileAsCommand is not supported
   */
  @Test
  public void testFileAsCommandOption() {

    HadoopFileOutputMeta hadoopFileOutputMeta = new HadoopFileOutputMeta();

    // we expect isFileAsCommand to be false
    assertFalse( hadoopFileOutputMeta.isFileAsCommand() );

    // we expect setFileAsCommand(true or false) to be unsupported
    try {
      hadoopFileOutputMeta.setFileAsCommand( true );
    } catch ( Exception e ) {
      // the expected message is "class name":" message from the package that HadoopFileOutputMeta is in
      String expectedMessage =
          e.getClass().getName() + ": "
              + BaseMessages.getString( MessagePKG, "HadoopFileOutput.MethodNotSupportedException.Message" );
      assertTrue( e.getMessage().equals( expectedMessage ) );
    }
  }

  @Test
  public void testLoadResource() {
    initXMLHandlerMocks();
    String fileName = hadoopFileOutputMeta.loadSource( nodeMock, metaStoreMock );
    assertEquals( FILE_NAME, fileName );
  }

  @Test
  public void testLoadResourceNullFileName() {
    initXMLHandlerMocks();
    String fileName = hadoopFileOutputMeta.loadSource( null, metaStoreMock );
    assertNull( fileName );
  }

  private void initXMLHandlerMocks() {
    //mocks for XMLHandler class
    Node childNodeMock = mock( Node.class );
    Node firstChild = mock( Node.class );
    Node tagNode = mock( Node.class );
    NodeList nodeListMock = mock( NodeList.class );
    NodeList tags = mock( NodeList.class );

    when( nodeMock.getChildNodes() ).thenReturn( nodeListMock );
    when( nodeListMock.getLength() ).thenReturn( 1 );
    when( nodeListMock.item( 0 ) ).thenReturn( childNodeMock );
    when( childNodeMock.getNodeName() ).thenReturn( "file" );
    when( childNodeMock.getChildNodes() ).thenReturn( tags );
    when( tags.getLength() ).thenReturn( 1 );
    when( tags.item( 0 ) ).thenReturn( tagNode );
    when( tagNode.getNodeName() ).thenReturn( "name" );
    when( tagNode.getFirstChild() ).thenReturn( firstChild );
    when( firstChild.getNodeValue() ).thenReturn( FILE_NAME );
  }

  @Test
  public void testLoadResourceRep() throws KettleException {
    when( repositoryMock.getStepAttributeString( any( ObjectId.class ), anyString() ) ).thenReturn( FILE_NAME );
    String fileName = hadoopFileOutputMeta.loadSourceRep( repositoryMock, objectIdMock );
    assertEquals( FILE_NAME, fileName );
  }

  @Test
  public void testLoadResourceRepNullFileName() throws KettleException {
    when( repositoryMock.getStepAttributeString( any( ObjectId.class ), anyString() ) ).thenReturn( null );
    String fileName = hadoopFileOutputMeta.loadSourceRep( repositoryMock, objectIdMock );
    assertNull( fileName );
  }

  /**
   * Wrap protected methods
   */
  private class HadoopFileOutputMetaWrapper extends HadoopFileOutputMeta {

    public String loadSource( Node stepNode, IMetaStore iMetaStore ) {
      return super.loadSource( stepNode, iMetaStore );
    }

    public String loadSourceRep( Repository repository, ObjectId objectId ) throws KettleException {
      return super.loadSourceRep( repository, objectId );
    }
  }
}
