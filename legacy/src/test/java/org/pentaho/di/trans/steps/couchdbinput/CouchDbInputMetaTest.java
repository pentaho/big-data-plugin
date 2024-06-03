/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2024 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.steps.couchdbinput;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.encryption.TwoWayPasswordEncoderPluginType;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LoggingObjectInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.steps.loadsave.LoadSaveTester;
import org.pentaho.di.trans.steps.mock.StepMockHelper;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 10/28/15.
 */
public class CouchDbInputMetaTest {
  private CouchDbInputMeta couchDbInputMeta;

  @BeforeClass
  public static void beforeClass() throws KettleException {
    PluginRegistry.addPluginType( TwoWayPasswordEncoderPluginType.getInstance() );
    PluginRegistry.init( false );
    Encr.init( "Kettle" );
  }

  @Before
  public void setup() {
    couchDbInputMeta = new CouchDbInputMeta();
  }

  @Test
  public void testLoadSave() throws KettleException {
    List<String> commonAttributes = new ArrayList<>();
    commonAttributes.add( "hostname" );
    commonAttributes.add( "port" );
    commonAttributes.add( "dbName" );
    commonAttributes.add( "designDocument" );
    commonAttributes.add( "viewName" );
    commonAttributes.add( "authenticationUser" );
    commonAttributes.add( "authenticationPassword" );

    LoadSaveTester<CouchDbInputMeta> couchDbInputLoadSaveTester =
      new LoadSaveTester<CouchDbInputMeta>( CouchDbInputMeta.class, commonAttributes );

    couchDbInputLoadSaveTester.testSerialization();
  }

  @Test
  public void testClone() {
    String testHostname = "testHostname";
    couchDbInputMeta.setHostname( testHostname );
    assertEquals( testHostname, ( (CouchDbInputMeta) couchDbInputMeta.clone() ).getHostname() );
  }

  @Test
  public void testSetDefault() {
    assertNull( couchDbInputMeta.getHostname() );
    assertNull( couchDbInputMeta.getPort() );
    assertNull( couchDbInputMeta.getDbName() );
    assertNull( couchDbInputMeta.getViewName() );
    couchDbInputMeta.setDefault();
    assertEquals( CouchDbInputMeta.DEFAULT_HOSTNAME, couchDbInputMeta.getHostname() );
    assertEquals( CouchDbInputMeta.DEFAULT_PORT, couchDbInputMeta.getPort() );
    assertEquals( CouchDbInputMeta.DEFAULT_DB_NAME, couchDbInputMeta.getDbName() );
    assertEquals( CouchDbInputMeta.DEFAULT_VIEW_NAME, couchDbInputMeta.getViewName() );
  }

  @Test
  public void testGetFields() throws KettleStepException {
    RowMetaInterface rowMetaInterface = mock( RowMetaInterface.class );
    String testOrigin = "testOrigin";
    couchDbInputMeta.getFields( rowMetaInterface, testOrigin, null, null, null, null, null );
    ArgumentCaptor<ValueMetaInterface> valueMetaInterfaceArgumentCaptor =
      ArgumentCaptor.forClass( ValueMetaInterface.class );
    verify( rowMetaInterface ).addValueMeta( valueMetaInterfaceArgumentCaptor.capture() );
    ValueMetaInterface valueMetaInterface = valueMetaInterfaceArgumentCaptor.getValue();
    assertEquals( CouchDbInputMeta.VALUE_META_NAME, valueMetaInterface.getName() );
    assertEquals( ValueMetaInterface.TYPE_STRING, valueMetaInterface.getType() );
    assertEquals( testOrigin, valueMetaInterface.getOrigin() );
  }

  @Test( expected = KettleXMLException.class )
  public void testLoadXmlException() throws KettleXMLException {
    Node node = mock( Node.class );
    when( node.getChildNodes() ).thenThrow( new RuntimeException() );
    couchDbInputMeta.loadXML( node, null, (IMetaStore) null );
  }

  @Test( expected = KettleException.class )
  public void testReadRepException() throws KettleException {
    Repository repository = mock( Repository.class );
    ObjectId objectId = mock( ObjectId.class );
    when( repository.getStepAttributeString( objectId, "hostname" ) ).thenThrow( new RuntimeException() );
    couchDbInputMeta.readRep( repository, null, objectId, null );
  }

  @Test( expected = KettleException.class )
  public void testSaveRepException() throws KettleException {
    couchDbInputMeta.setDefault();
    Repository repository = mock( Repository.class );
    ObjectId transId = mock( ObjectId.class );
    ObjectId stepId = mock( ObjectId.class );
    doThrow( new RuntimeException() ).when( repository )
      .saveStepAttribute( transId, stepId, "hostname", CouchDbInputMeta.DEFAULT_HOSTNAME );
    couchDbInputMeta.saveRep( repository, null, transId, stepId );
  }

  @Test
  public void testGetStep() {
    StepMockHelper<CouchDbInputMeta, CouchDbInputData> stepMockHelper =
      new StepMockHelper<CouchDbInputMeta, CouchDbInputData>( "testName", CouchDbInputMeta.class,
        CouchDbInputData.class );
    when( stepMockHelper.logChannelInterfaceFactory.create( any(), any( LoggingObjectInterface.class ) ) )
      .thenReturn( mock( LogChannelInterface.class ) );
    assertTrue( couchDbInputMeta
      .getStep( stepMockHelper.stepMeta, stepMockHelper.stepDataInterface, 0, stepMockHelper.transMeta,
        stepMockHelper.trans ) instanceof CouchDbInput );
  }

  @Test
  public void testGetStepData() {
    assertTrue( couchDbInputMeta.getStepData() instanceof CouchDbInputData );
  }
}
