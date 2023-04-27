/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.big.data.kettle.plugins.hdfs.trans.analyzer;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.pentaho.big.data.kettle.plugins.hdfs.trans.HadoopFileMeta;
import org.pentaho.di.core.bowl.DefaultBowl;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.file.BaseFileMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.dictionary.DictionaryConst;
import org.pentaho.metaverse.api.IComponentDescriptor;
import org.pentaho.metaverse.api.IMetaverseBuilder;
import org.pentaho.metaverse.api.IMetaverseNode;
import org.pentaho.metaverse.api.INamespace;
import org.pentaho.metaverse.api.MetaverseComponentDescriptor;
import org.pentaho.metaverse.api.MetaverseObjectFactory;
import org.pentaho.metaverse.api.model.IExternalResourceInfo;

import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public abstract class HadoopBaseStepAnalyzerTest<A extends HadoopBaseStepAnalyzer, M extends BaseFileMeta> {

  protected A analyzer;

  @Mock private INamespace mockNamespace;
  private IComponentDescriptor descriptor;
  private M meta;
  @Mock private TransMeta transMeta;

  @Before
  public void setUp() throws Exception {
    when( mockNamespace.getParentNamespace() ).thenReturn( mockNamespace );
    descriptor = new MetaverseComponentDescriptor( "test", DictionaryConst.NODE_TYPE_TRANS_STEP, mockNamespace );
    analyzer = spy( getAnalyzer() );
    analyzer.setDescriptor( descriptor );
    IMetaverseBuilder builder = mock( IMetaverseBuilder.class );
    analyzer.setMetaverseBuilder( builder );
    analyzer.setObjectFactory( new MetaverseObjectFactory() );

    meta = getMetaMock();
    StepMeta mockStepMeta = mock( StepMeta.class );
    when( meta.getParentStepMeta() ).thenReturn( mockStepMeta );

    lenient().when( transMeta.getBowl() ).thenReturn( DefaultBowl.getInstance() );
    lenient().when( mockStepMeta.getParentTransMeta() ).thenReturn( transMeta );
  }

  protected abstract A getAnalyzer();

  protected abstract M getMetaMock();

  @Test
  public void testGetUsedFields() throws Exception {
    assertNull( analyzer.getUsedFields( getMetaMock() ) );
  }

  @Test
  public void testGetResourceInputNodeType() throws Exception {
    assertEquals( DictionaryConst.NODE_TYPE_FILE_FIELD, analyzer.getResourceInputNodeType() );
  }

  @Test
  public void testGetResourceOutputNodeType() throws Exception {
    assertEquals( DictionaryConst.NODE_TYPE_FILE_FIELD, analyzer.getResourceOutputNodeType() );
  }

  @Test
  public void testGetSupportedSteps() {
    Set<Class<? extends BaseStepMeta>> types = analyzer.getSupportedSteps();
    assertNotNull( types );
    assertEquals( types.size(), 1 );
    assertTrue( types.contains( getMetaClass() ) );
  }

  protected abstract Class<M> getMetaClass();

  @Test
  public void testCreateResourceNode() throws Exception {
    // local
    IExternalResourceInfo localResource = mock( IExternalResourceInfo.class );
    when( localResource.getName() ).thenReturn( "file:///Users/home/tmp/xyz.ktr" );
    analyzer.validateState( descriptor, getMetaMock() );
    IMetaverseNode resourceNode = analyzer.createResourceNode( getMetaMock(), localResource );
    assertNotNull( resourceNode );
    assertEquals( DictionaryConst.NODE_TYPE_FILE, resourceNode.getType() );

    // remote
    final HadoopFileMeta hMeta = (HadoopFileMeta) getMetaMock();
    IExternalResourceInfo remoteResource = mock( IExternalResourceInfo.class );
    final String hostName = "foo.com";
    final String filePath = "hdfs://" + hostName + "/file.csv";
    when( remoteResource.getName() ).thenReturn( filePath );
    when( hMeta.getUrlHostName( filePath ) ).thenReturn( hostName );
    resourceNode = analyzer.createResourceNode( getMetaMock(), remoteResource );
    assertNotNull( resourceNode );
    assertEquals( DictionaryConst.NODE_TYPE_FILE, resourceNode.getType() );
  }
}
