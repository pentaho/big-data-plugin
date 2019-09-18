/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.plugins.common.ui.named.cluster.bridge;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.TreeSelection;
import org.pentaho.di.ui.spoon.delegates.SpoonTreeDelegateExtension;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.stores.delegate.DelegatingMetaStore;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 10/2/15.
 */
public class HadoopClusterTreeDelegateExtensionTest {
  private NamedClusterService namedClusterService;
  private HadoopClusterTreeDelegateExtension.SpoonProvider spoonProvider;
  private HadoopClusterTreeDelegateExtension hadoopClusterTreeDelegateExtension;
  private SpoonTreeDelegateExtension spoonTreeDelegateExtension;
  private LogChannelInterface logChannelInterface;
  private List<TreeSelection> treeSelectionList;
  private TransMeta transMeta;
  private Spoon spoon;
  private DelegatingMetaStore delegatingMetaStore;

  @Before
  public void setup() {
    namedClusterService = mock( NamedClusterService.class );
    spoonProvider = mock( HadoopClusterTreeDelegateExtension.SpoonProvider.class );
    spoon = mock( Spoon.class );
    when( spoonProvider.getSpoon() ).thenReturn( spoon );
    delegatingMetaStore = mock( DelegatingMetaStore.class );
    when( spoon.getMetaStore() ).thenReturn( delegatingMetaStore );
    hadoopClusterTreeDelegateExtension = new HadoopClusterTreeDelegateExtension( namedClusterService, spoonProvider );
    spoonTreeDelegateExtension = mock( SpoonTreeDelegateExtension.class );
    treeSelectionList = mock( List.class );
    when( spoonTreeDelegateExtension.getObjects() ).thenReturn( treeSelectionList );
    transMeta = mock( TransMeta.class );
    when( spoonTreeDelegateExtension.getTransMeta() ).thenReturn( transMeta );
    logChannelInterface = mock( LogChannelInterface.class );
  }

  @Test
  public void testOneArgConstructor() {
    assertNotNull( new HadoopClusterTreeDelegateExtension( namedClusterService ) );
  }

  @Test
  public void testSpoonProvider() {
    assertEquals( Spoon.getInstance(), new HadoopClusterTreeDelegateExtension.SpoonProvider().getSpoon() );
  }

  @Test
  public void testCase1() throws KettleException {
    when( spoonTreeDelegateExtension.getCaseNumber() ).thenReturn( 1 );
    hadoopClusterTreeDelegateExtension.callExtensionPoint( logChannelInterface, spoonTreeDelegateExtension );
    verifyNoMoreInteractions( treeSelectionList );
  }

  @Test
  public void testCase3NamedCluster() throws KettleException {
    when( spoonTreeDelegateExtension.getCaseNumber() ).thenReturn( 3 );
    String[] path = { "", "", HadoopClusterTreeDelegateExtension.STRING_NAMED_CLUSTERS };
    when( spoonTreeDelegateExtension.getPath() ).thenReturn( path );
    hadoopClusterTreeDelegateExtension.callExtensionPoint( logChannelInterface, spoonTreeDelegateExtension );
    ArgumentCaptor<TreeSelection> captor = ArgumentCaptor.forClass( TreeSelection.class );
    verify( treeSelectionList ).add( captor.capture() );
    TreeSelection treeSelection = captor.getValue();
    assertEquals( path[ 2 ], treeSelection.getItemText() );
    assertEquals( NamedCluster.class, treeSelection.getSelection() );
    assertEquals( transMeta, treeSelection.getParent() );
  }

  @Test
  public void testCase3NotNamedCluster() throws KettleException {
    when( spoonTreeDelegateExtension.getCaseNumber() ).thenReturn( 3 );
    String[] path = { "", "", "otherName" };
    when( spoonTreeDelegateExtension.getPath() ).thenReturn( path );
    hadoopClusterTreeDelegateExtension.callExtensionPoint( logChannelInterface, spoonTreeDelegateExtension );
    verifyNoMoreInteractions( treeSelectionList );
  }

  @Test
  public void testCase4NamedCluster() throws KettleException, MetaStoreException {
    when( spoonTreeDelegateExtension.getCaseNumber() ).thenReturn( 4 );
    String testPath3 = "testPath3";
    org.pentaho.hadoop.shim.api.cluster.NamedCluster namedCluster = mock(
      org.pentaho.hadoop.shim.api.cluster.NamedCluster.class );
    when( namedClusterService.read( testPath3, delegatingMetaStore ) ).thenReturn( namedCluster );
    when( namedCluster.getName() ).thenReturn( testPath3 );
    String[] path = { "", "", HadoopClusterTreeDelegateExtension.STRING_NAMED_CLUSTERS, testPath3 };
    when( spoonTreeDelegateExtension.getPath() ).thenReturn( path );
    hadoopClusterTreeDelegateExtension.callExtensionPoint( logChannelInterface, spoonTreeDelegateExtension );
    ArgumentCaptor<TreeSelection> captor = ArgumentCaptor.forClass( TreeSelection.class );
    verify( treeSelectionList ).add( captor.capture() );
    TreeSelection treeSelection = captor.getValue();
    assertEquals( path[ 3 ], treeSelection.getItemText() );
    assertEquals( testPath3, ( (NamedCluster) treeSelection.getSelection() ).getName() );
    assertEquals( transMeta, treeSelection.getParent() );
  }

  @Test
  public void testCase4NotNamedCluster() throws KettleException {
    when( spoonTreeDelegateExtension.getCaseNumber() ).thenReturn( 4 );
    String[] path = { "", "", "otherName" };
    when( spoonTreeDelegateExtension.getPath() ).thenReturn( path );
    hadoopClusterTreeDelegateExtension.callExtensionPoint( logChannelInterface, spoonTreeDelegateExtension );
    verifyNoMoreInteractions( treeSelectionList );
  }


  @Test
  public void testCase4MetastoreException() throws KettleException, MetaStoreException {
    when( spoonTreeDelegateExtension.getCaseNumber() ).thenReturn( 4 );
    String testPath3 = "testPath3";
    org.pentaho.hadoop.shim.api.cluster.NamedCluster namedCluster = mock(
      org.pentaho.hadoop.shim.api.cluster.NamedCluster.class );
    when( namedClusterService.read( testPath3, delegatingMetaStore ) ).thenThrow( new MetaStoreException() );
    when( namedCluster.getName() ).thenReturn( testPath3 );
    String[] path = { "", "", HadoopClusterTreeDelegateExtension.STRING_NAMED_CLUSTERS, testPath3 };
    when( spoonTreeDelegateExtension.getPath() ).thenReturn( path );
    hadoopClusterTreeDelegateExtension.callExtensionPoint( logChannelInterface, spoonTreeDelegateExtension );
    verifyNoMoreInteractions( treeSelectionList );
  }
}
