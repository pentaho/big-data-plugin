/*! ******************************************************************************
 *
 * Pentaho Data Integration
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
package org.pentaho.big.data.kettle.plugins.mapreduce.ui.entry.pmr;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.inOrder;

import java.util.Arrays;
import java.util.List;

import org.eclipse.swt.widgets.Shell;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.big.data.kettle.plugins.mapreduce.entry.pmr.JobEntryHadoopTransJobExecutor;
import org.pentaho.big.data.plugins.common.ui.HadoopClusterDelegateImpl;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.job.JobMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.containers.XulDialog;
import org.pentaho.ui.xul.dom.Document;

/**
 * @author Tatsiana_Kasiankova
 *
 */
public class JobEntryHadoopTransJobExecutorControllerTest {

  private static final String EDITED_NAMED_CLUSTER = "Edited Named Cluster";
  private static final String SELECTED_NAMED_CLUSTER = "Selected Named Cluster";
  private static final String A_NEW_NAMED_CLUSTER = "A New Named Cluster";

  private JobEntryHadoopTransJobExecutorController testController;
  private XulDomContainer containerMock = mock( XulDomContainer.class );
  private Document rootDocMock = mock( Document.class );
  private XulDialog xulDialogMock = mock( XulDialog.class );
  private HadoopClusterDelegateImpl ncDelegateMock = mock( HadoopClusterDelegateImpl.class );
  private NamedClusterService namedClusterServiceMock = mock( NamedClusterService.class );
  private NamedCluster selectedNamedClusterMock = mock( NamedCluster.class );
  private NamedCluster newNamedClusterMock = mock( NamedCluster.class );
  private NamedCluster editedNamedClusterMock = mock( NamedCluster.class );
  private IMetaStore metaStoreMock = mock( IMetaStore.class );
  private JobMeta jobMetaMock = mock( JobMeta.class );
  private JobEntryHadoopTransJobExecutor jobEntryHadoopTransJobExecutor = mock( JobEntryHadoopTransJobExecutor.class );

  private List<NamedCluster> ncList;

  @Before
  public void setUp() throws Throwable {
    testController = new JobEntryHadoopTransJobExecutorController( ncDelegateMock, namedClusterServiceMock );
    testController.setXulDomContainer( containerMock );
    testController.setSelectedNamedCluster( selectedNamedClusterMock );

    when( ncDelegateMock.newNamedCluster( any( VariableSpace.class ), any( IMetaStore.class ), any( Shell.class ) ) )
        .thenReturn( A_NEW_NAMED_CLUSTER );
    when( ncDelegateMock.editNamedCluster( any( IMetaStore.class ), any( NamedCluster.class ), any( Shell.class ) ) )
        .thenReturn( EDITED_NAMED_CLUSTER );
    when( selectedNamedClusterMock.getName() ).thenReturn( SELECTED_NAMED_CLUSTER );
    when( newNamedClusterMock.getName() ).thenReturn( A_NEW_NAMED_CLUSTER );
    when( editedNamedClusterMock.getName() ).thenReturn( EDITED_NAMED_CLUSTER );

    when( containerMock.getDocumentRoot() ).thenReturn( rootDocMock );
    when( rootDocMock.getElementById( "job-entry-dialog" ) ).thenReturn( xulDialogMock );

    testController.setJobMeta( jobMetaMock );
    when( jobMetaMock.getMetaStore() ).thenReturn( metaStoreMock );
    // do this controller as spy to check
    testController = spy( testController );

    ncList =
        Arrays.asList( new NamedCluster[] { selectedNamedClusterMock, newNamedClusterMock, editedNamedClusterMock } );
    when( namedClusterServiceMock.list( metaStoreMock ) ).thenReturn( ncList );
  }

  @Test
  public void testEditNamedCluster_NamedClusterIsSelected() throws MetaStoreException {
    testController.editNamedCluster();

    verify( ncDelegateMock ).editNamedCluster( any( IMetaStore.class ), any( NamedCluster.class ), any( Shell.class ) );
    // verify the times of call
    verify( testController ).namedClustersChanged();
    verify( testController ).selectedNamedClusterChanged( SELECTED_NAMED_CLUSTER, EDITED_NAMED_CLUSTER );
    // Verify order of execution
    InOrder order = inOrder( testController );
    order.verify( testController ).namedClustersChanged();
    order.verify( testController ).selectedNamedClusterChanged( SELECTED_NAMED_CLUSTER, EDITED_NAMED_CLUSTER );
  }

  @Test
  public void testEditNamedCluster_NamedClusterIsNOTSelected() throws MetaStoreException {
    testController.setSelectedNamedCluster( null );
    testController.editNamedCluster();

    verify( ncDelegateMock, never() ).editNamedCluster( any( IMetaStore.class ), any( NamedCluster.class ), any(
        Shell.class ) );
    // verify the times of call
    verify( testController, never() ).namedClustersChanged();
    verify( testController, never() ).selectedNamedClusterChanged( SELECTED_NAMED_CLUSTER, EDITED_NAMED_CLUSTER );
  }

  @Test
  public void testNewNamedCluster_NamedClusterIsSelected() throws MetaStoreException {
    testController.newNamedCluster();

    verify( ncDelegateMock ).newNamedCluster( any( VariableSpace.class ), any( IMetaStore.class ), any( Shell.class ) );
    // verify the times of call
    verify( testController ).namedClustersChanged();
    verify( testController ).selectedNamedClusterChanged( SELECTED_NAMED_CLUSTER, A_NEW_NAMED_CLUSTER );
    // Verify order of execution
    InOrder order = inOrder( testController );
    order.verify( testController ).namedClustersChanged();
    order.verify( testController ).selectedNamedClusterChanged( SELECTED_NAMED_CLUSTER, A_NEW_NAMED_CLUSTER );
  }

  @Test
  public void testNewNamedCluster_NamedClusterIsNotSelected() throws MetaStoreException {
    testController.setSelectedNamedCluster( null );
    testController.newNamedCluster();

    verify( ncDelegateMock ).newNamedCluster( any( VariableSpace.class ), any( IMetaStore.class ), any( Shell.class ) );
    // verify the times of call
    verify( testController ).namedClustersChanged();
    verify( testController ).selectedNamedClusterChanged( null, A_NEW_NAMED_CLUSTER );
    // Verify order of execution
    InOrder order = inOrder( testController );
    order.verify( testController ).namedClustersChanged();
    order.verify( testController ).selectedNamedClusterChanged( null, A_NEW_NAMED_CLUSTER );
  }

}
