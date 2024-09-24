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

package org.pentaho.big.data.kettle.plugins.hdfs.vfs;

import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Tree;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.big.data.plugins.common.ui.NamedClusterWidgetImpl;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;
import org.pentaho.vfs.ui.VfsBrowser;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;

public class HadoopVfsFileChooserDialogTest {

  private HadoopVfsFileChooserDialog hadoopVfsFileChooserDialog = null;
  private static final Integer SELECTED_INDEX = -1;
  private static final String[] NAMED_CLUSTER_NAMES = {"name1", "name2", "name3"};

  @Before
  public void Initialization() {
    hadoopVfsFileChooserDialog = mock( HadoopVfsFileChooserDialog.class );
  }

  @After
  public void finalize() {
    hadoopVfsFileChooserDialog = null;
  }

  @Test
  public void testActivate() {
    doCallRealMethod().when( hadoopVfsFileChooserDialog ).activate();

    VfsFileChooserDialog vfsFileChooserDialog = mock( VfsFileChooserDialog.class );
    Combo combo = mock( Combo.class );
    Tree tree = mock( Tree.class );
    VfsBrowser vfsBrowser = mock( VfsBrowser.class );
    doNothing().when( combo ).setText( anyString() );
    vfsFileChooserDialog.openFileCombo = combo;
    doNothing().when( tree ).removeAll();
    vfsBrowser.fileSystemTree = tree;
    vfsFileChooserDialog.vfsBrowser = vfsBrowser;
    doCallRealMethod().when( vfsFileChooserDialog ).setRootFile( null );
    doCallRealMethod().when( vfsFileChooserDialog ).setInitialFile( null );
    hadoopVfsFileChooserDialog.vfsFileChooserDialog = vfsFileChooserDialog;

    NamedClusterWidgetImplExtend namedClusterWidgetImpl = mock( NamedClusterWidgetImplExtend.class );
    Combo namedClusterCombo = mock( Combo.class );
    when( namedClusterCombo.getSelectionIndex() ).thenReturn( SELECTED_INDEX );
    doNothing().when( namedClusterCombo ).removeAll();
    doNothing().when( namedClusterCombo ).setItems( any() );
    doNothing().when( namedClusterCombo ).select( SELECTED_INDEX );
    when( namedClusterWidgetImpl.getNameClusterCombo() ).thenReturn( namedClusterCombo );
    when( namedClusterWidgetImpl.getNamedClusterNames() ).thenReturn( NAMED_CLUSTER_NAMES );
    doCallRealMethod().when( namedClusterWidgetImpl ).initiate();
    doNothing().when( namedClusterWidgetImpl ).setSelectedNamedCluster( anyString() );
    when( hadoopVfsFileChooserDialog.getNamedClusterWidget() ).thenReturn( namedClusterWidgetImpl );

    hadoopVfsFileChooserDialog.activate();
    verify( namedClusterWidgetImpl, times( 1 ) ).initiate();
  }

  private class NamedClusterWidgetImplExtend extends NamedClusterWidgetImpl {
    public NamedClusterWidgetImplExtend( Composite parent, boolean showLabel, NamedClusterService namedClusterService, RuntimeTestActionService runtimeTestActionService, RuntimeTester clusterTester ) {
      super( parent, showLabel, namedClusterService, runtimeTestActionService, clusterTester );
    }

    /*Overriding for visibility change only*/
    @Override
    public String[] getNamedClusterNames() {
      return super.getNamedClusterNames();
    }
  }

}
