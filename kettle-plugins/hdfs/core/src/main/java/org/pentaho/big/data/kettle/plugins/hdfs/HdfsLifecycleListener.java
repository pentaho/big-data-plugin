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


package org.pentaho.big.data.kettle.plugins.hdfs;

//import org.pentaho.di.core.plugins.ParentPlugin;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.big.data.kettle.plugins.hdfs.vfs.HadoopVfsFileChooserDialog;
import org.pentaho.big.data.kettle.plugins.hdfs.vfs.MapRFSFileChooserDialog;
import org.pentaho.big.data.kettle.plugins.hdfs.vfs.NamedClusterVfsFileChooserDialog;
import org.pentaho.big.data.kettle.plugins.hdfs.vfs.Schemes;
import org.pentaho.di.core.annotations.LifecyclePlugin;
import org.pentaho.di.core.lifecycle.LifeEventHandler;
import org.pentaho.di.core.lifecycle.LifecycleException;
import org.pentaho.di.core.lifecycle.LifecycleListener;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionHandler;
import org.pentaho.runtime.test.action.RuntimeTestActionService;
import org.pentaho.runtime.test.action.impl.LoggingRuntimeTestActionHandlerImpl;
import org.pentaho.runtime.test.action.impl.RuntimeTestActionServiceImpl;
import org.pentaho.runtime.test.i18n.impl.BaseMessagesMessageGetterFactoryImpl;
import org.pentaho.runtime.test.impl.RuntimeTesterImpl;
import org.pentaho.vfs.ui.VfsFileChooserDialog;
import org.pentaho.big.data.impl.cluster.NamedClusterManager;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by bryan on 11/23/15.
 */
@LifecyclePlugin( id = "HdfsLifecycleListener", name = "HdfsLifecycleListener" )
//@ParentPlugin( pathFromDataIntegration = "plugins/pentaho-big-data-plugin" )
public class HdfsLifecycleListener implements LifecycleListener {

  private final int hdfsPriority = 150;
  private final int maprPriority = 160;
  private final int ncPriority = 110;

  private final NamedClusterService ncService;
  private final RuntimeTestActionService rtTestActServ;
  private final RuntimeTester rtTester;
  private HadoopVfsFileChooserDialog hdfsFileChooserDialog;
  private MapRFSFileChooserDialog mapRFSFileChooserDialog;
  private NamedClusterVfsFileChooserDialog ncFileChooserDialog;

  public HdfsLifecycleListener() {
    this.ncService = NamedClusterManager.getInstance();
    this.rtTestActServ = RuntimeTestActionServiceImpl.getInstance();
    this.rtTester = RuntimeTesterImpl.getInstance();
  }

  @Deprecated
  // This OSGI constructor should be removed
  public HdfsLifecycleListener( NamedClusterService namedClusterService,
                                RuntimeTestActionService runtimeTestActionService, RuntimeTester runtimeTester ) {
    this.ncService = namedClusterService;
    this.rtTestActServ = runtimeTestActionService;
    this.rtTester = runtimeTester;
  }

  @Override public void onStart( LifeEventHandler lifeEventHandler ) throws LifecycleException {
    final Spoon spoon = Spoon.getInstance();

    // Add dialogs on display thread
    spoon.getDisplay().asyncExec( new Runnable() {
      @Override public void run() {
        VfsFileChooserDialog dialog = spoon.getVfsFileChooserDialog( null, null );
        hdfsFileChooserDialog = new HadoopVfsFileChooserDialog( Schemes.HDFS_SCHEME, Schemes.HDFS_SCHEME_DISPLAY_NAME, dialog, null, null, ncService, rtTestActServ, rtTester );
        dialog.addVFSUIPanel( hdfsPriority, hdfsFileChooserDialog );
        mapRFSFileChooserDialog = new MapRFSFileChooserDialog( Schemes.MAPRFS_SCHEME, Schemes.MAPRFS_SCHEME_DISPLAY_NAME, dialog );
        dialog.addVFSUIPanel( maprPriority, mapRFSFileChooserDialog );
        ncFileChooserDialog = new NamedClusterVfsFileChooserDialog( Schemes.NAMED_CLUSTER_SCHEME, Schemes.NAMED_CLUSTER_SCHEME_DISPLAY_NAME, dialog, null, null, ncService, rtTestActServ, rtTester );
        dialog.addVFSUIPanel( ncPriority, ncFileChooserDialog );
      }
    } );
  }

  @Override public void onExit( LifeEventHandler lifeEventHandler ) throws LifecycleException {

  }
}
