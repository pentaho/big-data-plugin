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

package org.pentaho.big.data.kettle.plugins.hdfs;

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
import org.pentaho.runtime.test.action.RuntimeTestActionService;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

/**
 * Created by bryan on 11/23/15.
 */
@LifecyclePlugin( id = "HdfsLifecycleListener", name = "HdfsLifecycleListener" )
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
