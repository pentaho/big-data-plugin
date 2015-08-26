/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.ui.core.namedcluster;

import org.pentaho.di.core.hadoop.HadoopSpoonPlugin;
import org.pentaho.di.core.namedcluster.NamedClusterManager;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.vfs.hadoopvfsfilechooserdialog.HadoopVfsFileChooserDialog;
import org.pentaho.di.ui.vfs.hadoopvfsfilechooserdialog.MapRFSFileChooserDialog;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class NamedClusterUIHelper {
  private static NamedClusterUIFactory namedClusterUIFactory = null;
  private static final AtomicBoolean hasInitializedDialog = new AtomicBoolean( false );

  public static NamedClusterUIFactory getNamedClusterUIFactory() {
    return namedClusterUIFactory;
  }

  /**
   * Being used to inject the widgets from OSGi (where all the test functionality is located) this should be removed
   * once we OSGiify the rest of the big data stuff
   * @param namedClusterUIFactory
   */
  public static void setNamedClusterUIFactory(
    NamedClusterUIFactory namedClusterUIFactory ) {
    NamedClusterUIHelper.namedClusterUIFactory = namedClusterUIFactory;
    initializeFileChooserDialog();
  }

  public static void initializeFileChooserDialog() {
    if ( namedClusterUIFactory != null && Spoon.getInstance() != null && !hasInitializedDialog.getAndSet( true ) ) {
      VfsFileChooserDialog dialog = Spoon.getInstance().getVfsFileChooserDialog( null, null );
      dialog.addVFSUIPanel(
        new HadoopVfsFileChooserDialog( HadoopSpoonPlugin.HDFS_SCHEME, HadoopSpoonPlugin.HDFS_SCHEME_DISPLAY_NAME,
          dialog,
          null, null ) );
      dialog.addVFSUIPanel(
        new MapRFSFileChooserDialog( HadoopSpoonPlugin.MAPRFS_SCHEME, HadoopSpoonPlugin.MAPRFS_SCHEME_DISPLAY_NAME,
          dialog ) );
    }
  }

  public static List<NamedCluster> getNamedClusters() {
    try {
      return NamedClusterManager.getInstance().list( Spoon.getInstance().getMetaStore() );
    } catch ( MetaStoreException e ) {
      return new ArrayList<>();
    }
  }

  public static NamedCluster getNamedCluster( String namedCluster ) throws MetaStoreException {
    return NamedClusterManager.getInstance().read( namedCluster, Spoon.getInstance().getMetaStore() );
  }

}
