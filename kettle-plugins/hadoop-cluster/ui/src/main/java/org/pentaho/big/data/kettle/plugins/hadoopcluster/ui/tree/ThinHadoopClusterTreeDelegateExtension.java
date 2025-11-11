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


package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.tree;

import org.pentaho.big.data.api.services.BigDataServicesHelper;
import org.pentaho.di.base.AbstractMeta;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.TreeSelection;
import org.pentaho.di.ui.spoon.delegates.SpoonTreeDelegateExtension;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.util.List;
import java.util.function.Supplier;

@ExtensionPoint( id = "ThinHadoopClusterTreeDelegateExtension", description = "",
  extensionPointId = "SpoonTreeDelegateExtension" )

public class ThinHadoopClusterTreeDelegateExtension implements ExtensionPointInterface {
  private Supplier<Spoon> spoonSupplier = Spoon::getInstance;
  private NamedClusterService namedClusterService;

  public void callExtensionPoint( LogChannelInterface log, Object extension ) {

    SpoonTreeDelegateExtension treeDelExt = (SpoonTreeDelegateExtension) extension;

    int caseNumber = treeDelExt.getCaseNumber();
    AbstractMeta meta = treeDelExt.getTransMeta();
    String[] path = treeDelExt.getPath();
    List<TreeSelection> objects = treeDelExt.getObjects();

    TreeSelection object = null;
    switch ( caseNumber ) {
      case 2:
        if ( path[ 1 ].equals( ThinHadoopClusterFolderProvider.STRING_NEW_HADOOP_CLUSTER ) ) {
          object = new TreeSelection( path[ 1 ], NamedCluster.class, meta );
        }
        break;
      case 3:
        if ( path[ 1 ].equals( ThinHadoopClusterFolderProvider.STRING_NEW_HADOOP_CLUSTER ) ) {
          try {
            String name = path[2];
            NamedClusterService ncs = getNamedClusterService();
            if ( ncs != null ) {
              NamedCluster nc = ncs.read( name, spoonSupplier.get().getMetaStore() );
              object = new TreeSelection( path[2], nc, meta );
            }
          } catch ( MetaStoreException e ) {
            // Ignore
          }
        }
        break;
    }

    if ( object != null ) {
      objects.add( object );
    }
  }

  private NamedClusterService getNamedClusterService() {
    if ( namedClusterService == null ) {
      namedClusterService = BigDataServicesHelper.getNamedClusterService();
    }
    return namedClusterService;
  }
}
