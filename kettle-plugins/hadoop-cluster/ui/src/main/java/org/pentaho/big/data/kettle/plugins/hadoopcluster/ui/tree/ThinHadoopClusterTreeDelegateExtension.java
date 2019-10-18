/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.tree;

import org.pentaho.di.core.namedcluster.NamedClusterManager;
import org.pentaho.di.base.AbstractMeta;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.TreeSelection;
import org.pentaho.di.ui.spoon.delegates.SpoonTreeDelegateExtension;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.util.List;
import java.util.function.Supplier;

@ExtensionPoint( id = "ThinHadoopClusterTreeDelegateExtension", description = "",
  extensionPointId = "SpoonTreeDelegateExtension" )

public class ThinHadoopClusterTreeDelegateExtension implements ExtensionPointInterface {
  private Supplier<Spoon> spoonSupplier = Spoon::getInstance;

  public void callExtensionPoint( LogChannelInterface log, Object extension ) {

    SpoonTreeDelegateExtension treeDelExt = (SpoonTreeDelegateExtension) extension;

    int caseNumber = treeDelExt.getCaseNumber();
    AbstractMeta meta = treeDelExt.getTransMeta();
    String[] path = treeDelExt.getPath();
    List<TreeSelection> objects = treeDelExt.getObjects();

    TreeSelection object = null;
    switch ( caseNumber ) {
      case 3:
        if ( path[ 2 ].equals( ThinHadoopClusterFolderProvider.STRING_NEW_HADOOP_CLUSTER ) ) {
          object = new TreeSelection( path[ 2 ], NamedCluster.class, meta );
        }
        break;
      case 4:
        if ( path[ 2 ].equals( ThinHadoopClusterFolderProvider.STRING_NEW_HADOOP_CLUSTER ) ) {
          try {
            NamedClusterManager ncm = NamedClusterManager.getInstance();
            String name = path[ 3 ];
            NamedCluster nc = ncm.read( name, spoonSupplier.get().getMetaStore() );
            object = new TreeSelection( path[ 3 ], nc, meta );
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
}
