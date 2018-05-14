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

import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.di.base.AbstractMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.TreeSelection;
import org.pentaho.di.ui.spoon.delegates.SpoonTreeDelegateExtension;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.util.List;

@ExtensionPoint( id = "HadoopClusterTreeDelegateExtension", description = "During the SpoonTreeDelegate execution",
  extensionPointId = "SpoonTreeDelegateExtension" )

public class HadoopClusterTreeDelegateExtension implements ExtensionPointInterface {
  private static Class<?> PKG = HadoopClusterTreeDelegateExtension.class;
  public static final String STRING_NAMED_CLUSTERS =
    BaseMessages.getString( PKG, "NamedClusterDialog.STRING_NAMED_CLUSTERS" );
  private final NamedClusterService namedClusterService;
  private final SpoonProvider spoonProvider;

  public HadoopClusterTreeDelegateExtension( NamedClusterService namedClusterService ) {
    this( namedClusterService, new SpoonProvider() );
  }

  public HadoopClusterTreeDelegateExtension( NamedClusterService namedClusterService,
                                             SpoonProvider spoonProvider ) {
    this.namedClusterService = namedClusterService;
    this.spoonProvider = spoonProvider;
  }

  public void callExtensionPoint( LogChannelInterface log, Object extension ) throws KettleException {

    SpoonTreeDelegateExtension treeDelExt = (SpoonTreeDelegateExtension) extension;

    int caseNumber = treeDelExt.getCaseNumber();
    AbstractMeta transMeta = treeDelExt.getTransMeta();
    String[] path = treeDelExt.getPath();
    List<TreeSelection> objects = treeDelExt.getObjects();

    TreeSelection object = null;
    Spoon spoon = spoonProvider.getSpoon();

    switch ( caseNumber ) {
      case 3:
        if ( path[ 2 ].equals( STRING_NAMED_CLUSTERS ) ) {
          object = new TreeSelection( path[ 2 ], org.pentaho.di.core.namedcluster.model.NamedCluster.class, transMeta );
        }
        break;
      case 4:
        if ( path[ 2 ].equals( STRING_NAMED_CLUSTERS ) ) {
          try {
            NamedCluster nc = namedClusterService.read( path[ 3 ], spoon.getMetaStore() );
            object = new TreeSelection( path[ 3 ], NamedClusterBridgeImpl.fromOsgiNamedCluster( nc ), transMeta );
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

  static class SpoonProvider {
    public Spoon getSpoon() {
      return Spoon.getInstance();
    }
  }
}
