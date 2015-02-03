/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.pentaho.di.ui.delegates;

import org.pentaho.di.base.AbstractMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.namedcluster.NamedClusterManager;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.namedcluster.dialog.NamedClusterDialog;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.TreeSelection;
import org.pentaho.di.ui.spoon.delegates.SpoonTreeDelegateExtension;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.util.List;

@ExtensionPoint( id = "HadoopClusterTreeDelegateExtension", description = "During the SpoonTreeDelegate execution",
    extensionPointId = "SpoonTreeDelegateExtension" )

public class HadoopClusterTreeDelegateExtension implements ExtensionPointInterface {

  private static Class<?> PKG = NamedClusterDialog.class;
  public static final String STRING_NAMED_CLUSTERS = BaseMessages.getString( PKG, "NamedClusterDialog.STRING_NAMED_CLUSTERS" );

  public void callExtensionPoint( LogChannelInterface log, Object extension ) throws KettleException {

    SpoonTreeDelegateExtension treeDelExt = (SpoonTreeDelegateExtension) extension;

    int caseNumber = treeDelExt.getCaseNumber();
    AbstractMeta transMeta = treeDelExt.getTransMeta();
    String path[] = treeDelExt.getPath();
    List<TreeSelection> objects = treeDelExt.getObjects();

    TreeSelection object = null;
    Spoon spoon = Spoon.getInstance();

    switch ( caseNumber ) {
      case 3:
        if ( path[2].equals( STRING_NAMED_CLUSTERS ) ) {
          object = new TreeSelection( path[2], NamedCluster.class, transMeta );
        }
        break;
      case 4:
        if ( path[2].equals( STRING_NAMED_CLUSTERS ) ) {
          try {
            NamedCluster nc = NamedClusterManager.getInstance().read( path[3], spoon.getMetaStore() );
            object = new TreeSelection( path[3], nc, transMeta );
          } catch ( MetaStoreException e ) {
          }
        }
        break;
    }

    if ( object != null ) {
      objects.add( object );
    }
  }
}
