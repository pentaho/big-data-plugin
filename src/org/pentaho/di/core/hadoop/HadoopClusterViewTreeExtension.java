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

package org.pentaho.di.core.hadoop;

import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.TreeItem;
import org.pentaho.di.base.AbstractMeta;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.namedcluster.NamedClusterManager;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.namedcluster.dialog.NamedClusterDialog;
import org.pentaho.di.ui.delegates.HadoopClusterDelegate;
import org.pentaho.di.ui.spoon.SelectionTreeExtension;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.util.List;

@ExtensionPoint( id = "HadoopClusterViewTreeExtension", description = "Refreshes named cluster subtree",
    extensionPointId = "SpoonViewTreeExtension" )

public class HadoopClusterViewTreeExtension implements ExtensionPointInterface {

  private Spoon spoon = null;
  private HadoopClusterDelegate ncDelegate = null;
  private static Class<?> PKG = Spoon.class;
  public static final String
      STRING_NAMED_CLUSTERS =
      BaseMessages.getString( NamedClusterDialog.class, "NamedClusterDialog.STRING_NAMED_CLUSTERS" );

  private LogChannelInterface log = new LogChannel( HadoopClusterViewTreeExtension.class.getName() );

  public HadoopClusterViewTreeExtension() {
    spoon = Spoon.getInstance();
    ncDelegate = new HadoopClusterDelegate( spoon );
  }

  public void callExtensionPoint( LogChannelInterface log, Object extension ) throws KettleException {

    SelectionTreeExtension selectionTreeExtension = (SelectionTreeExtension) extension;
    if ( selectionTreeExtension.getAction().equals( Spoon.REFRESH_SELECTION_EXTENSION ) ) {
      refreshNamedClusterSubtree( selectionTreeExtension );
    }
    if ( selectionTreeExtension.getAction().equals( Spoon.EDIT_SELECTION_EXTENSION ) ) {
      editNamedCluster( selectionTreeExtension );
    }
  }

  private void editNamedCluster( SelectionTreeExtension selectionTreeExtension ) {
    Object selection = selectionTreeExtension.getSelection();
    if ( selection instanceof NamedCluster) {
      NamedCluster selectedNamedCluster = ( NamedCluster ) selection;
      ncDelegate.editNamedCluster( spoon.metaStore, selectedNamedCluster, spoon.getShell() );
    }
  }

  private void refreshNamedClusterSubtree( SelectionTreeExtension selectionTreeExtension ) {

    TreeItem tiRootName = selectionTreeExtension.getTiRootName();
    GUIResource guiResource = selectionTreeExtension.getGuiResource();

    TreeItem tiNcTitle = createTreeItem( tiRootName, STRING_NAMED_CLUSTERS, guiResource.getImageFolder() );

    List<NamedCluster> namedClusters;
    try {
      namedClusters = NamedClusterManager.getInstance().list( spoon.metaStore );
    } catch ( MetaStoreException e ) {
      new ErrorDialog( spoon.getShell(), BaseMessages.getString( PKG, "Spoon.ErrorDialog.Title" ),
          BaseMessages.getString( PKG, "Spoon.ErrorDialog.ErrorFetchingFromRepo.NamedCluster" ), e );

      return;
    }

    for ( NamedCluster namedCluster : namedClusters ) {
      if ( !filterMatch( namedCluster.getName() ) ) {
        continue;
      }

      createTreeItem( tiNcTitle, namedCluster.getName(), guiResource.getImageConnectionTree() );
    }
  }

  private TreeItem createTreeItem( TreeItem parent, String text, Image image ) {
    TreeItem item = new TreeItem( parent, SWT.NONE );
    item.setText( text );
    item.setImage( image );
    return item;
  }

  boolean filterMatch( String string ) {
    if ( Const.isEmpty( string ) ) {
      return true;
    }

    String filter = spoon.selectionFilter.getText();
    if ( Const.isEmpty( filter ) ) {
      return true;
    }

    try {
      if ( string.matches( filter ) ) {
        return true;
      }
    } catch ( Exception e ) {
      log.logError( "Not a valid pattern [" + filter + "] : " + e.getMessage() );
    }

    return string.toUpperCase().contains( filter.toUpperCase() );
  }
}
