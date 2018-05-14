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

package org.pentaho.di.core.hadoop;

import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.TreeItem;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.SwtUniversalImage;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.namedcluster.NamedClusterManager;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.core.util.ExecutorUtil;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.namedcluster.HadoopClusterDelegate;
import org.pentaho.di.ui.core.namedcluster.NamedClusterUIHelper;
import org.pentaho.di.ui.spoon.SelectionTreeExtension;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.util.SwtSvgImageUtil;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@ExtensionPoint( id = "HadoopClusterViewTreeExtension", description = "Refreshes named cluster subtree",
    extensionPointId = "SpoonViewTreeExtension" )

public class HadoopClusterViewTreeExtension implements ExtensionPointInterface {
  private Spoon spoon = null;
  private Future<HadoopClusterDelegate> ncDelegate = null;
  private Image hadoopClusterImage = null;
  private static Class<?> PKG = Spoon.class;
  public static final String STRING_NAMED_CLUSTERS =
    BaseMessages.getString( HadoopClusterViewTreeExtension.class, "NamedClusterDialog.STRING_NAMED_CLUSTERS" );

  private LogChannelInterface log = new LogChannel( HadoopClusterViewTreeExtension.class.getName() );

  public HadoopClusterViewTreeExtension() {
    spoon = Spoon.getInstance();
    ncDelegate = ExecutorUtil.getExecutor().submit( new Callable<HadoopClusterDelegate>() {
      @Override public HadoopClusterDelegate call() throws Exception {
        return NamedClusterUIHelper.getNamedClusterUIFactory().createHadoopClusterDelegate( spoon );
      }
    } );
    hadoopClusterImage = getHadoopClusterImage( spoon.getDisplay() );
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

  private void editNamedCluster( SelectionTreeExtension selectionTreeExtension ) throws KettleException {
    Object selection = selectionTreeExtension.getSelection();
    if ( selection instanceof NamedCluster ) {
      NamedCluster selectedNamedCluster = (NamedCluster) selection;
      try {
        spoon.getTreeManager().update( HadoopClusterFolderProvider.STRING_NAMED_CLUSTERS );
        ncDelegate.get().editNamedCluster( spoon.metaStore, selectedNamedCluster, spoon.getShell() );
      } catch ( InterruptedException e ) {
        throw new KettleException( "Interrupted while waiting on " + HadoopClusterDelegate.class.getCanonicalName(),
          e );
      } catch ( ExecutionException e ) {
        throw new KettleException( "Execution exception getting " + HadoopClusterDelegate.class.getCanonicalName(), e );
      }
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

      createTreeItem( tiNcTitle, namedCluster.getName(), hadoopClusterImage );
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

  private Image getHadoopClusterImage( Display display ) {
    final SwtUniversalImage swtImage =
        SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), "hadoop_clusters.svg" );
    Image image = swtImage.getAsBitmapForSize( display, ConstUI.MEDIUM_ICON_SIZE, ConstUI.MEDIUM_ICON_SIZE );
    display.addListener( SWT.Dispose, new Listener() {
      public void handleEvent( Event event ) {
        swtImage.dispose();
      }
    } );
    return image;
  }
}
