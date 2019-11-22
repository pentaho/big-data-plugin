/*
 * *****************************************************************************
 *
 *  Pentaho Data Integration
 *
 *  Copyright (C) 2019 by Hitachi Vantara : http://www.pentaho.com
 *
 *  *******************************************************************************
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 *  this file except in compliance with the License. You may obtain a copy of the
 *  License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 * *****************************************************************************
 *
 */

package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.tree;

import com.google.common.collect.ImmutableMap;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Tree;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.HadoopClusterDelegate;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.endpoints.HadoopClusterManager;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.TreeSelection;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import static org.pentaho.di.i18n.BaseMessages.getString;

@ExtensionPoint( id = "HadoopClusterPopupMenuExtension", description = "Creates popup menus for Hadoop clusters",
  extensionPointId = "SpoonPopupMenuExtension" )
public class HadoopClusterPopupMenuExtension implements ExtensionPointInterface {

  private static final Class<?> PKG = HadoopClusterPopupMenuExtension.class;

  public static final String IMPORT_STATE = "import";
  public static final String NEW_EDIT_STATE = "new-edit";
  public static final String TESTING_STATE = "testing";
  public static final String ADD_DRIVER_STATE = "add-driver";
  public static final String DELETE_STATE = "delete";
  private static final int RESULT_YES = 0;

  private Supplier<Spoon> spoonSupplier = Spoon::getInstance;
  private Menu rootMenu;
  private Menu maintMenu;
  private HadoopClusterDelegate hadoopClusterDelegate;
  private NamedClusterService namedClusterService;
  private String internalShim;
  private static final Logger logChannel = LoggerFactory.getLogger( HadoopClusterPopupMenuExtension.class );

  public HadoopClusterPopupMenuExtension( HadoopClusterDelegate hadoopClusterDelegate,
                                          NamedClusterService namedClusterService, String internalShim ) {
    this.hadoopClusterDelegate = hadoopClusterDelegate;
    this.namedClusterService = namedClusterService;
    this.internalShim = internalShim;
  }

  public void callExtensionPoint( LogChannelInterface log, Object extension ) {
    final Tree selectionTree = (Tree) extension;
    createNewPopupMenu( selectionTree );
  }

  private void createNewPopupMenu( final Tree selectionTree ) {

    Menu popupMenu = null;

    TreeSelection[] objects = spoonSupplier.get().getTreeObjects( selectionTree );
    if ( objects.length != 1 ) {
      return;
    }

    TreeSelection object = objects[ 0 ];
    Object selection = object.getSelection();

    if ( selection instanceof Class<?> && selection.equals( NamedCluster.class ) ) {
      popupMenu = createRootPopupMenu( selectionTree );
    } else if ( selection instanceof NamedCluster ) {
      popupMenu = createMaintPopupMenu( selectionTree, (NamedCluster) selection );
    }

    if ( popupMenu != null ) {
      ConstUI.displayMenu( popupMenu, selectionTree );
    } else {
      selectionTree.setMenu( null );
    }
  }

  private Menu createRootPopupMenu( final Tree tree ) {
    if ( rootMenu == null ) {
      rootMenu = new Menu( tree );
      createPopupMenuItem( rootMenu, getString( PKG, "HadoopClusterPopupMenuExtension.MenuItem.New" ),
        NEW_EDIT_STATE );
      createPopupMenuItem( rootMenu, getString( PKG, "HadoopClusterPopupMenuExtension.MenuItem.Import" ),
        IMPORT_STATE );
      createPopupMenuItem( rootMenu, getString( PKG, "HadoopClusterPopupMenuExtension.MenuItem.Add.Driver" ),
        ADD_DRIVER_STATE );
    }
    return rootMenu;
  }

  public Menu createMaintPopupMenu( final Tree selectionTree, NamedCluster namedCluster ) {
    maintMenu = new Menu( selectionTree );
    try {
      String name = URLEncoder.encode( namedCluster.getName(), "UTF-8" );

      createPopupMenuItem( maintMenu, getString( PKG, "HadoopClusterPopupMenuExtension.MenuItem.Edit" ),
        NEW_EDIT_STATE, ImmutableMap.of( "name", name ) );

      createPopupMenuItem( maintMenu, getString( PKG, "HadoopClusterPopupMenuExtension.MenuItem.Duplicate" ),
        NEW_EDIT_STATE, ImmutableMap.of( "name", name, "duplicateName",
          getString( PKG, "HadoopClusterPopupMenuExtension.Duplicate.Prefix" ) + name ) );

      createPopupMenuItem( maintMenu, getString( PKG, "HadoopClusterPopupMenuExtension.MenuItem.Test" ),
        TESTING_STATE, ImmutableMap.of( "name", name ) );

      createDeleteMenuItem( maintMenu, getString( PKG, "HadoopClusterPopupMenuExtension.MenuItem.Delete" ),
        DELETE_STATE, name );
    } catch ( UnsupportedEncodingException e ) {
      logChannel.error( e.getMessage() );
    }
    return maintMenu;
  }

  private void createPopupMenuItem( Menu menu, String menuItemLabel, String state ) {
    createPopupMenuItem( menu, menuItemLabel, state, Collections.emptyMap() );
  }

  private void createPopupMenuItem( Menu menu, String menuItemLabel, String state, Map urlParams ) {
    MenuItem menuItem = new MenuItem( menu, SWT.NONE );
    menuItem.setText( menuItemLabel );
    menuItem.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent selectionEvent ) {
        hadoopClusterDelegate.openDialog( state, urlParams );
      }
    } );
  }

  private void createDeleteMenuItem( Menu menu, String menuItemLabel, String state, String namedCluster ) {
    MenuItem menuItem = new MenuItem( menu, SWT.NONE );
    menuItem.setText( menuItemLabel );
    menuItem.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent selectionEvent ) {
        String title = BaseMessages.getString( PKG, "PopupMenuFactory.NAMEDCLUSTERS.DeleteNamedClusterAsk.Title" );
        String message =
          BaseMessages.getString( PKG, "PopupMenuFactory.NAMEDCLUSTERS.DeleteNamedClusterAsk.Message",
            namedCluster );
        String deleteButton =
          BaseMessages.getString( PKG, "PopupMenuFactory.NAMEDCLUSTERS.DeleteNamedClusterAsk.Delete" );
        String doNotDeleteButton =
          BaseMessages.getString( PKG, "PopupMenuFactory.NAMEDCLUSTERS.DeleteNamedClusterAsk.DoNotDelete" );
        MessageDialog dialog =
          new MessageDialog( spoonSupplier.get().getShell(), title, null, message, MessageDialog.WARNING, new String[] {
            deleteButton, doNotDeleteButton }, 0 );
        int response = dialog.open();
        if ( response != RESULT_YES ) {
          return;
        }
        HadoopClusterManager hadoopClusterManager = new HadoopClusterManager( spoonSupplier.get(), namedClusterService,
          spoonSupplier.get().getMetaStore(),
          internalShim );
        hadoopClusterManager.deleteNamedCluster( spoonSupplier.get().getMetaStore(), namedCluster, true );
      }
    } );
  }

}

