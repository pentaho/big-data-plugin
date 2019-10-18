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

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Tree;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.HadoopClusterDelegate;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.TreeSelection;

import java.util.function.Supplier;

@ExtensionPoint( id = "HadoopClusterPopupMenuExtension", description = "Creates popup menus for Hadoop clusters",
  extensionPointId = "SpoonPopupMenuExtension" )
public class HadoopClusterPopupMenuExtension implements ExtensionPointInterface {

  private static final Class<?> PKG = HadoopClusterPopupMenuExtension.class;

  public static final String IMPORT_STATE = "import";
  public static final String NEW_EDIT_STATE = "new-edit";

  private Supplier<Spoon> spoonSupplier = Spoon::getInstance;
  private Menu rootMenu;
  private Menu maintMenu;
  private HadoopClusterDelegate hadoopClusterDelegate;
  private NamedCluster selectedNamedCluster = null;

  public HadoopClusterPopupMenuExtension( HadoopClusterDelegate hadoopClusterDelegate ) {
    this.hadoopClusterDelegate = hadoopClusterDelegate;
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

    TreeSelection object = objects[0];
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
    selectedNamedCluster = null;
    if ( rootMenu == null ) {
      rootMenu = new Menu( tree );
      createPopupMenuItem( rootMenu, BaseMessages.getString( PKG, "HadoopClusterPopupMenuExtension.MenuItem.New" ),
        NEW_EDIT_STATE );
      createPopupMenuItem( rootMenu, BaseMessages.getString( PKG, "HadoopClusterPopupMenuExtension.MenuItem.Import" ),
        IMPORT_STATE );
    }
    return rootMenu;
  }

  public Menu createMaintPopupMenu( final Tree selectionTree, NamedCluster namedCluster ) {
    selectedNamedCluster = namedCluster;
    if ( maintMenu == null ) {
      maintMenu = new Menu( selectionTree );
      createPopupMenuItem( maintMenu, BaseMessages.getString( PKG, "HadoopClusterPopupMenuExtension.MenuItem.Edit" ),
        NEW_EDIT_STATE );
    }
    return maintMenu;
  }

  private void createPopupMenuItem( Menu menu, String menuItemLabel, String state ) {
    MenuItem menuItem = new MenuItem( menu, SWT.NONE );
    menuItem.setText( menuItemLabel );
    menuItem.addSelectionListener( new SelectionListener() {
      public void widgetSelected( SelectionEvent selectionEvent ) {
        hadoopClusterDelegate.openDialog( state,  selectedNamedCluster != null ? selectedNamedCluster.getName() : null );
      }
      public void widgetDefaultSelected( SelectionEvent selectionEvent ) {
      }
    } );
  }
}

