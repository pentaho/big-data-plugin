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

import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.Tree;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.TreeSelection;

@ExtensionPoint( id = "HadoopPopupMenuExtension", description = "Creates popup menus for named clusters",
    extensionPointId = "SpoonPopupMenuExtension" )

public class HadoopPopupMenuExtension implements ExtensionPointInterface {

  private Spoon spoon = null;

  public HadoopPopupMenuExtension() {
    spoon = Spoon.getInstance();
  }

  public void callExtensionPoint( LogChannelInterface log, Object extension ) throws KettleException {
    final Tree selectionTree = (Tree) extension;
    createNewPopupMenu( selectionTree );
  }

  private void createNewPopupMenu( final Tree selectionTree ) {

    Menu popupMenu = null;
    PopupMenuFactory factory = PopupMenuFactory.newInstance();

    TreeSelection[] objects = spoon.getTreeObjects( selectionTree );
    if ( objects.length != 1 ) {
      return;
    }

    TreeSelection object = objects[0];
    Object selection = object.getSelection();

    if ( selection instanceof Class<?> && selection.equals( NamedCluster.class ) ) {
      popupMenu = factory.createNewPopupMenu( selectionTree );
    } else if ( selection instanceof NamedCluster ) {
      popupMenu = factory.createMaintPopupMenu( selectionTree, (NamedCluster) selection );
    }

    if ( popupMenu != null ) {
      ConstUI.displayMenu( popupMenu, selectionTree );
    } else {
      selectionTree.setMenu( null );
    }
  }

}
