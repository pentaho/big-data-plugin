/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Tree;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.namedcluster.HadoopClusterDelegate;
import org.pentaho.di.ui.core.namedcluster.NamedClusterUIHelper;
import org.pentaho.di.ui.spoon.Spoon;

public class PopupMenuFactory {
  private static final int RESULT_YES = 0;

  private Spoon spoon = null;
  private static Class<?> PKG = PopupMenuFactory.class;
  private HadoopClusterDelegate ncDelegate = null;
  public static PopupMenuFactory popupMenuFactory = null;
  private Menu newMenu = null;
  private Menu maintMenu = null;
  private NamedCluster selectedNamedCluster = null;

  public PopupMenuFactory() {
    spoon = Spoon.getInstance();
    ncDelegate = NamedClusterUIHelper.getNamedClusterUIFactory().createHadoopClusterDelegate( spoon );
  }

  public Menu createNewPopupMenu( final Tree selectionTree ) {
    if ( newMenu == null ) {
      newMenu = new Menu( selectionTree );
      createPopupMenu( newMenu, BaseMessages.getString( PKG, "PopupMenuFactory.NAMEDCLUSTERS.New" ),
          new NewNamedClusterCommand() );
    }
    return newMenu;
  }

  public Menu createMaintPopupMenu( final Tree selectionTree, NamedCluster selectedNamedCluster ) {
    this.selectedNamedCluster = selectedNamedCluster;
    if ( maintMenu == null ) {
      maintMenu = new Menu( selectionTree );
      createPopupMenu( maintMenu, BaseMessages.getString( PKG, "PopupMenuFactory.NAMEDCLUSTERS.New" ),
          new NewNamedClusterCommand() );
      createPopupMenu( maintMenu, BaseMessages.getString( PKG, "PopupMenuFactory.NAMEDCLUSTERS.Edit" ),
          new EditNamedClusterCommand() );
      createPopupMenu( maintMenu, BaseMessages.getString( PKG, "PopupMenuFactory.NAMEDCLUSTERS.Duplicate" ),
          new DuplicateNamedClusterCommand() );
      createPopupMenu( maintMenu, BaseMessages.getString( PKG, "PopupMenuFactory.NAMEDCLUSTERS.Delete" ),
          new DeleteNamedClusterCommand() );
    }
    return maintMenu;
  }

  private void createPopupMenu( Menu parentMenu, String label, final NamedClusterCommand command ) {
    MenuItem deleteMenuItem = new MenuItem( parentMenu, SWT.NONE );
    deleteMenuItem.setText( label );
    deleteMenuItem.addSelectionListener( new SelectionListener() {
      public void widgetSelected( SelectionEvent selectionEvent ) {
        command.execute();
      }

      public void widgetDefaultSelected( SelectionEvent selectionEvent ) {
      }
    } );
  }

  interface NamedClusterCommand {
    public void execute();
  }

  class NewNamedClusterCommand implements NamedClusterCommand {
    public void execute() {
      VariableSpace vs = null;
      if ( spoon.getActiveMeta() instanceof TransMeta ) {
        vs = spoon.getActiveTransformation();
      } else {
        vs = spoon.getActiveJob();
      }
      spoon.getTreeManager().update( HadoopClusterFolderProvider.STRING_NAMED_CLUSTERS );
      ncDelegate.newNamedCluster( vs, spoon.metaStore, spoon.getShell() );
    }
  }

  class EditNamedClusterCommand implements NamedClusterCommand {
    public void execute() {
      spoon.getTreeManager().update( HadoopClusterFolderProvider.STRING_NAMED_CLUSTERS );
      ncDelegate.editNamedCluster( spoon.metaStore, selectedNamedCluster, spoon.getShell() );
    }
  }

  class DuplicateNamedClusterCommand implements NamedClusterCommand {
    public void execute() {
      spoon.getTreeManager().update( HadoopClusterFolderProvider.STRING_NAMED_CLUSTERS );
      ncDelegate.dupeNamedCluster( spoon.metaStore, selectedNamedCluster, spoon.getShell() );
    }
  }

  class DeleteNamedClusterCommand implements NamedClusterCommand {
    public void execute() {
      String title = BaseMessages.getString( PKG, "PopupMenuFactory.NAMEDCLUSTERS.DeleteNamedClusterAsk.Title" );
      String message =
          BaseMessages.getString( PKG, "PopupMenuFactory.NAMEDCLUSTERS.DeleteNamedClusterAsk.Message",
              selectedNamedCluster.getName() );
      String deleteButton = BaseMessages.getString( PKG, "PopupMenuFactory.NAMEDCLUSTERS.DeleteNamedClusterAsk.Delete" );
      String doNotDeleteButton =
          BaseMessages.getString( PKG, "PopupMenuFactory.NAMEDCLUSTERS.DeleteNamedClusterAsk.DoNotDelete" );
      MessageDialog dialog =
          new MessageDialog( spoon.getShell(), title, null, message, MessageDialog.WARNING, new String[] {
            deleteButton, doNotDeleteButton }, 0 );
      int response = dialog.open();
      if ( response != RESULT_YES ) {
        return;
      }
      spoon.getTreeManager().update( HadoopClusterFolderProvider.STRING_NAMED_CLUSTERS );
      ncDelegate.delNamedCluster( spoon.metaStore, selectedNamedCluster );
    }
  }

  public static PopupMenuFactory newInstance() {
    if ( popupMenuFactory == null ) {
      popupMenuFactory = new PopupMenuFactory();
    }
    return popupMenuFactory;
  }
}
