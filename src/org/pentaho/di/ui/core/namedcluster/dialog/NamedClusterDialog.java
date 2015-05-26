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

package org.pentaho.di.ui.core.namedcluster.dialog;

import org.apache.commons.lang.StringUtils;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.hadoop.HadoopSpoonPlugin;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.core.plugins.LifecyclePluginType;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.core.namedcluster.NamedClusterUIHelper;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.util.HelpUtils;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

/**
 *
 * Dialog that allows you to edit the settings of a named cluster.
 *
 * @see <code>NamedCluster</code>
 *
 */
public class NamedClusterDialog extends Dialog {
  private static Class<?> PKG = NamedClusterDialog.class; // for i18n purposes, needed by Translator2!!

  private static final int RESULT_NO = 1;
  
  private Shell shell;
  private PropsUI props;
  private Button wOK, wCancel;
  
  private int margin;

  private NamedCluster originalNamedCluster;
  private NamedCluster namedCluster;
  private boolean newClusterCheck = false;
  private String result;

  public NamedClusterDialog( Shell parent ) {
    super( parent );
    props = PropsUI.getInstance();
  }

  public NamedClusterDialog( Shell parent, NamedCluster namedCluster ) {
    super( parent );
    props = PropsUI.getInstance();
    
    this.namedCluster = namedCluster;
    this.originalNamedCluster = namedCluster.clone();
  }

  public NamedCluster getNamedCluster() {
    return namedCluster;
  }

  public void setNamedCluster( NamedCluster namedCluster ) {
    this.namedCluster = namedCluster;
    this.originalNamedCluster = namedCluster.clone();
  }  
  
  public boolean isNewClusterCheck() {
    return newClusterCheck;
  }

  public void setNewClusterCheck(boolean newClusterCheck) {
    this.newClusterCheck = newClusterCheck;
  }
  
  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }
  
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();
    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.CLOSE | SWT.MAX | SWT.MIN | SWT.ICON );
    props.setLook( shell );
    shell.setImage( GUIResource.getInstance().getImageSpoon() );

    margin = Const.FORM_MARGIN;

    PluginInterface plugin =
        PluginRegistry.getInstance().findPluginWithId( LifecyclePluginType.class, HadoopSpoonPlugin.PLUGIN_ID );
    HelpUtils.createHelpButton( shell, HelpUtils.getHelpDialogTitle( plugin ),
        BaseMessages.getString( PKG, "NamedClusterDialog.Shell.Doc" ),
        BaseMessages.getString( PKG, "NamedClusterDialog.Shell.Title" ));

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setText( BaseMessages.getString( PKG, "NamedClusterDialog.Shell.Title" ) );
    shell.setLayout( formLayout );

    NamedClusterComposite namedClusterComposite = new NamedClusterComposite( shell, namedCluster, props );
    FormData fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    namedClusterComposite.setLayoutData( fd );
    
    shell.setSize( 431, 630 );
    shell.setMinimumSize( 431, 630 );
    
    // Buttons
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    Button[] buttons = new Button[] { wOK, wCancel };
    BaseStepDialog.positionBottomRightButtons( shell, buttons, margin, null );
    
    // Create a horizontal separator
    Label bottomSeparator = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    
    int bottomSeparatorOffset = ( wOK.getBounds().height + Const.FORM_MARGIN );
    fd = new FormData();
    fd.bottom = new FormAttachment( 100, -bottomSeparatorOffset );
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    bottomSeparator.setLayoutData( fd );

    // Add listeners
    wOK.addListener( SWT.Selection, new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    } );
    wCancel.addListener( SWT.Selection, new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    } );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    //BaseStepDialog.setSize( shell );
    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return result;
  }
  
  private void cancel() {
    result = null;
    dispose();
  }

  public void ok() {
    result = namedCluster.getName();
    if ( StringUtils.isBlank( result ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setText( BaseMessages.getString( PKG, "NamedClusterDialog.Error" ) );
      mb.setMessage( BaseMessages.getString( PKG, "NamedClusterDialog.ClusterNameMissing" ) );
      mb.open();
      return;
    } else if ( newClusterCheck || !originalNamedCluster.getName().equals( result ) ) {
      // check that the name does not already exist
      try {
        NamedCluster fetched = NamedClusterUIHelper.getNamedCluster( result );
        if ( fetched != null ) {

          String title = BaseMessages.getString( PKG, "NamedClusterDialog.ClusterNameExists.Title" );
          String message = BaseMessages.getString( PKG, "NamedClusterDialog.ClusterNameExists", result );
          String replaceButton = BaseMessages.getString( PKG, "NamedClusterDialog.ClusterNameExists.Replace" );
          String doNotReplaceButton = BaseMessages.getString( PKG, "NamedClusterDialog.ClusterNameExists.DoNotReplace" );
          MessageDialog dialog =
              new MessageDialog( shell, title, null, message, MessageDialog.WARNING, new String[] { replaceButton,
                doNotReplaceButton }, 0 );

          // there already exists a cluster with the new name, ask the user
          if ( RESULT_NO == dialog.open() ) {
            // do not exist dialog
            return;
          }
        }
      } catch ( MetaStoreException ignored ) {
        // the lookup failed, the cluster does not exist, move on to dispose
      }
    }
    dispose();
  }
  
}
