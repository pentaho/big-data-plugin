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

package org.pentaho.big.data.plugins.common.ui;

import org.apache.commons.lang.StringUtils;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
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
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.LifecyclePluginType;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.util.HelpUtils;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.runtime.test.RuntimeTestStatus;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;

/**
 * Dialog that allows you to edit the settings of a named cluster.
 *
 * @see <code>NamedCluster</code>
 */
public class NamedClusterDialogImpl extends Dialog {
  private static final int RESULT_NO = 1;
  private static final int DIALOG_WIDTH = 459;
  private static Class<?> PKG = NamedClusterDialogImpl.class; // for i18n purposes, needed by Translator2!!
  private final NamedClusterService namedClusterService;
  private final RuntimeTestActionService runtimeTestActionService;
  private final RuntimeTester runtimeTester;
  private Shell shell;
  private PropsUI props;
  private NamedCluster originalNamedCluster;
  private NamedCluster namedCluster;
  private boolean newClusterCheck = false;
  private String result;

  public NamedClusterDialogImpl( Shell parent, NamedClusterService namedClusterService,
                                 RuntimeTestActionService runtimeTestActionService, RuntimeTester runtimeTester ) {
    this( parent, namedClusterService, runtimeTestActionService, runtimeTester, null );
  }

  public NamedClusterDialogImpl( Shell parent, NamedClusterService namedClusterService,
                                 RuntimeTestActionService runtimeTestActionService, RuntimeTester runtimeTester,
                                 NamedCluster namedCluster ) {
    super( parent );
    this.namedClusterService = namedClusterService;
    this.runtimeTestActionService = runtimeTestActionService;
    this.runtimeTester = runtimeTester;
    props = PropsUI.getInstance();

    this.namedCluster = namedCluster;
    this.originalNamedCluster = namedCluster == null ? null : namedCluster.clone();
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

  public void setNewClusterCheck( boolean newClusterCheck ) {
    this.newClusterCheck = newClusterCheck;
  }

  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();
    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.CLOSE | SWT.ICON | SWT.RESIZE );
    props.setLook( shell );
    shell.setImage( GUIResource.getInstance().getImageSpoon() );
    shell.setMinimumSize( DIALOG_WIDTH, 458 );
    shell.setText( BaseMessages.getString( PKG, "NamedClusterDialog.Shell.Title" ) );
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 15;
    formLayout.marginHeight = 15;
    shell.setLayout( formLayout );

    BaseStepDialog.setSize( shell );

    // Create help button
    String docUrl = Const.getDocUrl( BaseMessages.getString( PKG, "NamedClusterDialog.Shell.Doc" ) );
    PluginInterface plugin =
        PluginRegistry.getInstance().findPluginWithId( LifecyclePluginType.class, /* TODO */ "HadoopSpoonPlugin" );
    HelpUtils.createHelpButton( shell, HelpUtils.getHelpDialogTitle( plugin ),
        docUrl,
        BaseMessages.getString( PKG, "NamedClusterDialog.Shell.Title" ) );

    // Buttons
    Button wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    FormData fd = new FormData();

    Button wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    Button wTest = new Button( shell, SWT.PUSH );
    wTest.setText( BaseMessages.getString( PKG, "System.Button.Test" ) );

    Button[] buttons = new Button[] { wTest, wOK, wCancel };

    BaseStepDialog.positionBottomRightButtons( shell, buttons, Const.FORM_MARGIN, null );

    // Create a horizontal separator
    Label bottomSeparator = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );

    fd = new FormData();
    fd.bottom = new FormAttachment( wCancel, -15 );
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    bottomSeparator.setLayoutData( fd );

    ScrolledComposite scrolledComposite = new ScrolledComposite( shell, SWT.V_SCROLL );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( 0, 0 );
    fd.bottom = new FormAttachment( bottomSeparator, -15 );
    scrolledComposite.setLayoutData( fd );
    props.setLook( scrolledComposite );

    NamedClusterComposite namedClusterComposite = new NamedClusterComposite( scrolledComposite, namedCluster, props, namedClusterService );
    scrolledComposite.setContent( namedClusterComposite );
    namedClusterComposite.pack();

    // Add listeners
    wTest.addListener( SWT.Selection, new Listener() {
      @Override
      public void handleEvent( Event event ) {
        try {
          RuntimeTestStatus testStatus = ClusterTestDialog.create( shell, getNamedCluster(), runtimeTester ).open();
          if ( testStatus != null ) {
            // We have good results, show the dialog
            try {
              new ClusterTestResultsDialog( shell, runtimeTestActionService, testStatus ).open();
            } catch ( KettleException ke ) {
              new ErrorDialog( shell, BaseMessages.getString( PKG, "ClusterTestResultsDialog.FailedToOpen" ),
                ke.getMessage(), ke );
            }
          }
        } catch ( KettleException e ) {
          // The exception already has the message localized
          new ErrorDialog( shell, BaseMessages.getString( PKG, "NamedClusterDialog.DialogError" ),
            e.getMessage(), e );
        }
      }
    } );
    wOK.addListener( SWT.Selection, e -> ok() );
    wCancel.addListener( SWT.Selection, e -> cancel() );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );


    namedClusterComposite.setStateChangeListener( () -> {
      boolean enabled = !namedCluster.isUseGateway()
          || ( StringUtils.isNotBlank( namedCluster.getName() )
          && StringUtils.isNotBlank( namedCluster.getGatewayUrl() )
          && StringUtils.isNotBlank( namedCluster.getGatewayUsername() )
          && StringUtils.isNotBlank( namedCluster.getGatewayPassword() ) );

      if ( wOK.isEnabled() != enabled ) {
        wOK.setEnabled( enabled );
      }
    } );

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
    } else if ( StringUtils.isBlank( namedCluster.getShimIdentifier() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setText( BaseMessages.getString( PKG, "NamedClusterDialog.Error" ) );
      mb.setMessage( BaseMessages.getString( PKG, "NamedClusterDialog.ShimIdentifierMissing" ) );
      mb.open();
      return;
    } else if ( newClusterCheck || !originalNamedCluster.getName().equals( result ) ) {
      // check that the getName does not already exist
      try {
        NamedCluster fetched = namedClusterService.read( result, Spoon.getInstance().getMetaStore() );
        if ( fetched != null ) {

          String title = BaseMessages.getString( PKG, "NamedClusterDialog.ClusterNameExists.Title" );
          String message = BaseMessages.getString( PKG, "NamedClusterDialog.ClusterNameExists", result );
          String replaceButton = BaseMessages.getString( PKG, "NamedClusterDialog.ClusterNameExists.Replace" );
          String doNotReplaceButton =
            BaseMessages.getString( PKG, "NamedClusterDialog.ClusterNameExists.DoNotReplace" );
          MessageDialog dialog =
            new MessageDialog( shell, title, null, message, MessageDialog.WARNING, new String[]{ replaceButton,
              doNotReplaceButton }, 0 );

          // there already exists a cluster with the new getName, ask the user
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
