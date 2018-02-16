/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.step.mqtt;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.ComboVar;
import org.pentaho.di.ui.core.widget.PasswordTextVar;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.util.List;

import static java.util.Arrays.stream;

public class MQTTProducerDialog extends BaseStepDialog implements StepDialogInterface {
  private static final int SHELL_MIN_WIDTH = 527;
  private static final int SHELL_MIN_HEIGHT = 540;
  private static final int INPUT_WIDTH = 350;

  private static Class<?> PKG = MQTTProducerDialog.class;
  // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  private MQTTProducerMeta meta;
  private ModifyListener lsMod;
  private Label wlMqttServer;
  private TextVar wMqttServer;
  private Label wlClientId;
  private TextVar wClientId;
  private Label wlTopic;
  private TextVar wTopic;
  private Label wlQOS;
  private ComboVar wQOS;
  private Label wlMessageField;
  private ComboVar wMessageField;
  private CTabFolder wTabFolder;
  private CTabItem wSetupTab;
  private Composite wSetupComp;
  private CTabItem wSecurityTab;
  private Composite wSecurityComp;
  private Label wlUsername;
  private TextVar wUsername;
  private Label wlPassword;
  private PasswordTextVar wPassword;
  private Button wUseSSL;
  private Label wlUseSSL;
  private Label wlSSLProperties;
  private TableView sslTable;

  public MQTTProducerDialog( Shell parent, Object in, TransMeta transMeta, String stepname ) {
    super( parent, (BaseStepMeta) in, transMeta, stepname );
    meta = (MQTTProducerMeta) in;
  }

  @Override
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();
    changed = meta.hasChanged();

    lsMod = e -> meta.setChanged();
    lsCancel = e -> cancel();
    lsOK = e -> ok();
    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE );
    props.setLook( shell );
    setShellImage( shell, meta );
    shell.setMinimumSize( SHELL_MIN_WIDTH, SHELL_MIN_HEIGHT );
    shell.setText( BaseMessages.getString( PKG, "MQTTProducerDialog.Shell.Title" ) );

    Label wicon = new Label( shell, SWT.RIGHT );
    wicon.setImage( getImage() );
    FormData fdlicon = new FormData();
    fdlicon.top = new FormAttachment( 0, 0 );
    fdlicon.right = new FormAttachment( 100, 0 );
    wicon.setLayoutData( fdlicon );
    props.setLook( wicon );

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 15;
    formLayout.marginHeight = 15;
    shell.setLayout( formLayout );
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "MQTTProducerDialog.Stepname.Label" ) );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.top = new FormAttachment( 0, 0 );
    wlStepname.setLayoutData( fdlStepname );

    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.width = 250;
    fdStepname.left = new FormAttachment( 0, 0 );
    fdStepname.top = new FormAttachment( wlStepname, 5 );
    wStepname.setLayoutData( fdStepname );
    wStepname.addSelectionListener( lsDef );

    Label topSeparator = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    FormData fdSpacer = new FormData();
    fdSpacer.height = 2;
    fdSpacer.left = new FormAttachment( 0, 0 );
    fdSpacer.top = new FormAttachment( wStepname, 15 );
    fdSpacer.right = new FormAttachment( 100, 0 );
    topSeparator.setLayoutData( fdSpacer );

    // Start of tabbed display
    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );
    wTabFolder.setSimple( false );
    wTabFolder.setUnselectedCloseVisible( true );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    FormData fdCancel = new FormData();
    fdCancel.right = new FormAttachment( 100, 0 );
    fdCancel.bottom = new FormAttachment( 100, 0 );
    wCancel.setLayoutData( fdCancel );
    wCancel.addListener( SWT.Selection, lsCancel );

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    FormData fdOk = new FormData();
    fdOk.right = new FormAttachment( wCancel, -5 );
    fdOk.bottom = new FormAttachment( 100, 0 );
    wOK.setLayoutData( fdOk );
    wOK.addListener( SWT.Selection, lsOK );

    Label bottomSeparator = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    props.setLook( bottomSeparator );
    FormData fdBottomSeparator = new FormData();
    fdBottomSeparator.height = 2;
    fdBottomSeparator.left = new FormAttachment( 0, 0 );
    fdBottomSeparator.bottom = new FormAttachment( wCancel, -15 );
    fdBottomSeparator.right = new FormAttachment( 100, 0 );
    bottomSeparator.setLayoutData( fdBottomSeparator );

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( topSeparator, 15 );
    fdTabFolder.bottom = new FormAttachment( bottomSeparator, -15 );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    wTabFolder.setLayoutData( fdTabFolder );

    buildSetupTab();
    buildSecurityTab();

    getData();

    setSize();

    meta.setChanged( changed );

    wTabFolder.setSelection( 0 );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }

    return stepname;
  }

  private void buildSetupTab() {
    wSetupTab = new CTabItem( wTabFolder, SWT.NONE );
    wSetupTab.setText( BaseMessages.getString( PKG, "MQTTProducerDialog.SetupTab" ) );

    wSetupComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wSetupComp );
    FormLayout setupLayout = new FormLayout();
    setupLayout.marginHeight = 15;
    setupLayout.marginWidth = 15;
    wSetupComp.setLayout( setupLayout );

    wlMqttServer = new Label( wSetupComp, SWT.LEFT );
    props.setLook( wlMqttServer );
    wlMqttServer.setText( BaseMessages.getString( PKG, "MQTTProducerDialog.Connection" ) );
    FormData fdlBootstrapServers = new FormData();
    fdlBootstrapServers.left = new FormAttachment( 0, 0 );
    fdlBootstrapServers.top = new FormAttachment( 0, 0 );
    fdlBootstrapServers.right = new FormAttachment( 0, INPUT_WIDTH );
    wlMqttServer.setLayoutData( fdlBootstrapServers );

    wMqttServer = new TextVar( transMeta, wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wMqttServer );
    wMqttServer.addModifyListener( lsMod );
    FormData fdBootstrapServers = new FormData();
    fdBootstrapServers.left = new FormAttachment( 0, 0 );
    fdBootstrapServers.top = new FormAttachment( wlMqttServer, 5 );
    fdBootstrapServers.right = new FormAttachment( 0, INPUT_WIDTH );
    wMqttServer.setLayoutData( fdBootstrapServers );

    wlClientId = new Label( wSetupComp, SWT.LEFT );
    props.setLook( wlClientId );
    wlClientId.setText( BaseMessages.getString( PKG, "MQTTProducerDialog.ClientId" ) );
    FormData fdlClientId = new FormData();
    fdlClientId.left = new FormAttachment( 0, 0 );
    fdlClientId.top = new FormAttachment( wMqttServer, 10 );
    fdlClientId.right = new FormAttachment( 50, 0 );
    wlClientId.setLayoutData( fdlClientId );

    wClientId = new TextVar( transMeta, wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wClientId );
    wClientId.addModifyListener( lsMod );
    FormData fdClientId = new FormData();
    fdClientId.left = new FormAttachment( 0, 0 );
    fdClientId.top = new FormAttachment( wlClientId, 5 );
    fdClientId.right = new FormAttachment( 0, INPUT_WIDTH );
    wClientId.setLayoutData( fdClientId );

    wlTopic = new Label( wSetupComp, SWT.LEFT );
    props.setLook( wlTopic );
    wlTopic.setText( BaseMessages.getString( PKG, "MQTTProducerDialog.Topic" ) );
    FormData fdlTopic = new FormData();
    fdlTopic.left = new FormAttachment( 0, 0 );
    fdlTopic.top = new FormAttachment( wClientId, 10 );
    fdlTopic.width = 200;
    wlTopic.setLayoutData( fdlTopic );

    wTopic = new TextVar( transMeta, wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTopic );
    wTopic.addModifyListener( lsMod );
    FormData fdTopic = new FormData();
    fdTopic.left = new FormAttachment( 0, 0 );
    fdTopic.top = new FormAttachment( wlTopic, 5 );
    fdTopic.width = 200;
    wTopic.setLayoutData( fdTopic );

    wlQOS = new Label( wSetupComp, SWT.LEFT );
    props.setLook( wlQOS );
    wlQOS.setText( BaseMessages.getString( PKG, "MQTTProducerDialog.QOS" ) );
    FormData fdlQOS = new FormData();
    fdlQOS.left = new FormAttachment( wlTopic, 15 );
    fdlQOS.top = new FormAttachment( wClientId, 10 );
    fdlQOS.width = 120;
    wlQOS.setLayoutData( fdlQOS );

    wQOS = new ComboVar( transMeta, wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wQOS );
    wQOS.addModifyListener( lsMod );
    FormData fdQOS = new FormData();
    fdQOS.left = new FormAttachment( wTopic, 15 );
    fdQOS.top = new FormAttachment( wlQOS, 5 );
    fdQOS.width = 135;
    wQOS.setLayoutData( fdQOS );
    wQOS.add( "0" );
    wQOS.add( "1" );
    wQOS.add( "2" );

    wlMessageField = new Label( wSetupComp, SWT.LEFT );
    props.setLook( wlMessageField );
    wlMessageField.setText( BaseMessages.getString( PKG, "MQTTProducerDialog.MessageField" ) );
    FormData fdlMessageField = new FormData();
    fdlMessageField.left = new FormAttachment( 0, 0 );
    fdlMessageField.top = new FormAttachment( wTopic, 10 );
    fdlMessageField.right = new FormAttachment( 50, 0 );
    wlMessageField.setLayoutData( fdlMessageField );

    wMessageField = new ComboVar( transMeta, wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wMessageField );
    wMessageField.addModifyListener( lsMod );
    FormData fdMessageField = new FormData();
    fdMessageField.left = new FormAttachment( 0, 0 );
    fdMessageField.top = new FormAttachment( wlMessageField, 5 );
    fdMessageField.right = new FormAttachment( 0, INPUT_WIDTH );
    wMessageField.setLayoutData( fdMessageField );
    Listener lsMessageFocus = new Listener() {
      @Override
      public void handleEvent( Event e ) {
        String current = wMessageField.getText();
        wMessageField.getCComboWidget().removeAll();
        wMessageField.setText( current );
        try {
          RowMetaInterface rmi = transMeta.getPrevStepFields( meta.getParentStepMeta().getName() );
          List ls = rmi.getValueMetaList();
          for ( int i = 0; i < ls.size(); i++ ) {
            ValueMetaBase vmb = (ValueMetaBase) ls.get( i );
            wMessageField.add( vmb.getName() );
          }
        } catch ( KettleStepException ex ) {
          // do nothing
        }
      }
    };
    wMessageField.getCComboWidget().addListener( SWT.FocusIn, lsMessageFocus );

    FormData fdSetupComp = new FormData();
    fdSetupComp.left = new FormAttachment( 0, 0 );
    fdSetupComp.top = new FormAttachment( 0, 0 );
    fdSetupComp.right = new FormAttachment( 100, 0 );
    fdSetupComp.bottom = new FormAttachment( 100, 0 );
    wSetupComp.setLayoutData( fdSetupComp );
    wSetupComp.layout();
    wSetupTab.setControl( wSetupComp );
  }

  private void buildSecurityTab() {
    wSecurityTab = new CTabItem( wTabFolder, SWT.NONE );
    wSecurityTab.setText( BaseMessages.getString( PKG, "MQTTDialog.Security.Tab" ) );

    wSecurityComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wSecurityComp );
    FormLayout securityLayout = new FormLayout();
    securityLayout.marginHeight = 15;
    securityLayout.marginWidth = 15;
    wSecurityComp.setLayout( securityLayout );

    // Authentication group
    Group wAuthenticationGroup = new Group( wSecurityComp, SWT.SHADOW_ETCHED_IN );
    props.setLook( wAuthenticationGroup );
    wAuthenticationGroup.setText( BaseMessages.getString( PKG, "MQTTDialog.Security.Authentication" ) );
    FormLayout flAuthentication = new FormLayout();
    flAuthentication.marginHeight = 15;
    flAuthentication.marginWidth = 15;
    wAuthenticationGroup.setLayout( flAuthentication );

    FormData fdAuthenticationGroup = new FormData();
    fdAuthenticationGroup.left = new FormAttachment( 0, 0 );
    fdAuthenticationGroup.top = new FormAttachment( 0, 0 );
    fdAuthenticationGroup.right = new FormAttachment( 100, 0 );
    fdAuthenticationGroup.width = INPUT_WIDTH;
    wAuthenticationGroup.setLayoutData( fdAuthenticationGroup );

    wlUsername = new Label( wAuthenticationGroup, SWT.LEFT );
    props.setLook( wlUsername );
    wlUsername.setText( BaseMessages.getString( PKG, "MQTTDialog.Security.Username" ) );
    FormData fdlUsername = new FormData();
    fdlUsername.left = new FormAttachment( 0, 0 );
    fdlUsername.top = new FormAttachment( 0, 0 );
    fdlUsername.right = new FormAttachment( 0, INPUT_WIDTH );
    wlUsername.setLayoutData( fdlUsername );

    wUsername = new TextVar( transMeta, wAuthenticationGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wUsername );
    wUsername.addModifyListener( lsMod );
    FormData fdUsername = new FormData();
    fdUsername.left = new FormAttachment( 0, 0 );
    fdUsername.top = new FormAttachment( wlUsername, 5 );
    fdUsername.right = new FormAttachment( 0, INPUT_WIDTH );
    wUsername.setLayoutData( fdUsername );

    wlPassword = new Label( wAuthenticationGroup, SWT.LEFT );
    props.setLook( wlPassword );
    wlPassword.setText( BaseMessages.getString( PKG, "MQTTDialog.Security.Password" ) );
    FormData fdlPassword = new FormData();
    fdlPassword.left = new FormAttachment( 0, 0 );
    fdlPassword.top = new FormAttachment( wUsername, 10 );
    fdlPassword.right = new FormAttachment( 0, INPUT_WIDTH );
    wlPassword.setLayoutData( fdlPassword );

    wPassword = new PasswordTextVar( transMeta, wAuthenticationGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPassword );
    wPassword.addModifyListener( lsMod );
    FormData fdPassword = new FormData();
    fdPassword.left = new FormAttachment( 0, 0 );
    fdPassword.top = new FormAttachment( wlPassword, 5 );
    fdPassword.right = new FormAttachment( 0, INPUT_WIDTH );
    wPassword.setLayoutData( fdPassword );


    wUseSSL = new Button( wSecurityComp, SWT.CHECK );
    props.setLook( wUseSSL );
    FormData fdUseSSL = new FormData();
    fdUseSSL.top = new FormAttachment( wAuthenticationGroup, 15 );
    fdUseSSL.left = new FormAttachment( 0, 0 );
    wUseSSL.setLayoutData( fdUseSSL );
    wUseSSL.addSelectionListener( new SelectionListener() {
      @Override public void widgetSelected( SelectionEvent selectionEvent ) {
        boolean selection = ( (Button) selectionEvent.getSource() ).getSelection();
        sslTable.setEnabled( selection );
      }

      @Override public void widgetDefaultSelected( SelectionEvent selectionEvent ) {
        boolean selection = ( (Button) selectionEvent.getSource() ).getSelection();
        sslTable.setEnabled( selection );
      }
    } );

    wlUseSSL = new Label( wSecurityComp, SWT.LEFT );
    props.setLook( wlUseSSL );
    wlUseSSL.setText( BaseMessages.getString( PKG, "MQTTDialog.Security.UseSSL" ) );
    FormData fdlUseSSL = new FormData();
    fdlUseSSL.left = new FormAttachment( wUseSSL, 0 );
    fdlUseSSL.top = new FormAttachment( wAuthenticationGroup, 15 );
    wlUseSSL.setLayoutData( fdlUseSSL );

    wlSSLProperties = new Label( wSecurityComp, SWT.LEFT );
    wlSSLProperties.setText( BaseMessages.getString( PKG, "MQTTDialog.Security.SSLProperties" ) );
    props.setLook( wlSSLProperties );
    FormData fdlSSLProperties = new FormData();
    fdlSSLProperties.top = new FormAttachment( wUseSSL, 10 );
    fdlSSLProperties.left = new FormAttachment( 0, 0 );
    wlSSLProperties.setLayoutData( fdlSSLProperties );

    FormData fdSecurityComp = new FormData();
    fdSecurityComp.left = new FormAttachment( 0, 0 );
    fdSecurityComp.top = new FormAttachment( 0, 0 );
    fdSecurityComp.right = new FormAttachment( 100, 0 );
    fdSecurityComp.bottom = new FormAttachment( 100, 0 );
    wSecurityComp.setLayoutData( fdSecurityComp );

    buildSSLTable( wSecurityComp, wlSSLProperties );

    wSecurityComp.layout();
    wSecurityTab.setControl( wSecurityComp );
  }

  private void buildSSLTable( Composite parentWidget, Control relativePosition ) {
    ColumnInfo[] columns = getSSLColumns();

    int fieldCount = 1;

    sslTable = new TableView(
      transMeta,
      parentWidget,
      SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
      columns,
      fieldCount,
      false,
      lsMod,
      props,
      false
    );

    sslTable.setSortable( false );
    sslTable.getTable().addListener( SWT.Resize, event -> {
      Table table = (Table) event.widget;
      table.getColumn( 1 ).setWidth( 179 );
      table.getColumn( 2 ).setWidth( 178 );
    } );

    populateSSLData();

    FormData fdData = new FormData();
    fdData.left = new FormAttachment( 0, 0 );
    fdData.top = new FormAttachment( relativePosition, 5 );
    fdData.bottom = new FormAttachment( 100, 0 );
    fdData.width = INPUT_WIDTH + 10;

    // resize the columns to fit the data in them
    stream( sslTable.getTable().getColumns() ).forEach( column -> {
      if ( column.getWidth() > 0 ) {
        // don't pack anything with a 0 width, it will resize it to make it visible (like the index column)
        column.setWidth( 120 );
      }
    } );

    sslTable.setLayoutData( fdData );
  }

  private ColumnInfo[] getSSLColumns() {
    ColumnInfo optionName = new ColumnInfo( BaseMessages.getString( PKG, "MQTTDialog.Security.SSL.Column.Name" ),
      ColumnInfo.COLUMN_TYPE_TEXT, false, false );

    ColumnInfo value = new ColumnInfo( BaseMessages.getString( PKG, "MQTTDialog.Security.SSL.Column.Value" ),
      ColumnInfo.COLUMN_TYPE_TEXT, false, false );
    value.setUsingVariables( true );

    return new ColumnInfo[] { optionName, value };
  }

  private void populateSSLData() {
    // TODO: fill out with SSL table with property and values
    int rowIndex = 0;
  }

  private void getData() {
    if ( null != meta.getMqttServer() ) {
      wMqttServer.setText( meta.getMqttServer() );
    }

    if ( null != meta.getClientId() ) {
      wClientId.setText( meta.getClientId() );
    }

    if ( null != meta.getTopic() ) {
      wTopic.setText( meta.getTopic() );
    }

    if ( null != meta.getQOS() ) {
      wQOS.setText( meta.getQOS() );
    }

    if ( null != meta.getMessageField() ) {
      wMessageField.setText( meta.getMessageField() );
    }

    if ( null != meta.getUsername() ) {
      wUsername.setText( meta.getUsername() );
    }

    if ( null != meta.getPassword() ) {
      wPassword.setText( meta.getPassword() );
    }
  }

  private void cancel() {
    meta.setChanged( false );
    dispose();
  }

  private void ok() {
    stepname = wStepname.getText();
    meta.setMqttServer( wMqttServer.getText() );
    meta.setClientId( wClientId.getText() );
    meta.setTopic( wTopic.getText() );
    meta.setQOS( wQOS.getText() );
    meta.setMessageField( wMessageField.getText() );
    meta.setUsername( wUsername.getText() );
    meta.setPassword( wPassword.getText() );
    dispose();
  }

  private Image getImage() {
    PluginInterface plugin =
      PluginRegistry.getInstance().getPlugin( StepPluginType.class, stepMeta.getStepMetaInterface() );
    String id = plugin.getIds()[ 0 ];
    if ( id != null ) {
      return GUIResource.getInstance().getImagesSteps().get( id ).getAsBitmapForSize( shell.getDisplay(),
        ConstUI.ICON_SIZE, ConstUI.ICON_SIZE );
    }
    return null;
  }
}
