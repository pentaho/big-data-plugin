/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.pentaho.di.trans.step.mqtt;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.streaming.common.BaseStreamStepMeta;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.ComboVar;
import org.pentaho.di.ui.core.widget.PasswordTextVar;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStreamingDialog;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.stream;

public class MQTTConsumerDialog extends BaseStreamingDialog implements StepDialogInterface {

  private static Class<?> PKG = MQTTConsumerMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  private MQTTConsumerMeta mqttMeta;
  protected Label wlConnection;
  protected TextVar wConnection;
  private Label wlTopics;
  private TableView topicsTable;
  private Label wlQOS;
  private ComboVar wQOS;
  private CTabItem wFieldsTab;
  private Composite wFieldsComp;
  private TableView fieldsTable;
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

  public MQTTConsumerDialog( Shell parent, Object in, TransMeta tr, String sname ) {
    super( parent, in, tr, sname );
    mqttMeta = (MQTTConsumerMeta) in;
  }

  @Override protected void getData() {
    super.getData();
    wConnection.setText( mqttMeta.getMqttServer() );
    populateTopicsData();
    wQOS.setText( mqttMeta.getQos() );
    wUsername.setText( mqttMeta.getUsername() );
    wPassword.setText( mqttMeta.getPassword() );
  }

  private void populateTopicsData() {
    List<String> topics = mqttMeta.getTopics();
    int rowIndex = 0;
    for ( String topic : topics ) {
      TableItem key = topicsTable.getTable().getItem( rowIndex++ );
      if ( topic != null ) {
        key.setText( 1, topic );
      }
    }
  }

  @Override protected String getDialogTitle() {
    return BaseMessages.getString( PKG, "MQTTConsumerDialog.Shell.Title" );
  }

  @Override protected void buildSetup( Composite wSetupComp ) {
    props.setLook( wSetupComp );
    FormLayout setupLayout = new FormLayout();
    setupLayout.marginHeight = 15;
    setupLayout.marginWidth = 15;
    wSetupComp.setLayout( setupLayout );

    wlConnection = new Label( wSetupComp, SWT.LEFT );
    props.setLook( wlConnection );
    wlConnection.setText( BaseMessages.getString( PKG, "MQTTConsumerDialog.Connection" ) );
    FormData fdlConnection = new FormData();
    fdlConnection.left = new FormAttachment( 0, 0 );
    fdlConnection.top = new FormAttachment( 0, 0 );
    fdlConnection.right = new FormAttachment( 50, 0 );
    wlConnection.setLayoutData( fdlConnection );

    wConnection = new TextVar( transMeta, wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wConnection );
    wConnection.addModifyListener( lsMod );
    FormData fdConnection = new FormData();
    fdConnection.left = new FormAttachment( 0, 0 );
    fdConnection.right = new FormAttachment( 75, 0 );
    fdConnection.top = new FormAttachment( wlConnection, 5 );
    wConnection.setLayoutData( fdConnection );

    wlTopics = new Label( wSetupComp, SWT.LEFT );
    props.setLook( wlTopics );
    wlTopics.setText( BaseMessages.getString( PKG, "MQTTConsumerDialog.Topics" ) );
    FormData fdlTopics = new FormData();
    fdlTopics.left = new FormAttachment( 0, 0 );
    fdlTopics.top = new FormAttachment( wConnection, 10 );
    fdlTopics.right = new FormAttachment( 50, 0 );
    wlTopics.setLayoutData( fdlTopics );

    wQOS = new ComboVar( transMeta, wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wQOS );
    wQOS.addModifyListener( lsMod );
    FormData fdQOS = new FormData();
    fdQOS.left = new FormAttachment( 0, 0 );
    fdQOS.bottom = new FormAttachment( 100, 0 );
    fdQOS.width = 135;
    wQOS.setLayoutData( fdQOS );
    wQOS.add( "0" );
    wQOS.add( "1" );
    wQOS.add( "2" );

    wlQOS = new Label( wSetupComp, SWT.LEFT );
    props.setLook( wlQOS );
    wlQOS.setText( BaseMessages.getString( PKG, "MQTTConsumerDialog.QOS" ) );
    FormData fdlQOS = new FormData();
    fdlQOS.left = new FormAttachment( 0, 0 );
    fdlQOS.bottom = new FormAttachment( wQOS, -5 );
    fdlQOS.right = new FormAttachment( 50, 0 );
    wlQOS.setLayoutData( fdlQOS );

    // Put last so it expands with the dialog. Anchoring itself to QOS Label and the Topics Label
    buildTopicsTable( wSetupComp, wlTopics, wlQOS );
  }

  private void buildTopicsTable( Composite parentWidget, Control controlAbove, Control controlBelow ) {
    ColumnInfo[] columns = new ColumnInfo[]{ new ColumnInfo( BaseMessages.getString( PKG, "MQTTConsumerDialog.TopicHeading" ),
      ColumnInfo.COLUMN_TYPE_TEXT, new String[1], false ) };

    columns[0].setUsingVariables( true );

    int topicsCount = mqttMeta.getTopics().size();

    topicsTable = new TableView(
      transMeta,
      parentWidget,
      SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
      columns,
      topicsCount,
      false,
      lsMod,
      props,
      false
    );

    topicsTable.setSortable( false );
    topicsTable.getTable().addListener( SWT.Resize, event -> {
      Table table = (Table) event.widget;
      table.getColumn( 1 ).setWidth( 316 );
    } );

    FormData fdData = new FormData();
    fdData.left = new FormAttachment( 0, 0 );
    fdData.top = new FormAttachment( controlAbove, 5 );
    fdData.right = new FormAttachment( 0, 337 );
    fdData.bottom = new FormAttachment( controlBelow, -10 );

    // resize the columns to fit the data in them
    stream( topicsTable.getTable().getColumns() ).forEach( column -> {
      if ( column.getWidth() > 0 ) {
        // don't pack anything with a 0 width, it will resize it to make it visible (like the index column)
        column.setWidth( 120 );
      }
    } );

    topicsTable.setLayoutData( fdData );
  }

  @Override protected void createAdditionalTabs() {
    // Set the height so the topics table has approximately 5 rows
    shell.setMinimumSize( 527, 600 );
    buildSecurityTab();
    buildFieldsTab();
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

    // TODO: Means of getting the SSL Config information
    int fieldCount = 1;
//    int fieldCount = consumerMeta.getSSLConfig().size();

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

    return new ColumnInfo[]{ optionName, value };
  }

  private void populateSSLData() {
    // TODO: fill out with SSL table with property and values
    int rowIndex = 0;
  }

  @Override protected String[] getFieldNames() {
    return stream( fieldsTable.getTable().getItems() ).map( row -> row.getText( 2 ) ).toArray( String[]::new );
  }

  @Override protected int[] getFieldTypes() {
    return stream( fieldsTable.getTable().getItems() )
      .mapToInt( row -> ValueMetaFactory.getIdForValueMeta( row.getText( 3 )  ) ).toArray();
  }

  private void buildFieldsTab() {
    wFieldsTab = new CTabItem( wTabFolder, SWT.NONE );
    wFieldsTab.setText( BaseMessages.getString( PKG, "MQTTConsumerDialog.FieldsTab" ) );

    wFieldsComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wFieldsComp );
    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginHeight = 15;
    fieldsLayout.marginWidth = 15;
    wFieldsComp.setLayout( fieldsLayout );

    FormData fieldsFormData = new FormData();
    fieldsFormData.left = new FormAttachment( 0, 0 );
    fieldsFormData.top = new FormAttachment( wFieldsComp, 0 );
    fieldsFormData.right = new FormAttachment( 100, 0 );
    fieldsFormData.bottom = new FormAttachment( 100, 0 );
    wFieldsComp.setLayoutData( fieldsFormData );

    buildFieldTable( wFieldsComp, wFieldsComp );

    wFieldsComp.layout();
    wFieldsTab.setControl( wFieldsComp );
  }

  private void buildFieldTable( Composite parentWidget, Control relativePosition ) {
    ColumnInfo[] columns = getFieldColumns();

    fieldsTable = new TableView(
      transMeta,
      parentWidget,
      SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
      columns,
      2,
      true,
      lsMod,
      props,
      false
    );

    fieldsTable.setSortable( false );
    fieldsTable.getTable().addListener( SWT.Resize, event -> {
      Table table = (Table) event.widget;
      table.getColumn( 1 ).setWidth( 147 );
      table.getColumn( 2 ).setWidth( 147 );
      table.getColumn( 3 ).setWidth( 147 );
    } );

    populateFieldData();

    FormData fdData = new FormData();
    fdData.left = new FormAttachment( 0, 0 );
    fdData.top = new FormAttachment( relativePosition, 5 );
    fdData.right = new FormAttachment( 100, 0 );

    // resize the columns to fit the data in them
    stream( fieldsTable.getTable().getColumns() ).forEach( column -> {
      if ( column.getWidth() > 0 ) {
        // don't pack anything with a 0 width, it will resize it to make it visible (like the index column)
        column.setWidth( 120 );
      }
    } );

    // don't let any rows get deleted or added (this does not affect the read-only state of the cells)
    fieldsTable.setReadonly( true );
    fieldsTable.setLayoutData( fdData );
  }

  private void populateFieldData() {
    TableItem messageItem = fieldsTable.getTable().getItem( 0 );
    messageItem.setText( 1, BaseMessages.getString( PKG, "MQTTConsumerDialog.InputName.Message" ) );
    messageItem.setText( 2, mqttMeta.getMsgOutputName() );
    messageItem.setText( 3, "String" );

    TableItem topicItem = fieldsTable.getTable().getItem( 1 );
    topicItem.setText( 1, BaseMessages.getString( PKG, "MQTTConsumerDialog.InputName.Topic" ) );
    topicItem.setText( 2, mqttMeta.getTopicOutputName() );
    topicItem.setText( 3, "String" );
  }

  private ColumnInfo[] getFieldColumns() {
    ColumnInfo referenceName = new ColumnInfo( BaseMessages.getString( PKG, "MQTTConsumerDialog.Column.Ref" ),
      ColumnInfo.COLUMN_TYPE_TEXT, false, true );

    ColumnInfo name = new ColumnInfo( BaseMessages.getString( PKG, "MQTTConsumerDialog.Column.Name" ),
      ColumnInfo.COLUMN_TYPE_TEXT, false, false );

    ColumnInfo type = new ColumnInfo( BaseMessages.getString( PKG, "MQTTConsumerDialog.Column.Type" ),
      ColumnInfo.COLUMN_TYPE_TEXT, false, true );

    return new ColumnInfo[]{ referenceName, name, type };
  }


  @Override protected void additionalOks( BaseStreamStepMeta meta ) {
    mqttMeta.setMqttServer( wConnection.getText() );
    mqttMeta.setTopics( stream( topicsTable.getTable().getItems() )
      .map( item -> item.getText( 1 ) )
      .filter( t -> !"".equals( t ) )
      .distinct()
      .collect( Collectors.toList() ) );
    mqttMeta.setMsgOutputName( fieldsTable.getTable().getItem( 0 ).getText( 2 ) );
    mqttMeta.setTopicOutputName( fieldsTable.getTable().getItem( 1 ).getText( 2 ) );
    mqttMeta.setQos( wQOS.getText() );
    mqttMeta.setUsername( wUsername.getText() );
    mqttMeta.setPassword( wPassword.getText() );
  }
}
