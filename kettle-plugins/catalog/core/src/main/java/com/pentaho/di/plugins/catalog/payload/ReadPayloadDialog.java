/*!
 * HITACHI VANTARA PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2020 Hitachi Vantara. All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Hitachi Vantara and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Hitachi Vantara and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Hitachi Vantara is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Hitachi Vantara,
 * explicitly covering such access.
 */

package com.pentaho.di.plugins.catalog.payload;

import com.pentaho.di.plugins.catalog.api.entities.DataResource;
import com.pentaho.di.plugins.catalog.api.entities.payload.AbstractField;
import com.pentaho.di.plugins.catalog.api.entities.search.Facet;
import com.pentaho.di.plugins.catalog.api.entities.search.FacetsResult;
import com.pentaho.di.plugins.catalog.common.DialogUtils;
import com.pentaho.di.plugins.catalog.provider.CatalogDetails;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.connections.ConnectionManager;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.steps.file.BaseFileField;
import org.pentaho.di.ui.core.FormDataBuilder;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.ColumnsResizer;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.trans.step.common.GetFieldsCapableStepDialog;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.pentaho.di.plugins.catalog.common.DialogUtils.getDataResource;

public class ReadPayloadDialog extends BaseStepDialog implements GetFieldsCapableStepDialog<ReadPayloadMeta>,  StepDialogInterface {

  Supplier<ConnectionManager> connectionManagerSupplier = ConnectionManager::getInstance;

  private static final Class<?> catalogMetaClass = ReadPayloadMeta.class;
  // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  private static final int MARGIN_SIZE = 15;
  private static final int LABEL_SPACING = 5;
  private static final int ELEMENT_SPACING = 10;
  private static final int DOUBLE_ELEMENT_SPACING = 20;

  private static final int LABEL_WIDTH = 150;
  private static final int RADIO_BUTTON_WIDTH = 150;

  private static final int MEDIUM_FIELD = 250;

  private ReadPayloadMeta meta;

  private Text wStepNameField;

  private CCombo wConnection;

  private TextVar wID;

  private Button wbIDAdd;

  private TableView wSelectedFiles;
  private TableView wFields;

  private TextVar wKeyword;
  private TextVar wTags;
  private CCombo wVirtualFolders;
  private CCombo wDataSources;
  private CCombo wResourceType;
  private CCombo wFileSize;
  private CCombo wFileFormat;
  private TextVar wAdvancedQuery;

  private ModifyListener lsMod;
  private Composite resourcesComposite;
  private Composite searchComposite;
  private Composite advancedSearchComposite;
  private Button resourcesButton;
  private Button searchButton;
  private Button advancedSearchButton;
  private FacetsResult facetsResult;

  private CTabFolder wTabFolder;
  private int middle;
  private TextVar idTextField;
  private TextVar dataSourceTextField;
  private TextVar virtualFolderTextField;

  public ReadPayloadDialog( Shell parent, Object in, TransMeta tr, String sname ) {
    super( parent, (BaseStepMeta) in, tr, sname );
    meta = (ReadPayloadMeta) in;
    facetsResult = new FacetsResult();
  }

  public String open() {
    //Set up window
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    shell.setMinimumSize( 450, 335 );
    props.setLook( shell );
    setShellImage( shell, meta );

    lsMod = e -> meta.setChanged();
    changed = meta.hasChanged();

    //15 pixel margins
    FormLayout formLayout = new FormLayout();
    formLayout.marginLeft = MARGIN_SIZE;
    formLayout.marginHeight = MARGIN_SIZE;
    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( catalogMetaClass, "ReadPayloadStepDialog.Shell.Title" ) );

    middle = props.getMiddlePct();

    //Build a scrolling composite and a composite for holding all content
    ScrolledComposite scrolledComposite = new ScrolledComposite( shell, SWT.V_SCROLL );
    Composite contentComposite = new Composite( scrolledComposite, SWT.NONE );
    FormLayout contentLayout = new FormLayout();
    contentLayout.marginRight = MARGIN_SIZE;
    contentComposite.setLayout( contentLayout );
    FormData compositeLayoutData = new FormDataBuilder().fullSize()
            .result();
    contentComposite.setLayoutData( compositeLayoutData );
    props.setLook( contentComposite );

    //Step name label and text field
    Label wStepNameLabel = new Label( contentComposite, SWT.RIGHT );
    wStepNameLabel.setText( BaseMessages.getString( catalogMetaClass, "ReadPayloadStepDialog.Stepname.Label" ) );
    props.setLook( wStepNameLabel );
    FormData fdStepNameLabel = new FormDataBuilder().left().top().result();
    wStepNameLabel.setLayoutData( fdStepNameLabel );

    wStepNameField = new Text( contentComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepNameField.setText( stepname );
    props.setLook( wStepNameField );
    wStepNameField.addModifyListener( lsMod );
    FormData fdStepName = new FormDataBuilder().left().top( wStepNameLabel, LABEL_SPACING ).width( MEDIUM_FIELD ).result();
    wStepNameField.setLayoutData( fdStepName );

    //Job icon, centered vertically between the top of the label and the bottom of the field.
    Label wicon = new Label( contentComposite, SWT.CENTER );
    wicon.setImage( DialogUtils.getImage( stepMeta, shell ) );
    FormData fdIcon = new FormDataBuilder().right().top( 0, 4 ).bottom( new FormAttachment( wStepNameField, 0, SWT.BOTTOM ) ).result();
    wicon.setLayoutData( fdIcon );
    props.setLook( wicon );

    //Spacer between entry info and content
    Label topSpacer = new Label( contentComposite, SWT.HORIZONTAL | SWT.SEPARATOR );
    FormData fdSpacer = new FormDataBuilder().fullWidth().top( wStepNameField, MARGIN_SIZE ).result();
    topSpacer.setLayoutData( fdSpacer );

    Label wlConnection = new Label( contentComposite, SWT.LEFT );
    wlConnection.setText( BaseMessages.getString( catalogMetaClass, "ReadPayloadStepDialog.Connection.Label" ) );
    props.setLook( wlConnection );
    FormData fdlConnection = new FormDataBuilder().left().top( topSpacer, ELEMENT_SPACING ).result();
    wlConnection.setLayoutData( fdlConnection );

    wConnection = new CCombo( contentComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wConnection );
    FormData fdConnection = new FormDataBuilder().left().top( wlConnection, LABEL_SPACING ).width( MEDIUM_FIELD ).result();
    wConnection.setLayoutData( fdConnection );
    wConnection.addModifyListener( modifyEvent -> {

        // Populate the drop down menus in the Search Criteria View
      if ( !Utils.isEmpty( wConnection.getText() ) ) {
        facetsResult = DialogUtils.getFacets( connectionManagerSupplier, wConnection, transMeta );
        if ( facetsResult != null ) {
          wDataSources.setItems( facetsResult.getFacetSelection( Facet.DATA_SOURCES ).toArray( new String[0] ) );
          wFileFormat.setItems( facetsResult.getFacetSelection( Facet.FILE_FORMAT ).toArray( new String[0] ) );
          wFileSize.setItems( facetsResult.getFacetSelection( Facet.FILE_SIZE ).toArray( new String[0] ) );
          wResourceType.setItems( facetsResult.getFacetSelection( Facet.RESOURCE_TYPE ).toArray( new String[0] ) );
          wVirtualFolders.setItems( facetsResult.getFacetSelection( Facet.VIRTUAL_FOLDERS ).toArray( new String[0] ) );
        }
      }
      wbIDAdd.setEnabled( !Utils.isEmpty( wID.getText() ) && !Utils.isEmpty( wConnection.getText() ) );
    } );

    List<String> connections = ConnectionManager.getInstance().getNamesByKey( CatalogDetails.CATALOG );
    wConnection.setItems( connections.toArray( new String[ 0 ] ) );

    wTabFolder = new CTabFolder( contentComposite, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );
    wTabFolder.setSimple( false );
    wTabFolder.setUnselectedCloseVisible( true );
    wTabFolder.addSelectionListener( new SelectionAdapter() {
        @Override
        public void widgetSelected( SelectionEvent selectionEvent ) {
        super.widgetSelected( selectionEvent );
        meta.setChanged();
        }
    } );

    //Cancel, action and OK buttons for the bottom of the window.
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( catalogMetaClass, "System.Button.Cancel" ) );
    FormData fdCancel = new FormDataBuilder().right( 100, -MARGIN_SIZE ).bottom().result();
    wCancel.setLayoutData( fdCancel );

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( catalogMetaClass, "System.Button.OK" ) );
    FormData fdOk = new FormDataBuilder().right( wCancel, -LABEL_SPACING ).bottom().result();
    wOK.setLayoutData( fdOk );

    //Space between bottom buttons and the table, final layout for table
    Label bottomSpacer = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    FormData fdhSpacer = new FormDataBuilder().left().right( 100, -MARGIN_SIZE ).bottom( wCancel, -MARGIN_SIZE ).result();
    bottomSpacer.setLayoutData( fdhSpacer );

    FormData fdTabFolder = new FormDataBuilder().left( 0, 0 ).top( wConnection, DOUBLE_ELEMENT_SPACING ).right( 100, 0 ).bottom( 100, 0 ).result();
    wTabFolder.setLayoutData( fdTabFolder );

    addInputTab();
    addFieldsTab();
    addAdditionalFieldsTab();

    //Add everything to the scrolling composite
    scrolledComposite.setContent( contentComposite );
    scrolledComposite.setExpandVertical( true );
    scrolledComposite.setExpandHorizontal( true );
    scrolledComposite.setMinSize( contentComposite.computeSize( SWT.DEFAULT, SWT.DEFAULT ) );

    scrolledComposite.setLayout( new FormLayout() );
    FormData fdScrolledComposite = new FormDataBuilder().fullWidth().top().bottom( bottomSpacer, -MARGIN_SIZE ).result();
    scrolledComposite.setLayoutData( fdScrolledComposite );
    props.setLook( scrolledComposite );

    //Listeners
    lsCancel = e -> cancel();
    lsOK = e -> ok();
    lsGet = new Listener() {
        public void handleEvent( Event e ) {
          get();
        }
    };

    wOK.addListener( SWT.Selection, lsOK );
    wCancel.addListener( SWT.Selection, lsCancel );
    wGet.addListener( SWT.Selection, lsGet );

    lsDef = new SelectionAdapter() {
        @Override
        public void widgetDefaultSelected( SelectionEvent e ) {
          ok();
        }
    };
    wStepNameField.addSelectionListener( lsDef );

    shell.addShellListener( new ShellAdapter() {
        @Override
        public void shellClosed( ShellEvent e ) {
          cancel();
        }
    } );

    //Setup the radio button event handler
    SelectionAdapter searchByRadioSelectionAdapter = new SelectionAdapter() {
        @Override
        public void widgetSelected( SelectionEvent e ) {
          updateDynamicComposite();
        }
    };
    resourcesButton.addSelectionListener( searchByRadioSelectionAdapter );
    searchButton.addSelectionListener( searchByRadioSelectionAdapter );
    advancedSearchButton.addSelectionListener( searchByRadioSelectionAdapter );

    if ( meta.getSelectedIndex() == 1 ) {
      searchButton.setSelection( true );
    } else if ( meta.getSelectedIndex() == 2 ) {
      advancedSearchButton.setSelection( true );
    } else {
      resourcesButton.setSelection( true );
    }

    updateDynamicComposite();

    // Populate the data
    getData( meta );

    //Show shell
    setSize();
    meta.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }

  private void addInputTab() {

    CTabItem inputTab = new CTabItem( wTabFolder, SWT.NONE );
    inputTab.setText( BaseMessages.getString( catalogMetaClass, "ReadPayload.Input.Label" ) );

    Composite inputComposite = new Composite( wTabFolder, 0 );
    props.setLook( inputComposite );

    FormLayout inputLayout = new FormLayout();
    inputLayout.marginWidth = 15;
    inputLayout.marginHeight = 15;
    inputComposite.setLayout( inputLayout );

    // Add the Search By Group
    Group searchByGroup = new Group( inputComposite, SWT.SHADOW_ETCHED_IN );
    searchByGroup.setText( BaseMessages.getString( catalogMetaClass, "ReadPayload.SearchBy.Title" ) );
    FormLayout layout = new FormLayout();
    layout.marginHeight = MARGIN_SIZE;
    layout.marginWidth = MARGIN_SIZE;
    searchByGroup.setLayout( layout );
    searchByGroup.setLayoutData( new FormDataBuilder().top().fullWidth().result() );

    resourcesButton = new Button( searchByGroup, SWT.RADIO );
    resourcesButton.setText( BaseMessages.getString( catalogMetaClass, "ReadPayload.Resources.Title" ) );
    resourcesButton.setLayoutData( new FormDataBuilder().left().top().width( RADIO_BUTTON_WIDTH ).result() );

    searchButton = new Button( searchByGroup, SWT.RADIO );
    searchButton.setText( BaseMessages.getString( catalogMetaClass, "ReadPayload.Search.Title" ) );
    searchButton.setLayoutData( new FormDataBuilder().left().top( resourcesButton, ELEMENT_SPACING ).width( RADIO_BUTTON_WIDTH ).result() );

    advancedSearchButton = new Button( searchByGroup, SWT.RADIO );
    advancedSearchButton.setText( BaseMessages.getString( catalogMetaClass, "ReadPayload.AdvancedSearch.Title" ) );
    advancedSearchButton.setLayoutData( new FormDataBuilder().left().top( searchButton, ELEMENT_SPACING ).width( RADIO_BUTTON_WIDTH ).result() );

    //Make a composite to hold the dynamic right side of the group
    Composite dynamicComposite = new Composite( inputComposite, SWT.NONE );
    FormLayout fileSettingsDynamicAreaSchemaLayout = new FormLayout();
    dynamicComposite.setLayout( fileSettingsDynamicAreaSchemaLayout );
    dynamicComposite.setLayoutData( new FormDataBuilder().top( searchByGroup, ELEMENT_SPACING ).left().right().bottom().result() );

    resourcesComposite = addResourcesComposite( dynamicComposite );
    searchComposite = addSearchComposite( dynamicComposite );
    advancedSearchComposite = addAdvancedSearchComposite( dynamicComposite );


    inputComposite.setLayoutData( new FormDataBuilder().left( 0, 0 ).top( 0, 0 ).right( 100, 0 ).bottom( 100, 0 ).result() );
    inputComposite.layout();
    inputTab.setControl( inputComposite );
  }


  private void addFieldsTab() {
    CTabItem wFieldsTab = new CTabItem( wTabFolder, SWT.NONE );
    wFieldsTab.setText( BaseMessages.getString( catalogMetaClass, "ReadPayload.Fields.Label" ) );

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = Const.FORM_MARGIN;
    fieldsLayout.marginHeight = Const.FORM_MARGIN;

    Composite wFieldsComp = new Composite( wTabFolder, SWT.NONE );
    wFieldsComp.setLayout( fieldsLayout );
    props.setLook( wFieldsComp );

    wGet = new Button( wFieldsComp, SWT.PUSH );
    wGet.setText( BaseMessages.getString( catalogMetaClass, "System.Button.GetFields" ) );
    fdGet = new FormData();
    fdGet.left = new FormAttachment( 50, 0 );
    fdGet.bottom = new FormAttachment( 100, 0 );
    wGet.setLayoutData( fdGet );

    Button wMinWidth = new Button( wFieldsComp, SWT.PUSH );
    wMinWidth.setText( BaseMessages.getString( catalogMetaClass, "ReadPayloadStepDialog.MinWidth.Button" ) );
    wMinWidth.setToolTipText( BaseMessages.getString( catalogMetaClass, "ReadPayloadStepDialog.MinWidth.Tooltip" ) );
    wMinWidth.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        meta.setChanged();
      }
    } );
    setButtonPositions( new Button[] { wGet, wMinWidth }, Const.MARGIN, null );

    final int FieldsRows = 10;

    ColumnInfo[] colinf = new ColumnInfo[] {
      new ColumnInfo( BaseMessages.getString( catalogMetaClass, "ReadPayloadStepDialog.NameColumn.Column" ), ColumnInfo.COLUMN_TYPE_TEXT, false ),
      new ColumnInfo( BaseMessages.getString( catalogMetaClass, "ReadPayloadStepDialog.TypeColumn.Column" ), ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaFactory.getValueMetaNames(), true ),
      new ColumnInfo( BaseMessages.getString( catalogMetaClass, "ReadPayloadStepDialog.FormatColumn.Column" ), ColumnInfo.COLUMN_TYPE_FORMAT, 2 ),
      new ColumnInfo( BaseMessages.getString( catalogMetaClass, "ReadPayloadStepDialog.PositionColumn.Column" ), ColumnInfo.COLUMN_TYPE_TEXT, false ),
      new ColumnInfo( BaseMessages.getString( catalogMetaClass, "ReadPayloadStepDialog.LengthColumn.Column" ), ColumnInfo.COLUMN_TYPE_TEXT, false ),
      new ColumnInfo( BaseMessages.getString( catalogMetaClass, "ReadPayloadStepDialog.PrecisionColumn.Column" ), ColumnInfo.COLUMN_TYPE_TEXT, false ),
      new ColumnInfo( BaseMessages.getString( catalogMetaClass, "ReadPayloadStepDialog.CurrencyColumn.Column" ), ColumnInfo.COLUMN_TYPE_TEXT, false ),
      new ColumnInfo( BaseMessages.getString( catalogMetaClass, "ReadPayloadStepDialog.DecimalColumn.Column" ), ColumnInfo.COLUMN_TYPE_TEXT, false ),
      new ColumnInfo( BaseMessages.getString( catalogMetaClass, "ReadPayloadStepDialog.GroupColumn.Column" ), ColumnInfo.COLUMN_TYPE_TEXT, false ),
      new ColumnInfo( BaseMessages.getString( catalogMetaClass, "ReadPayloadStepDialog.NullIfColumn.Column" ), ColumnInfo.COLUMN_TYPE_TEXT, false ),
      new ColumnInfo( BaseMessages.getString( catalogMetaClass, "ReadPayloadStepDialog.IfNullColumn.Column" ), ColumnInfo.COLUMN_TYPE_TEXT, false ),
      new ColumnInfo( BaseMessages.getString( catalogMetaClass, "ReadPayloadStepDialog.TrimTypeColumn.Column" ), ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaString.trimTypeDesc, true ),
      new ColumnInfo( BaseMessages.getString( catalogMetaClass, "ReadPayloadStepDialog.RepeatColumn.Column" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
      new String[] { BaseMessages.getString( catalogMetaClass, "System.Combo.Yes" ), BaseMessages.getString( catalogMetaClass, "System.Combo.No" ) }, true ) };

    colinf[12].setToolTip( BaseMessages.getString( catalogMetaClass, "ReadPayloadStepDialog.RepeatColumn.Tooltip" ) );

    wFields = new TableView( transMeta, wFieldsComp, SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( 0, 0 );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( wGet, -Const.MARGIN );
    wFields.setLayoutData( fdFields );

    FormData fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment( 0, 0 );
    fdFieldsComp.top = new FormAttachment( 0, 0 );
    fdFieldsComp.right = new FormAttachment( 100, 0 );
    fdFieldsComp.bottom = new FormAttachment( 100, 0 );
    wFieldsComp.setLayoutData( fdFieldsComp );

    wFieldsComp.layout();
    wFieldsTab.setControl( wFieldsComp );
  }

  private void addAdditionalFieldsTab() {
    CTabItem wAdditionalFieldsTab = new CTabItem( wTabFolder, SWT.NONE );
    wAdditionalFieldsTab.setText( BaseMessages.getString( catalogMetaClass, "ReadPayloadStepDialog.AdditionalFieldsTab.TabTitle" ) );

    Composite wAdditionalFieldsComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wAdditionalFieldsComp );

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = 3;
    fieldsLayout.marginHeight = 3;
    wAdditionalFieldsComp.setLayout( fieldsLayout );

    Label idLabel = new Label( wAdditionalFieldsComp, SWT.RIGHT );
    idLabel.setText( BaseMessages.getString( catalogMetaClass, "ReadPayloadStepDialog.ID.Label" ) );
    props.setLook( idLabel );
    FormData idLabelForm = new FormData();
    idLabelForm.left = new FormAttachment( 0, 0 );
    idLabelForm.top = new FormAttachment( Const.MARGIN, Const.MARGIN );
    idLabelForm.right = new FormAttachment( middle, -Const.MARGIN );
    idLabel.setLayoutData( idLabelForm );

    idTextField = new TextVar( transMeta, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( idTextField );
    idTextField.addModifyListener( lsMod );
    FormData idTextFieldForm = new FormData();
    idTextFieldForm.left = new FormAttachment( middle, 0 );
    idTextFieldForm.right = new FormAttachment( 100, -Const.MARGIN );
    idTextFieldForm.top = new FormAttachment( Const.MARGIN, Const.MARGIN );
    idTextField.setLayoutData( idTextFieldForm );
    idTextField.addModifyListener( lsMod );

    Label dataSourceLabel = new Label( wAdditionalFieldsComp, SWT.RIGHT );
    dataSourceLabel.setText( BaseMessages.getString( catalogMetaClass, "ReadPayloadStepDialog.DataSource.Label" ) );
    props.setLook( dataSourceLabel );
    FormData dataSourceLabelForm = new FormData();
    dataSourceLabelForm.left = new FormAttachment( 0, 0 );
    dataSourceLabelForm.top = new FormAttachment( idTextField, Const.MARGIN );
    dataSourceLabelForm.right = new FormAttachment( middle, -Const.MARGIN );
    dataSourceLabel.setLayoutData( dataSourceLabelForm );

    dataSourceTextField = new TextVar( transMeta, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( dataSourceTextField );
    dataSourceTextField.addModifyListener( lsMod );
    FormData dataSourceTextFieldForm = new FormData();
    dataSourceTextFieldForm.left = new FormAttachment( middle, 0 );
    dataSourceTextFieldForm.right = new FormAttachment( 100, -Const.MARGIN );
    dataSourceTextFieldForm.top = new FormAttachment( idTextField, Const.MARGIN );
    dataSourceTextField.setLayoutData( dataSourceTextFieldForm );
    dataSourceTextField.addModifyListener( lsMod );

    Label virtualFolderLabel = new Label( wAdditionalFieldsComp, SWT.RIGHT );
    virtualFolderLabel.setText( BaseMessages.getString( catalogMetaClass, "ReadPayloadStepDialog.VirtualFolder.Label" ) );
    props.setLook( virtualFolderLabel );
    FormData virtualFolderLabelForm = new FormData();
    virtualFolderLabelForm.left = new FormAttachment( 0, 0 );
    virtualFolderLabelForm.top = new FormAttachment( dataSourceTextField, Const.MARGIN );
    virtualFolderLabelForm.right = new FormAttachment( middle, -Const.MARGIN );
    virtualFolderLabel.setLayoutData( virtualFolderLabelForm );

    virtualFolderTextField = new TextVar( transMeta, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( virtualFolderTextField );
    virtualFolderTextField.addModifyListener( lsMod );
    FormData virtualFolderTextFieldForm = new FormData();
    virtualFolderTextFieldForm.left = new FormAttachment( middle, 0 );
    virtualFolderTextFieldForm.right = new FormAttachment( 100, -Const.MARGIN );
    virtualFolderTextFieldForm.top = new FormAttachment( dataSourceTextField, Const.MARGIN );
    virtualFolderTextField.setLayoutData( virtualFolderTextFieldForm );
    virtualFolderTextField.addModifyListener( lsMod );

    FormData fdAdditionalFieldsComp = new FormData();
    fdAdditionalFieldsComp.left = new FormAttachment( 0, 0 );
    fdAdditionalFieldsComp.top = new FormAttachment( 0, 0 );
    fdAdditionalFieldsComp.right = new FormAttachment( 100, 0 );
    fdAdditionalFieldsComp.bottom = new FormAttachment( 100, 0 );
    wAdditionalFieldsComp.setLayoutData( fdAdditionalFieldsComp );

    wAdditionalFieldsComp.layout();
    wAdditionalFieldsTab.setControl( wAdditionalFieldsComp );

  }

  private void updateDynamicComposite() {
    DialogUtils.recursiveSetEnabled( resourcesComposite, resourcesButton.getSelection() );
    resourcesComposite.setVisible( resourcesButton.getSelection() );

    DialogUtils.recursiveSetEnabled( searchComposite, searchButton.getSelection() );
    searchComposite.setVisible( searchButton.getSelection() );

    DialogUtils.recursiveSetEnabled( advancedSearchComposite, advancedSearchButton.getSelection() );
    advancedSearchComposite.setVisible( advancedSearchButton.getSelection() );
  }

  private Composite addResourcesComposite( Composite parent ) {
    Group resultComposite = new Group( parent, SWT.SHADOW_NONE );
    resultComposite.setText( BaseMessages.getString( catalogMetaClass, "ReadPayload.Resources.Title" ) );
    FormLayout layout = new FormLayout();
    layout.marginHeight = MARGIN_SIZE;
    layout.marginWidth = MARGIN_SIZE;
    resultComposite.setLayout( layout );
    resultComposite
            .setLayoutData( new FormDataBuilder().top().left().right( 100, -MARGIN_SIZE ).bottom().result() );

    Label wlID = new Label( resultComposite, SWT.LEFT );
    wlID.setText( "ID" );
    props.setLook( wlID );
    FormData fdlID = new FormDataBuilder().left()
            .top()
            .result();
    wlID.setLayoutData( fdlID );

    wID = new TextVar( transMeta, resultComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wID );
    FormData fdID = new FormDataBuilder().left()
            .top( wlID, LABEL_SPACING )
            .width( MEDIUM_FIELD )
            .result();
    wID.setLayoutData( fdID );
    wID.addModifyListener( modifyEvent ->
            wbIDAdd.setEnabled( !Utils.isEmpty( wID.getText() ) && !Utils.isEmpty( wConnection.getText() ) )
    );

    wbIDAdd = new Button( resultComposite, SWT.PUSH );
    props.setLook( wbIDAdd );
    wbIDAdd.setEnabled( false );
    wbIDAdd.setText( BaseMessages.getString( catalogMetaClass, "ReadPayload.Button.Add" ) );
    FormData fdbIDAdd = new FormDataBuilder().left( wID, ELEMENT_SPACING ).top( wlID, LABEL_SPACING ).result();
    wbIDAdd.setLayoutData( fdbIDAdd );
    wbIDAdd.addListener( SWT.Selection, event -> {
      DataResource dataResource = getDataResource( wID.getText(), connectionManagerSupplier, wConnection, transMeta );
      if ( dataResource == null ) {
        logError( BaseMessages.getString( catalogMetaClass, "ReadPayload.Error.DataResourceRetrievalFailed" ), wID.getText(), wConnection.getText() );
        return;
      }
      wSelectedFiles.add( dataResource.getResourcePath(), dataResource.getKey(), dataResource.getType(), dataResource.getDataSourceName() );
      wID.setText( "" );
      wSelectedFiles.removeEmptyRows();
      wSelectedFiles.setRowNums();
      wSelectedFiles.optWidth( true );
      meta.setChanged();
    } );

    Label wlSelectedFiles = new Label( resultComposite, SWT.LEFT );
    wlSelectedFiles.setText( BaseMessages.getString( catalogMetaClass, "ReadPayload.Resources.Label.SelectedFiles" ) );
    props.setLook( wlSelectedFiles );
    wlSelectedFiles.setLayoutData( new FormDataBuilder().left().top( wID, ELEMENT_SPACING ).left().result() );

    ColumnInfo[] selectedFilesColumns = new ColumnInfo[] {
      new ColumnInfo( BaseMessages.getString( catalogMetaClass, "ReadPayload.Resources.column.ResourceName" ), ColumnInfo.COLUMN_TYPE_TEXT, false, true ),
      new ColumnInfo( BaseMessages.getString( catalogMetaClass, "ReadPayload.Resources.column.ID" ), ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
      new ColumnInfo( BaseMessages.getString( catalogMetaClass, "ReadPayload.Resources.column.ResourceType" ), ColumnInfo.COLUMN_TYPE_TEXT, false, true ),
      new ColumnInfo( BaseMessages.getString( catalogMetaClass, "ReadPayload.Resources.column.Origin" ), ColumnInfo.COLUMN_TYPE_TEXT, false, true ) };

    selectedFilesColumns[ 1 ].setUsingVariables( true );

    wSelectedFiles =
            new TableView( transMeta, resultComposite, SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER,
                    selectedFilesColumns,
                    meta.getResourceFields().size(), false, lsMod, props, false );
    props.setLook( wSelectedFiles );
    FormData fdSelectedFiles =
            new FormDataBuilder().left( 0, 0 ).right( 100, 0 ).top( wlSelectedFiles, LABEL_SPACING ).bottom( 100, 0 )
                    .result();
    wSelectedFiles.setLayoutData( fdSelectedFiles );
    wSelectedFiles.getTable().addListener( SWT.Resize, new ColumnsResizer( 0, 33, 33, 33, 33 ) );

    wSelectedFiles.removeEmptyRows();
    wSelectedFiles.setRowNums();
    wSelectedFiles.optWidth( true );

    Button wbEdit = new Button( resultComposite, SWT.PUSH );
    props.setLook( wbEdit );
    wbEdit.setText( BaseMessages.getString( catalogMetaClass, "ReadPayload.Button.Edit" ) );
    wbEdit.setLayoutData( new FormDataBuilder().right( 100, 0 ).bottom( wSelectedFiles, -LABEL_SPACING ).result() );
    wbEdit.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent selectionEvent ) {
        int index = wSelectedFiles.getSelectionIndex();
        if ( index != -1 ) {
            wID.setText( wSelectedFiles.getItem( index )[ 1 ] );
            wSelectedFiles.remove( index );
            wSelectedFiles.removeEmptyRows();
            wSelectedFiles.setRowNums();
            wSelectedFiles.optWidth( true );
        }
      }
    } );

    Button wbDelete = new Button( resultComposite, SWT.PUSH );
    props.setLook( wbDelete );
    wbDelete.setText( BaseMessages.getString( catalogMetaClass, "ReadPayload.Button.Delete" ) );
    wbDelete.setLayoutData( new FormDataBuilder().right( wbEdit, -LABEL_SPACING ).bottom( wSelectedFiles, -LABEL_SPACING ).result() );
    wbDelete.addSelectionListener( new SelectionAdapter() {
        @Override public void widgetSelected( SelectionEvent selectionEvent ) {
        int index = wSelectedFiles.getSelectionIndex();
        if ( index != -1 ) {
            wSelectedFiles.remove( index );
            wSelectedFiles.removeEmptyRows();
            wSelectedFiles.setRowNums();
            wSelectedFiles.optWidth( true );
            meta.setChanged();
        }
        }
    } );
    return resultComposite;
  }

  private Composite addSearchComposite( Composite parent ) {
    Group resultComposite = new Group( parent, SWT.SHADOW_NONE );
    resultComposite.setText( BaseMessages.getString( catalogMetaClass, "ReadPayload.Search.Title" ) );
    FormLayout layout = new FormLayout();
    layout.marginHeight = MARGIN_SIZE;
    layout.marginWidth = MARGIN_SIZE;
    resultComposite.setLayout( layout );
    resultComposite.setLayoutData( new FormDataBuilder().top().left().right( 100, -MARGIN_SIZE ).bottom().result() );

    Label wlKeyword = new Label( resultComposite, SWT.LEFT );
    wlKeyword.setText( BaseMessages.getString( catalogMetaClass, "ReadPayload.Criteria.Label.Keyword" ) );
    props.setLook( wlKeyword );
    wlKeyword.setLayoutData( new FormDataBuilder().left().top().width( LABEL_WIDTH ).result() );

    wKeyword = new TextVar( transMeta, resultComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wKeyword );
    wKeyword.setLayoutData( new FormDataBuilder().left( wlKeyword, 0 ).top().width( MEDIUM_FIELD ).result() );
    wKeyword.addModifyListener( lsMod );

    Label wlTags = new Label( resultComposite, SWT.LEFT );
    wlTags.setText( BaseMessages.getString( catalogMetaClass, "ReadPayload.Criteria.Label.Tags" ) );
    props.setLook( wlTags );
    wlTags.setLayoutData( new FormDataBuilder().left().top( wKeyword, ELEMENT_SPACING ).width( LABEL_WIDTH ).result() );

    wTags = new TextVar( transMeta, resultComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTags );
    wTags.setLayoutData( new FormDataBuilder().left( wlTags, 0 ).top( wKeyword, ELEMENT_SPACING ).width( MEDIUM_FIELD ).result() );
    wTags.addModifyListener( lsMod );

    Label wlVirtualFolders = new Label( resultComposite, SWT.LEFT );
    wlVirtualFolders.setText( BaseMessages.getString( catalogMetaClass, "ReadPayload.Criteria.Label.VirtualFolders" ) );
    props.setLook( wlVirtualFolders );
    wlVirtualFolders.setLayoutData( new FormDataBuilder().left().top( wTags, ELEMENT_SPACING ).width( LABEL_WIDTH ).result() );

    wVirtualFolders = new CCombo( resultComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wVirtualFolders );
    wVirtualFolders.setLayoutData(
            new FormDataBuilder().left( wlVirtualFolders, 0 ).top( wTags, ELEMENT_SPACING ).width( MEDIUM_FIELD ).result() );
    wVirtualFolders.addModifyListener( lsMod );

    Label wlDataSources = new Label( resultComposite, SWT.LEFT );
    wlDataSources.setText( BaseMessages.getString( catalogMetaClass, "ReadPayload.Criteria.Label.DataSources" ) );
    props.setLook( wlDataSources );
    wlDataSources.setLayoutData( new FormDataBuilder().left().top( wVirtualFolders, ELEMENT_SPACING ).width( LABEL_WIDTH ).result() );

    wDataSources = new CCombo( resultComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDataSources );
    wDataSources.setLayoutData( new FormDataBuilder().left( wlDataSources, 0 ).top( wVirtualFolders, ELEMENT_SPACING ).width( MEDIUM_FIELD ).result() );
    wDataSources.addModifyListener( lsMod );

    Label wlResourceType = new Label( resultComposite, SWT.LEFT );
    wlResourceType.setText( BaseMessages.getString( catalogMetaClass, "ReadPayload.Criteria.Label.ResourceType" ) );
    props.setLook( wlResourceType );
    wlResourceType.setLayoutData( new FormDataBuilder().left().top( wDataSources, ELEMENT_SPACING ).width( LABEL_WIDTH ).result() );

    wResourceType = new CCombo( resultComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wResourceType );
    wResourceType.setLayoutData( new FormDataBuilder().left( wlResourceType, 0 ).top( wDataSources, ELEMENT_SPACING ).width( MEDIUM_FIELD ).result() );
    wResourceType.addModifyListener( lsMod );

    Label wlFileSize = new Label( resultComposite, SWT.LEFT );
    wlFileSize.setText( BaseMessages.getString( catalogMetaClass, "ReadPayload.Criteria.Label.FileSize" ) );
    props.setLook( wlFileSize );
    wlFileSize.setLayoutData( new FormDataBuilder().left().top( wResourceType, ELEMENT_SPACING ).width( LABEL_WIDTH ).result() );

    wFileSize = new CCombo( resultComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFileSize );
    wFileSize.setLayoutData( new FormDataBuilder().left( wlFileSize, 0 ).top( wResourceType, ELEMENT_SPACING ).width( MEDIUM_FIELD ).result() );
    wFileSize.addModifyListener( lsMod );

    Label wlFileFormat = new Label( resultComposite, SWT.LEFT );
    wlFileFormat.setText( BaseMessages.getString( catalogMetaClass, "ReadPayload.Criteria.Label.FileFormat" ) );
    props.setLook( wlFileFormat );
    wlFileFormat.setLayoutData( new FormDataBuilder().left().top( wFileSize, ELEMENT_SPACING ).width( LABEL_WIDTH ).result() );

    wFileFormat = new CCombo( resultComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFileFormat );
    wFileFormat.setLayoutData( new FormDataBuilder().left( wlFileFormat, 0 ).top( wFileSize, ELEMENT_SPACING ).width( MEDIUM_FIELD ).result() );
    wFileFormat.addModifyListener( lsMod );

    return resultComposite;
  }

  private Composite addAdvancedSearchComposite( Composite parent ) {
    Group resultComposite = new Group( parent, SWT.SHADOW_NONE );
    resultComposite.setText( BaseMessages.getString( catalogMetaClass, "ReadPayload.AdvancedSearch.Title" ) );
    FormLayout layout = new FormLayout();
    layout.marginHeight = MARGIN_SIZE;
    layout.marginWidth = MARGIN_SIZE;
    resultComposite.setLayout( layout );
    resultComposite.setLayoutData( new FormDataBuilder().top().left().right( 100, -MARGIN_SIZE ).bottom().result() );

    wAdvancedQuery = new TextVar( transMeta, resultComposite, SWT.MULTI | SWT.LEFT | SWT.BORDER );
    props.setLook( wAdvancedQuery );
    wAdvancedQuery.setLayoutData( new FormDataBuilder().left().top().right( 100, 0 ).bottom( 100, 0 ).result() );
    wFileFormat.addModifyListener( lsMod );
    return resultComposite;
  }

  private void getData( ReadPayloadMeta meta ) {
    getData( meta, true, null );
  }

  private void getData( ReadPayloadMeta meta, boolean reloadAllFields, Set<String> newFieldNames ) {
    wConnection.setText( Const.NVL( meta.getConnection(), "" ) );
    wSelectedFiles.removeAll();
    for ( ResourceField resourceField : meta.getResourceFields() ) {
      wSelectedFiles.add( resourceField.getName(), resourceField.getId(), resourceField.getType(), resourceField.getOrigin() );
    }
    wSelectedFiles.removeEmptyRows();
    wSelectedFiles.setRowNums();
    wSelectedFiles.optWidth( true );

    wTabFolder.setSelection( meta.getSelectedIndex() );
    wKeyword.setText( Const.NVL( meta.getKeyword(), "" ) );
    wTags.setText( Const.NVL( meta.getTags(), "" ) );
    wVirtualFolders.setText( Const.NVL( meta.getVirtualFolders(), "" ) );
    wDataSources.setText( Const.NVL( meta.getDataSources(), "" ) );
    wResourceType.setText( Const.NVL( meta.getResourceType(), "" ) );
    wFileSize.setText( Const.NVL( meta.getFileSize(), "" ) );
    wFileFormat.setText( Const.NVL( meta.getFileFormat(), "" ) );
    wAdvancedQuery.setText( Const.NVL( meta.getAdvancedQuery(), "" ) );

    getFieldsData( meta, false, reloadAllFields, newFieldNames );

    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth( true );

    idTextField.setText( Const.NVL( meta.getIdField(), "" ) );
    dataSourceTextField.setText( Const.NVL( meta.getDataSourceField(), "" ) );
    virtualFolderTextField.setText( Const.NVL( meta.getVirtualFolderField(), "" ) );
  }

  /**
   * Sets the input width to minimal width...
   *
   */
  public void setMinimalWidth() {
    int nrNonEmptyFields = wFields.nrNonEmpty();
    for ( int i = 0; i < nrNonEmptyFields; i++ ) {
      TableItem item = wFields.getNonEmpty( i );

      item.setText( 5, "" );
      item.setText( 6, "" );
      item.setText( 12, ValueMetaString.getTrimTypeDesc( ValueMetaInterface.TRIM_TYPE_BOTH ) );

      int type = ValueMetaFactory.getIdForValueMeta( item.getText( 2 ) );
      switch ( type ) {
        case ValueMetaInterface.TYPE_STRING:
          item.setText( 3, "" );
          break;
        case ValueMetaInterface.TYPE_INTEGER:
          item.setText( 3, "0" );
          break;
        case ValueMetaInterface.TYPE_NUMBER:
          item.setText( 3, "0.#####" );
          break;
        case ValueMetaInterface.TYPE_DATE:
          break;
        default:
          break;
      }
    }

    for ( int i = 0; i < meta.inputFields.length; i++ ) {
      meta.inputFields[i].setTrimType( ValueMetaInterface.TRIM_TYPE_BOTH );
    }

    wFields.optWidth( true );
  }

  /**
   * Overloading setMinimalWidth() in order to test trim functionality
   * @param wFields mocked TableView to avoid wFields.nrNonEmpty() from throwing NullPointerException
   */
  public void setMinimalWidth( TableView wFields ) {
    this.wFields = wFields;
    this.setMinimalWidth();
  }


  private void getFieldsData( ReadPayloadMeta in, boolean insertAtTop, final boolean reloadAllFields,
                             final Set<String> newFieldNames ) {
    final List<String> lowerCaseNewFieldNames = newFieldNames == null ? new ArrayList()
            : newFieldNames.stream().map( String::toLowerCase ).collect( Collectors.toList() );

    for ( int i = 0; i < in.inputFields.length; i++ ) {
      BaseFileField field = in.inputFields[i];

      TableItem item;

      if ( insertAtTop ) {
        item = new TableItem( wFields.table, SWT.NONE, i );
      } else {
        item = getTableItem( field.getName(), reloadAllFields );
      }
      if ( !reloadAllFields && !lowerCaseNewFieldNames.contains( field.getName().toLowerCase() ) ) {
        continue;
      }

      item.setText( 1, Const.NVL( field.getName(), "" ) );
      String type = field.getTypeDesc();
      String format = field.getFormat();
      String position = "" + field.getPosition();
      String length = "" + field.getLength();
      String prec = "" + field.getPrecision();
      String curr = field.getCurrencySymbol();
      String group = field.getGroupSymbol();
      String decim = field.getDecimalSymbol();
      String def = field.getNullString();
      String ifNull = field.getIfNullValue();
      String trim = field.getTrimTypeDesc();
      String rep =
              field.isRepeated() ? BaseMessages.getString( PKG, "System.Combo.Yes" ) : BaseMessages.getString( PKG,
                      "System.Combo.No" );

      if ( type != null ) {
        item.setText( 2, type );
      }
      if ( format != null ) {
        item.setText( 3, format );
      }
      if ( !"-1".equals( position ) ) {
        item.setText( 4, position );
      }
      if ( !"-1".equals( length ) ) {
        item.setText( 5, length );
      }
      if ( !"-1".equals( prec ) ) {
        item.setText( 6, prec );
      }
      if ( curr != null ) {
        item.setText( 7, curr );
      }
      if ( decim != null ) {
        item.setText( 8, decim );
      }
      if ( group != null ) {
        item.setText( 9, group );
      }
      if ( def != null ) {
        item.setText( 10, def );
      }
      if ( ifNull != null ) {
        item.setText( 11, ifNull );
      }
      if ( trim != null ) {
        item.setText( 12, trim );
      }
      if ( rep != null ) {
        item.setText( 13, rep );
      }
    }

  }

  private void getInfo( ReadPayloadMeta meta ) {
    meta.setConnection( wConnection.getText() );
    List<ResourceField> resourceFields = new ArrayList<>();
    for ( int i = 0; i < wSelectedFiles.nrNonEmpty(); i++ ) {
      TableItem tableItem = wSelectedFiles.getNonEmpty( i );
      ResourceField resourceField = new ResourceField();
      resourceField.setName( tableItem.getText( 1 ) );
      resourceField.setId( tableItem.getText( 2 ) );
      resourceField.setType( tableItem.getText( 3 ) );
      resourceField.setOrigin( tableItem.getText( 4 ) );
      resourceFields.add( resourceField );
    }
    meta.setResourceFields( resourceFields );
    meta.setKeyword( wKeyword.getText() );
    meta.setTags( wTags.getText() );
    meta.setVirtualFolders( wVirtualFolders.getText() );
    meta.setDataSources( wDataSources.getText() );
    meta.setResourceType( wResourceType.getText() );
    meta.setFileSize( wFileSize.getText() );
    meta.setFileFormat( wFileFormat.getText() );
    meta.setAdvancedQuery( wAdvancedQuery.getText() );

    BaseFileField[ ] inputFields = new BaseFileField[fieldCount];

    for ( int i = 0; i < fieldCount; i++ ) {

      BaseFileField field = new BaseFileField();

      field.setName( fields.get( i ).getName() );
      field.setType( fields.get( i ).getDataType() );
      inputFields[i] = field;
    }

    meta.setInputFields( inputFields );

    meta.setIdField( idTextField.getText() );
    meta.setDataSourceField( dataSourceTextField.getText() );
    meta.setVirtualFolderField( virtualFolderTextField.getText() );

    if ( searchButton.getSelection() ) {
      if ( meta.getSelectedIndex() != 1 ) {
        meta.setSelectedIndex( 1 );
        meta.setChanged();
      }
    } else if ( advancedSearchButton.getSelection() ) {
      if ( meta.getSelectedIndex() != 2 ) {
        meta.setSelectedIndex( 2 );
        meta.setChanged();
      }
    } else {
      if ( meta.getSelectedIndex() != 0 ) {
        meta.setSelectedIndex( 0 );
        meta.setChanged();
      }
    }
  }

  private void cancel() {
    dispose();
  }

  private void ok() {
    stepname = wStepNameField.getText();

    getInfo( meta );

    dispose();
  }

  private List<AbstractField> fields;
  private int fieldCount;

  @Override
  public Shell getShell() {
    return this.shell;
  }

  @Override
  public String[] getFieldNames( ReadPayloadMeta readPayloadMeta ) {

    if ( fieldCount == 0 ) {
      return new String[0];
    }

    String[] fieldNames =  new String[fieldCount];

    for ( int i = 0; i < fieldCount; i++ ) {
      fieldNames[i] = Const.trim( fields.get( i ).getName() );
    }
    return fieldNames;
  }

  @Override
  public TableView getFieldsTable() {
    return wFields;
  }

  @Override
  public String loadFieldsImpl( ReadPayloadMeta readPayloadMeta, int i ) {
    return "empty";
  }

  @Override
  public void getData( ReadPayloadMeta readPayloadMeta, final boolean copyStepname, final boolean reloadAllFields, final Set<String> newFieldNames ) {
    readPayloadMeta.setSelectedIndex( 1 );
    getData( readPayloadMeta, reloadAllFields, newFieldNames );
  }

  @Override
  public void populateMeta( ReadPayloadMeta readPayloadMeta ) {
    getInfo( readPayloadMeta );
  }

  @Override
  public ReadPayloadMeta getNewMetaInstance() {
    return new ReadPayloadMeta();
  }

  @Override
  public TransMeta getTransMeta() {
    return transMeta;
  }

  private void get() {

    getInfo( meta );
    String id = meta.getResourceFields().get( 0 ).getId();
    DataResource dataResource = getDataResource( id, connectionManagerSupplier, wConnection, transMeta );

    if ( dataResource != null ) {
      fieldCount = (int) dataResource.getFieldCount().doubleValue();
      fields = dataResource.getFields();
      getFields( getPopulatedMeta() );
    }
  }
}
