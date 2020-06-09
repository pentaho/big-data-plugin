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

package com.pentaho.di.plugins.catalog.search;

import com.pentaho.di.plugins.catalog.api.entities.search.Facet;
import com.pentaho.di.plugins.catalog.api.entities.search.FacetsResult;
import com.pentaho.di.plugins.catalog.common.DialogUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
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
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.connections.ConnectionManager;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import com.pentaho.di.plugins.catalog.api.entities.DataResource;
import com.pentaho.di.plugins.catalog.provider.CatalogDetails;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.FormDataBuilder;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.ColumnsResizer;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static com.pentaho.di.plugins.catalog.common.DialogUtils.getDataResource;

public class SearchCatalogDialog extends BaseStepDialog implements StepDialogInterface {

  public static final String CATALOG = "Catalog";
  Supplier<ConnectionManager> connectionManagerSupplier = ConnectionManager::getInstance;

  public static final String DATA_RESOURCE = "Data Resource";
  public static final String DATA_SOURCE = "Data Source";
  private static final Class<?> catalogMetaClass = SearchCatalogMeta.class;
  // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  private static final int MARGIN_SIZE = 15;
  private static final int LABEL_SPACING = 5;
  private static final int ELEMENT_SPACING = 10;

  private static final int LABEL_WIDTH = 150;
  private static final int RADIO_BUTTON_WIDTH = 150;

  private static final int MEDIUM_FIELD = 250;

  private SearchCatalogMeta meta;

  private Text wStepNameField;

  private CCombo wConnection;

  private TextVar wID;

  private Button wbIDAdd;

  private TableView wSelectedFiles;

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

  public SearchCatalogDialog( Shell parent, Object in, TransMeta tr, String sname ) {
    super( parent, (BaseStepMeta) in, tr, sname );
    meta = (SearchCatalogMeta) in;
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
    shell.setText( BaseMessages.getString( catalogMetaClass, "SearchCatalogStepDialog.Shell.Title" ) );

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
    wStepNameLabel.setText( BaseMessages.getString( catalogMetaClass, "SearchCatalogStepDialog.Stepname.Label" ) );
    props.setLook( wStepNameLabel );
    FormData fdStepNameLabel = new FormDataBuilder().left()
      .top()
      .result();
    wStepNameLabel.setLayoutData( fdStepNameLabel );

    wStepNameField = new Text( contentComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepNameField.setText( stepname );
    props.setLook( wStepNameField );
    wStepNameField.addModifyListener( lsMod );
    FormData fdStepName = new FormDataBuilder().left()
      .top( wStepNameLabel, LABEL_SPACING )
      .width( MEDIUM_FIELD )
      .result();
    wStepNameField.setLayoutData( fdStepName );

    //Job icon, centered vertically between the top of the label and the bottom of the field.
    Label wicon = new Label( contentComposite, SWT.CENTER );
    wicon.setImage( DialogUtils.getImage( stepMeta, shell ) );
    FormData fdIcon = new FormDataBuilder().right()
      .top( 0, 4 )
      .bottom( new FormAttachment( wStepNameField, 0, SWT.BOTTOM ) )
      .result();
    wicon.setLayoutData( fdIcon );
    props.setLook( wicon );

    //Spacer between entry info and content
    Label topSpacer = new Label( contentComposite, SWT.HORIZONTAL | SWT.SEPARATOR );
    FormData fdSpacer = new FormDataBuilder().fullWidth()
      .top( wStepNameField, MARGIN_SIZE )
      .result();
    topSpacer.setLayoutData( fdSpacer );

    Label wlConnection = new Label( contentComposite, SWT.LEFT );
    wlConnection.setText( BaseMessages.getString( catalogMetaClass, "SearchCatalogStepDialog.Connection.Label" ) );
    props.setLook( wlConnection );
    FormData fdlConnection = new FormDataBuilder().left().top( topSpacer, ELEMENT_SPACING ).result();
    wlConnection.setLayoutData( fdlConnection );

    wConnection = new CCombo( contentComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wConnection );
    FormData fdConnection =
      new FormDataBuilder().left().top( wlConnection, LABEL_SPACING ).width( MEDIUM_FIELD ).result();
    wConnection.setLayoutData( fdConnection );
    wConnection.addModifyListener( modifyEvent -> {

      // Populate the drop down menus in the Search Criteria View
      if ( !Utils.isEmpty( wConnection.getText() ) ) {
        facetsResult = DialogUtils.getFacets( connectionManagerSupplier, wConnection, transMeta );
        wDataSources.setItems( facetsResult.getFacetSelection( Facet.DATA_SOURCES ).toArray( new String[ 0 ] ) );
        wFileFormat.setItems( facetsResult.getFacetSelection( Facet.FILE_FORMAT ).toArray( new String[ 0 ] ) );
        wFileSize.setItems( facetsResult.getFacetSelection( Facet.FILE_SIZE ).toArray( new String[ 0 ] ) );
        wResourceType.setItems( facetsResult.getFacetSelection( Facet.RESOURCE_TYPE ).toArray( new String[ 0 ] ) );
        wVirtualFolders.setItems( facetsResult.getFacetSelection( Facet.VIRTUAL_FOLDERS ).toArray( new String[ 0 ] ) );
      }

      wbIDAdd.setEnabled( !Utils.isEmpty( wID.getText() ) && !Utils.isEmpty( wConnection.getText() ) );
    } );

    List<String> connections = ConnectionManager.getInstance().getNamesByKey( CatalogDetails.CATALOG );
    wConnection.setItems( connections.toArray( new String[ 0 ] ) );

    // Add the Search By Group
    Group searchByGroup = new Group( contentComposite, SWT.SHADOW_NONE );
    searchByGroup.setText( BaseMessages.getString( catalogMetaClass, "SearchCatalog.SearchBy.Title" ) );
    FormLayout layout = new FormLayout();
    layout.marginHeight = MARGIN_SIZE;
    layout.marginWidth = MARGIN_SIZE;
    searchByGroup.setLayout( layout );
    searchByGroup
      .setLayoutData( new FormDataBuilder().top( wlConnection, 35 ).left( 0, 0 ).right( 100, -MARGIN_SIZE ).result() );

    resourcesButton = new Button( searchByGroup, SWT.RADIO );
    resourcesButton.setText( BaseMessages.getString( catalogMetaClass, "SearchCatalog.Resources.Title" ) );
    resourcesButton.setLayoutData( new FormDataBuilder().left().top().width( RADIO_BUTTON_WIDTH ).result() );

    searchButton = new Button( searchByGroup, SWT.RADIO );
    searchButton.setText( BaseMessages.getString( catalogMetaClass, "SearchCatalog.Search.Title" ) );
    searchButton.setLayoutData(
      new FormDataBuilder().left().top( resourcesButton, ELEMENT_SPACING ).width( RADIO_BUTTON_WIDTH )
        .result() );

    advancedSearchButton = new Button( searchByGroup, SWT.RADIO );
    advancedSearchButton.setText( BaseMessages.getString( catalogMetaClass, "SearchCatalog.AdvancedSearch.Title" ) );
    advancedSearchButton
      .setLayoutData(
        new FormDataBuilder().left().top( searchButton, ELEMENT_SPACING ).width( RADIO_BUTTON_WIDTH )
          .result() );

    //Make a composite to hold the dynamic right side of the group
    Composite dynamicComposite = new Composite( contentComposite, SWT.NONE );
    FormLayout fileSettingsDynamicAreaSchemaLayout = new FormLayout();
    dynamicComposite.setLayout( fileSettingsDynamicAreaSchemaLayout );
    dynamicComposite.setLayoutData(
      new FormDataBuilder().top( searchByGroup, ELEMENT_SPACING ).left().right().bottom()
        .result() );

    resourcesComposite = addResourcesComposite( dynamicComposite );
    searchComposite = addSearchComposite( dynamicComposite );
    advancedSearchComposite = addAdvancedSearchComposite( dynamicComposite );

    //Cancel, action and OK buttons for the bottom of the window.
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( catalogMetaClass, "System.Button.Cancel" ) );
    FormData fdCancel = new FormDataBuilder().right( 100, -MARGIN_SIZE )
      .bottom()
      .result();
    wCancel.setLayoutData( fdCancel );

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( catalogMetaClass, "System.Button.OK" ) );
    FormData fdOk = new FormDataBuilder().right( wCancel, -LABEL_SPACING )
      .bottom()
      .result();
    wOK.setLayoutData( fdOk );

    //Space between bottom buttons and the table, final layout for table
    Label bottomSpacer = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    FormData fdhSpacer = new FormDataBuilder().left()
      .right( 100, -MARGIN_SIZE )
      .bottom( wCancel, -MARGIN_SIZE )
      .result();
    bottomSpacer.setLayoutData( fdhSpacer );

    //Add everything to the scrolling composite
    scrolledComposite.setContent( contentComposite );
    scrolledComposite.setExpandVertical( true );
    scrolledComposite.setExpandHorizontal( true );
    scrolledComposite.setMinSize( contentComposite.computeSize( SWT.DEFAULT, SWT.DEFAULT ) );

    scrolledComposite.setLayout( new FormLayout() );
    FormData fdScrolledComposite = new FormDataBuilder().fullWidth()
      .top()
      .bottom( bottomSpacer, -MARGIN_SIZE )
      .result();
    scrolledComposite.setLayoutData( fdScrolledComposite );
    props.setLook( scrolledComposite );

    //Listeners
    lsCancel = e -> cancel();
    lsOK = e -> ok();

    wOK.addListener( SWT.Selection, lsOK );
    wCancel.addListener( SWT.Selection, lsCancel );

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
    resultComposite.setText( BaseMessages.getString( catalogMetaClass, "SearchCatalog.Resources.Title" ) );
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
    wbIDAdd.setText( BaseMessages.getString( catalogMetaClass, "SearchCatalog.Button.Add" ) );
    FormData fdbIDAdd = new FormDataBuilder().left( wID, ELEMENT_SPACING ).top( wlID, LABEL_SPACING ).result();
    wbIDAdd.setLayoutData( fdbIDAdd );
    wbIDAdd.addListener( SWT.Selection, event -> {
      DataResource dataResource = getDataResource( wID.getText(), connectionManagerSupplier, wConnection, transMeta );
      if ( dataResource == null ) {
        logError(
          BaseMessages.getString( catalogMetaClass, "SearchCatalog.Error.DataResourceRetrievalFailed" ),
          wID.getText(), wConnection.getText() );
        return;
      }
      wSelectedFiles.add( dataResource.getResourcePath(), dataResource.getKey(), dataResource.getType(),
        dataResource.getDataSourceName() );
      wID.setText( "" );
      wSelectedFiles.removeEmptyRows();
      wSelectedFiles.setRowNums();
      wSelectedFiles.optWidth( true );
      meta.setChanged();
    } );

    Label wlSelectedFiles = new Label( resultComposite, SWT.LEFT );
    wlSelectedFiles.setText( BaseMessages.getString( catalogMetaClass, "SearchCatalog.Resources.Label.SelectedFiles" ) );
    props.setLook( wlSelectedFiles );
    wlSelectedFiles.setLayoutData( new FormDataBuilder().left().top( wID, ELEMENT_SPACING ).left().result() );

    ColumnInfo[] selectedFilesColumns =
      new ColumnInfo[] {
        new ColumnInfo( BaseMessages.getString( catalogMetaClass, "SearchCatalog.Resources.column.ResourceName" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, true ),
        new ColumnInfo( BaseMessages.getString( catalogMetaClass, "SearchCatalog.Resources.column.ID" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
        new ColumnInfo( BaseMessages.getString( catalogMetaClass, "SearchCatalog.Resources.column.ResourceType" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, true ),
        new ColumnInfo( BaseMessages.getString( catalogMetaClass, "SearchCatalog.Resources.column.Origin" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, true ) };

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
    wbEdit.setText( BaseMessages.getString( catalogMetaClass, "SearchCatalog.Button.Edit" ) );
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
    wbDelete.setText( BaseMessages.getString( catalogMetaClass, "SearchCatalog.Button.Delete" ) );
    wbDelete.setLayoutData(
      new FormDataBuilder().right( wbEdit, -LABEL_SPACING ).bottom( wSelectedFiles, -LABEL_SPACING ).result() );
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
    resultComposite.setText( BaseMessages.getString( catalogMetaClass, "SearchCatalog.Search.Title" ) );
    FormLayout layout = new FormLayout();
    layout.marginHeight = MARGIN_SIZE;
    layout.marginWidth = MARGIN_SIZE;
    resultComposite.setLayout( layout );
    resultComposite
      .setLayoutData( new FormDataBuilder().top().left().right( 100, -MARGIN_SIZE ).bottom().result() );

    Label wlKeyword = new Label( resultComposite, SWT.LEFT );
    wlKeyword.setText( BaseMessages.getString( catalogMetaClass, "SearchCatalog.Criteria.Label.Keyword" ) );
    props.setLook( wlKeyword );
    wlKeyword.setLayoutData( new FormDataBuilder().left().top().width( LABEL_WIDTH ).result() );

    wKeyword = new TextVar( transMeta, resultComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wKeyword );
    wKeyword.setLayoutData( new FormDataBuilder().left( wlKeyword, 0 ).top().width( MEDIUM_FIELD ).result() );
    wKeyword.addModifyListener( lsMod );

    Label wlTags = new Label( resultComposite, SWT.LEFT );
    wlTags.setText( BaseMessages.getString( catalogMetaClass, "SearchCatalog.Criteria.Label.Tags" ) );
    props.setLook( wlTags );
    wlTags
      .setLayoutData( new FormDataBuilder().left().top( wKeyword, ELEMENT_SPACING ).width( LABEL_WIDTH ).result() );

    wTags = new TextVar( transMeta, resultComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTags );
    wTags.setLayoutData(
      new FormDataBuilder().left( wlTags, 0 ).top( wKeyword, ELEMENT_SPACING ).width( MEDIUM_FIELD ).result() );
    wTags.addModifyListener( lsMod );

    Label wlVirtualFolders = new Label( resultComposite, SWT.LEFT );
    wlVirtualFolders.setText( BaseMessages.getString( catalogMetaClass, "SearchCatalog.Criteria.Label.VirtualFolders" ) );
    props.setLook( wlVirtualFolders );
    wlVirtualFolders
      .setLayoutData( new FormDataBuilder().left().top( wTags, ELEMENT_SPACING ).width( LABEL_WIDTH ).result() );

    wVirtualFolders = new CCombo( resultComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wVirtualFolders );
    wVirtualFolders.setLayoutData(
      new FormDataBuilder().left( wlVirtualFolders, 0 ).top( wTags, ELEMENT_SPACING ).width( MEDIUM_FIELD ).result() );
    wVirtualFolders.addModifyListener( lsMod );

    Label wlDataSources = new Label( resultComposite, SWT.LEFT );
    wlDataSources.setText( BaseMessages.getString( catalogMetaClass, "SearchCatalog.Criteria.Label.DataSources" ) );
    props.setLook( wlDataSources );
    wlDataSources.setLayoutData(
      new FormDataBuilder().left().top( wVirtualFolders, ELEMENT_SPACING ).width( LABEL_WIDTH ).result() );

    wDataSources = new CCombo( resultComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDataSources );
    wDataSources.setLayoutData(
      new FormDataBuilder().left( wlDataSources, 0 ).top( wVirtualFolders, ELEMENT_SPACING ).width( MEDIUM_FIELD )
        .result() );
    wDataSources.addModifyListener( lsMod );

    Label wlResourceType = new Label( resultComposite, SWT.LEFT );
    wlResourceType.setText( BaseMessages.getString( catalogMetaClass, "SearchCatalog.Criteria.Label.ResourceType" ) );
    props.setLook( wlResourceType );
    wlResourceType.setLayoutData(
      new FormDataBuilder().left().top( wDataSources, ELEMENT_SPACING ).width( LABEL_WIDTH ).result() );

    wResourceType = new CCombo( resultComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wResourceType );
    wResourceType.setLayoutData(
      new FormDataBuilder().left( wlResourceType, 0 ).top( wDataSources, ELEMENT_SPACING ).width( MEDIUM_FIELD )
        .result() );
    wResourceType.addModifyListener( lsMod );

    Label wlFileSize = new Label( resultComposite, SWT.LEFT );
    wlFileSize.setText( BaseMessages.getString( catalogMetaClass, "SearchCatalog.Criteria.Label.FileSize" ) );
    props.setLook( wlFileSize );
    wlFileSize.setLayoutData(
      new FormDataBuilder().left().top( wResourceType, ELEMENT_SPACING ).width( LABEL_WIDTH ).result() );

    wFileSize = new CCombo( resultComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFileSize );
    wFileSize.setLayoutData(
      new FormDataBuilder().left( wlFileSize, 0 ).top( wResourceType, ELEMENT_SPACING ).width( MEDIUM_FIELD )
        .result() );
    wFileSize.addModifyListener( lsMod );

    Label wlFileFormat = new Label( resultComposite, SWT.LEFT );
    wlFileFormat.setText( BaseMessages.getString( catalogMetaClass, "SearchCatalog.Criteria.Label.FileFormat" ) );
    props.setLook( wlFileFormat );
    wlFileFormat
      .setLayoutData( new FormDataBuilder().left().top( wFileSize, ELEMENT_SPACING ).width( LABEL_WIDTH ).result() );

    wFileFormat = new CCombo( resultComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFileFormat );
    wFileFormat.setLayoutData(
      new FormDataBuilder().left( wlFileFormat, 0 ).top( wFileSize, ELEMENT_SPACING ).width( MEDIUM_FIELD ).result() );
    wFileFormat.addModifyListener( lsMod );

    return resultComposite;
  }

  private Composite addAdvancedSearchComposite( Composite parent ) {
    Group resultComposite = new Group( parent, SWT.SHADOW_NONE );
    resultComposite.setText( BaseMessages.getString( catalogMetaClass, "SearchCatalog.AdvancedSearch.Title" ) );
    FormLayout layout = new FormLayout();
    layout.marginHeight = MARGIN_SIZE;
    layout.marginWidth = MARGIN_SIZE;
    resultComposite.setLayout( layout );
    resultComposite
      .setLayoutData( new FormDataBuilder().top().left().right( 100, -MARGIN_SIZE ).bottom().result() );

    wAdvancedQuery = new TextVar( transMeta, resultComposite, SWT.MULTI | SWT.LEFT | SWT.BORDER );
    props.setLook( wAdvancedQuery );
    wAdvancedQuery.setLayoutData(
      new FormDataBuilder().left().top().right( 100, 0 ).bottom( 100, 0 ).result() );
    wFileFormat.addModifyListener( lsMod );
    return resultComposite;
  }

  private void getData( SearchCatalogMeta meta ) {
    wConnection.setText( Const.NVL( meta.getConnection(), "" ) );
    wSelectedFiles.removeAll();
    for ( ResourceField resourceField : meta.getResourceFields() ) {
      wSelectedFiles
        .add( resourceField.getName(), resourceField.getId(), resourceField.getType(), resourceField.getOrigin() );
    }
    wSelectedFiles.removeEmptyRows();
    wSelectedFiles.setRowNums();
    wSelectedFiles.optWidth( true );

    wKeyword.setText( Const.NVL( meta.getKeyword(), "" ) );
    wTags.setText( Const.NVL( meta.getTags(), "" ) );
    wVirtualFolders.setText( Const.NVL( meta.getVirtualFolders(), "" ) );
    wDataSources.setText( Const.NVL( meta.getDataSources(), "" ) );
    wResourceType.setText( Const.NVL( meta.getResourceType(), "" ) );
    wFileSize.setText( Const.NVL( meta.getFileSize(), "" ) );
    wFileFormat.setText( Const.NVL( meta.getFileFormat(), "" ) );
    wAdvancedQuery.setText( Const.NVL( meta.getAdvancedQuery(), "" ) );
  }

  private void getInfo( SearchCatalogMeta meta ) {
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
}

