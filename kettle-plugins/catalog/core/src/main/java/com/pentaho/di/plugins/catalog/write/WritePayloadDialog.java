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

package com.pentaho.di.plugins.catalog.write;

import com.pentaho.di.plugins.catalog.api.CatalogClient;
import com.pentaho.di.plugins.catalog.api.entities.metadata.TagResult;
import com.pentaho.di.plugins.catalog.api.entities.search.Facet;
import com.pentaho.di.plugins.catalog.api.entities.search.FacetsResult;
import com.pentaho.di.plugins.catalog.provider.CatalogDetails;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Point;
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
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.connections.ConnectionManager;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.textfileoutput.TextFileField;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.core.FormDataBuilder;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.core.widget.TextVarButton;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.trans.step.TableItemInsertListener;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.pentaho.di.i18n.BaseMessages.getString;

public class WritePayloadDialog extends BaseStepDialog implements StepDialogInterface {

  public static final String CATALOG = "Catalog";

  Supplier<ConnectionManager> connectionManagerSupplier = ConnectionManager::getInstance;

  public static final String DATA_RESOURCE = "Data Resource";
  public static final String DATA_SOURCE = "Data Source";

  // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
  private static final Class<?> writerPayloadMetaClass = WritePayloadMeta.class;

  private static final int MARGIN_SIZE = 15;
  private static final int LABEL_SPACING = 5;
  private static final int ELEMENT_SPACING = 10;

  private static final int LABEL_WIDTH = 150;
  private static final int RADIO_BUTTON_WIDTH = 150;

  private static final int MEDIUM_FIELD = 250;

  private WritePayloadMeta meta;

  private FacetsResult facetsResult;

  /************************************ SWT Widgets ******************************************************************/

  // Tab Widgets
  private CTabFolder wTabFolder;

  // File Tab Widgets
  private Button bSaveByLocation;
  private Button bOverwriteExistingResource;

  private CCombo wConnection;

  // File Tab Dynamic Widgets
  private Composite dcAddSaveByLocationComposite;
  private Composite dcOverwriteExistingResourceComposite;
  private Button bOverwriteTheFile;
  private Button bAppend;
  private Button bKeepBoth;

  // File Tab Widgets
  private CCombo wVirtualFolders;
  private TextVar wName;
  private CCombo wFileFormat;
  private TextVarButton wResourceId;

  // Metadata Tab Widgets
  private TextVar wDescription;
  private CCombo wTags;
  private TextVar wPropA;
  private TextVar wPropB;
  private TextVar wPropC;
  private TextVar wPropD;

  // Get Fields Widgets
  private TableView wFields;
  private ColumnInfo[] colinf;
  private Map<String, Integer> inputFields;

  // Listeners
  private ModifyListener lsMod;

  public WritePayloadDialog( Shell parent, Object in, TransMeta tr, String sname ) {
    super( parent, (BaseStepMeta) in, tr, sname );
    meta = (WritePayloadMeta) in;
    inputFields = new HashMap<>();
  }

  @SuppressWarnings( "java:S1604" ) // Named inner classes are the Pentaho Style
  @Override public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    shell.setMinimumSize( 450, 335 );
    props.setLook( shell );
    setShellImage( shell, meta );

    lsMod = e -> meta.setChanged();
    changed = meta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 15;
    formLayout.marginHeight = 15;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( writerPayloadMetaClass, "WritePayloadDialog.Shell.Title" ) );

    Label wicon = new Label( shell, SWT.RIGHT );
    wicon.setImage( getImage() );
    wicon.setLayoutData( new FormDataBuilder().top().right().result() );
    props.setLook( wicon );

    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( getString( writerPayloadMetaClass, "WritePayloadDialog.Stepname.Label" ) );
    props.setLook( wlStepname );
    wlStepname.setLayoutData( new FormDataBuilder().left().top().result() );

    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    wStepname.setLayoutData( new FormDataBuilder().width( 250 ).left().top( wlStepname, 5 ).result() );

    Label topLine = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    props.setLook( topLine );
    topLine.setLayoutData( new FormDataBuilder().height( 2 ).left().top( wStepname, 15 ).right().result() );

    Label wlConnection = new Label( shell, SWT.LEFT );
    wlConnection.setText( BaseMessages.getString( writerPayloadMetaClass, "WritePayloadDialog.Connection.Label" ) );
    props.setLook( wlConnection );
    FormData fdlConnection = new FormDataBuilder().left().top( topLine, ELEMENT_SPACING ).result();
    wlConnection.setLayoutData( fdlConnection );

    wConnection = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wConnection );
    FormData fdConnection =
      new FormDataBuilder().left().top( wlConnection, LABEL_SPACING ).width( MEDIUM_FIELD ).result();
    wConnection.setLayoutData( fdConnection );
    wConnection.addModifyListener( modifyEvent -> {
      // If the connection has changed clear the fields
      if ( !wConnection.getText().equals( meta.getConnection() ) ) {
        wVirtualFolders.setText( "" );
        wVirtualFolders.removeAll();
        meta.setVirtualFolders( null );
        wName.setText( "" );
        meta.setResourceName( null );
        wFileFormat.setText( "" );
        wFileFormat.removeAll();
        meta.setFileFormat( null );
        wResourceId.setText( "" );
        meta.setResourceId( null );
        wDescription.setText( "" );
        meta.setDescription( null );
        wTags.setText( "" );
        wTags.removeAll();
        meta.setSelectedTag( null );
        wPropA.setText( "" );
        meta.setPropA( null );
        wPropB.setText( "" );
        meta.setPropB( null );
        wPropC.setText( "" );
        meta.setPropC( null );
        wPropD.setText( "" );
        meta.setPropD( null );

        // Set tab back to first tab
        wTabFolder.setSelection( 0 );

        // Initialize the Radio Buttons
        bSaveByLocation.setSelection( true );
        meta.setOperationTypeIndex( 0 );
        bOverwriteTheFile.setSelection( true );
        meta.setFileAlreadyExistsIndex( 0 );

        // Tell the metadata to save
        meta.setChanged();

        updateDynamicComposite();
      }

      // Fill out the drop down when the connection is not empty
      if ( !Utils.isEmpty( wConnection.getText() ) ) {
        facetsResult = getFacets();
        // Connection Succeeded
        if ( facetsResult != null ) {
          wFileFormat.setItems( facetsResult.getFacetSelection( Facet.FILE_FORMAT ).toArray( new String[ 0 ] ) );
          wTags.setItems( fetchTags() );
          wVirtualFolders
            .setItems( facetsResult.getFacetSelection( Facet.VIRTUAL_FOLDERS ).toArray( new String[ 0 ] ) );
        } else {
          logError( getString( writerPayloadMetaClass, "WritePayloadDialog.Connection.FAILED.MSG" ) + " " + wConnection
            .getText() );
        }
      }
    } );
    wConnection.addModifyListener( lsMod );

    List<String> connections = ConnectionManager.getInstance().getNamesByKey( CatalogDetails.CATALOG );
    wConnection.setItems( connections.toArray( new String[ 0 ] ) );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( getString( writerPayloadMetaClass, "System.Button.Cancel" ) );
    FormData fdCancel = new FormDataBuilder().right( 70, -MARGIN_SIZE )
      .bottom()
      .width( RADIO_BUTTON_WIDTH )
      .result();
    wCancel.setLayoutData( fdCancel );
    wCancel.setAlignment( BUTTON_ALIGNMENT_CENTER );

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( getString( writerPayloadMetaClass, "System.Button.OK" ) );
    FormData fdOk = new FormDataBuilder().right( wCancel, -LABEL_SPACING )
      .bottom()
      .width( RADIO_BUTTON_WIDTH )
      .result();
    wOK.setLayoutData( fdOk );
    wOK.setAlignment( BUTTON_ALIGNMENT_CENTER );

    Label bottomLine = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    props.setLook( bottomLine );
    bottomLine.setLayoutData( new FormDataBuilder().height( 2 ).left().bottom( wCancel, -15 ).right().result() );

    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );
    wTabFolder.setSimple( false );

    // Define the Tabs
    addFileTab();
    addMetaDataTab();
    addFieldsTab();

    wTabFolder
      .setLayoutData( new FormDataBuilder().left().top( wConnection, 15 ).bottom( bottomLine, -15 ).right().result() );

    wOK.addListener( SWT.Selection, e -> ok() );
    wCancel.addListener( SWT.Selection, e -> cancel() );

    lsDef = new SelectionAdapter() {
      @Override public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };


    wStepname.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      @Override public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    lsResize = new Listener() {
      public void handleEvent( Event event ) {
        Point size = shell.getSize();
        wFields.setSize( size.x - 10, size.y - 50 );
        wFields.table.setSize( size.x - 10, size.y - 50 );
        wFields.redraw();
      }
    };
    shell.addListener( SWT.Resize, lsResize );

    //Setup the radio button event handler
    SelectionAdapter selectionAdapter = new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        updateDynamicComposite();
      }
    };
    bSaveByLocation.addSelectionListener( selectionAdapter );
    bOverwriteExistingResource.addSelectionListener( selectionAdapter );

    getData( meta );
    updateDynamicComposite();
    meta.setChanged( changed );

    wTabFolder.setUnselectedCloseVisible( true );
    wTabFolder.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent selectionEvent ) {
        super.widgetSelected( selectionEvent );
        meta.setChanged();
      }
    } );
    // Set the shell size, based upon previous time...
    setSize();

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }

  private Composite getTabComposite() {
    Composite wcScript = new Composite( wTabFolder, SWT.NONE );
    FormLayout scriptLayout = new FormLayout();
    scriptLayout.marginWidth = 15;
    scriptLayout.marginHeight = 15;
    wcScript.setLayout( scriptLayout );
    return wcScript;
  }

  /**
   * Purpose Create the File Tab
   */
  private void addFileTab() {
    CTabItem wFileTab = new CTabItem( wTabFolder, SWT.NONE );
    wFileTab.setText( BaseMessages.getString( writerPayloadMetaClass, "WritePayloadDialog.FileTab.Title" ) );
    // The composite that contains both the "Operation Type" and the dynamic Fields.
    Composite wcFileTab = getTabComposite();

    // The composite that contains both the "Operation Type"
    Group cOperationType = new Group( wcFileTab, SWT.SHADOW_NONE );
    cOperationType
      .setText( BaseMessages.getString( writerPayloadMetaClass, "WritePayloadDialog.OperationType.Title" ) );
    FormLayout layout = new FormLayout();
    layout.marginHeight = MARGIN_SIZE;
    layout.marginWidth = MARGIN_SIZE;
    cOperationType.setLayout( layout );
    // DO NOT INCLUDE BOTTOM MESSES UP DYNAMIC COMPOSITE CAUSING IT NOT TO DISPLAY
    cOperationType.setLayoutData( new FormDataBuilder().top().left().right( 100, -MARGIN_SIZE ).result() );

    // Build "Save by location" radio button
    bSaveByLocation = new Button( cOperationType, SWT.RADIO );
    bSaveByLocation.setText(
      BaseMessages.getString( writerPayloadMetaClass, "WritePayloadDialog.OperationType.SaveByLocation.Button" ) );
    bSaveByLocation.setLayoutData( new FormDataBuilder().left().top().width( RADIO_BUTTON_WIDTH ).result() );

    // Build "Overwrite existing resource" radio button
    bOverwriteExistingResource = new Button( cOperationType, SWT.RADIO );
    bOverwriteExistingResource.setText( BaseMessages
      .getString( writerPayloadMetaClass, "WritePayloadDialog.OperationType.OverwriteExistingResource.Button" ) );
    // Label is too long increase the width.
    bOverwriteExistingResource.setLayoutData(
      new FormDataBuilder().left().top( bSaveByLocation, ELEMENT_SPACING ).width( MEDIUM_FIELD ).result() );

    //Make a composite to hold the dynamic right side of the group
    Composite cDynamicComposite = new Composite( wcFileTab, SWT.NONE );
    FormLayout fileSettingsDynamicAreaSchemaLayout = new FormLayout();
    cDynamicComposite.setLayout( fileSettingsDynamicAreaSchemaLayout );

    FormData formData = new FormDataBuilder().top( cOperationType, ELEMENT_SPACING ).left().right().bottom().result();
    cDynamicComposite.setLayoutData( formData );

    dcAddSaveByLocationComposite = addSaveByLocationComposite( cDynamicComposite );
    dcOverwriteExistingResourceComposite = addOverrideExistingResourseComposite( cDynamicComposite );

    updateDynamicComposite();

    props.setLook( wcFileTab );
    wcFileTab.layout();
    wFileTab.setControl( wcFileTab );
  }

  private Composite addOverrideExistingResourseComposite( Composite parent ) {
    Group cOverrideExistingResource = new Group( parent, SWT.SHADOW_NONE );
    cOverrideExistingResource.setText( BaseMessages
      .getString( writerPayloadMetaClass, "WritePayloadDialog.OperationType.OverwriteExistingResource.Button" ) );
    FormLayout layout = new FormLayout();
    layout.marginHeight = MARGIN_SIZE;
    layout.marginWidth = MARGIN_SIZE;
    cOverrideExistingResource.setLayout( layout );
    cOverrideExistingResource
      .setLayoutData( new FormDataBuilder().top().left().right( 100, -MARGIN_SIZE ).bottom().result() );

    // Build "Resource Id" Label
    Label lResourceId = new Label( cOverrideExistingResource, SWT.SHADOW_NONE );
    lResourceId.setText( BaseMessages
      .getString( writerPayloadMetaClass,
        "WritePayloadDialog.OpertaionType.OverwriteExistingResource.ResourceId.Label" ) );
    lResourceId.setLayoutData( new FormDataBuilder().left().top().result() );

    // Build "Resource Id" Text Box
    wResourceId = new TextVarButton( transMeta, cOverrideExistingResource, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wResourceId.setLayoutData(
      new FormDataBuilder().left( lResourceId, ELEMENT_SPACING ).width( MEDIUM_FIELD ).result() );

    // TODO Add Button Listener to get Resource Ids

    return cOverrideExistingResource;
  }

  /**
   * Purpose to create "Save by location" panel so it can be dynamically loaded.
   *
   * @param parent
   * @return SWT group representing the "Save by location"
   */
  @SuppressWarnings( "java:S125" )
  private Composite addSaveByLocationComposite( Composite parent ) {
    Group cSaveBy = new Group( parent, SWT.SHADOW_NONE );
    cSaveBy.setText( BaseMessages.getString( writerPayloadMetaClass, "WritePayloadDialog.SaveByLocation.Title" ) );
    FormLayout layout = new FormLayout();
    layout.marginHeight = MARGIN_SIZE;
    layout.marginWidth = MARGIN_SIZE;
    cSaveBy.setLayout( layout );
    cSaveBy.setLayoutData( new FormDataBuilder().top().left().right( 100, -MARGIN_SIZE ).bottom().result() );


    Label lVirtualFolder = new Label( cSaveBy, SWT.LEFT );
    lVirtualFolder.setText(
      BaseMessages.getString( writerPayloadMetaClass, "WritePayloadDialog.SaveByLocation.VirtualFolder.Label" ) );
    props.setLook( lVirtualFolder );
    lVirtualFolder.setLayoutData( new FormDataBuilder().left().top().width( LABEL_WIDTH ).result() );

    //wVirtualFolders = new TextVar( transMeta, cSaveBy, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wVirtualFolders = new CCombo( cSaveBy, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wVirtualFolders );
    wVirtualFolders
      .setLayoutData( new FormDataBuilder().left( lVirtualFolder, 0 ).top().width( MEDIUM_FIELD ).result() );
    wVirtualFolders.addModifyListener( lsMod );

    /*
    // TODO: Implement a browse button (Not required for Tapestry MVP)
    bVirtualFolder = new Button( cSaveBy, SWT.PUSH );
    bVirtualFolder.setText( BaseMessages.getString( writerPayloadMetaClass,   "WritePayloadDialog.SaveByLocation
    .VirtualFolder.Button" ) );
    bVirtualFolder.setLayoutData(
      new FormDataBuilder().left( wVirtualFolder, ELEMENT_SPACING).top().width( RADIO_BUTTON_WIDTH ).result() );
    */

    // NAME VARIABLE
    Label lName = new Label( cSaveBy, SWT.LEFT );
    lName.setText( BaseMessages.getString( writerPayloadMetaClass, "WritePayloadDialog.SaveByLocation.Name.Label" ) );
    props.setLook( lName );
    lName.setLayoutData(
      new FormDataBuilder().left().top( lVirtualFolder, ELEMENT_SPACING ).width( LABEL_WIDTH ).result() );
    wName = new TextVar( transMeta, cSaveBy, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    wName.setLayoutData(
      new FormDataBuilder().left( lName, 0 ).top( wVirtualFolders ).width( MEDIUM_FIELD ).result() );
    wName.addModifyListener( lsMod );

    // FILE FORMAT VARIABLE
    Label lFileFormat = new Label( cSaveBy, SWT.LEFT );
    lFileFormat.setText(
      BaseMessages.getString( writerPayloadMetaClass, "WritePayloadDialog.SaveByLocation.FileFormat.Label" ) );
    props.setLook( lFileFormat );
    lFileFormat
      .setLayoutData( new FormDataBuilder().left().top( lName, ELEMENT_SPACING * 3 ).width( LABEL_WIDTH ).result() );

    wFileFormat = new CCombo( cSaveBy, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFileFormat );
    wFileFormat.setLayoutData(
      new FormDataBuilder().left( lFileFormat, 0 ).top( wName, ELEMENT_SPACING ).width( MEDIUM_FIELD ).result() );
    wFileFormat.addModifyListener( lsMod );

    // Label
    Label lFileAlreadyExists = new Label( cSaveBy, SWT.LEFT );
    lFileAlreadyExists.setText(
      BaseMessages.getString( writerPayloadMetaClass, "WritePayloadDialog.SaveByLocation.FileAlreadyExists.Label" ) );
    lFileAlreadyExists.setLayoutData(
      new FormDataBuilder().left().top( lFileFormat, ELEMENT_SPACING * 4 ).width( MEDIUM_FIELD + 10 ).result() );

    // Build "Overwrite the file" radio button
    bOverwriteTheFile = new Button( cSaveBy, SWT.RADIO );
    bOverwriteTheFile.setText(
      BaseMessages.getString( writerPayloadMetaClass, "WritePayloadDialog.SaveByLocation.OverWriteTheFile.Button" ) );
    bOverwriteTheFile.setLayoutData(
      new FormDataBuilder().left().top( lFileAlreadyExists, ELEMENT_SPACING ).width( RADIO_BUTTON_WIDTH ).result() );

    // Build "Append" radio button
    bAppend = new Button( cSaveBy, SWT.RADIO );
    bAppend.setText( BaseMessages
      .getString( writerPayloadMetaClass, "WritePayloadDialog.SaveByLocation.FileAlreadyExistsType.Append.Button" ) );
    bAppend.setLayoutData(
      new FormDataBuilder().left().top( bOverwriteTheFile, ELEMENT_SPACING ).width( RADIO_BUTTON_WIDTH ).result() );

    // Build "Keep Both" radio button
    bKeepBoth = new Button( cSaveBy, SWT.RADIO );
    bKeepBoth.setText( BaseMessages
      .getString( writerPayloadMetaClass, "WritePayloadDialog.SaveByLocation.FileAlreadyExistsType.KeepBoth.Button" ) );
    bKeepBoth.setLayoutData(
      new FormDataBuilder().left().top( bAppend, ELEMENT_SPACING ).width( RADIO_BUTTON_WIDTH ).result() );

    return cSaveBy;
  }

  private void updateDynamicComposite() {
    recursiveSetEnabled( dcAddSaveByLocationComposite, bSaveByLocation.getSelection() );
    dcAddSaveByLocationComposite.setVisible( bSaveByLocation.getSelection() );

    recursiveSetEnabled( dcOverwriteExistingResourceComposite, bOverwriteExistingResource.getSelection() );
    dcOverwriteExistingResourceComposite.setVisible( bOverwriteExistingResource.getSelection() );
  }

  public void recursiveSetEnabled( Control ctrl, boolean enabled ) {
    if ( ctrl instanceof Composite ) {
      Composite comp = (Composite) ctrl;
      for ( Control c : comp.getChildren() ) {
        recursiveSetEnabled( c, enabled );
      }
    } else {
      ctrl.setEnabled( enabled );
    }
  }

  private void addFieldsTab() {
    CTabItem wFieldsTab = new CTabItem( wTabFolder, SWT.NONE );
    wFieldsTab.setText( BaseMessages.getString( writerPayloadMetaClass, "WritePayloadDialog.FieldsTab.Title" ) );
    // The composite that contains both the "Operation Type" and the dynamic Fields.
    Composite wcFieldsTab = getTabComposite();


    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = Const.FORM_MARGIN;
    fieldsLayout.marginHeight = Const.FORM_MARGIN;

    Composite wFieldsComp = new Composite( wcFieldsTab, SWT.NONE );
    wFieldsComp.setLayout( fieldsLayout );
    props.setLook( wFieldsComp );

    // wGet comes from BaseStepDialog
    wGet = new Button( wFieldsComp, SWT.PUSH );
    wGet.setText( BaseMessages.getString( writerPayloadMetaClass, "System.Button.GetFields" ) );
    wGet.setToolTipText( BaseMessages.getString( writerPayloadMetaClass, "System.Tooltip.GetFields" ) );
    wGet.addListener( SWT.Selection, e -> get() );

    Button bMinWidth = new Button( wFieldsComp, SWT.PUSH );
    bMinWidth
      .setText( BaseMessages.getString( writerPayloadMetaClass, "WritePayloadDialog.FieldsTab.MinWidth.Button" ) );
    bMinWidth.setToolTipText(
      BaseMessages.getString( writerPayloadMetaClass, "WritePayloadDialog.FieldsTab.MinWidth.Tooltip" ) );
    setButtonPositions( new Button[] { wGet, bMinWidth }, Const.MARGIN, null );
    bMinWidth.addListener( SWT.Selection, e -> setMinimalWidth() );

    final int FieldsCols = 10;
    final int FieldsRows = meta.getOutputFields().length;

    String[] dats = Const.getDateFormats();
    // Prepare a list of possible formats...
    String[] nums = Const.getNumberFormats();
    int totsize = dats.length + nums.length;
    String[] formats = Arrays.copyOf( dats, totsize );
    for ( int x = 0; x < nums.length; x++ ) {
      formats[ dats.length + x ] = nums[ x ];
    }

    colinf = new ColumnInfo[ FieldsCols ];
    colinf[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( writerPayloadMetaClass, "WritePayloadDialog.FieldsTab.NameColumn.Column" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
    colinf[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( writerPayloadMetaClass, "WritePayloadDialog.FieldsTab.Fields.TypeColumn.Column" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaFactory.getValueMetaNames() );
    colinf[ 2 ] =
      new ColumnInfo(
        BaseMessages.getString( writerPayloadMetaClass, "WritePayloadDialog.FieldsTab.Fields.FormatColumn.Column" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, formats );
    colinf[ 3 ] =
      new ColumnInfo(
        BaseMessages.getString( writerPayloadMetaClass, "WritePayloadDialog.FieldsTab.Fields.LengthColumn.Column" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinf[ 4 ] =
      new ColumnInfo(
        BaseMessages.getString( writerPayloadMetaClass, "WritePayloadDialog.FieldsTab.Fields.PrecisionColumn.Column" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinf[ 5 ] =
      new ColumnInfo(
        BaseMessages.getString( writerPayloadMetaClass, "WritePayloadDialog.FieldsTab.Fields.CurrencyColumn.Column" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinf[ 6 ] =
      new ColumnInfo(
        BaseMessages.getString( writerPayloadMetaClass, "WritePayloadDialog.FieldsTab.Fields.DecimalColumn.Column" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinf[ 7 ] =
      new ColumnInfo(
        BaseMessages.getString( writerPayloadMetaClass, "WritePayloadDialog.FieldsTab.Fields.GroupColumn.Column" ),
        ColumnInfo.COLUMN_TYPE_TEXT,
        false );
    colinf[ 8 ] =
      new ColumnInfo(
        BaseMessages.getString( writerPayloadMetaClass, "WritePayloadDialog.FieldsTab.Fields.TrimTypeColumn.Column" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaBase.trimTypeDesc, true );
    colinf[ 9 ] =
      new ColumnInfo(
        BaseMessages.getString( writerPayloadMetaClass, "WritePayloadDialog.FieldsTab.Fields.NullColumn.Column" ),
        ColumnInfo.COLUMN_TYPE_TEXT,
        false );

    wFields =
      new TableView(
        transMeta, wFieldsComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    // Note: This is the exact same broken button behavior exhibited by TextFileOutput Step
    // The broke behavior is the column line extend behind and past the buttons on resizing the
    // dialog. Do not fix it is expected behavior.
    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( 0, 0 );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( wGet, -MARGIN_SIZE );
    wFields.setLayoutData( fdFields );

    //
    // Search the fields in the background
    @SuppressWarnings( "java:S1604" ) // Namable Inner classes are the Pentaho Style
    final Runnable runnable = new Runnable() {
      public void run() {
        StepMeta stepMeta = transMeta.findStep( stepname );
        if ( stepMeta != null ) {
          try {
            RowMetaInterface row = transMeta.getPrevStepFields( stepMeta );

            // Remember these fields...
            for ( int i = 0; i < row.size(); i++ ) {
              inputFields.put( row.getValueMeta( i ).getName(), Integer.valueOf( i ) );
            }
            setComboBoxes();
          } catch ( KettleException e ) {
            logError( BaseMessages.getString( writerPayloadMetaClass, "System.Dialog.GetFieldsFailed.Message" ) );
          }
        }
      }
    };
    new Thread( runnable ).start();

    FormData fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment( 0, 0 );
    fdFieldsComp.top = new FormAttachment( 0, 0 );
    fdFieldsComp.right = new FormAttachment( 100, 0 );
    fdFieldsComp.bottom = new FormAttachment( 100, 0 );
    wFieldsComp.setLayoutData( fdFieldsComp );
    wFieldsComp.layout();
    wFieldsComp.pack();

    props.setLook( wcFieldsTab );
    wcFieldsTab.layout();
    wFieldsTab.setControl( wcFieldsTab );
  }

  private void addMetaDataTab() {
    CTabItem wMetadataTab = new CTabItem( wTabFolder, SWT.NONE );
    wMetadataTab.setText( BaseMessages.getString( writerPayloadMetaClass, "WritePayloadDialog.MetadataTab.Title" ) );
    Composite wcMetadataTab = getTabComposite();
    props.setLook( wcMetadataTab );


    // The composite that contains both the "Metadata"
    Group cMetadata = new Group( wcMetadataTab, SWT.SHADOW_NONE );
    cMetadata
      .setText( BaseMessages.getString( writerPayloadMetaClass, "WritePayloadDialog.MetadataTab.Metadata.Title" ) );
    FormLayout layout = new FormLayout();
    layout.marginHeight = MARGIN_SIZE;
    layout.marginWidth = MARGIN_SIZE;
    cMetadata.setLayout( layout );
    props.setLook( cMetadata );
    cMetadata.setLayoutData(
      new FormDataBuilder().top().left().right( 100, -MARGIN_SIZE ).result() );

    // Build "Description" text
    Label lDescription = new Label( cMetadata, SWT.LEFT );
    lDescription.setText(
      BaseMessages.getString( writerPayloadMetaClass, "WritePayloadDialog.MetadataTab.Metadata.Description.Label" ) );
    props.setLook( lDescription );
    lDescription.setLayoutData( new FormDataBuilder().left().top().width( LABEL_WIDTH ).result() );

    // Description Field
    wDescription =
      new TextVar( transMeta, cMetadata, SWT.MULTI | SWT.WRAP | SWT.LEFT | SWT.BORDER | SWT.SHADOW_ETCHED_IN );
    wDescription.setEditable( true );
    props.setLook( wDescription );
    // see for detials on setting https://mkyong.com/swt/swt-positioning-setbounds-or-setlocation/
    final int fieldSize = MEDIUM_FIELD + 15;
    // Fix the width and heitht to 4 1/2 lines and 44 characters
    wDescription.setLayoutData(
      new FormDataBuilder().left( lDescription, 0 ).top().width( fieldSize ).height( ELEMENT_SPACING * 8 ).result() );
    wDescription.addModifyListener( lsMod );

    // Build "Tags" text
    Label lTags = new Label( cMetadata, SWT.LEFT );
    lTags.setText(
      BaseMessages.getString( writerPayloadMetaClass, "WritePayloadDialog.MetadataTab.Metadata.Tags.Label" ) );
    props.setLook( lTags );
    lTags
      .setLayoutData(
        new FormDataBuilder().top( lDescription, ( ELEMENT_SPACING * 7 ) + 5 ).left().width( LABEL_WIDTH ).result() );

    wTags = new CCombo( cMetadata, SWT.LEFT | SWT.MULTI | SWT.BORDER | SWT.SHADOW_ETCHED_IN );
    props.setLook( wTags );
    wTags.setLayoutData(
      new FormDataBuilder().left( lTags, 0 ).top( wDescription, ELEMENT_SPACING ).width( MEDIUM_FIELD ).result() );
    wTags.addModifyListener( lsMod );

    //Make a composite to hold the "Properites" data right side of the group
    Group cProperties = new Group( wcMetadataTab, SWT.SHADOW_NONE );
    cProperties
      .setText( BaseMessages.getString( writerPayloadMetaClass, "WritePayloadDialog.MetadataTab.Properties.Title" ) );
    layout = new FormLayout();
    cProperties.setLayout( layout );
    props.setLook( cProperties );
    cProperties.setLayoutData(
      new FormDataBuilder().top( cMetadata, ELEMENT_SPACING ).left().right( 100, -MARGIN_SIZE ).result() );

    // Build "Property A" text
    final int leftOffset = 6;
    final int topOffset = 10;
    Label lPropA = new Label( cProperties, SWT.LEFT );
    lPropA.setText(
      BaseMessages.getString( writerPayloadMetaClass, "WritePayloadDialog.MetadataTab.Properties.PropA.Label" ) );
    props.setLook( lPropA );
    lPropA.setLayoutData(
      new FormDataBuilder().left( cProperties, ELEMENT_SPACING + leftOffset ).top( cProperties, ELEMENT_SPACING )
        .width( LABEL_WIDTH ).result() );

    wPropA = new TextVar( variables, cProperties, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPropA );
    wPropA.setLayoutData(
      new FormDataBuilder().top( cProperties, ELEMENT_SPACING ).left( lPropA, 0 ).width( MEDIUM_FIELD ).result() );
    wPropA.addModifyListener( lsMod );

    // Build "Property B" text
    Label lPropB = new Label( cProperties, SWT.NONE );
    lPropB.setText(
      BaseMessages.getString( writerPayloadMetaClass, "WritePayloadDialog.MetadataTab.Properties.PropB.Label" ) );
    props.setLook( lPropB );
    lPropB.setLayoutData(
      new FormDataBuilder().left( cProperties, ELEMENT_SPACING + leftOffset ).top( lPropA, ELEMENT_SPACING + topOffset )
        .width( LABEL_WIDTH ).result() );

    wPropB = new TextVar( variables, cProperties, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPropB );
    wPropB.setLayoutData(
      new FormDataBuilder().top( wPropA, ELEMENT_SPACING ).left( lPropB, 0 ).width( MEDIUM_FIELD ).result() );
    wPropB.addModifyListener( lsMod );

    // Build "Property C" text
    Label lPropC = new Label( cProperties, SWT.NONE );
    lPropC.setText(
      BaseMessages.getString( writerPayloadMetaClass, "WritePayloadDialog.MetadataTab.Properties.PropC.Label" ) );
    props.setLook( lPropC );
    lPropC.setLayoutData(
      new FormDataBuilder().left( cProperties, ELEMENT_SPACING + leftOffset ).top( lPropB, ELEMENT_SPACING + topOffset )
        .width( LABEL_WIDTH ).result() );

    wPropC = new TextVar( variables, cProperties, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPropC );
    wPropC.setLayoutData(
      new FormDataBuilder().top( wPropB, ELEMENT_SPACING ).left( lPropC, 0 ).width( MEDIUM_FIELD ).result() );
    wPropC.addModifyListener( lsMod );

    // Build "Property D" text
    Label lPropD = new Label( cProperties, SWT.NONE );
    lPropD.setText(
      BaseMessages.getString( writerPayloadMetaClass, "WritePayloadDialog.MetadataTab.Properties.PropD.Label" ) );
    props.setLook( lPropD );
    lPropD.setLayoutData(
      new FormDataBuilder().left( cProperties, ELEMENT_SPACING + leftOffset ).top( lPropC, ELEMENT_SPACING + topOffset )
        .width( LABEL_WIDTH ).result() );

    wPropD = new TextVar( variables, cProperties, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPropD );
    wPropD.setLayoutData(
      new FormDataBuilder().top( wPropC, ELEMENT_SPACING ).left( lPropD, 0 ).width( MEDIUM_FIELD ).result() );
    wPropD.addModifyListener( lsMod );

    props.setLook( wcMetadataTab );
    wcMetadataTab.layout();
    wMetadataTab.setControl( wcMetadataTab );
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

  private void cancel() {
    dispose();
  }

  private void ok() {
    stepname = wStepname.getText();

    getInfo( meta );

    dispose();
  }

  @SuppressWarnings( "java:S3776" )
  private void getData( WritePayloadMeta meta ) {
    // Radio Buttons
    if ( meta.getOperationTypeIndex() == 0 ) {
      bSaveByLocation.setSelection( true );
    } else {
      bOverwriteExistingResource.setSelection( true );
    }

    switch ( meta.getFileAlreadyExistsIndex() ) {
      case 0:
        bOverwriteTheFile.setSelection( true );
        break;
      case 1:
        bAppend.setSelection( true );
        break;
      case 2:
        bKeepBoth.setSelection( true );
        break;
      default:
        bOverwriteTheFile.setSelection( true );
        break;
    }

    // Tab and Connection
    wTabFolder.setSelection( meta.getTabSelectionIndex() );
    wConnection.setText( Const.NVL( meta.getConnection(), "" ) );

    // Dynamic Fields for "File" tab
    wVirtualFolders.setText( Const.NVL( meta.getVirtualFolders(), "" ) );
    wName.setText( Const.NVL( meta.getResourceName(), "" ) );
    wFileFormat.setText( Const.NVL( meta.getFileFormat(), "" ) );
    wResourceId.setText( Const.NVL( meta.getResourceId(), "" ) );

    // Fields for the "Metadata" tab
    wDescription.setText( Const.NVL( meta.getDescription(), "" ) );
    wTags.setText( Const.NVL( meta.getSelectedTag(), "" ) );
    wPropA.setText( Const.NVL( meta.getPropA(), "" ) );
    wPropB.setText( Const.NVL( meta.getPropB(), "" ) );
    wPropC.setText( Const.NVL( meta.getPropC(), "" ) );
    wPropD.setText( Const.NVL( meta.getPropD(), "" ) );

    // Fields for get Fields
    logDebug( "getting fields info..." );

    for ( int i = 0; i < meta.getOutputFields().length; i++ ) {
      TextFileField field = meta.getOutputFields()[ i ];

      TableItem item = wFields.table.getItem( i );
      if ( field.getName() != null ) {
        item.setText( 1, field.getName() );
      }
      item.setText( 2, field.getTypeDesc() );
      if ( field.getFormat() != null ) {
        item.setText( 3, field.getFormat() );
      }
      if ( field.getLength() >= 0 ) {
        item.setText( 4, "" + field.getLength() );
      }
      if ( field.getPrecision() >= 0 ) {
        item.setText( 5, "" + field.getPrecision() );
      }
      if ( field.getCurrencySymbol() != null ) {
        item.setText( 6, field.getCurrencySymbol() );
      }
      if ( field.getDecimalSymbol() != null ) {
        item.setText( 7, field.getDecimalSymbol() );
      }
      if ( field.getGroupingSymbol() != null ) {
        item.setText( 8, field.getGroupingSymbol() );
      }
      String trim = field.getTrimTypeDesc();
      if ( trim != null ) {
        item.setText( 9, trim );
      }
      if ( field.getNullString() != null ) {
        item.setText( 10, field.getNullString() );
      }
    }

    wFields.optWidth( true );

    wStepname.selectAll();
    wStepname.setFocus();

  }

  @SuppressWarnings( "java:S3776" )
  private void getInfo( WritePayloadMeta meta ) {

    // Radio Buttons For "File" tab
    if ( bSaveByLocation.getSelection() ) {
      if ( meta.getOperationTypeIndex() != 0 ) {
        meta.setOperationTypeIndex( 0 );
        meta.setChanged();
      }
    } else {
      if ( meta.getOperationTypeIndex() != 1 ) {
        meta.setOperationTypeIndex( 1 );
        meta.setChanged();
      }
    }

    if ( bOverwriteTheFile.getSelection() ) {
      if ( meta.getFileAlreadyExistsIndex() != 0 ) {
        meta.setFileAlreadyExistsIndex( 0 );
        meta.setChanged();
      }
    } else if ( ( bAppend.getSelection() ) && ( meta.getFileAlreadyExistsIndex() != 1 ) ) {
      meta.setFileAlreadyExistsIndex( 1 );
      meta.setChanged();
    } else if ( ( bKeepBoth.getSelection() ) && ( meta.getFileAlreadyExistsIndex() != 2 ) ) {
      meta.setFileAlreadyExistsIndex( 2 );
      meta.setChanged();
    }

    // Dynamic Fields For "File" Tab
    meta.setTabSelectionIndex( wTabFolder.getSelectionIndex() );
    meta.setConnection( wConnection.getText() );
    meta.setVirtualFolders( wVirtualFolders.getText() );
    meta.setResourceName( wName.getText() );
    meta.setFileFormat( wFileFormat.getText() );
    meta.setResourceId( wResourceId.getText() );

    // Fields for the "Metadata" Tab
    meta.setDescription( wDescription.getText() );
    meta.setSelectedTag( wTags.getText() );
    meta.setPropA( wPropA.getText() );
    meta.setPropB( wPropB.getText() );
    meta.setPropC( wPropC.getText() );
    meta.setPropD( wPropD.getText() );


    int nrfields = wFields.nrNonEmpty();

    meta.allocate( nrfields );

    for ( int i = 0; i < nrfields; i++ ) {
      TextFileField field = new TextFileField();

      TableItem item = wFields.getNonEmpty( i );
      field.setName( item.getText( 1 ) );
      field.setType( item.getText( 2 ) );
      field.setFormat( item.getText( 3 ) );
      field.setLength( Const.toInt( item.getText( 4 ), -1 ) );
      field.setPrecision( Const.toInt( item.getText( 5 ), -1 ) );
      field.setCurrencySymbol( item.getText( 6 ) );
      field.setDecimalSymbol( item.getText( 7 ) );
      field.setGroupingSymbol( item.getText( 8 ) );
      field.setTrimType( ValueMetaBase.getTrimTypeByDesc( item.getText( 9 ) ) );
      field.setNullString( item.getText( 10 ) );
      //CHECKSTYLE:Indentation:OFF
      meta.getOutputFields()[ i ] = field;
    }
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    final Map<String, Integer> fields = new HashMap<>();

    // Add the currentMeta fields...
    fields.putAll( inputFields );

    Set<String> keySet = fields.keySet();
    List<String> entries = new ArrayList<>( keySet );

    String[] fieldNames = entries.toArray( new String[ entries.size() ] );

    Const.sortStrings( fieldNames );
    colinf[ 0 ].setComboValues( fieldNames );
  }

  private String[] fetchTags() {
    CatalogDetails catalogDetails = (CatalogDetails) connectionManagerSupplier.get()
      .getConnectionDetails( CatalogDetails.CATALOG, wConnection.getText() );

    TagResult tagResult = new TagResult();

    URL url;
    try {
      url = new URL( transMeta.environmentSubstitute( catalogDetails.getUrl() ) );
      String username = transMeta.environmentSubstitute( catalogDetails.getUsername() );
      String password = transMeta.environmentSubstitute( catalogDetails.getPassword() );

      CatalogClient catalogClient =
        new CatalogClient( url.getHost(), String.valueOf( url.getPort() ),
          url.getProtocol().equals( CatalogClient.HTTPS ) );

      catalogClient.getAuthentication().login( username, password );
      tagResult = catalogClient.getTagDomains().doTags();
    } catch ( MalformedURLException mue ) {
      // Do nothing.
    }

    return tagResult.keySet().toArray( new String[ 0 ] );
  }

  @SuppressWarnings( "java:S3776" )
  private void get() {
    try {
      RowMetaInterface r = transMeta.getPrevStepFields( stepname );
      if ( r != null ) {
        // This is the Pentaho style for getFields
        @SuppressWarnings( "java:S1604" )
        TableItemInsertListener listener = new TableItemInsertListener() {
          public boolean tableItemInserted( TableItem tableItem, ValueMetaInterface v ) {

            if ( v.isNumeric() ) {
              // currency symbol
              tableItem.setText( 6, Const.NVL( v.getCurrencySymbol(), "" ) );

              // decimal and grouping
              tableItem.setText( 7, Const.NVL( v.getDecimalSymbol(), "" ) );
              tableItem.setText( 8, Const.NVL( v.getGroupingSymbol(), "" ) );
            }

            // trim type
            tableItem.setText( 9, Const.NVL( ValueMetaBase.getTrimTypeDesc( v.getTrimType() ), "" ) );

            // conversion mask
            if ( !Utils.isEmpty( v.getConversionMask() ) ) {
              tableItem.setText( 3, v.getConversionMask() );
            } else {
              if ( ( v.isNumber() ) && ( v.getLength() > 0 ) ) {
                int le = v.getLength();
                int pr = v.getPrecision();

                if ( v.getPrecision() <= 0 ) {
                  pr = 0;
                }

                StringBuilder mask = new StringBuilder();
                for ( int m = 0; m < le - pr; m++ ) {
                  mask.append( "0" );
                }
                if ( pr > 0 ) {
                  mask.append( "." );
                }
                for ( int m = 0; m < pr; m++ ) {
                  mask.append( "0" );
                }
                tableItem.setText( 3, mask.toString() );
              }
            }
            return true;
          }
        };
        BaseStepDialog.getFieldsFromPrevious( r, wFields, 1, new int[] { 1 }, new int[] { 2 }, 4, 5, listener );
        wGet.setSelection( false );
      }
    } catch ( KettleException ke ) {
      new ErrorDialog( shell, BaseMessages.getString( writerPayloadMetaClass, "System.Dialog.GetFieldsFailed.Title" ),
        BaseMessages
          .getString( writerPayloadMetaClass, "System.Dialog.GetFieldsFailed.Message" ), ke );
    }
    wFields.setFocusOnFirstEditableField();
  }

  /**
   * Sets the output width to minimal width.../
   */
  public void setMinimalWidth() {
    int nrNonEmptyFields = wFields.nrNonEmpty();
    for ( int i = 0; i < nrNonEmptyFields; i++ ) {
      TableItem item = wFields.getNonEmpty( i );

      item.setText( 4, "" );
      item.setText( 5, "" );
      item.setText( 9, ValueMetaBase.getTrimTypeDesc( ValueMetaInterface.TRIM_TYPE_BOTH ) );

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

    for ( int i = 0; i < meta.getOutputFields().length; i++ ) {
      meta.getOutputFields()[ i ].setTrimType( ValueMetaInterface.TRIM_TYPE_BOTH );
    }

    wFields.optWidth( true );
    wFields.setFocusOnFirstEditableField();
  }

  private FacetsResult getFacets() {
    CatalogDetails catalogDetails = (CatalogDetails) connectionManagerSupplier.get()
      .getConnectionDetails( CatalogDetails.CATALOG, wConnection.getText() );

    FacetsResult result = new FacetsResult();

    URL url;
    try {
      url = new URL( transMeta.environmentSubstitute( catalogDetails.getUrl() ) );
      String username = transMeta.environmentSubstitute( catalogDetails.getUsername() );
      String password = transMeta.environmentSubstitute( catalogDetails.getPassword() );

      CatalogClient catalogClient =
        new CatalogClient( url.getHost(), String.valueOf( url.getPort() ),
          url.getProtocol().equals( CatalogClient.HTTPS ) );

      catalogClient.getAuthentication().login( username, password );
      result = catalogClient.getSearch().doFacets();
    } catch ( MalformedURLException mue ) {
      // Do nothing.
    }

    return result;
  }
}
