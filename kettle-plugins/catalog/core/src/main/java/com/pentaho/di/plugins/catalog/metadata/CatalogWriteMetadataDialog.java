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

package com.pentaho.di.plugins.catalog.metadata;

import com.pentaho.di.plugins.catalog.api.CatalogClient;
import com.pentaho.di.plugins.catalog.api.entities.metadata.TagResult;
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
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.connections.ConnectionManager;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.core.FormDataBuilder;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.MultipleSelectionCombo;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.function.Supplier;

public class CatalogWriteMetadataDialog extends BaseStepDialog implements StepDialogInterface {
  private static final int MARGIN_SIZE = 15;
  private static final int LABEL_SPACING = 5;
  private static final int ELEMENT_SPACING = 10;
  private static final int DOUBLE_ELEMENT_SPACING = 20;
  private static final int MULTI_TEXT_HIEGHT = 100;
  private static final int LABEL_WIDTH = 150;

  private static final int MEDIUM_FIELD = 250;

  private static Class<?> catalogWriteMetadataMetaClass = CatalogWriteMetadataMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
  Supplier<ConnectionManager> connectionManagerSupplier = ConnectionManager::getInstance;

  private CatalogWriteMetadataMeta meta;
  private Text wStepNameField;
  private CCombo wConnection;
  private TextVar resourceIdInput;
  private Button acceptCheckbox;
  private Button passFieldCheckbox;
  private TextVar fieldInput;
  private TextVar descriptionText;
  private MultipleSelectionCombo tagDropdown;

  private CTabFolder wTabFolder;

  public CatalogWriteMetadataDialog( Shell parent, Object in, TransMeta transMeta, String stepname ) {
    super( parent, (BaseStepMeta) in, transMeta, stepname );
    meta = (CatalogWriteMetadataMeta) in;
  }

  public String open() {
    //Set up window
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    shell.setMinimumSize( 450, 335 );
    props.setLook( shell );
    setShellImage( shell, meta );

    ModifyListener lsMod = e -> meta.setChanged();
    changed = meta.hasChanged();

    //15 pixel margins
    FormLayout formLayout = new FormLayout();
    formLayout.marginLeft = MARGIN_SIZE;
    formLayout.marginHeight = MARGIN_SIZE;
    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( catalogWriteMetadataMetaClass, "CatalogWriteMetadata.StepDialog.Shell.Title" ) );

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
    wStepNameLabel.setText( BaseMessages.getString( catalogWriteMetadataMetaClass, "CatalogWriteMetadata.StepDialog.Stepname.Label" ) );
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
    wicon.setImage( getImage() );
    FormData fdIcon = new FormDataBuilder().right()
            .top( 0, 4 )
            .bottom( new FormAttachment( wStepNameField, 0, SWT.BOTTOM ) )
            .result();
    wicon.setLayoutData( fdIcon );
    props.setLook( wicon );

        /*
        Connection Header
         */
    Label wlConnection = new Label( contentComposite, SWT.LEFT );
    wlConnection.setText( BaseMessages
            .getString( catalogWriteMetadataMetaClass, "CatalogWriteMetadata.StepDialog.Connection.Label" ) );
    props.setLook( wlConnection );
    FormData fdlConnection = new FormDataBuilder().left().top( wStepNameField, ELEMENT_SPACING ).result();
    wlConnection.setLayoutData( fdlConnection );

    wConnection = new CCombo( contentComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wConnection );
    FormData fdConnection =
            new FormDataBuilder().left().top( wlConnection, LABEL_SPACING ).width( MEDIUM_FIELD ).result();
    wConnection.setLayoutData( fdConnection );
    wConnection.addModifyListener( modifyEvent -> {
      //modify tag dropdown
      if ( !Utils.isEmpty( wConnection.getText() ) ) {
        String[] tagItems = fetchTags();
        tagDropdown.setItems( tagItems );
      }
    } );

    List<String> connections = ConnectionManager.getInstance().getNamesByKey( CatalogDetails.CATALOG );
    wConnection.setItems( connections.toArray( new String[0] ) );

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
    wCancel.setText( BaseMessages.getString( catalogWriteMetadataMetaClass, "System.Button.Cancel" ) );
    FormData fdCancel = new FormDataBuilder().right( 100, -MARGIN_SIZE )
            .bottom()
            .result();
    wCancel.setLayoutData( fdCancel );

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( catalogWriteMetadataMetaClass, "System.Button.OK" ) );
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

    FormData fdTabFolder = new FormDataBuilder().left( 0, 0 ).top( wConnection, DOUBLE_ELEMENT_SPACING ).right( 100, 0 )
            .bottom( 100, 0 ).result();
    wTabFolder.setLayoutData( fdTabFolder );

    addInputTab();
    addMetadataTab();

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

  private Image getImage() {
    PluginInterface plugin =
            PluginRegistry.getInstance().getPlugin( StepPluginType.class, stepMeta.getStepMetaInterface() );
    String id = plugin.getIds()[0];
    if ( id != null ) {
      return GUIResource.getInstance().getImagesSteps().get( id ).getAsBitmapForSize( shell.getDisplay(),
              ConstUI.ICON_SIZE, ConstUI.ICON_SIZE );
    }
    return null;
  }

  private void addInputTab() {
    CTabItem inputTab = new CTabItem( wTabFolder, SWT.NONE );
    inputTab.setText( BaseMessages
            .getString( catalogWriteMetadataMetaClass, "CatalogWriteMetadata.Input.Label" ) );

    Composite inputComposite = new Composite( wTabFolder, 0 );
    props.setLook( inputComposite );

    FormLayout inputLayout = new FormLayout();
    inputLayout.marginWidth = 15;
    inputLayout.marginHeight = 15;
    inputComposite.setLayout( inputLayout );

        /*
        Upper Group
         */
    Group idGroup = new Group( inputComposite, SWT.SHADOW_ETCHED_IN );
    idGroup.setText( BaseMessages
            .getString( catalogWriteMetadataMetaClass, "CatalogWriteMetadata.Input.TopGroup.Label" ) );
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 15;
    formLayout.marginHeight = 15;
    idGroup.setLayout( formLayout );
    idGroup.setLayoutData( new FormDataBuilder().top().fullWidth().result() );

        /*
        Resource ID Input
         */
    Label resourceIdLabel = new Label( idGroup, SWT.LEFT );
    resourceIdLabel.setText( BaseMessages
            .getString( catalogWriteMetadataMetaClass, "CatalogWriteMetadata.Input.Resource.ID.Label" ) );
    props.setLook( resourceIdLabel );
    resourceIdLabel.setLayoutData( new FormDataBuilder()
            .top( idGroup, 7 )
            .left( idGroup, -10 )
            .result() );

    resourceIdInput = new TextVar( transMeta, idGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( resourceIdInput );
    resourceIdInput.setLayoutData( new FormDataBuilder()
            .left( resourceIdLabel, DOUBLE_ELEMENT_SPACING )
            .top( idGroup, 4 )
            .width( MEDIUM_FIELD )
            .result() );

        /*
        Lower Group
         */
    Group prevStepGroup = new Group( inputComposite, SWT.SHADOW_ETCHED_IN );
    prevStepGroup.setText( BaseMessages
            .getString( catalogWriteMetadataMetaClass, "CatalogWriteMetadata.Input.BottomGroup.Label" ) );
    FormLayout prevStepLayout = new FormLayout();
    prevStepLayout.marginHeight = 15;
    prevStepLayout.marginWidth = 15;
    prevStepGroup.setLayout( prevStepLayout );
    prevStepGroup.setLayoutData( new FormDataBuilder()
            .left()
            .top( idGroup, ELEMENT_SPACING )
            .fullWidth()
            .result() );

        /*
        Accept ID Checkbox
         */
    Label acceptLabel = new Label( prevStepGroup, SWT.LEFT );
    acceptLabel.setText( BaseMessages
            .getString( catalogWriteMetadataMetaClass, "CatalogWriteMetadata.Input.BottomGroup.Label" ) );
    props.setLook( acceptLabel );
    acceptLabel.setLayoutData( new FormDataBuilder()
            .left( prevStepGroup, -10 )
            .top()
            .width( MEDIUM_FIELD )
            .result() );

    acceptCheckbox = new Button( prevStepGroup, SWT.CHECK );
    props.setLook( acceptCheckbox );
    //TODO: add handler
    acceptCheckbox.setLayoutData( new FormDataBuilder()
            .left( acceptLabel, ELEMENT_SPACING ).result() );

        /*
        Previous Step Checkbox
         */
    Label passFieldsLabel = new Label( prevStepGroup, SWT.LEFT );
    passFieldsLabel.setText( BaseMessages
            .getString( catalogWriteMetadataMetaClass, "CatalogWriteMetadata.Input.Pass.Through.Label" ) );
    props.setLook( passFieldsLabel );
    passFieldsLabel.setLayoutData( new FormDataBuilder()
            .left( prevStepGroup, -10 )
            .top( acceptLabel, ELEMENT_SPACING )
            .width( MEDIUM_FIELD )
            .result() );

    passFieldCheckbox = new Button( prevStepGroup, SWT.CHECK );
    props.setLook( passFieldCheckbox );
    //TODO: add handler
    passFieldCheckbox.setLayoutData( new FormDataBuilder()
            .left( passFieldsLabel, ELEMENT_SPACING )
            .top( acceptCheckbox, ELEMENT_SPACING )
            .result() );

        /*
        Last Input Row
         */
    Label inputFieldLabel = new Label( prevStepGroup, SWT.LEFT );
    inputFieldLabel.setText( BaseMessages
            .getString( catalogWriteMetadataMetaClass, "CatalogWriteMetadata.Input.Field.Input.Label" ) );
    props.setLook( inputFieldLabel );
    inputFieldLabel.setLayoutData( new FormDataBuilder()
            .left( prevStepGroup, -10 )
            .top( passFieldsLabel, ELEMENT_SPACING + 5 )
            .width( MEDIUM_FIELD )
            .result() );

    fieldInput = new TextVar( transMeta, prevStepGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( fieldInput );
    fieldInput.setLayoutData( new FormDataBuilder()
            .left( inputFieldLabel, ELEMENT_SPACING )
            .top( passFieldCheckbox, ELEMENT_SPACING )
            .width( MEDIUM_FIELD )
            .result() );

    inputComposite.setLayoutData( new FormDataBuilder()
            .left( 0, 0 ).top( 0, 0 ).right( 100, 0 ).bottom( 100, 0 ).result() );
    inputComposite.layout();
    inputTab.setControl( inputComposite );
  }

  private void addMetadataTab() {
    CTabItem metadataTab = new CTabItem( wTabFolder, 0 );
    metadataTab.setText( BaseMessages
            .getString( catalogWriteMetadataMetaClass, "CatalogWriteMetadata.Metadata.Label" ) );

    Composite metadataComposite = new Composite( wTabFolder, 0 );
    props.setLook( metadataComposite );

    FormLayout inputTabLayout = new FormLayout();
    //TODO: move all margin padding to method
    inputTabLayout.marginWidth = 15;
    inputTabLayout.marginHeight = 15;
    metadataComposite.setLayout( inputTabLayout );

        /*
        Metadata Group
         */
    Group metadataGroup = new Group( metadataComposite, SWT.SHADOW_ETCHED_IN );
    props.setLook( metadataGroup );
    metadataGroup.setText( BaseMessages
            .getString( catalogWriteMetadataMetaClass, "CatalogWriteMetadata.Metadata.Label" ) );
    FormLayout formLayout = new FormLayout();
    formLayout.marginHeight = 15;
    formLayout.marginWidth = 15;
    metadataGroup.setLayout( formLayout );
    metadataGroup.setLayoutData( new FormDataBuilder()
            .left( metadataComposite, ELEMENT_SPACING )
            .top()
            .fullWidth()
            .result() );
        /*
        Description
         */
    Label descLabel = new Label( metadataGroup, SWT.LEFT );
    descLabel.setText( BaseMessages
            .getString( catalogWriteMetadataMetaClass, "CatalogWriteMetadata.Metadata.Description.Label" ) );
    props.setLook( descLabel );
    descLabel.setLayoutData( new FormDataBuilder().top().left().width( LABEL_WIDTH ).result() );
    descriptionText = new TextVar( transMeta, metadataGroup, SWT.MULTI | SWT.BORDER );
    props.setLook( descriptionText );
    descriptionText.setLayoutData( new FormDataBuilder()
            .top()
            .left( descLabel, ELEMENT_SPACING )
            .width( MEDIUM_FIELD )
            .height( MULTI_TEXT_HIEGHT )
            .result() );

        /*
        Tags Dropdown
         */
    Label tagLabel = new Label( metadataGroup, SWT.LEFT );
    tagLabel.setText( BaseMessages
            .getString( catalogWriteMetadataMetaClass, "CatalogWriteMetadata.Metadata.Tags.Label" ) );
    props.setLook( tagLabel );
    tagLabel.setLayoutData( new FormDataBuilder()
            .top( descLabel, MULTI_TEXT_HIEGHT - LABEL_SPACING )
            .left()
            .width( LABEL_WIDTH )
            .result() );

    tagDropdown = new MultipleSelectionCombo( metadataGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( tagDropdown );
    tagDropdown.setLayoutData( new FormDataBuilder()
            .left( tagLabel, ELEMENT_SPACING )
            .top( descriptionText, ELEMENT_SPACING )
            .width( MEDIUM_FIELD )
            .result() );

    metadataComposite.layout();
    metadataTab.setControl( metadataComposite );
  }

  private void cancel() {
    dispose();
  }

  private void ok() {
    stepname = wStepNameField.getText();

    saveData( meta );

    dispose();
  }

  private void getData( CatalogWriteMetadataMeta meta ) {
    wTabFolder.setSelection( meta.getSelectedIndex() );
    wConnection.setText( Const.NVL( meta.getConnection(), "" ) );
    resourceIdInput.setText( Const.NVL( meta.getResourceId(), "" ) );
    acceptCheckbox.setSelection( falseIfNull( meta.getResourceFromPrevious() ) );
    passFieldCheckbox.setSelection( falseIfNull( meta.getPassThroughFields() ) );
    fieldInput.setText( Const.NVL( meta.getInputFieldName(), "" ) );
    descriptionText.setText( Const.NVL( meta.getDescription(), "" ) );
    tagDropdown.setText( Const.NVL( meta.getTags(), "" ) );
  }

  private String[] fetchTags() {
    //TODO: bootstrap connection and call for tags
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

    return tagResult.keySet().toArray( new String[0] );
  }

  private void saveData( CatalogWriteMetadataMeta meta ) {
    meta.setConnection( wConnection.getText() );
    meta.setResourceId( resourceIdInput.getText() );
    meta.setResourceFromPrevious( acceptCheckbox.getSelection() );
    meta.setPassThroughFields( passFieldCheckbox.getSelection() );
    meta.setInputFieldName( fieldInput.getText() );
    meta.setDescription( descriptionText.getText() );
    meta.setTags( tagDropdown.getText() );
  }

  private boolean falseIfNull( Boolean value ) {
    if ( value == null ) {
      return false;
    }

    return value;
  }
}
