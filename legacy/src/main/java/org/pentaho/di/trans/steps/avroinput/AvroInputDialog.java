/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.trans.steps.avroinput;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.vfs2.FileObject;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyEvent;
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
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.hadoop.HadoopSpoonPlugin;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransPreviewFactory;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.textfileinput.TextFileInputMeta;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.core.dialog.EnterNumberDialog;
import org.pentaho.di.ui.core.dialog.EnterTextDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.dialog.PreviewRowsDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.ColumnsResizer;
import org.pentaho.di.ui.core.widget.ComboVar;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.trans.dialog.TransPreviewProgressDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.util.SwtSvgImageUtil;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

/**
 * Dialog for the Avro input step.
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision$
 */
public class AvroInputDialog extends BaseStepDialog implements StepDialogInterface {

  private static final Class<?> PKG = AvroInputMeta.class;

  private final AvroInputMeta m_currentMeta;
  private final AvroInputMeta m_originalMeta;

  private CTabFolder m_wTabFolder;
  private CTabItem m_wDataFileTab;
  private CTabItem m_wSchemaTab;
  private CTabItem m_wFieldsTab;
  private CTabItem m_wVarsTab;

  private Composite wFileSetComp;
  private Composite wFieldSetComp;

  private Composite wSchemaFileSetComp;
  private Composite wSchemaFieldSetComp;

  /**
   * various UI bits and pieces for the dialog
   */
  private Button m_sourceInFileBut;
  private Button m_sourceInFieldBut;

  private Label m_defaultSchemaL;

  private Button m_schemaInFileBut;
  private Button m_schemaInFieldBut;
  private Label m_schemaInFieldIsPathL;
  private Button m_schemaInFieldIsPathBut;
  private Label m_cacheSchemasL;
  private Button m_cacheSchemasBut;
  private Label m_schemaFieldNameL;
  private ComboVar m_schemaFieldNameText;

  private TextVar m_avroFilenameText;
  private Button m_avroFileBrowse;
  private TextVar m_schemaFilenameText;
  private Button m_schemaFileBrowse;

  private ComboVar m_avroFieldNameText;

  private Button m_jsonEncodedBut;

  private Button m_missingFieldsBut;
  private Button m_getFields;
  private TableView m_fieldsView;

  private Button m_getLookupFieldsBut;
  private TableView m_lookupView;

  private ModifyListener lsMod;

  public AvroInputDialog( Shell parent, Object in, TransMeta tr, String name ) {

    super( parent, (BaseStepMeta) in, tr, name );
    m_currentMeta = (AvroInputMeta) in;
    m_originalMeta = (AvroInputMeta) m_currentMeta.clone();
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );

    props.setLook( shell );
    setShellImage( shell, m_currentMeta );

    // used to listen to a text field (m_wStepname)
    lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        m_currentMeta.setChanged();
      }
    };

    changed = m_currentMeta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 15;
    formLayout.marginHeight = 15;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "AvroInputDialog.Shell.Title" ) );

    Label wicon = new Label( shell, SWT.RIGHT );
    wicon.setImage( getImage() );
    FormData fdlicon = new FormData();
    fdlicon.top = new FormAttachment( 0, 0 );
    fdlicon.right = new FormAttachment( 100, 0 );
    wicon.setLayoutData( fdlicon );
    props.setLook( wicon );

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "AvroInputDialog.StepName.Label" ) );
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

    Label spacer = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    FormData fdSpacer = new FormData();
    fdSpacer.left = new FormAttachment( 0, 0 );
    fdSpacer.top = new FormAttachment( wStepname, 15 );
    fdSpacer.right = new FormAttachment( 100, 0 );
    spacer.setLayoutData( fdSpacer );

    m_wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( m_wTabFolder, Props.WIDGET_STYLE_TAB );
    m_wTabFolder.setSimple( false );

    createDataFileTab();
    createSchemaTab();
    createAvroFieldsTab();
    createLookupFieldsTab();
    populateFieldsCombo();

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    FormData fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 100 );
    wCancel.setLayoutData( fd );

    // Buttons inherited from BaseStepDialog
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    fd = new FormData();
    fd.right = new FormAttachment( wCancel, Const.isOSX() ? 0 : -5 );
    fd.bottom = new FormAttachment( 100 );
    wOK.setLayoutData( fd );

    wPreview = new Button( shell, SWT.PUSH | SWT.CENTER );
    wPreview.setText( BaseMessages.getString( PKG, "System.Button.Preview" ) );
    wPreview.pack();
    props.setLook( wPreview );
    fd = new FormData();
    int offset = wPreview.getBounds().width / 2;
    fd.left = new FormAttachment( 50, -offset );
    fd.bottom = new FormAttachment( 100, 0 );
    wPreview.setLayoutData( fd );
    wPreview.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        preview();
      }
    } );

    Label hSpacer = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    FormData fdhSpacer = new FormData();
    fdhSpacer.left = new FormAttachment( 0, 0 );
    fdhSpacer.bottom = new FormAttachment( wCancel, -15 );
    fdhSpacer.right = new FormAttachment( 100, 0 );
    hSpacer.setLayoutData( fdhSpacer );

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( spacer, 20 );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( hSpacer, -15 );
    m_wTabFolder.setLayoutData( fdTabFolder );

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };

    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOK.addListener( SWT.Selection, lsOK );

    lsDef = new SelectionAdapter() {
      @Override
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      @Override
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    m_wTabFolder.setSelection( 0 );

    setSize( shell, 645, 450 );
    getData();

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }

    return stepname;
  }

  protected Image getImage() {
    return SwtSvgImageUtil
      .getImage( shell.getDisplay(), getClass().getClassLoader(), "Avro.svg", ConstUI.ICON_SIZE,
        ConstUI.ICON_SIZE );
  }

  protected void cancel() {
    stepname = null;
    m_currentMeta.setChanged( changed );

    dispose();
  }

  protected void ok() {
    if ( Utils.isEmpty( wStepname.getText() ) ) {
      return;
    }

    stepname = wStepname.getText();

    setMeta( m_currentMeta );

    if ( !m_originalMeta.equals( m_currentMeta ) ) {
      m_currentMeta.setChanged();
      changed = m_currentMeta.hasChanged();
    }

    dispose();
  }

  protected void setMeta( AvroInputMeta avroMeta ) {
    avroMeta.setFilename( m_avroFilenameText.getText() );
    avroMeta.setSchemaFilename( m_schemaFilenameText.getText() );
    avroMeta.setAvroIsJsonEncoded( m_jsonEncodedBut.getSelection() );
    avroMeta.setAvroInField( m_sourceInFieldBut.getSelection() );
    avroMeta.setAvroFieldName( m_avroFieldNameText.getText() );

    avroMeta.setSchemaInField( m_schemaInFieldBut.getSelection() );
    avroMeta.setSchemaInFieldIsPath( m_schemaInFieldIsPathBut.getSelection() );
    avroMeta.setCacheSchemasInMemory( m_cacheSchemasBut.getSelection() );
    avroMeta.setSchemaFieldName( m_schemaFieldNameText.getText() );
    avroMeta.setAllowNullValues( m_missingFieldsBut.getSelection() );

    int numNonEmpty = m_fieldsView.nrNonEmpty();
    if ( numNonEmpty > 0 ) {
      List<AvroInputMeta.AvroField> outputFields = new ArrayList<AvroInputMeta.AvroField>();

      for ( int i = 0; i < numNonEmpty; i++ ) {
        TableItem item = m_fieldsView.getNonEmpty( i );
        AvroInputMeta.AvroField newField = new AvroInputMeta.AvroField();
        newField.m_fieldName = item.getText( 1 ).trim();
        newField.m_fieldPath = item.getText( 2 ).trim();
        newField.m_kettleType = item.getText( 3 ).trim();

        if ( !Utils.isEmpty( item.getText( 4 ) ) ) {
          newField.m_indexedVals = AvroInputMeta.indexedValsList( item.getText( 4 ).trim() );
        }

        outputFields.add( newField );
      }
      avroMeta.setAvroFields( outputFields );
    }

    numNonEmpty = m_lookupView.nrNonEmpty();
    if ( numNonEmpty > 0 ) {
      List<AvroInputMeta.LookupField> varFields = new ArrayList<AvroInputMeta.LookupField>();

      for ( int i = 0; i < numNonEmpty; i++ ) {
        TableItem item = m_lookupView.getNonEmpty( i );
        AvroInputMeta.LookupField newField = new AvroInputMeta.LookupField();
        boolean add = false;

        newField.m_fieldName = item.getText( 1 ).trim();
        if ( !Utils.isEmpty( item.getText( 2 ) ) ) {
          newField.m_variableName = item.getText( 2 ).trim();
          add = true;
          if ( !Utils.isEmpty( item.getText( 3 ) ) ) {
            newField.m_defaultValue = item.getText( 3 ).trim();
          }
        }

        if ( add ) {
          varFields.add( newField );
        }
      }
      avroMeta.setLookupFields( varFields );
    }
  }

  protected void getFields() {
    if ( !Utils.isEmpty( m_schemaFilenameText.getText() ) ) {
      // this schema overrides any that might be in a container file
      String sName = m_schemaFilenameText.getText();
      sName = transMeta.environmentSubstitute( sName );
      try {
        Schema s = AvroInputData.loadSchema( sName );
        List<AvroInputMeta.AvroField> schemaFields = AvroInputData.getLeafFields( s );

        setTableFields( schemaFields );

      } catch ( Exception ex ) {
        logError( BaseMessages.getString( PKG, "AvroInputDialog.Error.KettleFileException" + " " + sName ), ex );
        new ErrorDialog( shell, stepname, BaseMessages.getString( PKG, "AvroInputDialog.Error.KettleFileException"
          + " " + sName ), ex );
      }
    } else {
      String avroFileName = m_avroFilenameText.getText();
      avroFileName = transMeta.environmentSubstitute( avroFileName );
      try {
        Schema s = AvroInputData.loadSchemaFromContainer( avroFileName );
        List<AvroInputMeta.AvroField> schemaFields = AvroInputData.getLeafFields( s );

        setTableFields( schemaFields );
      } catch ( Exception ex ) {
        logError( BaseMessages.getString( PKG, "AvroInput.Error.UnableToLoadSchemaFromContainerFile" ), ex );
        new ErrorDialog( shell, stepname, BaseMessages.getString( PKG,
          "AvroInput.Error.UnableToLoadSchemaFromContainerFile", avroFileName ), ex );
      }
    }
  }

  protected void createDataFileTab() {
    // start of the source tab
    m_wDataFileTab = new CTabItem( m_wTabFolder, SWT.NONE );
    m_wDataFileTab.setText( BaseMessages.getString( PKG, "AvroInputDialog.DataFileTab.Title" ) );

    Composite wDataFileComp = new Composite( m_wTabFolder, SWT.NONE );
    props.setLook( wDataFileComp );
    FormLayout sourceLayout = new FormLayout();
    sourceLayout.marginWidth = 15;
    sourceLayout.marginHeight = 15;
    wDataFileComp.setLayout( sourceLayout );

    Group wSourceGroup = new Group( wDataFileComp, SWT.NONE );
    props.setLook( wSourceGroup );
    wSourceGroup.setText( BaseMessages.getString( PKG, "AvroInputDialog.SourceGroup.Title" ) );
    wSourceGroup.setLayout( sourceLayout );

    Composite wSourceButtonComp = new Composite( wSourceGroup, SWT.NONE );
    props.setLook( wSourceButtonComp );
    wSourceButtonComp.setLayout( new FormLayout() );

    m_sourceInFileBut = new Button( wSourceButtonComp, SWT.RADIO );
    m_sourceInFileBut.setText( BaseMessages.getString( PKG, "AvroInputDialog.FileSource.Label" ) );
    props.setLook( m_sourceInFileBut );
    FormData fd = new FormData();
    fd.left = new FormAttachment( 0 );
    fd.top = new FormAttachment( 0 );
    m_sourceInFileBut.setLayoutData( fd );

    m_sourceInFileBut.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        m_currentMeta.setChanged();
        m_sourceInFieldBut.setSelection( !m_sourceInFileBut.getSelection() );
        checkWidgets();
      }
    } );

    m_sourceInFieldBut = new Button( wSourceButtonComp, SWT.RADIO );
    m_sourceInFieldBut.setText( BaseMessages.getString( PKG, "AvroInputDialog.FieldSource.Label" ) );
    props.setLook( m_sourceInFieldBut );
    fd = new FormData();
    fd.left = new FormAttachment( 0 );
    fd.top = new FormAttachment( m_sourceInFileBut, 15 );
    m_sourceInFieldBut.setLayoutData( fd );

    m_sourceInFieldBut.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        m_currentMeta.setChanged();
        m_sourceInFileBut.setSelection( !m_sourceInFieldBut.getSelection() );
        checkWidgets();
      }
    } );

    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, 0 );
    wSourceButtonComp.setLayoutData( fd );

    Label vSpacer = new Label( wSourceGroup, SWT.VERTICAL | SWT.SEPARATOR );
    FormData fdvSpacer = new FormData();
    fdvSpacer.width = 1;
    fdvSpacer.left = new FormAttachment( wSourceButtonComp, 30 );
    fdvSpacer.top = new FormAttachment( 0, 0 );
    fdvSpacer.height = 45;
    vSpacer.setLayoutData( fdvSpacer );

    wFileSetComp = new Composite( wSourceGroup, SWT.NONE );
    props.setLook( wFileSetComp );
    wFileSetComp.setLayout( new FormLayout() );

    // filename line
    Label filenameL = new Label( wFileSetComp, SWT.LEFT );
    props.setLook( filenameL );
    filenameL.setText( BaseMessages.getString( PKG, "AvroInputDialog.Filename.Label" ) );
    fd = new FormData();
    fd.top = new FormAttachment( 0, 0 );
    fd.left = new FormAttachment( 0, 0 );
    filenameL.setLayoutData( fd );

    m_avroFileBrowse = new Button( wFileSetComp, SWT.PUSH | SWT.CENTER );
    props.setLook( m_avroFileBrowse );
    m_avroFileBrowse.setText( BaseMessages.getString( PKG, "AvroInputDialog.Button.FileBrowse" ) );
    fd = new FormData();
    fd.top = new FormAttachment( filenameL, Const.isOSX() ? 0 : 5 );
    fd.right = new FormAttachment( 100, 0 );
    m_avroFileBrowse.setLayoutData( fd );

    // add listener to pop up VFS browse dialog
    m_avroFileBrowse.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        try {
          String[] fileFilters = new String[] { "*" };
          String[] fileFilterNames =
            new String[] { BaseMessages.getString( TextFileInputMeta.class, "System.FileType.AllFiles" ) };

          // get current file
          FileObject rootFile = null;
          FileObject initialFile = null;
          FileObject defaultInitialFile = null;

          if ( m_avroFilenameText.getText() != null ) {
            String fname = transMeta.environmentSubstitute( m_avroFilenameText.getText() );

            if ( !Utils.isEmpty( fname ) ) {
              initialFile = KettleVFS.getFileObject( fname );
              rootFile = initialFile.getFileSystem().getRoot();
            } else {
              defaultInitialFile = KettleVFS.getFileObject( Spoon.getInstance().getLastFileOpened() );
            }
          } else {
            defaultInitialFile = KettleVFS.getFileObject( "file:///c:/" );
          }

          if ( rootFile == null ) {
            rootFile = defaultInitialFile.getFileSystem().getRoot();
          }

          VfsFileChooserDialog fileChooserDialog = Spoon.getInstance().getVfsFileChooserDialog( rootFile, initialFile );
          fileChooserDialog.defaultInitialFile = defaultInitialFile;
          FileObject selectedFile =
            fileChooserDialog.open( shell, null, HadoopSpoonPlugin.HDFS_SCHEME, true, null, fileFilters,
              fileFilterNames, VfsFileChooserDialog.VFS_DIALOG_OPEN_FILE );

          if ( selectedFile != null ) {
            m_avroFilenameText.setText( selectedFile.getURL().toString() );
          }
        } catch ( Exception ex ) {
          logError( BaseMessages.getString( PKG, "AvroInputDialog.Error.KettleFileException" ), ex );
          new ErrorDialog( shell, stepname, BaseMessages.getString( PKG, "AvroInputDialog.Error.KettleFileException" ),
            ex );
        }
      }
    } );

    m_avroFilenameText = new TextVar( transMeta, wFileSetComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( m_avroFilenameText );
    m_avroFilenameText.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        m_currentMeta.setChanged();
        m_avroFilenameText.setToolTipText( transMeta.environmentSubstitute( m_avroFilenameText.getText() ) );
        checkWidgets();
      }
    } );
    fd = new FormData();
    fd.left = new FormAttachment( 0 );
    fd.top = new FormAttachment( filenameL, 5 );
    fd.right = new FormAttachment( m_avroFileBrowse, Const.isOSX() ? 0 : -5 );
    m_avroFilenameText.setLayoutData( fd );

    fd = new FormData();
    fd.left = new FormAttachment( vSpacer, 20 );
    fd.top = new FormAttachment( 0 );
    fd.right = new FormAttachment( 100 );
    wFileSetComp.setLayoutData( fd );

    wFieldSetComp = new Composite( wSourceGroup, SWT.NONE );
    props.setLook( wFieldSetComp );
    wFieldSetComp.setLayout( new FormLayout() );

    Label avroFieldNameL = new Label( wFieldSetComp, SWT.LEFT );
    props.setLook( avroFieldNameL );
    avroFieldNameL.setText( BaseMessages.getString( PKG, "AvroInputDialog.AvroField.Label" ) );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, 0 );
    avroFieldNameL.setLayoutData( fd );

    m_avroFieldNameText = new ComboVar( transMeta, wFieldSetComp, SWT.BORDER );
    props.setLook( m_avroFieldNameText );
    m_avroFieldNameText.addModifyListener( e -> {
      m_currentMeta.setChanged();
      m_avroFieldNameText.setToolTipText( transMeta.environmentSubstitute( m_avroFieldNameText.getText() ) );
      checkWidgets();
    } );
    fd = new FormData();
    fd.width = 250;
    fd.left = new FormAttachment( 0 );
    fd.top = new FormAttachment( avroFieldNameL, 5 );
    m_avroFieldNameText.setLayoutData( fd );

    fd = new FormData();
    fd.left = new FormAttachment( vSpacer, 20 );
    fd.top = new FormAttachment( 0 );
    fd.right = new FormAttachment( 100 );
    wFieldSetComp.setLayoutData( fd );

    m_jsonEncodedBut = new Button( wDataFileComp, SWT.CHECK );
    props.setLook( m_jsonEncodedBut );
    m_jsonEncodedBut.setText( BaseMessages.getString( PKG, "AvroInputDialog.JsonEncoded.Label" ) );
    m_jsonEncodedBut.setToolTipText( BaseMessages.getString( PKG, "AvroInputDialog.JsonEncoded.TipText" ) );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wSourceGroup, 15 );
    m_jsonEncodedBut.setLayoutData( fd );
    m_jsonEncodedBut.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        m_currentMeta.setChanged();
      }
    } );

    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    wSourceGroup.setLayoutData( fd );

    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 100, 0 );
    wDataFileComp.setLayoutData( fd );
    wDataFileComp.layout();
    m_wDataFileTab.setControl( wDataFileComp );
  }

  protected void createSchemaTab() {

    m_wSchemaTab = new CTabItem( m_wTabFolder, SWT.NONE );
    m_wSchemaTab.setText( BaseMessages.getString( PKG, "AvroInputDialog.SchemaTab.Title" ) );

    Composite wSchemaComp = new Composite( m_wTabFolder, SWT.NONE );
    props.setLook( wSchemaComp );
    FormLayout sourceLayout = new FormLayout();
    sourceLayout.marginWidth = 15;
    sourceLayout.marginHeight = 15;
    wSchemaComp.setLayout( sourceLayout );

    Group wSourceGroup = new Group( wSchemaComp, SWT.NONE );
    props.setLook( wSourceGroup );
    wSourceGroup.setText( BaseMessages.getString( PKG, "AvroInputDialog.SourceGroup.Title" ) );
    wSourceGroup.setLayout( sourceLayout );

    Composite wSourceButtonComp = new Composite( wSourceGroup, SWT.NONE );
    props.setLook( wSourceButtonComp );
    wSourceButtonComp.setLayout( new FormLayout() );

    m_schemaInFileBut = new Button( wSourceButtonComp, SWT.RADIO );
    m_schemaInFileBut.setText( BaseMessages.getString( PKG, "AvroInputDialog.FileSource.Label" ) );
    props.setLook( m_schemaInFileBut );
    FormData fd = new FormData();
    fd.left = new FormAttachment( 0 );
    fd.top = new FormAttachment( 0 );
    m_schemaInFileBut.setLayoutData( fd );

    m_schemaInFileBut.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        m_currentMeta.setChanged();
        m_sourceInFieldBut.setSelection( !m_schemaInFileBut.getSelection() );
        checkWidgets();
      }
    } );

    m_schemaInFieldBut = new Button( wSourceButtonComp, SWT.RADIO );
    m_schemaInFieldBut.setText( BaseMessages.getString( PKG, "AvroInputDialog.FieldSource.Label" ) );
    props.setLook( m_schemaInFieldBut );
    fd = new FormData();
    fd.left = new FormAttachment( 0 );
    fd.top = new FormAttachment( m_schemaInFileBut, 15 );
    m_schemaInFieldBut.setLayoutData( fd );

    m_schemaInFieldBut.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        m_currentMeta.setChanged();
        checkWidgets();
      }
    } );

    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, 0 );
    wSourceButtonComp.setLayoutData( fd );

    Label vSpacer = new Label( wSourceGroup, SWT.VERTICAL | SWT.SEPARATOR );
    FormData fdvSpacer = new FormData();
    fdvSpacer.width = 1;
    fdvSpacer.left = new FormAttachment( wSourceButtonComp, 30 );
    fdvSpacer.top = new FormAttachment( 0, 0 );
    fdvSpacer.bottom = new FormAttachment( 100, 0 );
    vSpacer.setLayoutData( fdvSpacer );

    wSchemaFileSetComp = new Composite( wSourceGroup, SWT.NONE );
    props.setLook( wSchemaFileSetComp );
    wSchemaFileSetComp.setLayout( new FormLayout() );

    // schema filename line
    m_defaultSchemaL = new Label( wSchemaFileSetComp, SWT.RIGHT );
    props.setLook( m_defaultSchemaL );
    m_defaultSchemaL.setText( BaseMessages.getString( PKG, "AvroInputDialog.SchemaFilename.Label" ) );
    fd = new FormData();
    fd.left = new FormAttachment( 0 );
    fd.top = new FormAttachment( 0 );
    m_defaultSchemaL.setLayoutData( fd );
    m_defaultSchemaL.setToolTipText( BaseMessages.getString( PKG, "AvroInputDialog.SchemaFilename.TipText" ) );

    m_schemaFileBrowse = new Button( wSchemaFileSetComp, SWT.PUSH | SWT.CENTER );
    props.setLook( m_schemaFileBrowse );
    m_schemaFileBrowse.setText( BaseMessages.getString( PKG, "AvroInputDialog.Button.FileBrowse" ) );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( m_defaultSchemaL, Const.isOSX() ? 0 : 5 );
    m_schemaFileBrowse.setLayoutData( fd );

    // add listener to pop up VFS browse dialog
    m_schemaFileBrowse.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        try {
          String[] fileFilters = new String[] { "*" };
          String[] fileFilterNames =
            new String[] { BaseMessages.getString( TextFileInputMeta.class, "System.FileType.AllFiles" ) };

          // get current file
          FileObject rootFile = null;
          FileObject initialFile = null;
          FileObject defaultInitialFile = null;

          if ( m_schemaFilenameText.getText() != null ) {
            String fname = transMeta.environmentSubstitute( m_schemaFilenameText.getText() );

            if ( !Utils.isEmpty( fname ) ) {
              initialFile = KettleVFS.getFileObject( fname );
              rootFile = initialFile.getFileSystem().getRoot();
            } else {
              defaultInitialFile = KettleVFS.getFileObject( Spoon.getInstance().getLastFileOpened() );
            }
          } else {
            defaultInitialFile = KettleVFS.getFileObject( "file:///c:/" );
          }

          if ( rootFile == null ) {
            rootFile = defaultInitialFile.getFileSystem().getRoot();
          }

          VfsFileChooserDialog fileChooserDialog = Spoon.getInstance().getVfsFileChooserDialog( rootFile, initialFile );
          fileChooserDialog.defaultInitialFile = defaultInitialFile;
          FileObject selectedFile =
            fileChooserDialog.open( shell, null, HadoopSpoonPlugin.HDFS_SCHEME, true, null, fileFilters,
              fileFilterNames, VfsFileChooserDialog.VFS_DIALOG_OPEN_FILE );

          if ( selectedFile != null ) {
            m_schemaFilenameText.setText( selectedFile.getURL().toString() );
          }
        } catch ( Exception ex ) {
          logError( BaseMessages.getString( PKG, "AvroInputDialog.Error.KettleFileException" ), ex );
          new ErrorDialog( shell, stepname, BaseMessages.getString( PKG, "AvroInputDialog.Error.KettleFileException" ),
            ex );
        }
      }
    } );

    m_schemaFilenameText = new TextVar( transMeta, wSchemaFileSetComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( m_schemaFilenameText );
    m_schemaFilenameText.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        m_currentMeta.setChanged();
        m_schemaFilenameText.setToolTipText( transMeta.environmentSubstitute( m_schemaFilenameText.getText() ) );
      }
    } );
    fd = new FormData();
    fd.left = new FormAttachment( 0 );
    fd.top = new FormAttachment( m_defaultSchemaL, 5 );
    fd.right = new FormAttachment( m_schemaFileBrowse, Const.isOSX() ? 0 : -5 );
    m_schemaFilenameText.setLayoutData( fd );

    fd = new FormData();
    fd.left = new FormAttachment( vSpacer, 20 );
    fd.top = new FormAttachment( 0 );
    fd.right = new FormAttachment( 100 );
    wSchemaFileSetComp.setLayoutData( fd );

    wSchemaFieldSetComp = new Composite( wSourceGroup, SWT.NONE );
    props.setLook( wSchemaFieldSetComp );
    wSchemaFieldSetComp.setLayout( new FormLayout() );

    // schema field name line
    m_schemaFieldNameL = new Label( wSchemaFieldSetComp, SWT.RIGHT );
    props.setLook( m_schemaFieldNameL );
    m_schemaFieldNameL.setText( BaseMessages.getString( PKG, "AvroInputDialog.SchemaFieldName.Label" ) );
    fd = new FormData();
    fd.left = new FormAttachment( 0 );
    fd.top = new FormAttachment( 0 );
    m_schemaFieldNameL.setLayoutData( fd );

    m_schemaFieldNameText = new ComboVar( transMeta, wSchemaFieldSetComp, SWT.BORDER );
    props.setLook( m_schemaFieldNameText );
    m_schemaFieldNameText.addModifyListener( e -> {
      m_currentMeta.setChanged();
      m_schemaFieldNameText.setToolTipText( transMeta.environmentSubstitute( m_schemaFieldNameText.getText() ) );
    } );
    fd = new FormData();
    fd.width = 250;
    fd.top = new FormAttachment( m_schemaFieldNameL, 5 );
    fd.left = new FormAttachment( 0 );
    m_schemaFieldNameText.setLayoutData( fd );

    m_schemaInFieldIsPathBut = new Button( wSchemaFieldSetComp, SWT.CHECK );
    m_schemaInFieldIsPathBut.setText( BaseMessages.getString( PKG, "AvroInputDialog.SchemaInFieldIsPath.Label" ) );
    props.setLook( m_schemaInFieldIsPathBut );
    fd = new FormData();
    fd.left = new FormAttachment( 0 );
    fd.top = new FormAttachment( m_schemaFieldNameText, 10 );
    m_schemaInFieldIsPathBut.setLayoutData( fd );

    m_cacheSchemasBut = new Button( wSchemaFieldSetComp, SWT.CHECK );
    m_cacheSchemasBut.setText( BaseMessages.getString( PKG, "AvroInputDialog.CacheSchemas.Label" ) );
    props.setLook( m_cacheSchemasBut );
    fd = new FormData();
    fd.left = new FormAttachment( 0 );
    fd.top = new FormAttachment( m_schemaInFieldIsPathBut, 10 );
    m_cacheSchemasBut.setLayoutData( fd );

    fd = new FormData();
    fd.left = new FormAttachment( vSpacer, 20 );
    fd.top = new FormAttachment( 0 );
    fd.right = new FormAttachment( 100 );
    wSchemaFieldSetComp.setLayoutData( fd );

    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    wSourceGroup.setLayoutData( fd );

    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 100, 0 );
    wSchemaComp.setLayoutData( fd );

    wSchemaComp.layout();
    m_wSchemaTab.setControl( wSchemaComp );
  }

  protected void createAvroFieldsTab() {
    // -- start of the fields tab
    m_wFieldsTab = new CTabItem( m_wTabFolder, SWT.NONE );
    m_wFieldsTab.setText( BaseMessages.getString( PKG, "AvroInputDialog.FieldsTab.Title" ) );
    Composite wFieldsComp = new Composite( m_wTabFolder, SWT.NONE );
    props.setLook( wFieldsComp );

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = 15;
    fieldsLayout.marginHeight = 15;
    wFieldsComp.setLayout( fieldsLayout );

    m_missingFieldsBut = new Button( wFieldsComp, SWT.CHECK );
    m_missingFieldsBut.setText( BaseMessages.getString( PKG, "AvroInputDialog.MissingFields.Label" ) );
    props.setLook( m_missingFieldsBut );
    FormData fd = new FormData();
    fd.left = new FormAttachment( 0 );
    fd.top = new FormAttachment( 0 );
    m_missingFieldsBut.setLayoutData( fd );

    // get fields button
    m_getFields = new Button( wFieldsComp, SWT.PUSH );
    m_getFields.setText( BaseMessages.getString( PKG, "AvroInputDialog.Button.GetFields" ) );
    props.setLook( m_getFields );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 100, 0 );
    m_getFields.setLayoutData( fd );
    m_getFields.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        // populate table from schema
        getFields();
      }
    } );

    // fields stuff
    final ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo( BaseMessages.getString( PKG, "AvroInputDialog.Fields.FIELD_NAME" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( BaseMessages.getString( PKG, "AvroInputDialog.Fields.FIELD_PATH" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( BaseMessages.getString( PKG, "AvroInputDialog.Fields.FIELD_TYPE" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, false ),
        new ColumnInfo( BaseMessages.getString( PKG, "AvroInputDialog.Fields.FIELD_INDEXED" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ), };

    colinf[ 2 ].setComboValues( ValueMeta.getTypes() );

    m_fieldsView =
      new TableView( transMeta, wFieldsComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, 1, false, lsMod, props, false );

    fd = new FormData();
    fd.top = new FormAttachment( m_missingFieldsBut, 10 );
    fd.bottom = new FormAttachment( m_getFields, -10 );
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    m_fieldsView.setLayoutData( fd );
    m_fieldsView.getTable().addListener( SWT.Resize, new ColumnsResizer( 0, 20, 40, 20, 20 ) );

    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 100, 0 );
    wFieldsComp.setLayoutData( fd );

    wFieldsComp.layout();
    m_wFieldsTab.setControl( wFieldsComp );
  }

  protected void createLookupFieldsTab() {
    m_wVarsTab = new CTabItem( m_wTabFolder, SWT.NONE );
    m_wVarsTab.setText( BaseMessages.getString( PKG, "AvroInputDialog.VarsTab.Title" ) );
    Composite wVarsComp = new Composite( m_wTabFolder, SWT.NONE );
    props.setLook( wVarsComp );

    FormLayout varsLayout = new FormLayout();
    varsLayout.marginWidth = 15;
    varsLayout.marginHeight = 15;
    wVarsComp.setLayout( varsLayout );

    // lookup fields (variables) tab
    final ColumnInfo[] colinf2 =
      new ColumnInfo[] {
        new ColumnInfo( BaseMessages.getString( PKG, "AvroInputDialog.Fields.LOOKUP_NAME" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( BaseMessages.getString( PKG, "AvroInputDialog.Fields.LOOKUP_VARIABLE" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( BaseMessages.getString( PKG, "AvroInputDialog.Fields.LOOKUP_DEFAULT_VALUE" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ), };

    // get lookup fields but
    m_getLookupFieldsBut = new Button( wVarsComp, SWT.PUSH | SWT.CENTER );
    props.setLook( m_getLookupFieldsBut );
    m_getLookupFieldsBut.setText( BaseMessages.getString( PKG, "AvroInputDialog.Button.GetLookupFields" ) );
    FormData fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 100 );
    m_getLookupFieldsBut.setLayoutData( fd );

    m_getLookupFieldsBut.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        // get incoming field names
        getIncomingFields();
      }
    } );

    m_lookupView =
      new TableView( transMeta, wVarsComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf2, 1, false, lsMod, props, false );
    fd = new FormData();
    fd.top = new FormAttachment( 0 );
    fd.bottom = new FormAttachment( m_getLookupFieldsBut, -10 );
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    m_lookupView.setLayoutData( fd );
    m_lookupView.getTable().addListener( SWT.Resize, new ColumnsResizer( 0, 33, 33, 33 ) );

    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 100, 0 );
    wVarsComp.setLayoutData( fd );

    wVarsComp.layout();
    m_wVarsTab.setControl( wVarsComp );
  }

  protected void setTableFields( List<AvroInputMeta.AvroField> fields ) {
    m_fieldsView.clearAll();
    for ( AvroInputMeta.AvroField f : fields ) {
      TableItem item = new TableItem( m_fieldsView.table, SWT.NONE );

      if ( !Utils.isEmpty( f.m_fieldName ) ) {
        item.setText( 1, f.m_fieldName );
      }

      if ( !Utils.isEmpty( f.m_fieldPath ) ) {
        item.setText( 2, f.m_fieldPath );
      }

      if ( !Utils.isEmpty( f.m_kettleType ) ) {
        item.setText( 3, f.m_kettleType );
      }

      if ( f.m_indexedVals != null && f.m_indexedVals.size() > 0 ) {
        item.setText( 4, AvroInputMeta.indexedValsList( f.m_indexedVals ) );
      }
    }

    m_fieldsView.removeEmptyRows();
    m_fieldsView.setRowNums();
    m_fieldsView.optWidth( true );
  }

  protected void setVariableTableFields( List<AvroInputMeta.LookupField> fields ) {
    m_lookupView.clearAll();

    for ( AvroInputMeta.LookupField f : fields ) {
      TableItem item = new TableItem( m_lookupView.table, SWT.NONE );

      if ( !Utils.isEmpty( f.m_fieldName ) ) {
        item.setText( 1, f.m_fieldName );
      }

      if ( !Utils.isEmpty( f.m_variableName ) ) {
        item.setText( 2, f.m_variableName );
      }

      if ( !Utils.isEmpty( f.m_defaultValue ) ) {
        item.setText( 3, f.m_defaultValue );
      }
    }

    m_lookupView.removeEmptyRows();
    m_lookupView.setRowNums();
    m_lookupView.optWidth( true );
  }

  protected void getData() {
    if ( !Utils.isEmpty( m_currentMeta.getFilename() ) ) {
      m_avroFilenameText.setText( m_currentMeta.getFilename() );
    }

    if ( !Utils.isEmpty( m_currentMeta.getSchemaFilename() ) ) {
      m_schemaFilenameText.setText( m_currentMeta.getSchemaFilename() );
    }

    if ( !Utils.isEmpty( m_currentMeta.getAvroFieldName() ) ) {
      m_avroFieldNameText.setText( m_currentMeta.getAvroFieldName() );
    }

    m_jsonEncodedBut.setSelection( m_currentMeta.getAvroIsJsonEncoded() );
    m_sourceInFieldBut.setSelection( m_currentMeta.getAvroInField() );
    if ( !m_currentMeta.getAvroInField() ) {
      m_sourceInFileBut.setSelection( true );
    }
    if ( !m_currentMeta.getSchemaInField() ) {
      m_schemaInFileBut.setSelection( true );
    }

    m_schemaInFieldBut.setSelection( m_currentMeta.getSchemaInField() );
    m_schemaInFieldIsPathBut.setSelection( m_currentMeta.getSchemaInFieldIsPath() );
    m_cacheSchemasBut.setSelection( m_currentMeta.getCacheSchemasInMemory() );
    m_missingFieldsBut.setSelection( m_currentMeta.getAllowNullValues() );
    if ( !Utils.isEmpty( m_currentMeta.getSchemaFieldName() ) ) {
      m_schemaFieldNameText.setText( m_currentMeta.getSchemaFieldName() );
    }

    // fields
    if ( m_currentMeta.getAvroFields() != null && m_currentMeta.getAvroFields().size() > 0 ) {
      setTableFields( m_currentMeta.getAvroFields() );
    }

    if ( m_currentMeta.getLookupFields() != null && m_currentMeta.getLookupFields().size() > 0 ) {
      setVariableTableFields( m_currentMeta.getLookupFields() );
    }

    checkWidgets();
  }

  private void checkWidgets() {

    wFileSetComp.setVisible( m_sourceInFileBut.getSelection() );
    wFieldSetComp.setVisible( m_sourceInFieldBut.getSelection() );

    wSchemaFileSetComp.setVisible( m_schemaInFileBut.getSelection() );
    wSchemaFieldSetComp.setVisible( m_schemaInFieldBut.getSelection() );

    boolean sifile = m_sourceInFileBut.getSelection();

    m_avroFilenameText.setEnabled( sifile );
    m_avroFileBrowse.setEnabled( sifile );

    boolean sifield = m_sourceInFieldBut.getSelection();
    if ( sifield ) {
      m_sourceInFileBut.setSelection( !sifield );
    }
    m_avroFilenameText.setEnabled( !sifield );
    m_avroFileBrowse.setEnabled( !sifield );

    m_avroFieldNameText.setEnabled( sifield );
    // }

    wPreview
      .setEnabled( !Utils.isEmpty( m_avroFilenameText.getText() ) || !Utils.isEmpty( m_avroFieldNameText.getText() ) );

    boolean sField = m_schemaInFieldBut.getSelection();
    if ( sField ) {
      m_defaultSchemaL.setText( BaseMessages.getString( PKG, "AvroInputDialog.DefaultSchemaFilename.Label" ) );
    } else {
      m_defaultSchemaL.setText( BaseMessages.getString( PKG, "AvroInputDialog.SchemaFilename.Label" ) );
    }
  }

  private void preview() {
    AvroInputMeta tempMeta = new AvroInputMeta();
    setMeta( tempMeta );

    TransMeta previewMeta =
      TransPreviewFactory.generatePreviewTransformation( transMeta, tempMeta, wStepname.getText() );
    transMeta.getVariable( "Internal.Transformation.Filename.Directory" );
    previewMeta.getVariable( "Internal.Transformation.Filename.Directory" );

    EnterNumberDialog numberDialog =
      new EnterNumberDialog( shell, props.getDefaultPreviewSize(), BaseMessages.getString( PKG,
        "CsvInputDialog.PreviewSize.DialogTitle" ), BaseMessages.getString( PKG,
        "AvroInputDialog.PreviewSize.DialogMessage" ) );
    int previewSize = numberDialog.open();

    if ( previewSize > 0 ) {
      TransPreviewProgressDialog progressDialog =
        new TransPreviewProgressDialog( shell, previewMeta, new String[] { wStepname.getText() },
          new int[] { previewSize } );
      progressDialog.open();

      Trans trans = progressDialog.getTrans();
      String loggingText = progressDialog.getLoggingText();

      if ( !progressDialog.isCancelled() ) {
        if ( trans.getResult() != null && trans.getResult().getNrErrors() > 0 ) {
          EnterTextDialog etd =
            new EnterTextDialog( shell, BaseMessages.getString( PKG, "System.Dialog.PreviewError.Title" ),
              BaseMessages.getString( PKG, "System.Dialog.PreviewError.Message" ), loggingText, true );
          etd.setReadOnly();
          etd.open();
        }
      }

      PreviewRowsDialog prd =
        new PreviewRowsDialog( shell, transMeta, SWT.NONE, wStepname.getText(), progressDialog
          .getPreviewRowsMeta( wStepname.getText() ),
          progressDialog.getPreviewRows( wStepname.getText() ), loggingText );
      prd.open();
    }
  }

  private void getIncomingFields() {
    try {
      RowMetaInterface r = transMeta.getPrevStepFields( stepname );
      if ( r != null ) {
        BaseStepDialog.getFieldsFromPrevious( r, m_lookupView, 1, new int[] { 1 }, null, -1, -1, null );
      }
    } catch ( KettleException e ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Title" ), BaseMessages
        .getString( PKG, "System.Dialog.GetFieldsFailed.Message" ), e );
    }
  }

  private void populateFieldsCombo() {
    StepMeta stepMeta = transMeta.findStep( stepname );

    if ( stepMeta != null ) {
      try {
        RowMetaInterface rowMeta = transMeta.getPrevStepFields( stepMeta );
        if ( rowMeta != null && rowMeta.size() > 0 ) {
          m_avroFieldNameText.removeAll();
          m_schemaFieldNameText.removeAll();
          for ( int i = 0; i < rowMeta.size(); i++ ) {
            ValueMetaInterface vm = rowMeta.getValueMeta( i );
            String fieldName = vm.getName();
            m_avroFieldNameText.add( fieldName );
            m_schemaFieldNameText.add( fieldName );
          }
        }
      } catch ( KettleException ex ) {
        new ErrorDialog( shell, BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Title" ), BaseMessages
          .getString( PKG, "System.Dialog.GetFieldsFailed.Message" ), ex );
      }
    }
  }
}
