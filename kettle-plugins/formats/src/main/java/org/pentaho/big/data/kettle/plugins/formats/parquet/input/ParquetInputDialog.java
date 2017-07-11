/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2017 by Pentaho : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.formats.parquet.input;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.big.data.kettle.plugins.formats.FormatInputField;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.trans.step.BaseFileStepDialog;
import org.pentaho.di.ui.util.SwtSvgImageUtil;
import org.pentaho.vfs.ui.CustomVfsUiPanel;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

public class ParquetInputDialog extends BaseFileStepDialog<ParquetInputMetaBase> {

  private static final int DIALOG_WIDTH = 526;

  private static final int DIALOG_HEIGHT = 506;

  private static final int MARGIN = 15;

  private static final String[] FILES_FILTERS = { "*.*" };

  private static final int AVRO_PATH_COLUMN_INDEX = 1;

  private static final int FIELD_NAME_COLUMN_INDEX = 2;

  private static final int FIELD_TYPE_COLUMN_INDEX = 3;

  private final String[] fileFilterNames = new String[] { BaseMessages.getString( PKG, "System.FileType.AllFiles" ) };

  private TableView wInputFields;

  private TextVar wPath;

  private Button wbBrowse;

  private VFSScheme selectedVFSScheme;

  public ParquetInputDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (ParquetInputMetaBase) in, transMeta, sname );
  }

  @Override
  protected void createUI() {
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = MARGIN;
    formLayout.marginHeight = MARGIN;

    shell.setSize( DIALOG_WIDTH, DIALOG_HEIGHT );
    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "ParquetInputDialog.Shell.Title" ) );

    lsOK = event -> ok();
    lsCancel = event -> cancel();

    Label wicon = new Label( shell, SWT.RIGHT );
    wicon.setImage( getImage() );
    props.setLook( wicon );
    new FD( wicon ).top( 0, 0 ).right( 100, 0 ).apply();

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "ParquetInputDialog.StepName.Label" ) );
    props.setLook( wlStepname );
    new FD( wlStepname ).left( 0, 0 ).top( 0, 0 ).apply();

    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    new FD( wStepname ).left( 0, 0 ).top( wlStepname, 5 ).width( 250 ).apply();

    Label spacer = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    new FD( spacer ).height( 1 ).left( 0, 0 ).top( wStepname, 15 ).right( 100, 0 ).apply();

    Label wlLocation = new Label( shell, SWT.RIGHT );
    wlLocation.setText( BaseMessages.getString( PKG, "ParquetInputDialog.Location.Label" ) );
    props.setLook( wlLocation );
    new FD( wlLocation ).left( 0, 0 ).top( spacer, MARGIN ).apply();

    CCombo wLocation = new CCombo( shell, SWT.BORDER );
    try {
      List<VFSScheme> availableVFSSchemes = getAvailableVFSSchemes();
      availableVFSSchemes.forEach( scheme -> wLocation.add( scheme.getSchemeName() ) );
      wLocation.addListener( SWT.Selection, event -> {
        this.selectedVFSScheme = availableVFSSchemes.get( wLocation.getSelectionIndex() );
      } );
      if ( !availableVFSSchemes.isEmpty() ) {
        wLocation.select( 0 );
        this.selectedVFSScheme = availableVFSSchemes.get( wLocation.getSelectionIndex() );
      }
      //FIXME add some UI message for these exceptions 
    } catch ( KettleFileException ex ) {
      log.logError( BaseMessages.getString( PKG, "ParquetInputDialog.FileBrowser.KettleFileException" ) );
    } catch ( FileSystemException ex ) {
      log.logError( BaseMessages.getString( PKG, "ParquetInputDialog.FileBrowser.FileSystemException" ) );
    }
    props.setLook( wLocation );
    wLocation.addModifyListener( lsMod );
    new FD( wLocation ).left( 0, 0 ).top( wlLocation, 5 ).width( 150 ).apply();

    Label wlPath = new Label( shell, SWT.RIGHT );
    wlPath.setText( BaseMessages.getString( PKG, "ParquetInputDialog.Filename.Label" ) );
    props.setLook( wlPath );
    new FD( wlPath ).left( 0, 0 ).top( wLocation, 10 ).apply();

    wPath = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPath );
    new FD( wPath ).left( 0, 0 ).top( wlPath, 5 ).width( 350 ).apply();

    wbBrowse = new Button( shell, SWT.PUSH );
    props.setLook( wbBrowse );
    wbBrowse.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    wbBrowse.addListener( SWT.Selection, event -> browseForFileInputPath() );
    new FD( wbBrowse ).left( wPath, 5 ).top( wlPath, 5 ).apply();

    Label wlFields = new Label( shell, SWT.RIGHT );
    wlFields.setText( BaseMessages.getString( PKG, "ParquetInputDialog.Fields.Label" ) );
    props.setLook( wlFields );
    new FD( wlFields ).left( 0, 0 ).top( wPath, 10 ).apply();

    Button wGetFields = new Button( shell, SWT.PUSH );
    wGetFields.setText( BaseMessages.getString( PKG, "ParquetInputDialog.Fields.Get" ) );
    wGetFields.addListener( SWT.Selection, event -> {
      throw new RuntimeException( "Requires Shim API changes" );
    } );
    props.setLook( wGetFields );

    ColumnInfo[] parameterColumns =
        new ColumnInfo[] { new ColumnInfo( BaseMessages.getString( PKG, "ParquetInputDialog.Fields.column.AvroPath" ),
            ColumnInfo.COLUMN_TYPE_TEXT, false, false, 248 ), new ColumnInfo( BaseMessages.getString( PKG,
                "ParquetInputDialog.Fields.column.Name" ), ColumnInfo.COLUMN_TYPE_TEXT, false, false, 119 ),
          new ColumnInfo( BaseMessages.getString( PKG, "ParquetInputDialog.Fields.column.Type" ),
              ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaFactory.getValueMetaNames() ) };

    wInputFields =
        new TableView( transMeta, shell, SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER, parameterColumns, 0, lsMod,
            props );
    props.setLook( wInputFields );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, lsCancel );
    new FD( wCancel ).right( 100, 0 ).bottom( 100, 0 ).apply();

    // Some buttons
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOK.addListener( SWT.Selection, lsOK );
    new FD( wOK ).right( wCancel, -5 ).bottom( 100, 0 ).apply();

    wPreview = new Button( shell, SWT.PUSH );
    wPreview.setText( BaseMessages.getString( PKG, "ParquetInputDialog.Preview.Button" ) );
    wPreview.addListener( SWT.Selection, event -> {
      throw new RuntimeException( "Requires Shim API changes" );
    } );
    new FD( wPreview ).right( 50, 0 ).bottom( 100, 0 ).apply();

    Label hSpacer = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    new FD( hSpacer ).height( 1 ).left( 0, 0 ).bottom( wCancel, -15 ).right( 100, 0 ).apply();

    new FD( wGetFields ).bottom( hSpacer, -15 ).right( 100, 0 ).apply();
    new FD( wInputFields ).left( 0, 0 ).right( 100, 0 ).top( wlFields, 5 ).bottom( wGetFields, -10 ).apply();

  }

  protected void browseForFileInputPath() {
    try {
      FileObject initialFile = getInitialFile();
      FileObject rootFile = initialFile.getFileSystem().getRoot();
      VfsFileChooserDialog fileChooserDialog = getVfsFileChooserDialog( rootFile, initialFile );
      FileObject selectedFile =
          fileChooserDialog.open( shell, new String[] {}, selectedVFSScheme.getScheme(), true, null, FILES_FILTERS,
              fileFilterNames, true, VfsFileChooserDialog.VFS_DIALOG_OPEN_FILE_OR_DIRECTORY, false, true );
      if ( selectedFile != null ) {
        wPath.setText( selectedFile.getURL().toString() );
      }
    } catch ( KettleFileException ex ) {
      log.logError( BaseMessages.getString( PKG, "ParquetInputDialog.FileBrowser.KettleFileException" ) );
    } catch ( FileSystemException ex ) {
      log.logError( BaseMessages.getString( PKG, "ParquetInputDialog.FileBrowser.FileSystemException" ) );
    }
  }

  List<VFSScheme> getAvailableVFSSchemes() throws KettleFileException, FileSystemException {
    VfsFileChooserDialog fileChooserDialog = getVfsFileChooserDialog();
    List<CustomVfsUiPanel> customVfsUiPanels = fileChooserDialog.getCustomVfsUiPanels();
    List<VFSScheme> vfsSchemes = new ArrayList<>();
    customVfsUiPanels.forEach( vfsPanel -> {
      VFSScheme scheme = new VFSScheme( vfsPanel.getVfsScheme(), vfsPanel.getVfsSchemeDisplayText() );
      vfsSchemes.add( scheme );
    } );
    return vfsSchemes;
  }

  FileObject getInitialFile() throws KettleFileException {
    FileObject initialFile = null;
    String filePath = wPath.getText();
    if ( filePath != null && !filePath.isEmpty() ) {
      String fileName = transMeta.environmentSubstitute( filePath );
      if ( fileName != null && !fileName.isEmpty() ) {
        initialFile = KettleVFS.getFileObject( fileName );
      }
    }
    if ( initialFile == null ) {
      initialFile = KettleVFS.getFileObject( Spoon.getInstance().getLastFileOpened() );
    }
    return initialFile;
  }

  VfsFileChooserDialog getVfsFileChooserDialog() throws KettleFileException, FileSystemException {
    return getVfsFileChooserDialog( null, null );
  }

  VfsFileChooserDialog getVfsFileChooserDialog( FileObject rootFile, FileObject initialFile )
    throws KettleFileException, FileSystemException {
    return getSpoon().getVfsFileChooserDialog( rootFile, initialFile );
  }

  Spoon getSpoon() {
    return Spoon.getInstance();
  }

  protected Image getImage() {
    return SwtSvgImageUtil.getImage( shell.getDisplay(), getClass().getClassLoader(), "PI.svg", ConstUI.ICON_SIZE,
        ConstUI.ICON_SIZE );
  }

  /**
   * Read the data from the meta object and show it in this dialog.
   */
  @Override
  protected void getData( ParquetInputMetaBase meta ) {
    if ( meta.inputFiles.fileName.length > 0 ) {
      wPath.setText( meta.inputFiles.fileName[0] );
    }
    int nrFields = meta.inputFields.length;
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = null;
      if ( i >= wInputFields.table.getItemCount() ) {
        item = wInputFields.table.getItem( i );
      } else {
        item = new TableItem( wInputFields.table, SWT.NONE );
      }

      FormatInputField inputField = meta.inputFields[i];
      if ( inputField.getPath() != null ) {
        item.setText( AVRO_PATH_COLUMN_INDEX, inputField.getPath() );
      }
      if ( inputField.getName() != null ) {
        item.setText( FIELD_NAME_COLUMN_INDEX, inputField.getName() );
      }
      item.setText( FIELD_TYPE_COLUMN_INDEX, inputField.getTypeDesc() );
    }
  }

  /**
   * Fill meta object from UI options.
   */
  @Override
  protected void getInfo( ParquetInputMetaBase meta, boolean preview ) {
    String filePath = wPath.getText();
    if ( filePath != null && !filePath.isEmpty() ) {
      meta.allocateFiles( 1 );
      meta.inputFiles.fileName[0] = wPath.getText();
    }
    int nrFields = wInputFields.nrNonEmpty();
    meta.inputFields = new FormatInputField[nrFields];
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = wInputFields.getNonEmpty( i );
      FormatInputField field = new FormatInputField();
      field.setPath( item.getText( AVRO_PATH_COLUMN_INDEX ) );
      field.setName( item.getText( FIELD_NAME_COLUMN_INDEX ) );
      field.setType( ValueMetaFactory.getIdForValueMeta( item.getText( FIELD_TYPE_COLUMN_INDEX ) ) );
      meta.inputFields[i] = field;
    }
  }
}
