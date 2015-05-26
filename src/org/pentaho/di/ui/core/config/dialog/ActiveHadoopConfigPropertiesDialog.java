/**
 * ****************************************************************************
 * <p/>
 * Pentaho Big Data
 * <p/>
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
 * <p/>
 * ******************************************************************************
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * <p/>
 * ****************************************************************************
 */
package org.pentaho.di.ui.core.config.dialog;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.FieldDisabledListener;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.spi.HadoopShim;

import java.io.FileOutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * This dialog collects and displays configuration information about the active Hadoop configuration. The information
 * is collected on the client-side, from the shim's config.properties file and the Hadoop config files (*-site.xml)
 */
public class ActiveHadoopConfigPropertiesDialog extends Dialog {
  private static Class<?> PKG = ActiveHadoopConfigPropertiesDialog.class; // for i18n purposes, needed by Translator2!!

  private TableView wFields;
  private FormData fdFields;

  private Button wOK;
  private Listener lsOK;

  private Button wExport;

  private Shell shell;
  private PropsUI props;

  private Properties hadoopConfigProperties;

  private HadoopConfiguration hadoopConfig;

  /**
   * Constructs a new dialog for displaying the active Hadoop config (aka shim) properties
   *
   * @param parent The parent shell to link to
   * @param style  The style in which we want to draw this shell.
   */
  public ActiveHadoopConfigPropertiesDialog( Shell parent, int style ) {
    super( parent, style );
    props = PropsUI.getInstance();
    hadoopConfigProperties = null;
  }

  /**
   * Open the dialog
   *
   * @return the active shim's configuration properties
   */
  public Properties open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE );
    shell.setImage( GUIResource.getInstance().getImageTransGraph() );
    props.setLook( shell );

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "ActiveHadoopConfigPropertiesDialog.Title" ) );

    int margin = Const.MARGIN;

    // Retrieve the active Hadoop configuration (aka "shim")
    try {
      hadoopConfig = HadoopConfigurationBootstrap.getHadoopConfigurationProvider().getActiveConfiguration();
    } catch ( ConfigurationException ce ) {
      // Spoon wouldn't have started in this case, if they deleted the folder afterward then there's not much we
      // can/should do here. Possibly display an error
    }

    Group group = new Group( shell, SWT.SHADOW_ETCHED_IN );
    group.setText( BaseMessages.getString( PKG, "ActiveHadoopConfigPropertiesDialog.GroupTitle" ) );
    FormLayout groupLayout = new FormLayout();
    groupLayout.marginWidth = 3;
    groupLayout.marginHeight = 3;
    group.setLayout( groupLayout );
    FormData fdlGroup = new FormData();
    fdlGroup.left = new FormAttachment( 0, margin );
    fdlGroup.top = new FormAttachment( 0, margin );
    fdlGroup.right = new FormAttachment( 100, margin );
    group.setLayoutData( fdlGroup );
    props.setLook( group );

    // Cluster type line
    //
    Label wlName = new Label( group, SWT.NONE );
    wlName.setText( BaseMessages.getString( PKG,
      "ActiveHadoopConfigPropertiesDialog.ClusterType",
      hadoopConfig.getConfigProperties().getProperty( "name", "unknown" ) ) );
    props.setLook( wlName );
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment( 0, 0 );
    fdlName.top = new FormAttachment( 0, margin );
    wlName.setLayoutData( fdlName );

    // Config identifier (shim folder name, e.g.)
    //
    Label wlIdentifier = new Label( group, SWT.NONE );
    wlIdentifier.setText( BaseMessages.getString( PKG,
      "ActiveHadoopConfigPropertiesDialog.Identifier", hadoopConfig.getIdentifier() ) );
    props.setLook( wlIdentifier );
    FormData fdlIdentifier = new FormData();
    fdlIdentifier.left = new FormAttachment( 0, 0 );
    fdlIdentifier.top = new FormAttachment( wlName, margin );
    wlIdentifier.setLayoutData( fdlIdentifier );

    // Shim location
    //
    Label wlLocation = new Label( group, SWT.NONE );
    wlLocation.setText( BaseMessages.getString( PKG,
      "ActiveHadoopConfigPropertiesDialog.Location", hadoopConfig.getLocation().getName().getPath() ) );
    props.setLook( wlLocation );
    FormData fdlLocation = new FormData();
    fdlLocation.left = new FormAttachment( 0, 0 );
    fdlLocation.top = new FormAttachment( wlIdentifier, margin );
    wlLocation.setLayoutData( fdlLocation );

    // Security
    //
    Label wlSecurity = new Label( group, SWT.NONE );
    wlLocation.setText( BaseMessages.getString( PKG,
      "ActiveHadoopConfigPropertiesDialog.AuthId",
      hadoopConfig.getConfigProperties().getProperty( "authentication.superuser.provider", "none" ) ) );
    props.setLook( wlSecurity );
    FormData fdlSecurity = new FormData();
    fdlSecurity.left = new FormAttachment( 0, 0 );
    fdlSecurity.top = new FormAttachment( wlLocation, margin );
    wlSecurity.setLayoutData( fdlLocation );

    int FieldsRows = 0;

    ColumnInfo[] colinf =
      new ColumnInfo[]{
        new ColumnInfo(
          BaseMessages.getString( PKG, "ActiveHadoopConfigPropertiesDialog.Name.Label" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "ActiveHadoopConfigPropertiesDialog.Value.Label" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, true ),
      };
    colinf[0].setDisabledListener( new FieldDisabledListener() {
      public boolean isFieldDisabled( int rowNr ) {
        return false;
      }
    } );
    colinf[1].setDisabledListener( new FieldDisabledListener() {
      public boolean isFieldDisabled( int rowNr ) {
        return false;
      }
    } );

    wFields =
      new TableView(
        Variables.getADefaultVariableSpace(), shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf,
        FieldsRows, null, props );

    wFields.setReadonly( true );

    fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( group, 30 );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( 100, -50 );
    wFields.setLayoutData( fdFields );

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    wExport = new Button( shell, SWT.PUSH );
    wExport.setText( BaseMessages.getString( PKG, "ActiveHadoopConfigPropertiesDialog.Button.Export" ) );

    BaseStepDialog.positionBottomButtons( shell, new Button[]{ wOK, wExport }, margin, null );

    // Add listeners
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wOK.addListener( SWT.Selection, lsOK );

    wExport.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        FileDialog dialog = new FileDialog( shell, SWT.SAVE );
        dialog.setFilterExtensions( new String[]{ "*" } );
        dialog.setFilterNames( new String[]{
          BaseMessages.getString( PKG, "System.FileType.AllFiles" ) } );
        if ( dialog.open() != null ) {
          String filename = dialog.getFilterPath() + System.getProperty( "file.separator" ) + dialog.getFileName();
          export( filename );
        }
      }
    } );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    BaseStepDialog.setSize( shell );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return hadoopConfigProperties;
  }

  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    try {
      // Load the active Hadoop config
      //
      HadoopShim shim = hadoopConfig.getHadoopShim();
      ClassLoader loader = shim.getClass().getClassLoader();
      Configuration conf = shim.createConfiguration();
      // Treat the config as a JobConf (which is like a map), so we can get the properties
      Iterable<Map.Entry<String, String>> entries =
        (Iterable<Map.Entry<String, String>>) conf.getAsDelegateConf(
          loader.loadClass( "org.apache.hadoop.mapred.JobConf" ) );
      hadoopConfigProperties = new Properties();
      for ( Map.Entry<String, String> entry : entries ) {
        hadoopConfigProperties.put( entry.getKey(), entry.getValue() );
      }

      // Obtain and sort the list of keys...
      //
      List<String> keys = new ArrayList<String>();
      Enumeration<Object> keysEnum = hadoopConfigProperties.keys();
      while ( keysEnum.hasMoreElements() ) {
        keys.add( (String) keysEnum.nextElement() );
      }
      Collections.sort( keys );

      // Populate the grid...
      //
      for ( int i = 0; i < keys.size(); i++ ) {
        TableItem item = new TableItem( wFields.table, SWT.NONE );

        int pos = 1;
        String key = keys.get( i );
        String value = hadoopConfigProperties.getProperty( key, "" );
        item.setText( pos++, key );
        item.setText( pos++, value );
      }

      wFields.removeEmptyRows();
      wFields.setRowNums();
      wFields.optWidth( true );
    } catch ( Exception e ) {
      new ErrorDialog( shell,
        BaseMessages.getString( PKG, "ActiveHadoopConfigPropertiesDialog.Exception.ErrorLoadingData.Title" ),
        BaseMessages.getString( PKG, "ActiveHadoopConfigPropertiesDialog.Exception.ErrorLoadingData.Message" ), e );
    }
  }

  private void cancel() {
    hadoopConfigProperties = null;
    dispose();
  }

  private void ok() {
    saveConfig();
    dispose();
  }

  /**
   * Saves the properties in the UI elements to a Properties object
   */
  private void saveConfig() {
    hadoopConfigProperties = new Properties();

    int nr = wFields.nrNonEmpty();
    for ( int i = 0; i < nr; i++ ) {
      TableItem item = wFields.getNonEmpty( i );
      int pos = 1;
      String variable = item.getText( pos++ );
      String value = item.getText( pos++ );

      if ( !Const.isEmpty( variable ) ) {
        hadoopConfigProperties.put( variable, value );
      }
    }
  }

  /**
   * Exports all properties to the given file.
   *
   * @param filename The name of the file to contain the exported properties
   */
  private void export( String filename ) {
    // Save the properties file...
    //
    FileOutputStream out = null;
    try {
      saveConfig();
      hadoopConfigProperties.putAll( hadoopConfig.getConfigProperties() );
      out = new FileOutputStream( filename );
      SimpleDateFormat df = new SimpleDateFormat( "yyyy/MM/dd HH:mm:ss.SSS" );
      hadoopConfigProperties.store( out,
        BaseMessages.getString( PKG, "ActiveHadoopConfigPropertiesDialog.Export.Header" ) );

    } catch ( Exception e ) {
      new ErrorDialog( shell,
        BaseMessages.getString( PKG, "ActiveHadoopConfigPropertiesDialog.Exception.ErrorSavingData.Title" ),
        BaseMessages.getString( PKG, "ActiveHadoopConfigPropertiesDialog.Exception.ErrorSavingData.Message" ), e );
    } finally {
      try {
        out.close();
      } catch ( Exception e ) {
        LogChannel.GENERAL.logError( BaseMessages.getString(
          PKG, "ActiveHadoopConfigPropertiesDialog.Exception.ErrorSavingData.Message", filename ), e );
      }
    }
  }
}

