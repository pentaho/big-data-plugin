/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util;

import org.apache.commons.io.FileUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.endpoints.CachedFileItemStream;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.endpoints.HadoopClusterManager;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.model.ThinNameClusterModel;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.widget.TextVar;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public abstract class NamedClusterHelper {

  public static final int ONE_COLUMN = 1;
  public static final int TWO_COLUMNS = 2;

  enum FileType {
    CONFIGURATION( "configuration" ),
    DRIVER( ".kar" );

    private final String val;

    FileType( String val ) {
      this.val = val;
    }

    String getValue() {
      return this.val;
    }
  }

  public static Label createLabel( Composite parent, String text, GridData gd, PropsUI props ) {
    Label label = new Label( parent, SWT.NONE );
    label.setText( text );
    label.setLayoutData( gd );
    props.setLook( label );
    return label;
  }

  public static Label createLabelWithStyle( Composite parent, String text, GridData gd, PropsUI props, int style ) {
    Label label = new Label( parent, style );
    label.setText( text );
    label.setLayoutData( gd );
    props.setLook( label );
    return label;
  }

  public static TextVar createText( Composite parent, String text, GridData gd, PropsUI props,
                                    VariableSpace variableSpace ) {
    return createText( parent, text, gd, props, variableSpace, null );
  }

  public static TextVar createText( Composite parent, String text, GridData gd, PropsUI props,
                                    VariableSpace variableSpace, Listener listener ) {
    TextVar textVar =
      new TextVar( variableSpace, parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    textVar.setText( text );
    textVar.setLayoutData( gd );
    if ( listener != null ) {
      textVar.getTextWidget().addListener( SWT.CHANGED, listener );
    }
    props.setLook( textVar );
    return textVar;
  }

  public static Map<String, CachedFileItemStream> processSiteFiles( ThinNameClusterModel model,
                                                                    HadoopClusterManager manager )
    throws BadSiteFilesException, IOException {
    Map<String, CachedFileItemStream> siteFiles = new HashMap<>();
    List<AbstractMap.SimpleImmutableEntry<String, String>> files = model.getSiteFiles();
    for ( AbstractMap.SimpleImmutableEntry<String, String> file : files ) {
      File siteFile = null;
      String fileName = null;
      if ( file.getValue().equals( "keytabAuthFile" ) ) {
        siteFile = new File( file.getKey() );
        fileName = "keytabAuthFile";
      } else if ( file.getValue().equals( "keytabImpFile" ) ) {
        siteFile = new File( file.getKey() );
        fileName = "keytabImpFile";
      } else {
        siteFile = new File( file.getKey() + file.getValue() );
        fileName = siteFile.getName();
      }
      InputStream fileInputStream = null;
      try {
        fileInputStream = new FileInputStream( siteFile );
      } catch ( FileNotFoundException e ) {
        if ( file.getKey().isEmpty() ) {
          if ( manager.getNamedClusterByName( model.getName() ) != null ) {
            fileInputStream = manager.getSiteFileInputStream( model.getName(), file.getValue() );
          } else {
            fileInputStream = manager.getSiteFileInputStream( model.getOldName(), file.getValue() );
          }
        } else {
          throw new BadSiteFilesException();
        }
      }
      List<CachedFileItemStream> fileItemStreams =
        copyAndUnzip( fileInputStream, FileType.CONFIGURATION, siteFile.getName(), fileName, manager );
      for ( CachedFileItemStream cachedFileItemStream : fileItemStreams ) {
        siteFiles.put( cachedFileItemStream.getFieldName(), cachedFileItemStream );
      }
    }
    return siteFiles;
  }

  public static List<CachedFileItemStream> copyAndUnzip( InputStream fileInputStream, FileType fileType,
                                                         String fileName, String realFileName,
                                                         HadoopClusterManager manager )
    throws IOException {
    List<CachedFileItemStream> unzippedFileItemStreams = new ArrayList<>();
    if ( realFileName.endsWith( ".zip" ) ) {
      try ( ZipInputStream zis = new ZipInputStream( fileInputStream ) ) {
        for ( ZipEntry zipEntry = zis.getNextEntry(); zipEntry != null; zipEntry = zis.getNextEntry() ) {
          if ( !zipEntry.isDirectory() ) {
            // Remove all directory structure from the zip file names and only unzip the files
            String[] split = zipEntry.getName().split( "/" ); //zip files always use forward slash
            String unzippedFileName = split[ split.length - 1 ];
            if ( isValidUpload( unzippedFileName, fileType, manager ) ) {
              CachedFileItemStream unzippedFileItemStream =
                new CachedFileItemStream( zis, unzippedFileName, unzippedFileName );
              unzippedFileItemStream.setLastModified( zipEntry.getLastModifiedTime().toMillis() );
              unzippedFileItemStreams.add( unzippedFileItemStream );
            }
          }
        }
      }
    } else {
      // File is not zipped
      if ( isValidUpload( realFileName, fileType, manager ) ) {
        unzippedFileItemStreams.add( new CachedFileItemStream( fileInputStream, fileName,
          realFileName ) );
      }
    }
    return unzippedFileItemStreams;
  }

  private static boolean isValidUpload( String fileName, FileType fileType, HadoopClusterManager manager ) {
    boolean valid = ( fileType.equals( FileType.CONFIGURATION ) && manager.isValidConfigurationFile( fileName ) )
      ||
      ( fileType.equals( FileType.DRIVER ) && fileName.endsWith( FileType.DRIVER.getValue() ) );
    return valid;
  }

  public static boolean processDriverFile( String driverFile, HadoopClusterManager manager ) throws Exception {
    boolean result = false;
    File file = new File( driverFile );
    FileInputStream driverStream = new FileInputStream( file );
    if ( isValidUpload( file.getName(), FileType.DRIVER, manager ) ) {
      String destination = Const.getShimDriverDeploymentLocation();
      FileUtils.copyInputStreamToFile( driverStream,
        new File( destination + File.separator + file.getName() ) );
      result = true;
    }
    return result;
  }
}
