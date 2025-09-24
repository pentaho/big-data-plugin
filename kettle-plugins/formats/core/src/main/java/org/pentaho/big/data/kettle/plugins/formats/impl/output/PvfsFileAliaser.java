/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package org.pentaho.big.data.kettle.plugins.formats.impl.output;

import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.pentaho.di.core.bowl.Bowl;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.vfs.AliasedFileObject;
import org.pentaho.di.core.vfs.IKettleVFS;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.hadoop.shim.api.format.IPvfsAliasGenerator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;

/**
 * Logic to use a temporary file for output and then copy that file to some VFS/PVFS scheme that wasn't original
 * supoorted for the output content.
 */
public class PvfsFileAliaser {
  private String finalFilePath;

  private String temporaryFilePath;

  private VariableSpace variableSpace;

  private IPvfsAliasGenerator aliasGenerator;

  private boolean isOverwriteOutput;

  private LogChannelInterface log;

  private IKettleVFS ikettleVFS;

  public PvfsFileAliaser( Bowl bowl, String finalFilePath, VariableSpace variableSpace,
                          IPvfsAliasGenerator aliasGenerator, boolean isOverwriteOutput, LogChannelInterface log ) {
    this.ikettleVFS = KettleVFS.getInstance( bowl );
    this.finalFilePath = finalFilePath;
    this.variableSpace = variableSpace;
    this.aliasGenerator = aliasGenerator;
    this.isOverwriteOutput = isOverwriteOutput;
    this.log = log;
  }

  public String generateAlias() throws KettleFileException, FileSystemException, FileAlreadyExistsException {

    FileObject pvfsFileObject = ikettleVFS.getFileObject( finalFilePath, variableSpace );
    if ( AliasedFileObject.isAliasedFile( pvfsFileObject ) ) {
      finalFilePath = ( (AliasedFileObject) pvfsFileObject ).getOriginalURIString();
    }
    //See if we need to use a another URI because the HadoopFileSystem is not supported for this URL.
    String aliasedFile = aliasGenerator.generateAlias( finalFilePath );
    temporaryFilePath = finalFilePath;
    if ( aliasedFile != null ) {
      if ( pvfsFileObject.exists() ) {
        if ( isOverwriteOutput ) {
          pvfsFileObject.delete();
        } else {
          throw new FileAlreadyExistsException( temporaryFilePath );
        }
      }
      temporaryFilePath = aliasedFile;  //set the outputFile to the temporary alias file
    }
    return temporaryFilePath;
  }

  public void copyFileToFinalDestination() throws KettleFileException, IOException {
    if ( aliasingIsActive() ) {
      FileObject srcFile = ikettleVFS.getFileObject( temporaryFilePath, variableSpace );
      FileObject destFile = ikettleVFS.getFileObject( finalFilePath, variableSpace );
      try ( InputStream in = KettleVFS.getInputStream( srcFile );
            OutputStream out = ikettleVFS.getOutputStream( destFile, false ) ) {
        IOUtils.copy( in, out );
      }
    }
  }

  public void deleteTempFileAndFolder() {
    try {
      if ( aliasingIsActive() ) {
        FileObject srcFile = ikettleVFS.getFileObject( temporaryFilePath, variableSpace );
        srcFile.getParent().deleteAll();
      }
    } catch ( FileSystemException | KettleFileException e ) {
      log.logError( e.getMessage(), e );
    }
  }

  private boolean aliasingIsActive() {
    return !finalFilePath.equals( temporaryFilePath ) && temporaryFilePath != null && !s3nSwitchedTos3a();
  }

  private boolean s3nSwitchedTos3a() {
    return finalFilePath != null && temporaryFilePath != null && finalFilePath.startsWith( "s3n" ) && temporaryFilePath
      .startsWith( "s3a" );
  }
}
