/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2020 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.big.data.kettle.plugins.formats.impl.output;

import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.vfs.AliasedFileObject;
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

  public PvfsFileAliaser( String finalFilePath, VariableSpace variableSpace, IPvfsAliasGenerator aliasGenerator,
                          boolean isOverwriteOutput, LogChannelInterface log ) {
    this.finalFilePath = finalFilePath;
    this.variableSpace = variableSpace;
    this.aliasGenerator = aliasGenerator;
    this.isOverwriteOutput = isOverwriteOutput;
    this.log = log;
  }

  public String generateAlias() throws KettleFileException, FileSystemException, FileAlreadyExistsException {

    FileObject pvfsFileObject = KettleVFS.getFileObject( finalFilePath, variableSpace );
    if ( AliasedFileObject.isAliasedFile( pvfsFileObject ) ) {
      finalFilePath = ( (AliasedFileObject) pvfsFileObject ).getAELSafeURIString();
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
      FileObject srcFile = KettleVFS.getFileObject( temporaryFilePath, variableSpace );
      FileObject destFile = KettleVFS.getFileObject( finalFilePath, variableSpace );
      try ( InputStream in = KettleVFS.getInputStream( srcFile );
            OutputStream out = KettleVFS.getOutputStream( destFile, false ) ) {
        IOUtils.copy( in, out );
      }
    }
  }

  public void deleteTempFileAndFolder() {
    try {
      if ( aliasingIsActive() ) {
        FileObject srcFile = KettleVFS.getFileObject( temporaryFilePath, variableSpace );
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
