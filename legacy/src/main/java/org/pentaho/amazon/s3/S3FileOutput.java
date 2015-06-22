/*
 * ! ******************************************************************************
 *  *
 *  * Pentaho Data Integration
 *  *
 *  * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
 *  *
 *  *******************************************************************************
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with
 *  * the License. You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  *
 *  *****************************************************************************
 */

package org.pentaho.amazon.s3;

import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.FileSystemOptions;
import org.apache.commons.vfs.auth.StaticUserAuthenticator;
import org.apache.commons.vfs.impl.DefaultFileSystemConfigBuilder;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.textfileoutput.TextFileOutput;

import java.io.OutputStream;

public class S3FileOutput extends TextFileOutput {

  private FileSystemOptions fsOptions;

  public S3FileOutput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  protected FileObject getFileObject( String vfsFilename ) throws KettleFileException {
    return KettleVFS.getFileObject( vfsFilename, getFsOptions() );
  }

  protected FileObject getFileObject( String vfsFilename, VariableSpace space ) throws KettleFileException {
    return KettleVFS.getFileObject( vfsFilename, space, getFsOptions() );
  }

  protected OutputStream getOutputStream( String vfsFilename, VariableSpace space, boolean append )
    throws KettleFileException {
    return KettleVFS.getOutputStream( vfsFilename, space, getFsOptions(), append );
  }

  protected FileSystemOptions createFileSystemOptions() throws KettleFileException {
    try {
      FileSystemOptions opts = new FileSystemOptions();
      S3FileOutputMeta s3Meta = (S3FileOutputMeta) meta;
      DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator( opts,
          new StaticUserAuthenticator( null,
            Encr.decryptPasswordOptionallyEncrypted( environmentSubstitute( s3Meta.getAccessKey() ) ),
            Encr.decryptPasswordOptionallyEncrypted( environmentSubstitute( s3Meta.getSecretKey() ) ) ) );
      return opts;
    } catch ( FileSystemException e ) {
      throw new KettleFileException( e );
    }
  }

  protected FileSystemOptions getFsOptions() throws KettleFileException {
    if ( fsOptions == null ) {
      fsOptions = createFileSystemOptions();
    }
    return fsOptions;
  }

}
