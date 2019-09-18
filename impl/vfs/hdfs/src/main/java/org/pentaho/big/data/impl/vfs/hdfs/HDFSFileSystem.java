/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.impl.vfs.hdfs;

import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileSystem;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystem;

import java.util.Collection;

public class HDFSFileSystem extends AbstractFileSystem implements FileSystem {
  private final HadoopFileSystem hdfs;

  public HDFSFileSystem( final FileName rootName, final FileSystemOptions fileSystemOptions,
                            HadoopFileSystem hdfs ) {
    super( rootName, null, fileSystemOptions );
    this.hdfs = hdfs;
  }

  @Override
  @SuppressWarnings( { "unchecked", "rawtypes" } )
  protected void addCapabilities( Collection caps ) {
    caps.addAll( HDFSFileProvider.capabilities );
    // Adding capabilities depending on configuration settings
    try {
      if ( Boolean.parseBoolean( getHDFSFileSystem().getProperty( "dfs.support.append", "true" ) ) ) {
        caps.add( Capability.APPEND_CONTENT );
      }
    } catch ( FileSystemException e ) {
      throw new RuntimeException( e );
    }
  }

  @Override protected FileObject createFile( AbstractFileName name ) throws Exception {
    return new HDFSFileObject( name, this );
  }

  public HadoopFileSystem getHDFSFileSystem() throws FileSystemException {
    return hdfs;
  }
}
