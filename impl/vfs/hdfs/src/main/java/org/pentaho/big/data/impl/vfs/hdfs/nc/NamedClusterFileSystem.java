/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
 * <p>
 * ******************************************************************************
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 ******************************************************************************/

package org.pentaho.big.data.impl.vfs.hdfs.nc;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.pentaho.big.data.impl.vfs.hdfs.HDFSFileSystem;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystem;

import java.net.URI;


public class NamedClusterFileSystem extends HDFSFileSystem {

  private final URI realFileSystemURI;

  public NamedClusterFileSystem( final FileName rootName, final URI realFileSystemURI, final FileSystemOptions fileSystemOptions,
                                 HadoopFileSystem hdfs ) {
    super( rootName, fileSystemOptions, hdfs );
    this.realFileSystemURI = realFileSystemURI;
  }

  @Override protected FileObject createFile( AbstractFileName name ) throws Exception {
    return new NamedClusterFileObject( name, this );
  }

  public URI getRealFileSystemURI() {
    return realFileSystemURI;
  }

}
