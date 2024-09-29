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
