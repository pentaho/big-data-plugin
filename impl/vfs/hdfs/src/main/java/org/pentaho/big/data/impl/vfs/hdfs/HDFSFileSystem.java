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
      if ( getHDFSFileSystem() != null && Boolean.parseBoolean( getHDFSFileSystem().getProperty( "dfs.support.append", "true" ) ) ) {
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
