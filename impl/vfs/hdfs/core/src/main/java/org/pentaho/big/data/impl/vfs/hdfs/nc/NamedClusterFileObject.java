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


package org.pentaho.big.data.impl.vfs.hdfs.nc;


import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.pentaho.big.data.impl.vfs.hdfs.HDFSFileObject;
import org.pentaho.di.core.vfs.AliasedFileObject;

public class NamedClusterFileObject extends HDFSFileObject implements AliasedFileObject {

  private final String realFileSystemURI;

  public NamedClusterFileObject( final AbstractFileName name, final NamedClusterFileSystem fileSystem ) throws FileSystemException {
    super( name, fileSystem );
    realFileSystemURI = fileSystem.getRealFileSystemURI().toString();
  }

  @Override
  public String getOriginalURIString() {
    return realFileSystemURI + getName().getPath();
  }

  @Override
  public String getAELSafeURIString() {
    return getOriginalURIString();
  }

  @Override
  public boolean delete() throws FileSystemException {
    return delete( Selectors.SELECT_SELF_AND_CHILDREN ) > 0;
  }
}
