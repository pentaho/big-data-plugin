/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2020 by Hitachi Vantara : http://www.pentaho.com
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


import org.apache.commons.vfs2.FileSystemException;
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


}
