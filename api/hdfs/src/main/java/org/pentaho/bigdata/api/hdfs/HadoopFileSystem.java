/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.pentaho.bigdata.api.hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface HadoopFileSystem {
  public static final String FS_DEFAULT_NAME = "fs.default.name";

  OutputStream append( HadoopFileSystemPath path ) throws IOException;

  OutputStream create( HadoopFileSystemPath path ) throws IOException;

  boolean delete( HadoopFileSystemPath path, boolean arg1 ) throws IOException;

  HadoopFileStatus getFileStatus( HadoopFileSystemPath path ) throws IOException;

  boolean mkdirs( HadoopFileSystemPath path ) throws IOException;

  InputStream open( HadoopFileSystemPath path ) throws IOException;

  boolean rename( HadoopFileSystemPath path, HadoopFileSystemPath path2 ) throws IOException;

  void setTimes( HadoopFileSystemPath path, long mtime, long atime ) throws IOException;

  HadoopFileStatus[] listStatus( HadoopFileSystemPath path ) throws IOException;

  HadoopFileSystemPath getPath( String path );

  HadoopFileSystemPath getHomeDirectory();

  HadoopFileSystemPath makeQualified( HadoopFileSystemPath hadoopFileSystemPath );

  void chmod( HadoopFileSystemPath hadoopFileSystemPath, int permissions ) throws IOException;

  boolean exists( HadoopFileSystemPath path ) throws IOException;

  public HadoopFileSystemPath resolvePath( HadoopFileSystemPath path ) throws IOException;

  String getFsDefaultName();
}
