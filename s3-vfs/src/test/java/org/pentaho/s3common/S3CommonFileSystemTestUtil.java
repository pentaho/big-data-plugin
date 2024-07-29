/*!
 * Copyright 2024 Hitachi Vantara.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.pentaho.s3common;

import org.apache.commons.vfs2.CacheStrategy;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FilesCache;
import org.apache.commons.vfs2.cache.DefaultFilesCache;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.VfsComponentContext;
import org.mockito.Mockito;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.pentaho.s3.vfs.S3FileName;
import org.pentaho.s3.vfs.S3FileProvider;
import org.pentaho.s3.vfs.S3FileSystem;

import com.amazonaws.services.s3.AmazonS3;

public class S3CommonFileSystemTestUtil {

  /** Get an S3FileSystem using given test client */
  public static S3FileSystem createS3FileSystem( S3FileName root, FileSystemOptions opts, AmazonS3 client ) throws FileSystemException {
    // generic wiring
    final DefaultFileSystemManager fsm = new DefaultFileSystemManager();
    FilesCache cache = new DefaultFilesCache();// mock( FilesCache.class );
    fsm.setFilesCache( cache );
    fsm.setCacheStrategy( CacheStrategy.ON_RESOLVE );
    VfsComponentContext context = mock( VfsComponentContext.class );
    when( context.getFileSystemManager() ).thenReturn( fsm );

    // s3 specific
    S3FileSystem fileSystem = new S3FileSystem( root, new FileSystemOptions() ) {
      @Override
      public AmazonS3 getS3Client() {
        return client;
      }
    };
    fsm.addProvider( S3FileProvider.SCHEME, new S3FileProvider() {
      @Override
      protected FileSystem doCreateFileSystem( FileName name, FileSystemOptions fileSystemOptions ) {
        return fileSystem;
      }
    });
    fileSystem.setContext( context );

    // add capabilities
    fileSystem.init();
    return fileSystem;
  }

  public static S3CommonFileSystem stubRegionUnSet( S3CommonFileSystem fileSystem ) {
    S3CommonFileSystem fileSystemSpy = Mockito.spy( fileSystem );
    doReturn(false).when(fileSystemSpy).isRegionSet();
    return fileSystemSpy;
  }
}
