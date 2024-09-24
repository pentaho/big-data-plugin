/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2020 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.amazon;

import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.pentaho.amazon.s3.provider.S3Provider;
import org.pentaho.di.connections.ConnectionManager;
import org.pentaho.di.connections.vfs.VFSLookupFilter;
import org.pentaho.di.core.annotations.KettleLifecyclePlugin;
import org.pentaho.di.core.lifecycle.KettleLifecycleListener;
import org.pentaho.di.core.lifecycle.LifecycleException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.s3.vfs.S3FileProvider;
import org.pentaho.s3n.vfs.S3NFileProvider;

import java.util.Arrays;
import java.util.function.Supplier;

/**
 * Registers the Amazon S3 VFS File Provider dynamically since it is bundled with our plugin and will not automatically
 * be registered through the normal class path search the default FileSystemManager performs.
 */
@KettleLifecyclePlugin( id = "AmazonS3NFileSystemBootstrap", name = "Amazon S3N FileSystem Bootstrap" )
public class AmazonS3NFileSystemBootstrap implements KettleLifecycleListener {
  private static Class<?> amazonS3NFileSystemBootstrapClass = AmazonS3NFileSystemBootstrap.class;
  private LogChannelInterface log = new LogChannel( AmazonS3NFileSystemBootstrap.class.getName() );
  private Supplier<ConnectionManager> connectionManager = ConnectionManager::getInstance;

  /**
   * @return the i18n display text for the S3 file system
   */
  public static String getS3NFileSystemDisplayText() {
    return BaseMessages
      .getString( amazonS3NFileSystemBootstrapClass, "S3NVfsFileChooserDialog.FileSystemChoice.S3.Label" );
  }

  @Override
  public void onEnvironmentInit() throws LifecycleException {
    try {
      // Register S3 as a file system type with VFS
      FileSystemManager fsm = KettleVFS.getInstance().getFileSystemManager();
      if ( fsm instanceof DefaultFileSystemManager && !Arrays.asList( fsm.getSchemes() )
        .contains( S3NFileProvider.SCHEME ) ) {
        ( (DefaultFileSystemManager) fsm ).addProvider( S3NFileProvider.SCHEME, new S3NFileProvider() );
      }

      if ( connectionManager.get() != null ) {
        connectionManager.get().addConnectionProvider( S3FileProvider.SCHEME, new S3Provider() );
        VFSLookupFilter vfsLookupFilter = new VFSLookupFilter();
        connectionManager.get().addLookupFilter( vfsLookupFilter );
      }
    } catch ( FileSystemException e ) {
      log.logError( BaseMessages
        .getString( amazonS3NFileSystemBootstrapClass, "AmazonSpoonPlugin.StartupError.FailedToLoadS3Driver" ) );
    }
  }

  @Override
  public void onEnvironmentShutdown() {
    // noop
  }
}
