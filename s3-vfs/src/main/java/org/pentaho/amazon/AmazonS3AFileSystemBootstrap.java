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
import org.pentaho.s3a.vfs.S3AFileProvider;

import java.util.Arrays;
import java.util.function.Supplier;

/**
 * Registers the Amazon S3 VFS File Provider dynamically since it is bundled with our plugin and will not automatically
 * be registered through the normal class path search the default FileSystemManager performs.
 */
@KettleLifecyclePlugin( id = "AmazonS3AFileSystemBootstrap", name = "Amazon S3A FileSystem Bootstrap" )
public class AmazonS3AFileSystemBootstrap implements KettleLifecycleListener {
  private static final Class<?> amazonS3AFileSystemBootstrapClass = AmazonS3AFileSystemBootstrap.class;
  private LogChannelInterface log = new LogChannel( AmazonS3AFileSystemBootstrap.class.getName() );
  private Supplier<ConnectionManager> connectionManager = ConnectionManager::getInstance;

  /**
   * @return the i18n display text for the S3 file system
   */
  public static String getS3AFileSystemDisplayText() {
    return BaseMessages.getString( amazonS3AFileSystemBootstrapClass, "S3AVfsFileChooserDialog.FileSystemChoice.S3.Label" );
  }

  @Override
  public void onEnvironmentInit() throws LifecycleException {
    try {
      // Register S3 as a file system type with VFS
      FileSystemManager fsm = KettleVFS.getInstance().getFileSystemManager();
      if ( fsm instanceof DefaultFileSystemManager
        && !Arrays.asList( fsm.getSchemes() ).contains( S3AFileProvider.SCHEME ) ) {
        ( (DefaultFileSystemManager) fsm ).addProvider( S3AFileProvider.SCHEME, new S3AFileProvider() );
      }

      if ( connectionManager.get() != null ) {
        connectionManager.get().addConnectionProvider( S3FileProvider.SCHEME, new S3Provider() );
        VFSLookupFilter vfsLookupFilter = new VFSLookupFilter();
        connectionManager.get().addLookupFilter( vfsLookupFilter );
      }
    } catch ( FileSystemException e ) {
      log.logError( BaseMessages.getString( amazonS3AFileSystemBootstrapClass, "AmazonSpoonPlugin.StartupError.FailedToLoadS3Driver" ) );
    }
  }

  @Override
  public void onEnvironmentShutdown() {
    // noop
  }
}
