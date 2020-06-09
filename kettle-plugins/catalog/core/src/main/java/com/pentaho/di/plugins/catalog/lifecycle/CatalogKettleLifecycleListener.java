/*!
 * HITACHI VANTARA PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2020 Hitachi Vantara. All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Hitachi Vantara and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Hitachi Vantara and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Hitachi Vantara is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Hitachi Vantara,
 * explicitly covering such access.
 */

package com.pentaho.di.plugins.catalog.lifecycle;

import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.pentaho.di.connections.ConnectionManager;
import org.pentaho.di.core.annotations.KettleLifecyclePlugin;
import org.pentaho.di.core.lifecycle.KettleLifecycleListener;
import org.pentaho.di.core.lifecycle.LifecycleException;
import org.pentaho.di.core.vfs.KettleVFS;
import com.pentaho.di.plugins.catalog.provider.CatalogDetails;
import com.pentaho.di.plugins.catalog.provider.CatalogProvider;
import com.pentaho.di.plugins.catalog.vfs.CatalogFileProvider;

import java.util.Arrays;

/**
 * Add the VFS provider and Named Connection
 */
@KettleLifecyclePlugin( id = "CatalogKettleLifecycleListener", name = "CatalogKettleLifecycleListener" )
public class CatalogKettleLifecycleListener implements KettleLifecycleListener {

  public void onEnvironmentInit() throws LifecycleException {
    try {
      FileSystemManager fsm = KettleVFS.getInstance().getFileSystemManager();
      if ( ( fsm instanceof DefaultFileSystemManager )
        && !Arrays.asList( fsm.getSchemes() ).contains( CatalogFileProvider.SCHEME ) ) {
        ( (DefaultFileSystemManager) fsm )
          .addProvider( CatalogFileProvider.SCHEME, new CatalogFileProvider() );
      }
      ConnectionManager.getInstance().addConnectionProvider( CatalogDetails.CATALOG, new CatalogProvider() );
    } catch ( FileSystemException e ) {
      throw new LifecycleException( e.getMessage(), false );
    }
  }

  public void onEnvironmentShutdown() {
    // Do nothing.
  }
}
