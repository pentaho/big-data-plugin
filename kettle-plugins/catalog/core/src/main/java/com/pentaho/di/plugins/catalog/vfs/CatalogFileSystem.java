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

package com.pentaho.di.plugins.catalog.vfs;

import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileSystem;
import org.apache.commons.vfs2.provider.GenericURLFileName;
import org.pentaho.di.core.vfs.KettleVFS;
import com.pentaho.di.plugins.catalog.api.CatalogClient;

import java.util.Collection;

public class CatalogFileSystem extends AbstractFileSystem {

  public CatalogFileSystem( FileName rootName, FileObject parentLayer,
                            FileSystemOptions fileSystemOptions ) {
    super( rootName, parentLayer, fileSystemOptions );
  }

  @Override protected FileObject createFile( AbstractFileName abstractFileName ) throws Exception {
    GenericURLFileName urlFileName = (GenericURLFileName) abstractFileName;

    String host = urlFileName.getHostName();
    String port = String.valueOf( urlFileName.getPort() );

    CatalogClient catalogClient = new CatalogClient( host, port );
    catalogClient.getAuthentication().login( "", "" );

    String key = abstractFileName.getBaseName();

    catalogClient.getDataResources().read( key );

    return KettleVFS.getFileObject( null );
  }

  @Override protected void addCapabilities( Collection<Capability> collection ) {
    collection.addAll( CatalogFileProvider.capabilities );
  }
}
