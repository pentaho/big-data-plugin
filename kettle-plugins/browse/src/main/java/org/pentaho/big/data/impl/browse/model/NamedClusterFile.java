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


package org.pentaho.big.data.impl.browse.model;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.pentaho.big.data.impl.browse.NamedClusterProvider;
import org.pentaho.di.plugins.fileopensave.api.providers.BaseEntity;
import org.pentaho.di.plugins.fileopensave.api.providers.EntityType;
import org.pentaho.di.plugins.fileopensave.api.providers.File;

import java.util.Date;

public class NamedClusterFile extends BaseEntity implements File {
  public static final String TYPE = "file";

  public NamedClusterFile() {
    // Needed for JSON marshalling
  }

  @Override public String getType() {
    return TYPE;
  }

  @Override public String getProvider() {
    return NamedClusterProvider.TYPE;
  }

  public static NamedClusterFile create( String parent, FileObject fileObject ) {
    NamedClusterFile namedClusterFile = new NamedClusterFile();
    namedClusterFile.setName( fileObject.getName().getBaseName() );
    namedClusterFile.setPath( fileObject.getName().getFriendlyURI() );
    namedClusterFile.setParent( parent );
    namedClusterFile.setRoot( NamedClusterProvider.NAME );
    namedClusterFile.setCanEdit( true );
    try {
      namedClusterFile.setDate( new Date( fileObject.getContent().getLastModifiedTime() ) );
    } catch ( FileSystemException ignored ) {
      namedClusterFile.setDate( new Date() );
    }
    return namedClusterFile;
  }

  @Override public boolean equals( Object obj ) {
    // If the object is compared with itself then return true
    if ( obj == this ) {
      return true;
    }

    if ( !( obj instanceof NamedClusterFile ) ) {
      return false;
    }

    NamedClusterFile compare = (NamedClusterFile) obj;
    // This comparison depends on `getProvider()` to always return a hardcoded value
    return compare.getProvider().equals( getProvider() )
      && StringUtils.equals( compare.getPath(), getPath() );
  }

  @Override
  public EntityType getEntityType(){
    return EntityType.NAMED_CLUSTER_FILE;
  }

}
