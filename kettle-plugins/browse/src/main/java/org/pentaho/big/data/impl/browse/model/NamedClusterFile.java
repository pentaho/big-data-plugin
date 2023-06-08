/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2019-2023 by Hitachi Vantara : http://www.pentaho.com
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
