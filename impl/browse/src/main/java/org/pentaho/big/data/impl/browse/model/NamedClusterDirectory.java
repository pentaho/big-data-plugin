/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2019 by Hitachi Vantara : http://www.pentaho.com
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

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.pentaho.big.data.impl.browse.NamedClusterProvider;
import org.pentaho.di.plugins.fileopensave.api.providers.Directory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class NamedClusterDirectory extends NamedClusterFile implements Directory {
  private boolean hasChildren;
  private boolean canAddChildren;
  private List<NamedClusterFile> children = new ArrayList<>();

  public static final String DIRECTORY = "folder";

  @Override public String getType() {
    return DIRECTORY;
  }

  public boolean hasChildren() {
    return hasChildren;
  }

  public void setHasChildren( boolean hasChildren ) {
    this.hasChildren = hasChildren;
  }

  public List<NamedClusterFile> getChildren() {
    return children;
  }

  public void setChildren( List<NamedClusterFile> children ) {
    this.children = children;
  }

  public void addChild( NamedClusterFile file ) {
    this.children.add( file );
  }

  public boolean isHasChildren() {
    return hasChildren;
  }

  public void setCanAddChildren( boolean canAddChildren ) {
    this.canAddChildren = canAddChildren;
  }

  @Override public boolean isCanAddChildren() {
    return this.canAddChildren;
  }

  public static NamedClusterDirectory create( String parent, FileObject fileObject ) {
    NamedClusterDirectory namedClusterDirectory = new NamedClusterDirectory();
    namedClusterDirectory.setName( fileObject.getName().getBaseName() );
    namedClusterDirectory.setPath( fileObject.getName().getFriendlyURI() );
    namedClusterDirectory.setParent( parent );
    namedClusterDirectory.setRoot( NamedClusterProvider.NAME );
    namedClusterDirectory.setCanEdit( true );
    namedClusterDirectory.setHasChildren( true );
    namedClusterDirectory.setCanAddChildren( true );
    try {
      namedClusterDirectory.setDate( new Date( fileObject.getContent().getLastModifiedTime() ) );
    } catch ( FileSystemException e ) {
      namedClusterDirectory.setDate( new Date() );
    }
    return namedClusterDirectory;
  }
}
