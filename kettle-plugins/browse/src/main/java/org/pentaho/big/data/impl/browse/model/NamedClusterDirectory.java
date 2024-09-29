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

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.pentaho.big.data.impl.browse.NamedClusterProvider;
import org.pentaho.di.plugins.fileopensave.api.providers.Directory;
import org.pentaho.di.plugins.fileopensave.api.providers.EntityType;

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

  @Override
  public EntityType getEntityType(){
    return EntityType.NAMED_CLUSTER_DIRECTORY;
  }
}
