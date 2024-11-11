/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.big.data.impl.browse.model;

import org.pentaho.big.data.impl.browse.NamedClusterProvider;
import org.pentaho.di.plugins.fileopensave.api.providers.Tree;

import java.util.ArrayList;
import java.util.List;

public class NamedClusterTree implements Tree<NamedClusterFile> {

  private static final int ORDER = 4;
  private String name;

  private List<NamedClusterFile> namedClusters = new ArrayList<>();

  public NamedClusterTree( String name ) {
    this.name = name;
  }

  @Override public String getName() {
    return name;
  }

  @Override public List<NamedClusterFile> getChildren() {
    return namedClusters;
  }

  @Override public void addChild( NamedClusterFile namedClusterFile ) {
    namedClusters.add( namedClusterFile );
  }

  @Override public boolean isCanAddChildren() {
    return false;
  }

  @Override public int getOrder() {
    return ORDER;
  }

  @Override public String getProvider() {
    return NamedClusterProvider.TYPE;
  }
}
