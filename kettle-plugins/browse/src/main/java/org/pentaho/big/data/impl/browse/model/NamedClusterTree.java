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
