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

package org.pentaho.di.ui.repository.repositoryexplorer.model;

import java.util.List;

import org.pentaho.ui.xul.util.AbstractModelList;

public class UINamedClusters extends AbstractModelList<UINamedCluster> {

  private static final long serialVersionUID = -5835985536358581894L;

  public UINamedClusters() {
  }

  public UINamedClusters( List<UINamedCluster> namedClusters ) {
    super( namedClusters );
  }

  @Override
  protected void fireCollectionChanged() {
    this.changeSupport.firePropertyChange( "children", null, this.getChildren() );
  }

}
