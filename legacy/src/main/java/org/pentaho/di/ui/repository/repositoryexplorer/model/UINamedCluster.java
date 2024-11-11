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


package org.pentaho.di.ui.repository.repositoryexplorer.model;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.pentaho.di.core.hadoop.HadoopSpoonPlugin;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.Repository;
import org.pentaho.ui.xul.XulEventSourceAdapter;

public class UINamedCluster extends XulEventSourceAdapter {

  private static final Class<?> CLZ = HadoopSpoonPlugin.class;

  protected NamedCluster namedCluster;
  // inheriting classes may need access to the repository
  protected Repository rep;

  public UINamedCluster() {
    super();
  }

  public UINamedCluster( NamedCluster namedCluster, Repository rep ) {
    super();
    this.namedCluster = namedCluster;
    this.rep = rep;
  }

  public String getName() {
    if ( namedCluster != null ) {
      return namedCluster.getName();
    }
    return null;
  }

  public String getDisplayName() {
    return getName();
  }

  public String getType() {
    return BaseMessages.getString( CLZ, "NamedClustersController.Type" );
  }

  public String getDateModified() {
    return SimpleDateFormat.getDateTimeInstance().format( new Date( namedCluster.getLastModifiedDate() ) );
  }

  public NamedCluster getNamedCluster() {
    return namedCluster;
  }

}
