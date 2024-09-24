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

import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.repository.Repository;

import java.lang.reflect.Constructor;

public class UINamedClusterObjectRegistry {

  public static final Class<?> DEFAULT_NAMED_CLUSTER_CLASS = UINamedCluster.class;
  private static UINamedClusterObjectRegistry instance;

  private Class<?> namedClusterClass = DEFAULT_NAMED_CLUSTER_CLASS;

  private UINamedClusterObjectRegistry() {
  }

  public static UINamedClusterObjectRegistry getInstance() {
    if ( instance == null ) {
      instance = new UINamedClusterObjectRegistry();
    }
    return instance;
  }

  public UINamedCluster constructUINamedCluster( NamedCluster namedCluster, Repository rep ) throws UIObjectCreationException {
    try {
      Constructor<?> constructor = namedClusterClass.getConstructor( NamedCluster.class, Repository.class );
      if ( constructor != null ) {
        return (UINamedCluster) constructor.newInstance( namedCluster, rep );
      } else {
        throw new UIObjectCreationException( "Unable to get the constructor for " + namedClusterClass );
      }
    } catch ( Exception e ) {
      throw new UIObjectCreationException( "Unable to instantiate object for " + namedClusterClass );
    }
  }
}
