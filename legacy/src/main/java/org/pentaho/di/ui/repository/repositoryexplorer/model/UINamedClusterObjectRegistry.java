/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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
