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

package org.pentaho.di.ui.core.namedcluster;

import org.pentaho.di.core.namedcluster.NamedClusterManager;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.util.ArrayList;
import java.util.List;

public class NamedClusterUIHelper {
  private static final NamedClusterUIFactoryHolder NAMED_CLUSTER_UI_FACTORY_HOLDER = new NamedClusterUIFactoryHolder();

  public static synchronized NamedClusterUIFactory getNamedClusterUIFactory() {
    return NAMED_CLUSTER_UI_FACTORY_HOLDER.getNamedClusterUIFactory();
  }

  /**
   * Being used to inject the widgets from OSGi (where all the test functionality is located) this should be removed
   * once we OSGiify the rest of the big data stuff
   *
   * @param namedClusterUIFactory
   */
  public static void setNamedClusterUIFactory(
    NamedClusterUIFactory namedClusterUIFactory ) {
    NAMED_CLUSTER_UI_FACTORY_HOLDER.setNamedClusterUIFactory( namedClusterUIFactory );
  }

  /**
   * Being used to inject the widgets from OSGi (where all the test functionality is located) this should be removed
   * once we OSGiify the rest of the big data stuff
   *
   * WARNING: THIS WILL BLOCK UNTIL THE FACTORY IS AVAILABLE, DO NOT CALL FROM ANYTHING THAT COULD BLOCK STARTUP
   */
  public static List<NamedCluster> getNamedClusters() {
    try {
      return NamedClusterManager.getInstance().list( Spoon.getInstance().getMetaStore() );
    } catch ( MetaStoreException e ) {
      return new ArrayList<>();
    }
  }

  public static NamedCluster getNamedCluster( String namedCluster ) throws MetaStoreException {
    return NamedClusterManager.getInstance().read( namedCluster, Spoon.getInstance().getMetaStore() );
  }

  static class NamedClusterUIFactoryHolder {
    private NamedClusterUIFactory namedClusterUIFactory;

    public synchronized NamedClusterUIFactory getNamedClusterUIFactory() {
      while ( namedClusterUIFactory == null ) {
        try {
          wait();
        } catch ( InterruptedException e ) {
          Thread.currentThread().interrupt();
        }
      }
      return namedClusterUIFactory;
    }

    public synchronized void setNamedClusterUIFactory( NamedClusterUIFactory namedClusterUIFactory ) {
      this.namedClusterUIFactory = namedClusterUIFactory;
      notifyAll();
    }
  }
}
