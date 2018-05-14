/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.impl.cluster;

import com.google.common.annotations.VisibleForTesting;

import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.di.core.attributes.metastore.EmbeddedMetaStore;
import org.pentaho.di.trans.steps.named.cluster.NamedClusterEmbedManager;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;

import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NamedClusterManager implements NamedClusterService {

  private BundleContext bundleContext;

  private Map<IMetaStore, MetaStoreFactory<NamedClusterImpl>> factoryMap = new HashMap<>();

  private NamedCluster clusterTemplate;

  private Map<String, Object> properties = new HashMap<>();

  public BundleContext getBundleContext() {
    return bundleContext;
  }

  public void setBundleContext( BundleContext bundleContext ) {
    this.bundleContext = bundleContext;
    initProperties();
  }

  protected void initProperties() {
    final ServiceReference<?> serviceReference =
      getBundleContext().getServiceReference( ConfigurationAdmin.class.getName() );
    if ( serviceReference != null ) {
      try {
        final ConfigurationAdmin admin = (ConfigurationAdmin) getBundleContext().getService( serviceReference );
        final Configuration configuration = admin.getConfiguration( "pentaho.big.data.impl.cluster" );
        Dictionary<String, Object> rawProperties = configuration.getProperties();
        for ( Enumeration<String> keys = rawProperties.keys(); keys.hasMoreElements(); ) {
          String key = keys.nextElement();
          properties.put( key, rawProperties.get( key ) );
        }
      } catch ( Exception e ) {
        properties = new HashMap<>();
      }
    }
  }

  /**
   * returns a NamedClusterMetaStoreFactory for a given MetaStore instance. NOTE:  This method caches and returns a
   * factory for Embedded MetaStores.  For all other MetaStores, a new instance of MetaStoreFactory will always be
   * returned.
   *
   * @param metastore - the MetaStore for which to to get a MetaStoreFactory.
   * @return a MetaStoreFactory for the given MetaStore.
   */
  @VisibleForTesting
  MetaStoreFactory<NamedClusterImpl> getMetaStoreFactory( IMetaStore metastore ) {
    MetaStoreFactory<NamedClusterImpl> namedClusterMetaStoreFactory = null;

    // Only MetaStoreFactories for EmbeddedMetaStores are cached.  For all other MetaStore types, create a new
    // MetaStoreFactory
    if ( !( metastore instanceof EmbeddedMetaStore ) ) {
      return new MetaStoreFactory<>( NamedClusterImpl.class, metastore, PentahoDefaults.NAMESPACE );
    }

    // cache MetaStoreFactories for Embedded MetaStores
    namedClusterMetaStoreFactory = factoryMap.get( metastore );

    if ( namedClusterMetaStoreFactory == null ) {
      namedClusterMetaStoreFactory =
        new MetaStoreFactory<>( NamedClusterImpl.class, metastore, NamedClusterEmbedManager.NAMESPACE );

      factoryMap.put( metastore, namedClusterMetaStoreFactory );
    }

    return namedClusterMetaStoreFactory;
  }

  @VisibleForTesting
  void putMetaStoreFactory( IMetaStore metastore, MetaStoreFactory<NamedClusterImpl> metaStoreFactory ) {
    factoryMap.put( metastore, metaStoreFactory );
  }

  @Override public void close( IMetaStore metastore ) {
    factoryMap.remove( metastore );
  }

  @Override
  public NamedCluster getClusterTemplate() {
    if ( clusterTemplate == null ) {
      clusterTemplate = new NamedClusterImpl();
      clusterTemplate.setName( "" );
      clusterTemplate.setHdfsHost( "localhost" );
      clusterTemplate.setHdfsPort( "8020" );
      clusterTemplate.setHdfsUsername( "user" );
      clusterTemplate.setHdfsPassword( "password" );
      clusterTemplate.setJobTrackerHost( "localhost" );
      clusterTemplate.setJobTrackerPort( "8032" );
      clusterTemplate.setZooKeeperHost( "localhost" );
      clusterTemplate.setZooKeeperPort( "2181" );
      clusterTemplate.setOozieUrl( "http://localhost:8080/oozie" );
    }
    return clusterTemplate.clone();
  }

  @Override
  public void setClusterTemplate( NamedCluster clusterTemplate ) {
    this.clusterTemplate = clusterTemplate;
  }

  @Override
  public void create( NamedCluster namedCluster, IMetaStore metastore ) throws MetaStoreException {
    getMetaStoreFactory( metastore ).saveElement( new NamedClusterImpl( namedCluster ) );
  }

  @Override
  public NamedCluster read( String clusterName, IMetaStore metastore ) throws MetaStoreException {
    MetaStoreFactory<NamedClusterImpl> factory = getMetaStoreFactory( metastore );
    NamedCluster namedCluster = null;
    try {
      namedCluster = factory.loadElement( clusterName );
    } catch ( MetaStoreException e ) {
      // While executing Pentaho MapReduce on a secure cluster, the .lock file
      // might not be able to be created due to permissions.
      // In this case, try and read the MetaStore without locking.
      namedCluster = factory.loadElement( clusterName, false );
    }
    return namedCluster;
  }

  @Override
  public void update( NamedCluster namedCluster, IMetaStore metastore ) throws MetaStoreException {
    MetaStoreFactory<NamedClusterImpl> factory = getMetaStoreFactory( metastore );
    List<NamedCluster> namedClusters = list( metastore );
    for ( NamedCluster nc : namedClusters ) {
      if ( namedCluster.getName().equals( nc.getName() ) ) {
        factory.deleteElement( nc.getName() );
        factory.saveElement( new NamedClusterImpl( namedCluster ) );
      }
    }
  }

  @Override
  public void delete( String clusterName, IMetaStore metastore ) throws MetaStoreException {
    getMetaStoreFactory( metastore ).deleteElement( clusterName );
  }

  @Override
  public List<NamedCluster> list( IMetaStore metastore ) throws MetaStoreException {
    MetaStoreFactory<NamedClusterImpl> factory = getMetaStoreFactory( metastore );
    List<NamedCluster> namedClusters;

    try {
      namedClusters = new ArrayList<>( factory.getElements( true ) );
    } catch ( MetaStoreException ex ) {
      // While executing Pentaho MapReduce on a secure cluster, the .lock file
      // might not be able to be created due to permissions.
      // In this case, try and read the MetaStore without locking.
      namedClusters = new ArrayList<>( factory.getElements( false ) );
    }

    return namedClusters;
  }

  @Override
  public List<String> listNames( IMetaStore metastore ) throws MetaStoreException {
    return getMetaStoreFactory( metastore ).getElementNames();
  }

  @Override
  public boolean contains( String clusterName, IMetaStore metastore ) throws MetaStoreException {
    if ( metastore == null ) {
      return false;
    }
    return listNames( metastore ).contains( clusterName );
  }

  @Override
  public NamedCluster getNamedClusterByName( String namedCluster, IMetaStore metastore ) {
    if ( metastore == null ) {
      return null;
    }
    try {
      List<NamedCluster> namedClusters = list( metastore );
      for ( NamedCluster nc : namedClusters ) {
        if ( nc.getName().equals( namedCluster ) ) {
          return nc;
        }
      }
    } catch ( MetaStoreException e ) {
      return null;
    }
    return null;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  @Override
  public NamedCluster getNamedClusterByHost( String hostName, IMetaStore metastore ) {
    if ( metastore == null || hostName == null ) {
      return null;
    }
    try {
      List<NamedCluster> namedClusters = list( metastore );
      for ( NamedCluster nc : namedClusters ) {
        if ( nc.getHdfsHost().equals( hostName ) ) {
          return nc;
        }
      }
    } catch ( MetaStoreException e ) {
      return null;
    }
    return null;
  }

  @Override
  public void updateNamedClusterTemplate( String hostName, int port, boolean isMapr ) {
    clusterTemplate.setHdfsHost( hostName );
    if ( port > 0 ) {
      clusterTemplate.setHdfsPort( String.valueOf( port ) );
    } else {
      clusterTemplate.setHdfsPort( "" );
    }
    clusterTemplate.setMapr( isMapr );
  }
}
