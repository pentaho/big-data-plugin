/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NamedClusterManager implements NamedClusterService {
  private Map<IMetaStore, MetaStoreFactory<NamedClusterImpl>> factoryMap = new HashMap<>();

  private NamedCluster clusterTemplate;

  private MetaStoreFactory<NamedClusterImpl> getMetaStoreFactory( IMetaStore metastore ) {
    if ( factoryMap.get( metastore ) == null ) {
      factoryMap.put( metastore,
        new MetaStoreFactory<>( NamedClusterImpl.class, metastore, PentahoDefaults.NAMESPACE ) );
    }
    return factoryMap.get( metastore );
  }

  @Override public NamedCluster getClusterTemplate() {
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

  @Override public void setClusterTemplate( NamedCluster clusterTemplate ) {
    this.clusterTemplate = clusterTemplate;
  }

  @Override public void create( NamedCluster namedCluster, IMetaStore metastore ) throws MetaStoreException {
    MetaStoreFactory<NamedClusterImpl> factory = getMetaStoreFactory( metastore );
    factory.saveElement( new NamedClusterImpl( namedCluster ) );
  }

  @Override public NamedCluster read( String clusterName, IMetaStore metastore ) throws MetaStoreException {
    MetaStoreFactory<NamedClusterImpl> factory = getMetaStoreFactory( metastore );
    return factory.loadElement( clusterName );
  }


  @Override public void update( NamedCluster namedCluster, IMetaStore metastore ) throws MetaStoreException {
    MetaStoreFactory<NamedClusterImpl> factory = getMetaStoreFactory( metastore );
    factory.deleteElement( namedCluster.getName() );
    factory.saveElement( new NamedClusterImpl( namedCluster ) );
  }

  @Override public void delete( String clusterName, IMetaStore metastore ) throws MetaStoreException {
    MetaStoreFactory<NamedClusterImpl> factory = getMetaStoreFactory( metastore );
    factory.deleteElement( clusterName );
  }

  @Override public List<NamedCluster> list( IMetaStore metastore ) throws MetaStoreException {
    MetaStoreFactory<NamedClusterImpl> factory = getMetaStoreFactory( metastore );
    return new ArrayList<NamedCluster>( factory.getElements() );
  }

  @Override public List<String> listNames( IMetaStore metastore ) throws MetaStoreException {
    MetaStoreFactory<NamedClusterImpl> factory = getMetaStoreFactory( metastore );
    return factory.getElementNames();
  }

  @Override public boolean contains( String clusterName, IMetaStore metastore ) throws MetaStoreException {
    if ( metastore == null ) {
      return false;
    }
    for ( String name : listNames( metastore ) ) {
      if ( name.equals( clusterName ) ) {
        return true;
      }
    }
    return false;
  }

  @Override public NamedCluster getNamedClusterByName( String namedCluster, IMetaStore metastore ) {
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
}
