/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
 * <p>
 * ******************************************************************************
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 ******************************************************************************/
package org.pentaho.big.data.impl.cluster;

import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.di.core.osgi.api.NamedClusterOsgi;
import org.pentaho.di.core.osgi.api.NamedClusterServiceOsgi;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by tkafalas on 7/3/2017.
 */
public class NamedClusterServiceOsgiImpl implements NamedClusterServiceOsgi {

  private NamedClusterService namedClusterService;

  NamedClusterServiceOsgiImpl( NamedClusterService namedClusterService ) {
    this.namedClusterService = namedClusterService;
  }

  @Override public NamedClusterOsgi getClusterTemplate() {
    return (NamedClusterOsgi) namedClusterService.getClusterTemplate();
  }

  @Override public void setClusterTemplate( NamedClusterOsgi clusterTemplate ) {
    namedClusterService.setClusterTemplate( (NamedCluster) clusterTemplate );
  }

  @Override public void create( NamedClusterOsgi namedCluster, IMetaStore metastore ) throws MetaStoreException {
    namedClusterService.create( (NamedCluster) namedCluster, metastore );
  }

  @Override public NamedClusterOsgi read( String clusterName, IMetaStore metastore ) throws MetaStoreException {
    return (NamedClusterOsgi) namedClusterService.read( clusterName, metastore );
  }

  @Override public void update( NamedClusterOsgi namedCluster, IMetaStore metastore ) throws MetaStoreException {
    namedClusterService.update( (NamedCluster) namedCluster, metastore );
  }

  @Override public void delete( String clusterName, IMetaStore metastore ) throws MetaStoreException {
    namedClusterService.delete( clusterName, metastore );
  }

  @Override public List<NamedClusterOsgi> list( IMetaStore metastore ) throws MetaStoreException {
    List<NamedCluster> list = namedClusterService.list( metastore );
    return list.stream().map( i -> (NamedClusterOsgi) i ).collect( Collectors.toList() );
  }

  @Override public List<String> listNames( IMetaStore metastore ) throws MetaStoreException {
    return namedClusterService.listNames( metastore );
  }

  @Override public boolean contains( String clusterName, IMetaStore metastore ) throws MetaStoreException {
    return namedClusterService.contains( clusterName, metastore );
  }

  @Override public NamedClusterOsgi getNamedClusterByName( String namedCluster, IMetaStore metastore ) {
    return (NamedClusterOsgi) namedClusterService.getNamedClusterByName( namedCluster, metastore );
  }

  @Override public Map<String, Object> getProperties() {
    return namedClusterService.getProperties();
  }

  @Override public void close( IMetaStore metastore ) {
    namedClusterService.close( metastore );
  }
}
