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
