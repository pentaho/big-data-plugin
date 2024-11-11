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

package org.pentaho.big.data.impl.cluster;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.di.core.osgi.api.NamedClusterOsgi;
import org.pentaho.di.core.osgi.api.NamedClusterServiceOsgi;
import org.pentaho.metastore.api.IMetaStore;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Created by tkafalas on 7/25/2017.
 */
public class NamedClusterServiceOsgiImplTest {
  public final String CLUSTER_NAME = "clusterName";
  NamedClusterService mockNamedClusterService;
  NamedClusterServiceOsgi mockNamedClusterServiceOsgi;
  NamedClusterOsgi mockNamedClusterOsgi;
  IMetaStore mockMetastore;

  @Before
  public void setup() {
    mockNamedClusterService = mock( NamedClusterService.class );
    mockNamedClusterServiceOsgi = new NamedClusterServiceOsgiImpl( mockNamedClusterService );
    mockNamedClusterOsgi = mock( NamedClusterImpl.class );
    mockMetastore = mock( IMetaStore.class );
  }

  @Test
  public void getClusterTemplateTest() {
    mockNamedClusterServiceOsgi.getClusterTemplate();
    verify( mockNamedClusterService ).getClusterTemplate();
  }

  @Test
  public void setClusterTemplateTest() {
    mockNamedClusterServiceOsgi.setClusterTemplate( mockNamedClusterOsgi );
    verify( mockNamedClusterService ).setClusterTemplate( any( NamedCluster.class ) );
  }

  @Test
  public void createTest() throws Exception {
    mockNamedClusterServiceOsgi.create( mockNamedClusterOsgi, mockMetastore );
    verify( mockNamedClusterService ).create( any( NamedCluster.class ), eq( mockMetastore ) );
  }

  @Test
  public void readTest() throws Exception {
    mockNamedClusterServiceOsgi.read( CLUSTER_NAME, mockMetastore );
    verify( mockNamedClusterService ).read( CLUSTER_NAME, mockMetastore );
  }

  @Test
  public void updateTest() throws Exception {
    mockNamedClusterServiceOsgi.update( mockNamedClusterOsgi, mockMetastore );
    verify( mockNamedClusterService ).update( any( NamedCluster.class ), eq( mockMetastore ) );
  }

  @Test
  public void deleteTest() throws Exception {
    mockNamedClusterServiceOsgi.delete( CLUSTER_NAME, mockMetastore );
    verify( mockNamedClusterService ).delete( CLUSTER_NAME, mockMetastore );
  }

  @Test
  public void listTest() throws Exception {
    mockNamedClusterServiceOsgi.list( mockMetastore );
    verify( mockNamedClusterService ).list( mockMetastore );
  }

  @Test
  public void listNamesTest() throws Exception {
    mockNamedClusterServiceOsgi.listNames( mockMetastore );
    verify( mockNamedClusterService ).listNames( mockMetastore );
  }

  @Test
  public void containsTest() throws Exception {
    mockNamedClusterServiceOsgi.contains( CLUSTER_NAME, mockMetastore );
    verify( mockNamedClusterService ).contains( CLUSTER_NAME, mockMetastore );
  }

  @Test
  public void getNamedClusterByNameTest() throws Exception {
    mockNamedClusterServiceOsgi.getNamedClusterByName( CLUSTER_NAME, mockMetastore );
    verify( mockNamedClusterService ).getNamedClusterByName( CLUSTER_NAME, mockMetastore );
  }

  @Test
  public void getPropertiesTest() throws Exception {
    mockNamedClusterServiceOsgi.getProperties();
    verify( mockNamedClusterService ).getProperties();
  }
}
