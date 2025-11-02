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


package org.pentaho.big.data.kettle.plugins.formats.impl;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;

import org.junit.After;
import org.junit.AfterClass;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.KettleLoggingEventListener;
import org.pentaho.di.core.service.PluginServiceLoader;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.metastore.locator.api.MetastoreLocator;

@RunWith(MockitoJUnitRunner.class)
public class NamedClusterResolverTest {
  @Mock
  private MetastoreLocator metaStoreService;
  @Mock
  private NamedClusterService namedClusterService;
  @Mock
  private KettleLoggingEventListener kettleLoggingEventListener;
  @Mock
  private NamedCluster namedCluster;
  @Mock
  private NamedClusterServiceLocator namedClusterServiceLocator;

  private NamedClusterResolver namedClusterResolver;

  private static MockedStatic<PluginServiceLoader> pluginServiceLoaderMockedStatic;

  @BeforeClass
  public static void setupClass() {
    // Create a class-level static mock that will stay open for all tests
    pluginServiceLoaderMockedStatic = Mockito.mockStatic( PluginServiceLoader.class );
  }

  @AfterClass
  public static void tearDownClass() {
    // Close the static mock after all tests complete
    if ( pluginServiceLoaderMockedStatic != null ) {
      pluginServiceLoaderMockedStatic.close();
    }
  }

  @Before
  public void before() throws Exception {
    // Reset the singleton before each test
    resetSingleton();

    KettleLogStore.init();
    KettleLogStore.getAppender().addLoggingEventListener( kettleLoggingEventListener );
    when( namedClusterService.getNamedClusterByName( "testhc", null ) )
      .thenReturn( namedCluster );
    when( namedClusterService.getNamedClusterByHost( "somehost", null ) )
      .thenReturn( namedCluster );

    Collection<NamedClusterServiceLocator> namedClusterServiceLocatorCollection = new ArrayList<>();
    namedClusterServiceLocatorCollection.add( namedClusterServiceLocator );

    pluginServiceLoaderMockedStatic.when( () -> PluginServiceLoader.loadServices( NamedClusterServiceLocator.class ) )
      .thenReturn( namedClusterServiceLocatorCollection );

    Collection<NamedClusterService> namedClusterServiceCollection = new ArrayList<>();
    namedClusterServiceCollection.add( namedClusterService );

    pluginServiceLoaderMockedStatic.when( () -> PluginServiceLoader.loadServices( NamedClusterService.class ) )
      .thenReturn( namedClusterServiceCollection );

    namedClusterResolver = NamedClusterResolver.getInstance();
  }

  @After
  public void after() throws Exception {
    // Reset the singleton after each test to ensure test isolation
    resetSingleton();
  }

  private void resetSingleton() throws Exception {
    // Use reflection to reset the singleton instance
    Field instance = NamedClusterResolver.class.getDeclaredField( "namedClusterResolver" );
    instance.setAccessible( true );
    instance.set( null, null );
  }

  @Test
  public void windowsFilePathsAreHandled() {
    assertNull(
      namedClusterResolver.resolveNamedCluster( "C:/path/to some/file" ) );
    verify( kettleLoggingEventListener, times( 0 ) ).eventAdded( ArgumentMatchers.any() );
  }

  @Test
  public void testNamedClusterByName() {
    Collection<MetastoreLocator> metastoreLocatorCollection = new ArrayList<>();
    metastoreLocatorCollection.add( metaStoreService );

    pluginServiceLoaderMockedStatic.when( () -> PluginServiceLoader.loadServices( MetastoreLocator.class ) )
      .thenReturn( metastoreLocatorCollection );

    NamedCluster cluster = namedClusterResolver.resolveNamedCluster( "hc://testhc/path" );
    assertEquals( namedCluster, cluster );

    cluster = namedClusterResolver.resolveNamedCluster( "hc://nosuchhc/path" );
    assertNull( cluster );
  }

  @Test
  public void testNamedClusterByHost() {
    Collection<MetastoreLocator> metastoreLocatorCollection = new ArrayList<>();
    metastoreLocatorCollection.add( metaStoreService );

    pluginServiceLoaderMockedStatic.when( () -> PluginServiceLoader.loadServices( MetastoreLocator.class ) )
      .thenReturn( metastoreLocatorCollection );

    NamedCluster cluster = namedClusterResolver.resolveNamedCluster( "hdfs://somehost/path" );
    assertEquals( namedCluster, cluster );
  }

}
