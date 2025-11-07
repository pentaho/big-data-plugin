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

package org.pentaho.big.data.services.bootstrap;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.big.data.api.cluster.service.locator.impl.NamedClusterServiceLocatorImpl;
import org.pentaho.big.data.api.jdbc.impl.DriverLocatorImpl;
import org.pentaho.big.data.api.jdbc.impl.JdbcUrlParserImpl;
import org.pentaho.big.data.hadoop.bootstrap.HadoopConfigurationBootstrap;
import org.pentaho.big.data.impl.cluster.NamedClusterManager;
import org.pentaho.bigdata.api.hdfs.impl.HadoopFileSystemLocatorImpl;
import org.pentaho.di.core.lifecycle.LifecycleException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.HadoopConfigurationLocator;
import org.pentaho.hadoop.shim.api.ConfigurationException;
import org.pentaho.hadoop.shim.api.internal.ShimIdentifier;
import org.pentaho.hadoop.shim.api.jdbc.JdbcUrlParser;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.runtime.test.impl.RuntimeTesterImpl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Unit test for BigDataPluginLifecycleListener
 */
@RunWith(MockitoJUnitRunner.class)
public class BigDataPluginLifecycleListenerTest {

    private BigDataPluginLifecycleListener listener;

    @Mock(lenient = true)
    private HadoopConfigurationBootstrap hadoopConfigurationBootstrap;

    @Mock(lenient = true)
    private HadoopConfigurationLocator hadoopConfigurationLocator;

    @Mock(lenient = true)
    private HadoopConfiguration hadoopConfiguration;

  @Mock(lenient = true)
  private HadoopShim hadoopShim;

  @Mock(lenient = true)
  private ShimIdentifier shimIdentifier;

  @Mock(lenient = true)
  private JdbcUrlParserImpl jdbcUrlParser;    @Mock(lenient = true)
    private DriverLocatorImpl driverLocator;

    @Mock(lenient = true)
    private NamedClusterManager namedClusterManager;

    @Mock(lenient = true)
    private HadoopFileSystemLocatorImpl hadoopFileSystemLocator;

    @Mock(lenient = true)
    private NamedClusterServiceLocatorImpl namedClusterServiceLocator;

    @Mock(lenient = true)
    private RuntimeTesterImpl runtimeTester;

    private MockedStatic<HadoopConfigurationBootstrap> hadoopConfigurationBootstrapMockedStatic;
    private MockedStatic<JdbcUrlParserImpl> jdbcUrlParserMockedStatic;
    private MockedStatic<DriverLocatorImpl> driverLocatorMockedStatic;
    private MockedStatic<NamedClusterManager> namedClusterManagerMockedStatic;
    private MockedStatic<HadoopFileSystemLocatorImpl> hadoopFileSystemLocatorMockedStatic;
    private MockedStatic<NamedClusterServiceLocatorImpl> namedClusterServiceLocatorMockedStatic;
    private MockedStatic<RuntimeTesterImpl> runtimeTesterMockedStatic;

  @Before
  public void setUp() throws ConfigurationException {
    listener = new BigDataPluginLifecycleListener();        // Setup static mocks
        hadoopConfigurationBootstrapMockedStatic = Mockito.mockStatic(HadoopConfigurationBootstrap.class);
        hadoopConfigurationBootstrapMockedStatic.when(HadoopConfigurationBootstrap::getInstance)
                .thenReturn(hadoopConfigurationBootstrap);

        jdbcUrlParserMockedStatic = Mockito.mockStatic(JdbcUrlParserImpl.class);
        jdbcUrlParserMockedStatic.when(JdbcUrlParserImpl::getInstance)
                .thenReturn(jdbcUrlParser);

        driverLocatorMockedStatic = Mockito.mockStatic(DriverLocatorImpl.class);
        driverLocatorMockedStatic.when(DriverLocatorImpl::getInstance)
                .thenReturn(driverLocator);

        namedClusterManagerMockedStatic = Mockito.mockStatic(NamedClusterManager.class);
        namedClusterManagerMockedStatic.when(NamedClusterManager::getInstance)
                .thenReturn(namedClusterManager);

        hadoopFileSystemLocatorMockedStatic = Mockito.mockStatic(HadoopFileSystemLocatorImpl.class);
        hadoopFileSystemLocatorMockedStatic.when(HadoopFileSystemLocatorImpl::getInstance)
                .thenReturn(hadoopFileSystemLocator);

        namedClusterServiceLocatorMockedStatic = Mockito.mockStatic(NamedClusterServiceLocatorImpl.class);
        namedClusterServiceLocatorMockedStatic.when(() -> NamedClusterServiceLocatorImpl.getInstance(anyString()))
                .thenReturn(namedClusterServiceLocator);

        runtimeTesterMockedStatic = Mockito.mockStatic(RuntimeTesterImpl.class);
        runtimeTesterMockedStatic.when(RuntimeTesterImpl::getInstance)
                .thenReturn(runtimeTester);

        // Setup common mock behavior
        when(hadoopConfigurationBootstrap.getProvider()).thenReturn(hadoopConfigurationLocator);
        when(hadoopConfigurationLocator.getActiveConfiguration()).thenReturn(hadoopConfiguration);
        when(hadoopConfiguration.getHadoopShim()).thenReturn(hadoopShim);
        when(hadoopShim.getShimIdentifier()).thenReturn(shimIdentifier);
        when(shimIdentifier.getId()).thenReturn("test-shim");
    }

    @After
    public void tearDown() {
        if (hadoopConfigurationBootstrapMockedStatic != null) {
            hadoopConfigurationBootstrapMockedStatic.close();
        }
        if (jdbcUrlParserMockedStatic != null) {
            jdbcUrlParserMockedStatic.close();
        }
        if (driverLocatorMockedStatic != null) {
            driverLocatorMockedStatic.close();
        }
        if (namedClusterManagerMockedStatic != null) {
            namedClusterManagerMockedStatic.close();
        }
        if (hadoopFileSystemLocatorMockedStatic != null) {
            hadoopFileSystemLocatorMockedStatic.close();
        }
        if (namedClusterServiceLocatorMockedStatic != null) {
            namedClusterServiceLocatorMockedStatic.close();
        }
        if (runtimeTesterMockedStatic != null) {
            runtimeTesterMockedStatic.close();
        }
    }

    @Test
    public void testOnEnvironmentInit_WithNoConfiguration() throws LifecycleException, ConfigurationException {
        when(hadoopConfigurationBootstrap.getProvider()).thenReturn(null);

        // Should complete without throwing exception
        listener.onEnvironmentInit();

        // Verify no further initialization was attempted
        verify(hadoopConfigurationLocator, never()).getActiveConfiguration();
    }

    @Test
    public void testOnEnvironmentInit_WithBasicConfiguration() throws LifecycleException, ConfigurationException {
        List<String> shimAvailableServices = new ArrayList<>();
        when(hadoopShim.getAvailableServices()).thenReturn(shimAvailableServices);

        // Should complete without throwing exception
        listener.onEnvironmentInit();

        // Verify common services were initialized
        verify(hadoopConfigurationBootstrap).getProvider();
        verify(hadoopConfigurationLocator).getActiveConfiguration();
    }

    @Test
    @Ignore("VFS filesystem registration causes conflicts in test suite")
    public void testOnEnvironmentInit_WithFullServices() throws LifecycleException, ConfigurationException {
        List<String> shimAvailableServices = Arrays.asList(
                "hdfs", "common_formats", "mapreduce", "sqoop", "hive", "hbase"
        );
        List<String> hdfsOptions = Arrays.asList("hdfs");
        List<String> hdfsSchemas = Arrays.asList("hdfs", "maprfs", "wasb", "wasbs", "abfs", "hc");
        List<String> mapreduceOptions = Arrays.asList("mapreduce");
        List<String> sqoopOptions = Arrays.asList("sqoop");
        List<String> hbaseOptions = Arrays.asList("hbase");
        List<String> hiveDrivers = Arrays.asList("hive", "impala", "impala_simba", "spark_simba");

        when(hadoopShim.getAvailableServices()).thenReturn(shimAvailableServices);
        when(hadoopShim.getServiceOptions("hdfs")).thenReturn(hdfsOptions);
        when(hadoopShim.getAvailableHdfsSchemas()).thenReturn(hdfsSchemas);
        when(hadoopShim.getServiceOptions("mapreduce")).thenReturn(mapreduceOptions);
        when(hadoopShim.getServiceOptions("sqoop")).thenReturn(sqoopOptions);
        when(hadoopShim.getServiceOptions("hbase")).thenReturn(hbaseOptions);
        when(hadoopShim.getAvailableHiveDrivers()).thenReturn(hiveDrivers);

        // Should complete without throwing exception
        listener.onEnvironmentInit();

        // Verify all services were initialized
        verify(hadoopShim).getAvailableServices();
        verify(hadoopFileSystemLocator).setHadoopFileSystemFactories(any());
    }

    @Test
    public void testInitializeCommonServices_Success() throws ConfigurationException {
        HadoopShim result = listener.initializeCommonServices();

        assertNotNull(result);
        assertEquals(hadoopShim, result);
        verify(hadoopConfigurationBootstrap).getProvider();
        verify(hadoopConfigurationLocator).getActiveConfiguration();
    }

    @Test
    public void testInitializeCommonServices_NoProvider() throws ConfigurationException {
        when(hadoopConfigurationBootstrap.getProvider()).thenReturn(null);

        HadoopShim result = listener.initializeCommonServices();

        assertNull(result);
    }

    @Test
    public void testInitializeAuthenticationManager() {
        List<String> services = new ArrayList<>();

        // CE version should return null
        assertNull(listener.initializeAuthenticationManager(hadoopShim, services));
    }

    @Test
    @Ignore("VFS filesystem registration causes conflicts in test suite")
    public void testInitializeHdfsServices_WithHdfsService() {
        List<String> shimAvailableServices = Arrays.asList("hdfs");
        List<String> hdfsOptions = Arrays.asList("hdfs");
        List<String> hdfsSchemas = Arrays.asList("hdfs");

        when(hadoopShim.getAvailableServices()).thenReturn(shimAvailableServices);
        when(hadoopShim.getServiceOptions("hdfs")).thenReturn(hdfsOptions);
        when(hadoopShim.getAvailableHdfsSchemas()).thenReturn(hdfsSchemas);

        var result = listener.initializeHdfsServices(hadoopShim, shimAvailableServices, null);

        assertNotNull(result);
        verify(hadoopFileSystemLocator).setHadoopFileSystemFactories(any());
    }

    @Test
    public void testInitializeHdfsServices_WithoutHdfsService() {
        List<String> shimAvailableServices = new ArrayList<>();

        var result = listener.initializeHdfsServices(hadoopShim, shimAvailableServices, null);

        assertNull(result);
    }

    @Test
    @Ignore("VFS filesystem registration causes conflicts in test suite")
    public void testInitializeHdfsSchemas_AllSchemas() {
        List<String> hdfsSchemas = Arrays.asList("hdfs", "maprfs", "escalefs", "wasb", "wasbs", "abfs", "hc");

        // Should not throw exception
        listener.initializeHdfsSchemas(hadoopFileSystemLocator, hdfsSchemas);
    }

    @Test
    public void testInitializeHdfsSchemas_OnlyHdfs() {
        List<String> hdfsSchemas = Arrays.asList("hdfs");

        // Should not throw exception
        listener.initializeHdfsSchemas(hadoopFileSystemLocator, hdfsSchemas);
    }

    @Test
    public void testInitializeHdfsSchemas_EmptyList() {
        List<String> hdfsSchemas = new ArrayList<>();

        // Should not throw exception
        listener.initializeHdfsSchemas(hadoopFileSystemLocator, hdfsSchemas);
    }

    @Test
    public void testInitializeFormatServices_WithCommonFormats() {
        List<String> shimAvailableServices = Arrays.asList("common_formats");

        when(hadoopShim.getAvailableServices()).thenReturn(shimAvailableServices);

        // Should not throw exception
        listener.initializeFormatServices(hadoopShim, shimAvailableServices, namedClusterServiceLocator);

        verify(namedClusterServiceLocator).factoryAdded(any(), any());
    }

    @Test
    public void testInitializeFormatServices_WithoutCommonFormats() {
        List<String> shimAvailableServices = new ArrayList<>();

        // Should not throw exception
        listener.initializeFormatServices(hadoopShim, shimAvailableServices, namedClusterServiceLocator);

        verify(namedClusterServiceLocator, never()).factoryAdded(any(), any());
    }

    @Test
    public void testInitializeMapReduceServices_WithMapReduce() {
        List<String> shimAvailableServices = Arrays.asList("mapreduce");
        List<String> mapreduceOptions = Arrays.asList("mapreduce");

        when(hadoopShim.getAvailableServices()).thenReturn(shimAvailableServices);
        when(hadoopShim.getServiceOptions("mapreduce")).thenReturn(mapreduceOptions);

        // Should not throw exception
        listener.initializeMapReduceServices(hadoopShim, shimAvailableServices, null, namedClusterServiceLocator);

        verify(namedClusterServiceLocator).factoryAdded(any(), any());
    }

    @Test
    public void testInitializeMapReduceServices_WithoutMapReduce() {
        List<String> shimAvailableServices = new ArrayList<>();

        // Should not throw exception
        listener.initializeMapReduceServices(hadoopShim, shimAvailableServices, null, namedClusterServiceLocator);

        verify(namedClusterServiceLocator, never()).factoryAdded(any(), any());
    }

    @Test
    public void testInitializeSqoopServices_WithSqoop() {
        List<String> shimAvailableServices = Arrays.asList("sqoop");
        List<String> sqoopOptions = Arrays.asList("sqoop");

        when(hadoopShim.getAvailableServices()).thenReturn(shimAvailableServices);
        when(hadoopShim.getServiceOptions("sqoop")).thenReturn(sqoopOptions);

        // Should not throw exception
        listener.initializeSqoopServices(hadoopShim, shimAvailableServices, null, namedClusterServiceLocator);

        verify(namedClusterServiceLocator).factoryAdded(any(), any());
    }

    @Test
    public void testInitializeSqoopServices_WithoutSqoop() {
        List<String> shimAvailableServices = new ArrayList<>();

        // Should not throw exception
        listener.initializeSqoopServices(hadoopShim, shimAvailableServices, null, namedClusterServiceLocator);

        verify(namedClusterServiceLocator, never()).factoryAdded(any(), any());
    }

    @Test
    public void testInitializeHiveServices_WithHive() throws Exception {
        List<String> shimAvailableServices = Arrays.asList("hive");
        List<String> hiveDrivers = Arrays.asList("hive", "impala", "impala_simba", "spark_simba");

        when(hadoopShim.getAvailableServices()).thenReturn(shimAvailableServices);
        when(hadoopShim.getAvailableHiveDrivers()).thenReturn(hiveDrivers);

        // Should not throw exception
        listener.initializeHiveServices(hadoopShim, shimAvailableServices, null);

        // Verify driver registration was attempted
        verify(driverLocator, atLeastOnce()).registerDriver(any());
    }

    @Test
    public void testInitializeHiveServices_WithoutHive() throws Exception {
        List<String> shimAvailableServices = new ArrayList<>();

        // Should not throw exception
        listener.initializeHiveServices(hadoopShim, shimAvailableServices, null);

        // Verify no driver registration was attempted
        verify(driverLocator, never()).registerDriver(any());
    }

    @Test
    public void testRegisterHiveDrivers_AllDriverTypes() throws Exception {
        List<String> hiveDrivers = Arrays.asList("hive", "impala", "impala_simba", "spark_simba");

        listener.registerHiveDrivers(hadoopShim, hiveDrivers, jdbcUrlParser, driverLocator);

        // Verify all 4 drivers were registered
        verify(driverLocator, times(4)).registerDriver(any());
    }

    @Test
    public void testRegisterHiveDrivers_OnlyHive() throws Exception {
        List<String> hiveDrivers = Arrays.asList("hive");

        listener.registerHiveDrivers(hadoopShim, hiveDrivers, jdbcUrlParser, driverLocator);

        // Verify only 1 driver was registered
        verify(driverLocator, times(1)).registerDriver(any());
    }

    @Test
    public void testRegisterHiveDrivers_OnlyImpala() throws Exception {
        List<String> hiveDrivers = Arrays.asList("impala");

        listener.registerHiveDrivers(hadoopShim, hiveDrivers, jdbcUrlParser, driverLocator);

        // Verify only 1 driver was registered
        verify(driverLocator, times(1)).registerDriver(any());
    }

    @Test
    public void testRegisterHiveDrivers_OnlyImpalaSimba() throws Exception {
        List<String> hiveDrivers = Arrays.asList("impala_simba");

        listener.registerHiveDrivers(hadoopShim, hiveDrivers, jdbcUrlParser, driverLocator);

        // Verify only 1 driver was registered
        verify(driverLocator, times(1)).registerDriver(any());
    }

    @Test
    public void testRegisterHiveDrivers_OnlySparkSimba() throws Exception {
        List<String> hiveDrivers = Arrays.asList("spark_simba");

        listener.registerHiveDrivers(hadoopShim, hiveDrivers, jdbcUrlParser, driverLocator);

        // Verify only 1 driver was registered
        verify(driverLocator, times(1)).registerDriver(any());
    }

    @Test
    public void testRegisterHiveDrivers_NoDrivers() throws Exception {
        List<String> hiveDrivers = new ArrayList<>();

        listener.registerHiveDrivers(hadoopShim, hiveDrivers, jdbcUrlParser, driverLocator);

        // Verify no drivers were registered
        verify(driverLocator, never()).registerDriver(any());
    }

    @Test
    public void testInitializeHBaseServices_WithHBase() throws Exception {
        List<String> shimAvailableServices = Arrays.asList("hbase");
        List<String> hbaseOptions = Arrays.asList("hbase");

        when(hadoopShim.getAvailableServices()).thenReturn(shimAvailableServices);
        when(hadoopShim.getServiceOptions("hbase")).thenReturn(hbaseOptions);

        // Should not throw exception
        listener.initializeHBaseServices(hadoopShim, shimAvailableServices, null, namedClusterServiceLocator);

        verify(namedClusterServiceLocator).factoryAdded(any(), any());
    }

    @Test
    public void testInitializeHBaseServices_WithoutHBase() throws Exception {
        List<String> shimAvailableServices = new ArrayList<>();

        // Should not throw exception
        listener.initializeHBaseServices(hadoopShim, shimAvailableServices, null, namedClusterServiceLocator);

        verify(namedClusterServiceLocator, never()).factoryAdded(any(), any());
    }

    @Test
    public void testInitializeYarnServices() {
        List<String> shimAvailableServices = new ArrayList<>();

        // CE version does nothing - should not throw exception
        listener.initializeYarnServices(hadoopShim, shimAvailableServices, null, hadoopFileSystemLocator,
                namedClusterServiceLocator);

        // No verification needed - CE version doesn't do anything
    }

    @Test
    public void testInitializeUINamedClusterProvider() {
        // Should not throw exception even if UI provider is not available
        listener.initializeUINamedClusterProvider();
    }

    @Test
    public void testInitializeRuntimeTests() {
        // Should not throw exception
        listener.initializeRuntimeTests(hadoopFileSystemLocator, namedClusterServiceLocator);

        // Verify runtime tests were added
        verify(runtimeTester, times(8)).addRuntimeTest(any());
    }

    @Test
    public void testOnEnvironmentShutdown() {
        // Should not throw exception
        listener.onEnvironmentShutdown();
        // No verification needed - method does nothing
    }
}
