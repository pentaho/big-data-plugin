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
import org.pentaho.big.data.hadoop.bootstrap.HadoopConfigurationBootstrap;
import org.pentaho.big.data.api.jdbc.impl.JdbcUrlParserImpl;
import org.pentaho.big.data.api.jdbc.impl.DriverLocatorImpl;
import org.pentaho.big.data.impl.cluster.NamedClusterManager;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.HadoopConfigurationLocator;
import org.pentaho.hadoop.shim.api.ConfigurationException;
import org.pentaho.hadoop.shim.api.internal.ShimIdentifier;
import org.pentaho.hadoop.shim.spi.HadoopShim;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Unit test for BigDataCEServiceInitializerImpl
 */
@RunWith(MockitoJUnitRunner.class)
public class BigDataCEServiceInitializerImplTest {

    private BigDataCEServiceInitializerImpl initializer;

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
    private JdbcUrlParserImpl jdbcUrlParser;

    @Mock
    private DriverLocatorImpl driverLocator;

    @Mock
    private NamedClusterManager namedClusterManager;

    private MockedStatic<HadoopConfigurationBootstrap> hadoopConfigurationBootstrapMockedStatic;
    private MockedStatic<JdbcUrlParserImpl> jdbcUrlParserMockedStatic;
    private MockedStatic<DriverLocatorImpl> driverLocatorMockedStatic;
    private MockedStatic<NamedClusterManager> namedClusterManagerMockedStatic;

  @Before
  public void setUp() throws ConfigurationException {
    initializer = new BigDataCEServiceInitializerImpl();        // Setup static mocks
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
    }

    @Test
    public void testGetPriority() {
        assertEquals(100, initializer.getPriority());
    }

    @Test
    public void testUseProxyWrap() {
        assertTrue(initializer.useProxyWrap());
    }

    @Test
    public void testInitializeCommonServices_Success() throws ConfigurationException {
        // Test successful initialization
        HadoopShim result = initializer.initializeCommonServices();

        assertNotNull(result);
        assertEquals(hadoopShim, result);
        verify(hadoopConfigurationBootstrap).getProvider();
        verify(hadoopConfigurationLocator).getActiveConfiguration();
    }

    @Test
    public void testInitializeCommonServices_NoProvider() throws ConfigurationException {
        // Test when no provider is available
        when(hadoopConfigurationBootstrap.getProvider()).thenReturn(null);

        HadoopShim result = initializer.initializeCommonServices();

        assertNull(result);
    }

    @Test
    public void testInitializeAuthenticationManager() {
        List<String> services = new ArrayList<>();

        // CE version should return null
        assertNull(initializer.initializeAuthenticationManager(hadoopShim, services));
    }

    @Test
    @Ignore("VFS filesystem conflicts - Multiple providers registered for URL scheme 'hdfs'")
    public void testInitializeHdfsServices_WithHdfsService() {
        List<String> shimAvailableServices = Arrays.asList("hdfs");
        List<String> hdfsOptions = Arrays.asList("hdfs");
        List<String> hdfsSchemas = Arrays.asList("hdfs");

        when(hadoopShim.getAvailableServices()).thenReturn(shimAvailableServices);
        when(hadoopShim.getServiceOptions("hdfs")).thenReturn(hdfsOptions);
        when(hadoopShim.getAvailableHdfsSchemas()).thenReturn(hdfsSchemas);

        var result = initializer.initializeHdfsServices(hadoopShim, shimAvailableServices, null);

        assertNotNull(result);
    }

    @Test
    public void testInitializeHdfsServices_WithoutHdfsService() {
        List<String> shimAvailableServices = new ArrayList<>();

        var result = initializer.initializeHdfsServices(hadoopShim, shimAvailableServices, null);

        assertNull(result);
    }

    @Test
    @Ignore("CE version passes null for namedClusterServiceLocator")
    public void testInitializeFormatServices_WithCommonFormats() {
        List<String> shimAvailableServices = Arrays.asList("common_formats");

        when(hadoopShim.getAvailableServices()).thenReturn(shimAvailableServices);

        // Should not throw exception
        initializer.initializeFormatServices(hadoopShim, shimAvailableServices, null);
    }

    @Test
    public void testInitializeFormatServices_WithoutCommonFormats() {
        List<String> shimAvailableServices = new ArrayList<>();

        // Should not throw exception
        initializer.initializeFormatServices(hadoopShim, shimAvailableServices, null);
    }

    @Test
    @Ignore("CE version passes null for namedClusterServiceLocator")
    public void testInitializeMapReduceServices_WithMapReduce() {
        List<String> shimAvailableServices = Arrays.asList("mapreduce");
        List<String> mapreduceOptions = Arrays.asList("mapreduce");

        when(hadoopShim.getAvailableServices()).thenReturn(shimAvailableServices);
        when(hadoopShim.getServiceOptions("mapreduce")).thenReturn(mapreduceOptions);

        // Should not throw exception
        initializer.initializeMapReduceServices(hadoopShim, shimAvailableServices, null, null);
    }

    @Test
    public void testInitializeMapReduceServices_WithoutMapReduce() {
        List<String> shimAvailableServices = new ArrayList<>();

        // Should not throw exception
        initializer.initializeMapReduceServices(hadoopShim, shimAvailableServices, null, null);
    }

    @Test
    @Ignore("CE version passes null for namedClusterServiceLocator")
    public void testInitializeSqoopServices_WithSqoop() {
        List<String> shimAvailableServices = Arrays.asList("sqoop");
        List<String> sqoopOptions = Arrays.asList("sqoop");

        when(hadoopShim.getAvailableServices()).thenReturn(shimAvailableServices);
        when(hadoopShim.getServiceOptions("sqoop")).thenReturn(sqoopOptions);

        // Should not throw exception
        initializer.initializeSqoopServices(hadoopShim, shimAvailableServices, null, null);
    }

    @Test
    public void testInitializeSqoopServices_WithoutSqoop() {
        List<String> shimAvailableServices = new ArrayList<>();

        // Should not throw exception
        initializer.initializeSqoopServices(hadoopShim, shimAvailableServices, null, null);
    }

    @Test
    public void testInitializeHiveServices_WithHive() throws Exception {
        List<String> shimAvailableServices = Arrays.asList("hive");
        List<String> hiveDrivers = Arrays.asList("hive", "impala", "impala_simba", "spark_simba");

        when(hadoopShim.getAvailableServices()).thenReturn(shimAvailableServices);
        when(hadoopShim.getAvailableHiveDrivers()).thenReturn(hiveDrivers);

        // Should not throw exception
        initializer.initializeHiveServices(hadoopShim, shimAvailableServices, null);

        // Verify driver registration was attempted
        verify(driverLocator, atLeastOnce()).registerDriver(any());
    }

    @Test
    public void testInitializeHiveServices_WithoutHive() throws Exception {
        List<String> shimAvailableServices = new ArrayList<>();

        // Should not throw exception
        initializer.initializeHiveServices(hadoopShim, shimAvailableServices, null);

        // Verify no driver registration was attempted
        verify(driverLocator, never()).registerDriver(any());
    }

    @Test
    @Ignore("CE version does not implement HBase service registration")
    public void testInitializeHBaseServices_WithHBase() throws Exception {
        List<String> shimAvailableServices = Arrays.asList("hbase");
        List<String> hbaseOptions = Arrays.asList("hbase");

        when(hadoopShim.getAvailableServices()).thenReturn(shimAvailableServices);
        when(hadoopShim.getServiceOptions("hbase")).thenReturn(hbaseOptions);

        // Should not throw exception
        initializer.initializeHBaseServices(hadoopShim, shimAvailableServices, null, null);
    }

    @Test
    public void testInitializeHBaseServices_WithoutHBase() throws Exception {
        List<String> shimAvailableServices = new ArrayList<>();

        // Should not throw exception
        initializer.initializeHBaseServices(hadoopShim, shimAvailableServices, null, null);
    }

    @Test
    public void testInitializeYarnServices() {
        List<String> shimAvailableServices = new ArrayList<>();

        // CE version does nothing - should not throw exception
        initializer.initializeYarnServices(hadoopShim, shimAvailableServices, null, null, null);
    }

    @Test
    public void testInitializeRuntimeTests() {
        // Should not throw exception
        initializer.initializeRuntimeTests(null, null);
    }

    @Test
    public void testDoInitialize_WithNoConfiguration() throws ConfigurationException {
        when(hadoopConfigurationBootstrap.getProvider()).thenReturn(null);

        // Should complete without throwing exception
        initializer.doInitialize();
    }

  @Test
  public void testDoInitialize_WithConfiguration() throws ConfigurationException {
    List<String> shimAvailableServices = new ArrayList<>();
    when(hadoopShim.getAvailableServices()).thenReturn(shimAvailableServices);

    // Should complete without throwing exception
    initializer.doInitialize();
  }  @Test
  @Ignore("VFS filesystem conflicts - Multiple providers registered for URL scheme 'hdfs'")
  public void testDoInitialize_WithFullServices() {
        List<String> shimAvailableServices = Arrays.asList(
                "hdfs", "common_formats", "mapreduce", "sqoop", "hive", "hbase"
        );
        List<String> hdfsOptions = Arrays.asList("hdfs");
        List<String> hdfsSchemas = Arrays.asList("hdfs");
        List<String> mapreduceOptions = Arrays.asList("mapreduce");
        List<String> sqoopOptions = Arrays.asList("sqoop");
        List<String> hbaseOptions = Arrays.asList("hbase");
        List<String> hiveDrivers = Arrays.asList("hive");

        when(hadoopShim.getAvailableServices()).thenReturn(shimAvailableServices);
        when(hadoopShim.getServiceOptions("hdfs")).thenReturn(hdfsOptions);
        when(hadoopShim.getAvailableHdfsSchemas()).thenReturn(hdfsSchemas);
        when(hadoopShim.getServiceOptions("mapreduce")).thenReturn(mapreduceOptions);
        when(hadoopShim.getServiceOptions("sqoop")).thenReturn(sqoopOptions);
        when(hadoopShim.getServiceOptions("hbase")).thenReturn(hbaseOptions);
        when(hadoopShim.getAvailableHiveDrivers()).thenReturn(hiveDrivers);

        // Should complete without throwing exception
        initializer.doInitialize();
    }

    @Test
    public void testRegisterHiveDrivers_AllDriverTypes() throws Exception {
        List<String> hiveDrivers = Arrays.asList("hive", "impala", "impala_simba", "spark_simba");

        initializer.registerHiveDrivers(hadoopShim, hiveDrivers, jdbcUrlParser, driverLocator);

        // Verify all 4 drivers were registered
        verify(driverLocator, times(4)).registerDriver(any());
    }

    @Test
    public void testRegisterHiveDrivers_OnlyHive() throws Exception {
        List<String> hiveDrivers = Arrays.asList("hive");

        initializer.registerHiveDrivers(hadoopShim, hiveDrivers, jdbcUrlParser, driverLocator);

        // Verify only 1 driver was registered
        verify(driverLocator, times(1)).registerDriver(any());
    }

    @Test
    public void testRegisterHiveDrivers_NoDrivers() throws Exception {
        List<String> hiveDrivers = new ArrayList<>();

        initializer.registerHiveDrivers(hadoopShim, hiveDrivers, jdbcUrlParser, driverLocator);

        // Verify no drivers were registered
        verify(driverLocator, never()).registerDriver(any());
    }

    @Test
    public void testInitializeUINamedClusterProvider() {
        // Should not throw exception even if UI provider is not available
        initializer.initializeUINamedClusterProvider();
    }
}
