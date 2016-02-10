/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

package com.pentaho.big.data.bundles.impl.shim.hbase;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;
import org.pentaho.hbase.shim.spi.HBaseConnection;
import org.pentaho.hbase.shim.spi.HBaseShim;

import java.util.ArrayList;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * Created by bryan on 2/2/16.
 */
public class HBaseServiceImplTest {
  private NamedCluster namedCluster;
  private HadoopConfiguration hadoopConfiguration;
  private HBaseServiceImpl hBaseService;
  private HBaseShim hBaseShim;
  private org.pentaho.hbase.shim.spi.HBaseConnection hBaseConnection;
  private HBaseBytesUtilShim hBaseBytesUtilShim;

  @Before
  public void setup() throws Exception {
    namedCluster = mock( NamedCluster.class );
    hadoopConfiguration = mock( HadoopConfiguration.class );
    hBaseShim = mock( HBaseShim.class );
    hBaseConnection = mock( HBaseConnectionTestImpls.HBaseConnectionWithResultField.class );
    hBaseBytesUtilShim = mock( HBaseBytesUtilShim.class );

    when( hadoopConfiguration.getHBaseShim() ).thenReturn( hBaseShim );
    when( hBaseShim.getHBaseConnection() ).thenReturn( hBaseConnection );
    when( hBaseConnection.getBytesUtil() ).thenReturn( hBaseBytesUtilShim );

    hBaseService = new HBaseServiceImpl( namedCluster, hadoopConfiguration );
  }

  @Test
  public void testGetHBaseConnectionFull() throws Exception {
    String siteConfig = "site";
    String defaultConfig = "default";
    String zkHostRaw = "zkHostRaw";
    String zkHostFinal = "zkHostFinal";
    String zkPortRaw = "zkPortRaw";
    String zkPortFinal = "zkPortFinal";

    VariableSpace variableSpace = mock( VariableSpace.class );
    LogChannelInterface logChannelInterface = mock( LogChannelInterface.class );

    when( namedCluster.getZooKeeperHost() ).thenReturn( zkHostRaw );
    when( namedCluster.getZooKeeperPort() ).thenReturn( zkPortRaw );
    when( variableSpace.environmentSubstitute( zkHostRaw ) ).thenReturn( zkHostFinal );
    when( variableSpace.environmentSubstitute( zkPortRaw ) ).thenReturn( zkPortFinal );

    try (
      HBaseConnectionImpl hBaseConnection = hBaseService.getHBaseConnection( variableSpace, siteConfig, defaultConfig,
        logChannelInterface ) ) {
      hBaseConnection.checkHBaseAvailable();
    }
    ArgumentCaptor<Properties> propertiesArgumentCaptor = ArgumentCaptor.forClass( Properties.class );
    verify( hBaseConnection ).configureConnection( propertiesArgumentCaptor.capture(), eq( new ArrayList
      <String>() ) );
    Properties properties = propertiesArgumentCaptor.getValue();
    assertEquals( zkHostFinal, properties.get( HBaseConnection.ZOOKEEPER_QUORUM_KEY ) );
    assertEquals( zkPortFinal, properties.get( HBaseConnection.ZOOKEEPER_PORT_KEY ) );
    assertEquals( siteConfig, properties.get( HBaseConnection.SITE_KEY ) );
    assertEquals( defaultConfig, properties.get( HBaseConnection.DEFAULTS_KEY ) );
  }

  @Test
  public void testGetHBaseConnectionMinimal() throws Exception {
    VariableSpace variableSpace = mock( VariableSpace.class );
    LogChannelInterface logChannelInterface = mock( LogChannelInterface.class );
    try ( HBaseConnectionImpl hBaseConnection = hBaseService
      .getHBaseConnection( variableSpace, null, null, logChannelInterface ) ) {
      hBaseConnection.checkHBaseAvailable();
    }
    ArgumentCaptor<Properties> propertiesArgumentCaptor = ArgumentCaptor.forClass( Properties.class );
    verify( hBaseConnection ).configureConnection( propertiesArgumentCaptor.capture(), eq( new ArrayList
      <String>() ) );
    Properties properties = propertiesArgumentCaptor.getValue();
    assertEquals( 0, properties.size() );
  }

  @Test
  public void testGetColumnFilterFactory() {
    assertNotNull( hBaseService.getColumnFilterFactory() );
  }

  @Test
  public void testGetMappingFactory() {
    assertNotNull( hBaseService.getMappingFactory() );
  }

  @Test
  public void testGetHBaseValueMetaInterfaceFactory() {
    assertNotNull( hBaseService.getHBaseValueMetaInterfaceFactory() );
  }

  @Test
  public void testGetByteConversionUtil() {
    assertNotNull( hBaseService.getByteConversionUtil() );
  }

  @Test
  public void testGetResultFactory() {
    assertNotNull( hBaseService.getResultFactory() );
  }
}
