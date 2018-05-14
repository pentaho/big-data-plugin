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
package org.pentaho.big.data.impl.cluster.tests.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SaslConfigs;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.big.data.impl.cluster.tests.ClusterRuntimeTestEntry;
import org.pentaho.hadoop.shim.api.jaas.JaasConfigService;
import org.pentaho.runtime.test.i18n.MessageGetter;
import org.pentaho.runtime.test.i18n.MessageGetterFactory;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;
import org.pentaho.runtime.test.result.RuntimeTestResultSummary;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class KafkaConnectTestTest {
  @Mock Consumer consumer;
  @Mock NamedClusterServiceLocator namedClusterServiceLocator;
  @Mock NamedCluster namedCluster;
  @Mock MessageGetter messageGetter;
  @Mock MessageGetterFactory messageGetterFactory;
  @Mock JaasConfigService jaasConfigService;

  @Before
  public void setUp() throws Exception {
    when( messageGetterFactory.create( KafkaConnectTest.PKG ) ).thenReturn( messageGetter );
    when( messageGetterFactory.create( ClusterRuntimeTestEntry.class ) ).thenReturn( messageGetter );
  }

  @Test
  public void testSuccess() throws Exception {
    when( consumer.listTopics() ).thenReturn( Collections.emptyMap() );
    when( namedCluster.getKafkaBootstrapServers() ).thenReturn( "kafkaHost:9092" );
    when( messageGetter.getMessage( anyString() ) ).thenReturn( "success message" );
    KafkaConnectTest kafkaConnectTest =
      new KafkaConnectTest( messageGetterFactory, (map) -> consumer, namedClusterServiceLocator );
    RuntimeTestResultSummary summary = kafkaConnectTest.runTest( namedCluster );
    assertEquals( RuntimeTestEntrySeverity.INFO, summary.getOverallStatusEntry().getSeverity() );
    assertEquals( "success message", summary.getOverallStatusEntry().getMessage() );
  }

  @Test
  public void testSuccessKerberos() throws Exception {
    when( consumer.listTopics() ).thenReturn( Collections.emptyMap() );
    when( namedCluster.getKafkaBootstrapServers() ).thenReturn( "kafkaHost:9092" );
    when( messageGetter.getMessage( anyString() ) ).thenReturn( "success message" );
    when( namedClusterServiceLocator.getService( namedCluster, JaasConfigService.class ) )
      .thenReturn( jaasConfigService );
    when( jaasConfigService.isKerberos() ).thenReturn( true );
    when( jaasConfigService.getJaasConfig() ).thenReturn( "pretend-jaas-config" );
    KafkaConnectTest kafkaConnectTest =
      new KafkaConnectTest( messageGetterFactory, this::assertConsumer, namedClusterServiceLocator );
    RuntimeTestResultSummary summary = kafkaConnectTest.runTest( namedCluster );
    assertEquals( RuntimeTestEntrySeverity.INFO, summary.getOverallStatusEntry().getSeverity() );
    assertEquals( "success message", summary.getOverallStatusEntry().getMessage() );
  }

  @Test
  public void testError() throws Exception {
    when( consumer.listTopics() ).thenThrow( new KafkaException( "oops" ) );
    when( namedCluster.getKafkaBootstrapServers() ).thenReturn( "kafkaHost:9092" );
    when( messageGetter.getMessage( anyString(), eq( "kafkaHost:9092" ) ) ).thenReturn( "error message" );
    KafkaConnectTest kafkaConnectTest =
      new KafkaConnectTest( messageGetterFactory, (map) -> consumer, namedClusterServiceLocator );
    RuntimeTestResultSummary summary = kafkaConnectTest.runTest( namedCluster );
    assertEquals( RuntimeTestEntrySeverity.ERROR, summary.getOverallStatusEntry().getSeverity() );
    assertEquals( "error message", summary.getOverallStatusEntry().getMessage() );
  }

  @Test
  public void testSkip() throws Exception {
    when( namedCluster.getKafkaBootstrapServers() ).thenReturn( "  " );
    when( messageGetter.getMessage( anyString() ) ).thenReturn( "skipped message" );
    KafkaConnectTest kafkaConnectTest =
      new KafkaConnectTest( messageGetterFactory, (map) -> consumer, namedClusterServiceLocator );
    RuntimeTestResultSummary summary = kafkaConnectTest.runTest( namedCluster );
    assertEquals( RuntimeTestEntrySeverity.SKIPPED, summary.getOverallStatusEntry().getSeverity() );
    assertEquals( "skipped message", summary.getOverallStatusEntry().getMessage() );
    verify( consumer, never() ).listTopics();
  }

  private Consumer assertConsumer( Map<String, Object> actualMap ) {
    assertEquals( "pretend-jaas-config", actualMap.get( SaslConfigs.SASL_JAAS_CONFIG ) );
    return consumer;
  }
}
