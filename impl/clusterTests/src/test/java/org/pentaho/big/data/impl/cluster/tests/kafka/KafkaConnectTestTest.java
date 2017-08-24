/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
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
import org.junit.Test;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.impl.cluster.tests.ClusterRuntimeTestEntry;
import org.pentaho.runtime.test.i18n.MessageGetter;
import org.pentaho.runtime.test.i18n.MessageGetterFactory;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;
import org.pentaho.runtime.test.result.RuntimeTestResultSummary;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KafkaConnectTestTest {
  @Test
  public void testSuccess() throws Exception {
    NamedCluster namedCluster = mock( NamedCluster.class );
    final MessageGetterFactory messageGetterFactory = mock( MessageGetterFactory.class );
    Consumer consumer = mock( Consumer.class );
    MessageGetter messageGetter = mock( MessageGetter.class );
    when( consumer.listTopics() ).thenReturn( Collections.emptyMap() );
    when( namedCluster.getKafkaBootstrapServers() ).thenReturn( "kafkaHost:9092" );
    when( messageGetterFactory.create( KafkaConnectTest.PKG ) ).thenReturn( messageGetter );
    when( messageGetter.getMessage( anyString() ) ).thenReturn( "success message" );
    KafkaConnectTest kafkaConnectTest = new KafkaConnectTest( messageGetterFactory, (map) -> consumer );
    RuntimeTestResultSummary summary = kafkaConnectTest.runTest( namedCluster );
    assertEquals( RuntimeTestEntrySeverity.INFO, summary.getOverallStatusEntry().getSeverity() );
    assertEquals( "success message", summary.getOverallStatusEntry().getMessage() );
  }

  @Test
  public void testError() throws Exception {
    NamedCluster namedCluster = mock( NamedCluster.class );
    final MessageGetterFactory messageGetterFactory = mock( MessageGetterFactory.class );
    Consumer consumer = mock( Consumer.class );
    MessageGetter messageGetter = mock( MessageGetter.class );
    when( consumer.listTopics() ).thenThrow( new KafkaException( "oops" ) );
    when( namedCluster.getKafkaBootstrapServers() ).thenReturn( "kafkaHost:9092" );
    when( messageGetterFactory.create( KafkaConnectTest.PKG ) ).thenReturn( messageGetter );
    when( messageGetterFactory.create( ClusterRuntimeTestEntry.class ) ).thenReturn( messageGetter );
    when( messageGetter.getMessage( anyString(), eq( "kafkaHost:9092" ) ) ).thenReturn( "error message" );
    KafkaConnectTest kafkaConnectTest = new KafkaConnectTest( messageGetterFactory, (map) -> consumer );
    RuntimeTestResultSummary summary = kafkaConnectTest.runTest( namedCluster );
    assertEquals( RuntimeTestEntrySeverity.ERROR, summary.getOverallStatusEntry().getSeverity() );
    assertEquals( "error message", summary.getOverallStatusEntry().getMessage() );
  }

  @Test
  public void testSkip() throws Exception {
    NamedCluster namedCluster = mock( NamedCluster.class );
    final MessageGetterFactory messageGetterFactory = mock( MessageGetterFactory.class );
    Consumer consumer = mock( Consumer.class );
    MessageGetter messageGetter = mock( MessageGetter.class );
    when( namedCluster.getKafkaBootstrapServers() ).thenReturn( "  " );
    when( messageGetterFactory.create( KafkaConnectTest.PKG ) ).thenReturn( messageGetter );
    when( messageGetterFactory.create( ClusterRuntimeTestEntry.class ) ).thenReturn( messageGetter );
    when( messageGetter.getMessage( anyString() ) ).thenReturn( "skipped message" );
    KafkaConnectTest kafkaConnectTest = new KafkaConnectTest( messageGetterFactory, (map) -> consumer );
    RuntimeTestResultSummary summary = kafkaConnectTest.runTest( namedCluster );
    assertEquals( RuntimeTestEntrySeverity.SKIPPED, summary.getOverallStatusEntry().getSeverity() );
    assertEquals( "skipped message", summary.getOverallStatusEntry().getMessage() );
    verify( consumer, never() ).listTopics();
  }
}
