/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.step.mqtt;

import com.google.common.collect.ImmutableList;
import org.apache.activemq.broker.BrokerService;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.streaming.api.StreamSource;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static java.nio.charset.Charset.defaultCharset;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNot.not;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( PowerMockRunner.class )
@PrepareForTest( MQTTClientBuilder.class )
public class MQTTStreamSourceTest {

  int port;
  private BrokerService brokerService;

  private ExecutorService executorService = Executors.newSingleThreadExecutor();

  @Mock MQTTConsumer mqttConsumer;
  @Mock MQTTConsumerMeta consumerMeta;
  @Mock LogChannelInterface logger;
  @Mock StepMeta stepMeta;

  @Before
  public void startBroker() throws Exception {
    port = findFreePort();
    brokerService = new BrokerService();
    brokerService.setDeleteAllMessagesOnStartup( true );
    brokerService.setPersistent( false );
    brokerService.addConnector( "mqtt://localhost:" + port );
    brokerService.start();
    brokerService.waitUntilStarted();
    when( consumerMeta.getQos() ).thenReturn( "2" );
    when( consumerMeta.getMqttServer() ).thenReturn( "127.0.0.1:" + port );
    when( consumerMeta.getTopics() ).thenReturn( singletonList( "mytopic" ) );
    when( mqttConsumer.environmentSubstitute( anyString() ) )
      .thenAnswer( answer -> answer.getArguments()[ 0 ] );
    when( mqttConsumer.environmentSubstitute( any( String[].class ) ) )
      .thenAnswer( answer -> answer.getArguments()[ 0 ] );
    when( mqttConsumer.getLogChannel() ).thenReturn( logger );
    when( mqttConsumer.getStepMeta() ).thenReturn( stepMeta );
    when( stepMeta.getName() ).thenReturn( "Mqtt Step" );
  }

  @After
  public void stopBroker() throws Exception {
    brokerService.stop();
  }

  @Test
  public void testMqttStreamSingleTopic() throws Exception {
    StreamSource<List<Object>> source = new MQTTStreamSource( consumerMeta, mqttConsumer );
    source.open();

    final String[] messages = { "foo", "bar", "baz" };
    publish( "mytopic", messages );

    List<List<Object>> rows = getQuickly(
      iterateSource( source.observable().blockingIterable().iterator(), 3 ) );
    assertThat( messagesToRows( "mytopic", messages ), equalTo( rows ) );
    source.close();
  }

  @Test
  public void testMultipleTopics() throws MqttException, InterruptedException {
    when( consumerMeta.getTopics() ).thenReturn(
      Arrays.asList( "mytopic-1", "vermilion.minotaur", "nosuchtopic" ) );
    StreamSource<List<Object>> source = new MQTTStreamSource( consumerMeta, mqttConsumer );
    source.open();

    String[] topic1Messages = { "foo", "bar", "baz" };
    publish( "mytopic-1", topic1Messages );
    String[] topic2Messages = { "chuntttttt", "usidor", "arnie" };
    publish( "vermilion.minotaur", topic2Messages );

    Thread.sleep( 200 );
    List<List<Object>> rows = getQuickly(
      iterateSource( source.observable().blockingIterable().iterator(), 6 ) );
    List<List<Object>> expectedResults = ImmutableList.<List<Object>>builder()
      .addAll( messagesToRows( "mytopic-1", topic1Messages ) )
      .addAll( messagesToRows( "vermilion.minotaur", topic2Messages ) )
      .build();

    // contains any order wan't working for me for some reason, this should be similar
    assertThat( expectedResults.size(), equalTo( rows.size() ) );
    rows.forEach( row -> assertTrue( expectedResults.contains( row ) ) );
    source.close();
  }

  @Test
  public void testServernameCheck() {
    // valid server:port
    StreamSource<List<Object>> source = new MQTTStreamSource( consumerMeta, mqttConsumer );

    source.open();
    source.close();

    when( consumerMeta.getMqttServer() ).thenReturn( "tcp:/127.0.0.1:" + port );
    //invalid tcp://server/port
    try {
      source.open();
      fail( "Expected exception." );
    } catch ( Exception e ) {
      assertThat( e, instanceOf( IllegalArgumentException.class ) );
    }
  }

  @Test
  public void testClientIdNotReused() {
    MQTTStreamSource source1 = new MQTTStreamSource( consumerMeta, mqttConsumer );
    source1.open();

    MQTTStreamSource source2 = new MQTTStreamSource( consumerMeta, mqttConsumer );
    source2.open();

    assertThat( source1.mqttClient.getClientId(), not( equalTo( source2.mqttClient.getClientId() ) ) );

    source1.close();
    source2.close();
  }

  @Test
  public void testMQTTOpenException() throws Exception {
    PowerMockito.mockStatic( MQTTClientBuilder.class );
    MQTTClientBuilder.ClientFactory clientFactory = mock( MQTTClientBuilder.ClientFactory.class );
    MqttClient mqttClient = mock( MqttClient.class );
    MQTTClientBuilder builder = spy( MQTTClientBuilder.class );
    MqttException mqttException = mock( MqttException.class );

    when( clientFactory.getClient( any(), any(), any() ) ).thenReturn( mqttClient );
    when( mqttException.toString() ).thenReturn( "There is an error connecting" );
    doThrow( mqttException ).when( builder ).buildAndConnect();
    PowerMockito.when( MQTTClientBuilder.builder() ).thenReturn( builder );

    MQTTStreamSource source = new MQTTStreamSource( consumerMeta, mqttConsumer );
    source.open();

    verify( mqttConsumer ).stopAll();
    verify( mqttConsumer ).logError( "There is an error connecting" );
  }

  private Future<List<List<Object>>> iterateSource( Iterator<List<Object>> iter, int numRowsExpected ) {
    return executorService.submit( () -> {
      List<List<Object>> rows = new ArrayList<>();
      for ( int i = 0; i < numRowsExpected; i++ ) {
        rows.add( iter.next() );
      }
      return rows;
    } );
  }

  private List<List<Object>> messagesToRows( String topic, String[] messages ) {
    return Arrays.stream( messages )
      .map( message -> (Object) message )
      .map( s -> ImmutableList.of( s, topic ) )
      .collect( Collectors.toList() );
  }


  private void publish( String topic, String... messages ) throws MqttException {
    MqttClient pub = null;
    try {
      pub = new MqttClient( "tcp://127.0.0.1:" + port, "producer",
        new MemoryPersistence() );
      pub.connect();
      for ( String msg : messages ) {
        pub.publish( topic, new MqttMessage( msg.getBytes( defaultCharset() ) ) );
      }
    } finally {
      assert pub != null;
      pub.disconnect();
      pub.close();
    }
  }

  private <T> T getQuickly( Future<T> future ) {
    try {
      return future.get( 50, MILLISECONDS );
    } catch ( InterruptedException | ExecutionException | TimeoutException e ) {
      fail( e.getMessage() );
    }
    return null;
  }

  private int findFreePort() throws IOException {
    ServerSocket socket = new ServerSocket( 0 ); // 0 = allocate port automatically
    int freePort = socket.getLocalPort();
    socket.close();
    return freePort;
  }
}
