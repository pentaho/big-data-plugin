/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.trans.streaming.api.StreamSource;

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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNot.not;

@RunWith ( MockitoJUnitRunner.class )
public class MQTTStreamSourceTest {

  int port;
  private BrokerService brokerService;

  private ExecutorService executorService = Executors.newSingleThreadExecutor();


  @Before
  public void startBroker() throws Exception {
    port = findFreePort();
    brokerService = new BrokerService();
    brokerService.setDeleteAllMessagesOnStartup( true );
    brokerService.setPersistent( false );
    brokerService.addConnector( "mqtt://localhost:" + port );
    brokerService.start();
    brokerService.waitUntilStarted();
  }

  @After
  public void stopBroker() throws Exception {
    brokerService.stop();
  }


  @Test
  public void testMqttStreamSingleTopic() throws Exception {
    StreamSource<List<Object>> source =
      new MQTTStreamSource( "127.0.0.1:" + port, Arrays.asList( "mytopic" ), 2 );
    source.open();

    final String[] messages = { "foo", "bar", "baz" };
    publish( "mytopic", messages );

    List<List<Object>> rows = getQuickly(
      iterateSource( source.rows().iterator(), 3 ) );
    assertThat( messagesToRows( "mytopic", messages ), equalTo( rows ) );
    source.close();
  }

  @Test
  public void multipleTopics() throws MqttException, InterruptedException {
    StreamSource<List<Object>> source =
      new MQTTStreamSource( "127.0.0.1:" + port,
        Arrays.asList( "mytopic-1", "vermilion.minotaur", "nosuchtopic" ), 2 );
    source.open();

    String[] topic1Messages = { "foo", "bar", "baz" };
    publish( "mytopic-1", topic1Messages );
    String[] topic2Messages = { "chuntttttt", "usidor", "arnie" };
    publish( "vermilion.minotaur", topic2Messages );

    Thread.sleep( 200 );
    List<List<Object>> rows = getQuickly(
      iterateSource( source.rows().iterator(), 6 ) );
    List<List<Object>> expectedResults = ImmutableList.<List<Object>>builder()
      .addAll( messagesToRows( "mytopic-1", topic1Messages ) )
      .addAll( messagesToRows( "vermilion.minotaur", topic2Messages ) )
      .build();

    // contains any order wan't working for me for some reason, this should be similar
    assertThat( expectedResults.size(), equalTo( rows.size() ) );
    rows.stream().forEach( row -> assertTrue( expectedResults.contains( row ) ) );
    source.close();
  }


  @Test
  public void servernameCheck() {
    // valid server:port
    MQTTStreamSource source =
      new MQTTStreamSource( "127.0.0.1:" + port, Arrays.asList( "mytopic" ), 2 );
    source.open();
    source.close();

    //invalid tcp://server/port
    try {
      source =
        new MQTTStreamSource( "tcp://127.0.0.1:" + port, Arrays.asList( "mytopic" ), 2 );
      source.open();
      fail( "Expected exception." );
    } catch ( Exception e ) {
      assertThat( e, instanceOf( IllegalArgumentException.class ) );
    }
  }

  @Test
  public void clientIdNotReused() {
    MQTTStreamSource source1 =
      new MQTTStreamSource( "127.0.0.1:" + port, Arrays.asList( "mytopic" ), 2 );
    source1.open();

    MQTTStreamSource source2 =
      new MQTTStreamSource( "127.0.0.1:" + port, Arrays.asList( "mytopic" ), 2 );
    source2.open();

    assertThat( source1.mqttClient.getClientId(), not( equalTo( source2.mqttClient.getClientId() ) ) );

    source1.close();
    source2.close();
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
        pub.publish( topic, new MqttMessage( msg.getBytes() ) );
      }
    } finally {
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
