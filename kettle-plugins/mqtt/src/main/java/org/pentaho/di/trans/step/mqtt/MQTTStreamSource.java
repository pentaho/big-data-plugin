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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.trans.streaming.common.BaseStreamStep;
import org.pentaho.di.trans.streaming.common.BlockingQueueStreamSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static org.pentaho.di.i18n.BaseMessages.getString;

/**
 * StreamSource implementation which supplies a blocking iterable of MQTT messages based on the specified topics,
 * broker, and qos. The parent class .rows() method is responsible for creating the blocking iterable.
 */
public class MQTTStreamSource extends BlockingQueueStreamSource<List<Object>> {
  private final Logger logger = LoggerFactory.getLogger( this.getClass() );
  private static final Class<?> PKG = MQTTStreamSource.class;
  private static final String UNSECURE_PROTOCOL = "tcp://";
  private static final String SECURE_PROTOCOL = "ssl://";

  private final String broker;
  private final List<String> topics;
  private final int qos;
  private final boolean isSecure;
  private final String username;
  private final String password;

  private final Map<String, String> sslConfig;


  @VisibleForTesting protected MqttClient mqttClient;

  private MqttCallback callback = new MqttCallback() {
    @Override public void connectionLost( Throwable cause ) {
      error( cause );
    }

    @Override public void messageArrived( String topic, MqttMessage message ) throws Exception {
      acceptRows( singletonList( ImmutableList.of( new String( message.getPayload() ), topic ) ) );
    }

    @Override public void deliveryComplete( IMqttDeliveryToken token ) {
      // no op
    }
  };

  public MQTTStreamSource( String broker, List<String> topics, int qualityOfService,
                           BaseStreamStep mqttConsumer, boolean isSecure, String username,
                           String password, Map<String, String> sslConfig ) {
    super( mqttConsumer );
    this.broker = broker;
    this.topics = topics;
    this.qos = qualityOfService;
    this.isSecure = isSecure;
    this.username = username;
    this.password = password;
    this.sslConfig = sslConfig;
  }

  //  public MQTTStreamSource( String broker, List<String> topics, int qualityOfService,
  //                           MQTTConsumer mqttConsumer, Map<String, String> sslConfig ) {
  //    this( broker, topics, qualityOfService, mqttConsumer );
  //    this.sslConfig = sslConfig;
  //  }

  @Override public void open() {
    try {
      Preconditions
        .checkArgument( broker.matches( "^[^ :/]+:\\d+" ), getString( PKG, "MQTTInput.Error.ConnectionURL" ) );
      // expectation that the broker will contain the server:port.
      mqttClient = new MqttClient(
        UNSECURE_PROTOCOL + broker, MqttAsyncClient.generateClientId(), new MemoryPersistence() );


      mqttClient.connect( new MqttConnectOptions() ); // keeping default ops for now
      mqttClient.setCallback( callback );
      streamStep.logDebug( "Subscribing to topics with a quality of service level of " + qos );
      mqttClient.subscribe( topics.toArray( new String[ 0 ] ), initializedIntAray( qos ) );
    } catch ( MqttException e ) {
      logger.error( e.getMessage(), e );
    }
  }

  // create an int array filled with the same val
  private int[] initializedIntAray( int val ) {
    return IntStream.range( 0, topics.size() ).map( i -> val ).toArray();
  }

  @Override public void close() {
    super.close();
    try {
      mqttClient.disconnect();
      mqttClient.close();
    } catch ( MqttException e ) {
      logger.error( e.getMessage(), e );
    }

  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private String broker;
    private List<String> topics;
    private String qos;
    private boolean isSecure;
    private String username;
    private String password;
    private Map<String, String> sslConfig;
    private BaseStreamStep baseStreamStep;
    private VariableSpace space = new Variables();

    private Builder() {
    }

    public Builder withBroker( String broker ) {
      this.broker = broker;
      return this;
    }

    public Builder withTopics( List<String> topics ) {
      this.topics = topics;
      return this;
    }

    public Builder withQos( String qos ) {
      this.qos = qos;
      return this;
    }

    public Builder withIsSecure( boolean isSecure ) {
      this.isSecure = isSecure;
      return this;
    }

    public Builder withUsername( String username ) {
      this.username = username;
      return this;
    }

    public Builder withPassword( String password ) {
      this.password = password;
      return this;
    }

    public Builder withSslConfig( Map<String, String> sslConfig ) {
      this.sslConfig = sslConfig;
      return this;
    }

    public Builder withBaseStep( BaseStreamStep step ) {
      this.baseStreamStep = step;
      return this;
    }

    public Builder withVariables( VariableSpace space ) {
      this.space = space;
      return this;
    }

    public MQTTStreamSource build() {
      MQTTStreamSource streamSource =
        new MQTTStreamSource(
          space.environmentSubstitute( broker ),
          Arrays.asList( space.environmentSubstitute( topics.toArray( new String[ 0 ] ) ) ),
          Integer.parseInt( space.environmentSubstitute( this.qos ) ),
          baseStreamStep,
          isSecure,
          space.environmentSubstitute( username ),
          space.environmentSubstitute( password ),
          sslConfig );
      // TODO variable subs for ssl stuff.
      return streamSource;
    }
  }
}
