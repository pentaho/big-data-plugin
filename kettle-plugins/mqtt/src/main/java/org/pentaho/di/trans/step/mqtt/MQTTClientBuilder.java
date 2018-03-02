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
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.BooleanUtils;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.trans.step.StepInterface;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static org.pentaho.di.i18n.BaseMessages.getString;

final class MQTTClientBuilder {
  private static final Class<?> PKG = MQTTClientBuilder.class;

  private static final String UNSECURE_PROTOCOL = "tcp://";
  private static final String SECURE_PROTOCOL = "ssl://";
  // the paho library specifies ssl prop names as com.ibm, though not necessarily using the ibm implementations
  private static final String SSL_PROP_PREFIX = "com.ibm.";

  public static final Map<String, String> DEFAULT_SSL_OPTS = ImmutableMap.<String, String>builder()
    .put( "ssl.protocol", "TLS" )
    .put( "ssl.contextProvider", "" )
    .put( "ssl.keyStore", "" )
    .put( "ssl.keyStorePassword", "" )
    .put( "ssl.keyStoreType", "JKS" )
    .put( "ssl.keyStoreProvider", "" )
    .put( "ssl.trustStore", "" )
    .put( "ssl.trustStorePassword", "" )
    .put( "ssl.trustStoreType", "" )
    .put( "ssl.trustStoreProvider", "" )
    .put( "ssl.enabledCipherSuites", "" )
    .put( "ssl.keyManager", "" )
    .put( "ssl.trustManager", "" )
    .build();

  private String broker;
  private List<String> topics;
  private String qos = "0";
  private boolean isSecure;
  private String username;
  private String password;
  private Map<String, String> sslConfig;
  private String keepAliveInterval;
  private String maxInflight;
  private String connectionTimeout;
  private String cleanSession;
  private String storageLevel;
  private String serverUris;
  private String mqttVersion;
  private String automaticReconnect;
  private MqttCallback callback;
  private String clientId = MqttAsyncClient.generateClientId();  // default
  private StepInterface step;


  @VisibleForTesting @FunctionalInterface interface ClientFactory {
    MqttClient getClient( String broker, String clientId, MqttClientPersistence persistence )
      throws MqttException;
  }

  @VisibleForTesting ClientFactory clientFactory = MqttClient::new;

  private MQTTClientBuilder() {
  }

  static MQTTClientBuilder builder() {
    return new MQTTClientBuilder();
  }

  MQTTClientBuilder withCallback( MqttCallback callback ) {
    this.callback = callback;
    return this;
  }

  MQTTClientBuilder withBroker( String broker ) {
    this.broker = broker;
    return this;
  }

  MQTTClientBuilder withTopics( List<String> topics ) {
    this.topics = topics;
    return this;
  }

  MQTTClientBuilder withQos( String qos ) {
    this.qos = qos;
    return this;
  }

  MQTTClientBuilder withIsSecure( boolean isSecure ) {
    this.isSecure = isSecure;
    return this;
  }

  MQTTClientBuilder withClientId( String clientId ) {
    this.clientId = clientId;
    return this;
  }

  MQTTClientBuilder withUsername( String username ) {
    this.username = username;
    return this;
  }

  MQTTClientBuilder withPassword( String password ) {
    this.password = password;
    return this;
  }

  MQTTClientBuilder withStep( StepInterface step ) {
    this.step = step;
    return this;
  }

  MQTTClientBuilder withSslConfig( Map<String, String> sslConfig ) {
    this.sslConfig = sslConfig;
    return this;
  }

  MQTTClientBuilder withKeepAliveInterval( String keepAliveInterval ) {
    this.keepAliveInterval = keepAliveInterval;
    return this;
  }

  MQTTClientBuilder withMaxInflight( String maxInflight ) {
    this.maxInflight = maxInflight;
    return this;
  }

  MQTTClientBuilder withConnectionTimeout( String connectionTimeout ) {
    this.connectionTimeout = connectionTimeout;
    return this;
  }

  MQTTClientBuilder withCleanSession( String cleanSession ) {
    this.cleanSession = cleanSession;
    return this;
  }

  MQTTClientBuilder withStorageLevel( String storageLevel ) {
    this.storageLevel = storageLevel;
    return this;
  }

  MQTTClientBuilder withServerUris( String serverUris ) {
    this.serverUris = serverUris;
    return this;
  }

  MQTTClientBuilder withMqttVersion( String mqttVersion ) {
    this.mqttVersion = mqttVersion;
    return this;
  }

  MQTTClientBuilder withAutomaticReconnect( String automaticReconnect ) {
    this.automaticReconnect = automaticReconnect;
    return this;
  }

  MqttClient buildAndConnect() throws MqttException {
    validateArgs();

    String broker = getProtocol() + getBroker();
    MqttClientPersistence persistence = new MemoryPersistence();
    String storageLevelOption = step.environmentSubstitute( storageLevel );
    if ( StringUtil.isEmpty( storageLevelOption ) ) {
      step.getLogChannel().logDebug( "Using Memory Storage Level" );
    } else {
      step.getLogChannel().logDebug( "Using File Storage Level to " + storageLevelOption );
      persistence = new MqttDefaultFilePersistence( storageLevelOption );
    }
    MqttClient client = clientFactory.getClient( broker, clientId, persistence );

    client.setCallback( callback );

    step.getLogChannel().logDebug( "Subscribing to topics with a quality of service level of "
      + step.environmentSubstitute( qos ) );
    step.getLogChannel().logDebug( "Server URIs is set to " + step.environmentSubstitute( serverUris ) );
    step.getLogChannel().logDebug( "Max Inflight is set to " + step.environmentSubstitute( maxInflight ) );
    step.getLogChannel()
      .logDebug( "Automatic Reconnect is set to " + step.environmentSubstitute( automaticReconnect ) );
    step.getLogChannel().logDebug( loggableOptions().toString() );

    client.connect( getOptions() );
    if ( topics != null && topics.size() > 0 ) {
      client.subscribe(
        step.environmentSubstitute( topics.toArray( new String[ topics.size() ] ) ),
        initializedIntAray( Integer.parseInt( step.environmentSubstitute( this.qos ) ) )
      );
    }
    return client;
  }

  private String getBroker() {
    return step.environmentSubstitute( this.broker );
  }

  private String getProtocol() {
    return isSecure ? SECURE_PROTOCOL : UNSECURE_PROTOCOL;
  }

  private void validateArgs() {
    // expectation that the broker will contain the server:port.
    checkArgument( getBroker().matches( "^[^ :/]+:\\d+" ),
      getString( PKG, "MQTTInput.Error.ConnectionURL" ) );
    try {
      int qosVal = Integer.parseInt( step.environmentSubstitute( this.qos ) );
      checkArgument( qosVal >= 0 && qosVal <= 2 );
    } catch ( Exception e ) {
      String errorMsg = getString( PKG, "MQTTClientBuilder.Error.QOS",
        step.getStepMeta().getName(), step.environmentSubstitute( qos ) );
      step.getLogChannel().logError( errorMsg );
      throw new IllegalArgumentException( errorMsg );
    }
  }

  private int[] initializedIntAray( int val ) {
    return IntStream.range( 0, topics.size() ).map( i -> val ).toArray();
  }

  private MqttConnectOptions getOptions() {
    MqttConnectOptions options = new MqttConnectOptions();

    if ( isSecure ) {
      setSSLProps( options );
    }
    if ( !StringUtil.isEmpty( username ) ) {
      options.setUserName( step.environmentSubstitute( username ) );
    }
    if ( !StringUtil.isEmpty( password ) ) {
      options.setPassword( step.environmentSubstitute( password ).toCharArray() );
    }

    String optionValue = step.environmentSubstitute( keepAliveInterval );
    if ( !StringUtil.isEmpty( optionValue ) ) {
      options.setKeepAliveInterval( Integer.parseInt( optionValue ) );
    }

    optionValue = step.environmentSubstitute( maxInflight );
    if ( !StringUtil.isEmpty( optionValue ) ) {
      options.setMaxInflight( Integer.parseInt( optionValue ) );
    }

    optionValue = step.environmentSubstitute( connectionTimeout );
    if ( !StringUtil.isEmpty( optionValue ) ) {
      options.setConnectionTimeout( Integer.parseInt( optionValue ) );
    }

    optionValue = step.environmentSubstitute( cleanSession );
    if ( !StringUtil.isEmpty( optionValue ) ) {
      options.setCleanSession( BooleanUtils.toBoolean( optionValue ) );
    }

    optionValue = step.environmentSubstitute( serverUris );
    if ( !StringUtil.isEmpty( optionValue ) ) {
      options.setServerURIs(
        Arrays.stream( optionValue.split( ";" ) ).map( uri -> getProtocol() + uri ).toArray( String[]::new ) );
    }

    optionValue = step.environmentSubstitute( mqttVersion );
    if ( !StringUtil.isEmpty( optionValue ) ) {
      options.setMqttVersion( Integer.parseInt( optionValue ) );
    }

    optionValue = step.environmentSubstitute( automaticReconnect );
    if ( !StringUtil.isEmpty( optionValue ) ) {
      options.setAutomaticReconnect( BooleanUtils.toBoolean( optionValue ) );
    }

    return options;
  }

  /**
   * Returns a copy of loggable options with sensitive data stripped.
   */
  private MqttConnectOptions loggableOptions() {
    MqttConnectOptions loggableOptions = getOptions();

    Optional.ofNullable( loggableOptions.getSSLProperties() )
      .orElseGet( Properties::new )
      .keySet().stream()
      .filter( key -> key.toString().toUpperCase().contains( "PASSWORD" ) )
      .forEach( key -> loggableOptions.getSSLProperties().put( key, "*****" ) );
    return loggableOptions;
  }

  private void setSSLProps( MqttConnectOptions options ) {
    Properties props = new Properties();
    props.putAll(
      sslConfig.entrySet().stream()
        .filter( entry -> !isNullOrEmpty( entry.getValue() ) )
        .collect( Collectors.toMap( e -> SSL_PROP_PREFIX + e.getKey(),
          Map.Entry::getValue ) ) );
    options.setSSLProperties( props );
  }
}
