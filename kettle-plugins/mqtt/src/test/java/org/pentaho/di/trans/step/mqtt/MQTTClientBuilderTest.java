/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
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

import com.google.common.collect.ImmutableMap;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;

import java.util.Collections;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith ( MockitoJUnitRunner.class )
public class MQTTClientBuilderTest {

  @Mock LogChannelInterface logger;
  @Mock MqttCallback callback;
  @Mock MQTTClientBuilder.ClientFactory factory;
  @Mock MqttClient client;
  @Mock StepInterface step;
  @Mock StepMeta meta;

  @Captor ArgumentCaptor<MqttConnectOptions> connectOptsCapture;

  VariableSpace space = new Variables();
  MQTTClientBuilder builder = MQTTClientBuilder.builder();

  @Before
  public void before() throws MqttException {
    builder
      .withBroker( "127.0.0.1:101010" )
      .withStep( step );
    builder.clientFactory = factory;
    when( factory.getClient( any(), any(), any() ) )
      .thenReturn( client );
    when( step.getParentVariableSpace() ).thenReturn( space );
    when( step.getLogChannel() ).thenReturn( logger );
    when( step.getStepMeta() ).thenReturn( meta );
    when( step.getStepMeta().getName() ).thenReturn( "Step Name" );
    when( step.environmentSubstitute( anyString() ) ).thenAnswer( ( answer -> answer.getArguments()[ 0 ] ) );
    when( step.environmentSubstitute( any( String[].class ) ) )
      .thenAnswer( ( answer -> answer.getArguments()[ 0 ] ) );
  }

  @Test
  public void testValidBuilderParams() throws MqttException {
    MqttClient client = builder
      .withQos( "2" )
      .withUsername( "user" )
      .withPassword( "pass" )
      .withIsSecure( true )
      .withTopics( Collections.singletonList( "SomeTopic" ) )
      .withSslConfig( ImmutableMap.of( "ssl.trustStore", "/some/path" ) )
      .withCallback( callback ).
        buildAndConnect();
    verify( client ).setCallback( callback );
    verify( factory ).getClient( anyString(), anyString(), any( MemoryPersistence.class ) );
    verify( client ).connect( connectOptsCapture.capture() );

    MqttConnectOptions opts = connectOptsCapture.getValue();
    assertThat( opts.getUserName(), equalTo( "user" ) );
    assertThat( opts.getPassword(), equalTo( "pass".toCharArray() ) );
    Properties props = opts.getSSLProperties();
    assertThat( props.size(), equalTo( 1 ) );
    assertThat( props.getProperty( "com.ibm.ssl.trustStore" ), equalTo( "/some/path" ) );
  }

  @Test
  public void testFailParsingQOSLevelNotInteger() {
    try {
      builder
        .withQos( "hello" )
        .buildAndConnect();
      fail( "Should of failed initialization." );
    } catch ( Exception e ) {
      // Initialization failed because QOS level isn't 0, 1 or 2
      assertTrue( e instanceof IllegalArgumentException );
    }
    verify( logger )
      .logError( BaseMessages.getString( MQTTConsumer.class, "MQTTClientBuilder.Error.QOS", "Step Name", "hello" ) );
  }

  @Test
  public void testFailParsingQOSLevelNotInRange() {
    try {
      builder
        .withQos( "72" )
        .buildAndConnect();
      fail( "Should of failed initialization." );
    } catch ( Exception e ) {
      // Initialization failed because QOS level isn't 0, 1 or 2
      assertTrue( e instanceof IllegalArgumentException );
    }
    verify( logger )
      .logError( BaseMessages.getString( MQTTConsumer.class, "MQTTClientBuilder.Error.QOS", "Step Name", "72" ) );
  }

}
