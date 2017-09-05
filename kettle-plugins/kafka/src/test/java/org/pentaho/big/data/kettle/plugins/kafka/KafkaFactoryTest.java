/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.pentaho.big.data.kettle.plugins.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceLocator;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.bigdata.api.jaas.JaasConfigService;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.osgi.metastore.locator.api.MetastoreLocator;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;

@RunWith( MockitoJUnitRunner.class )
public class KafkaFactoryTest {
  @Mock Function<Map<String, Object>, Consumer> consumerFun;
  @Mock Function<Map<String, Object>, Producer<Object, Object>> producerFun;
  @Mock NamedClusterService namedClusterService;
  @Mock NamedClusterServiceLocator namedClusterServiceLocator;
  @Mock JaasConfigService jaasConfigService;
  @Mock MetastoreLocator metastoreLocator;
  @Mock IMetaStore metastore;
  @Mock NamedCluster namedCluster;
  @Mock TransMeta transMeta;
  KafkaConsumerInputMeta inputMeta;
  KafkaProducerOutputMeta outputMeta;
  StepMeta stepMeta;

  @Before
  public void setUp() throws ClusterInitializationException {
    when( metastoreLocator.getMetastore() ).thenReturn( metastore );
    when( namedCluster.getKafkaBootstrapServers() ).thenReturn( "server:1234" );
    when( namedClusterService.getNamedClusterByName( "some_cluster", metastore ) ).thenReturn( namedCluster );
    when( namedClusterServiceLocator.getService( namedCluster, JaasConfigService.class ) )
      .thenReturn( jaasConfigService );
    when( transMeta.environmentSubstitute( "${clusterName}" ) ).thenReturn( "some_cluster" );

    inputMeta = new KafkaConsumerInputMeta();
    inputMeta.setNamedClusterService( namedClusterService );
    inputMeta.setMetastoreLocator( metastoreLocator );
    inputMeta.setClusterName( "${clusterName}" );
    inputMeta.setConnectionType( KafkaConsumerInputMeta.ConnectionType.CLUSTER );
    outputMeta = new KafkaProducerOutputMeta();
    outputMeta.setNamedClusterService( namedClusterService );
    outputMeta.setMetastoreLocator( metastoreLocator );
    outputMeta.setClusterName( "${clusterName}" );
    outputMeta.setConnectionType( KafkaProducerOutputMeta.ConnectionType.CLUSTER );

    stepMeta = new StepMeta();
    stepMeta.setParentTransMeta( transMeta );
    inputMeta.setParentStepMeta( stepMeta );
    outputMeta.setParentStepMeta( stepMeta );
  }

  @Test
  public void testMapsConsumers() throws Exception {
    ArrayList<String> topicList = new ArrayList<>();
    topicList.add( "topic" );
    inputMeta.setTopics( topicList );
    inputMeta.setConsumerGroup( "cg" );

    inputMeta.setKeyField( new KafkaConsumerField( KafkaConsumerField.Name.KEY, "key" ) );
    inputMeta.setMessageField( new KafkaConsumerField( KafkaConsumerField.Name.MESSAGE, "msg" ) );
    inputMeta.setNamedClusterServiceLocator( namedClusterServiceLocator );

    Map<String, String> advancedConfig = new LinkedHashMap<>();
    advancedConfig.put( "advanced.config1", "advancedPropertyValue1" );
    advancedConfig.put( "advanced.config2", "advancedPropertyValue2" );
    inputMeta.setConfig( advancedConfig );

    when( jaasConfigService.isKerberos() ).thenReturn( false );

    new KafkaFactory( consumerFun, producerFun ).consumer( inputMeta, Function.identity() );
    Map<String, Object> expectedMap = new HashMap<>();
    expectedMap.put( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "server:1234" );
    expectedMap.put( ConsumerConfig.GROUP_ID_CONFIG, "cg" );
    expectedMap.put( ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class );
    expectedMap.put( ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class );
    expectedMap.put( "advanced.config1", "advancedPropertyValue1" );
    expectedMap.put( "advanced.config2", "advancedPropertyValue2" );

    Mockito.verify( consumerFun ).apply( expectedMap  );
  }

  @Test
  public void testMapsConsumersWithDeserilalizer() throws Exception {
    ArrayList<String> topicList = new ArrayList<>();
    topicList.add( "topic" );
    inputMeta.setTopics( topicList );
    inputMeta.setConsumerGroup( "cg" );

    inputMeta.setKeyField( new KafkaConsumerField( KafkaConsumerField.Name.KEY, "key", KafkaConsumerField.Type.Integer ) );
    inputMeta.setMessageField( new KafkaConsumerField( KafkaConsumerField.Name.MESSAGE, "msg", KafkaConsumerField.Type.Number ) );
    inputMeta.setNamedClusterServiceLocator( namedClusterServiceLocator );
    when( jaasConfigService.isKerberos() ).thenReturn( false );

    new KafkaFactory( consumerFun, producerFun ).consumer( inputMeta, Function.identity(), inputMeta.getKeyField().getOutputType(),
      inputMeta.getMessageField().getOutputType() );
    Map<String, Object> expectedMap = new HashMap<>();
    expectedMap.put( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "server:1234" );
    expectedMap.put( ConsumerConfig.GROUP_ID_CONFIG, "cg" );
    expectedMap.put( ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DoubleDeserializer.class );
    expectedMap.put( ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class );
    Mockito.verify( consumerFun ).apply( expectedMap  );
  }

  @Test
  public void testMapsConsumersWithVariables() throws Exception {
    inputMeta.setConsumerGroup( "${consumerGroup}" );
    inputMeta.setNamedClusterServiceLocator( namedClusterServiceLocator );

    Map<String, String> advancedConfig = new LinkedHashMap<>();
    advancedConfig.put( "advanced.variable", "${advanced.var}" );
    inputMeta.setConfig( advancedConfig );

    when( jaasConfigService.isKerberos() ).thenReturn( false );

    Variables variables = new Variables();
    variables.setVariable( "server", "server:1234" );
    variables.setVariable( "consumerGroup", "cg" );
    variables.setVariable( "advanced.var", "advancedVarValue" );
    new KafkaFactory( consumerFun, producerFun ).consumer( inputMeta, variables::environmentSubstitute );
    Map<String, Object> expectedMap = new HashMap<>();
    expectedMap.put( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "server:1234" );
    expectedMap.put( ConsumerConfig.GROUP_ID_CONFIG, "cg" );
    expectedMap.put( ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class );
    expectedMap.put( ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class );
    expectedMap.put( "advanced.variable", "advancedVarValue" );
    Mockito.verify( consumerFun ).apply( expectedMap  );
  }

  @Test
  public void testMapsProducers() throws Exception {
    outputMeta.setTopic( "topic" );
    outputMeta.setClientId( "client" );
    outputMeta.setKeyField( "key" );
    outputMeta.setMessageField( "msg" );
    outputMeta.setNamedClusterServiceLocator( namedClusterServiceLocator );

    Map<String, String> advancedConfig = new LinkedHashMap<>();
    advancedConfig.put( "advanced.config1", "advancedPropertyValue1" );
    advancedConfig.put( "advanced.config2", "advancedPropertyValue2" );
    outputMeta.setConfig( advancedConfig );

    when( jaasConfigService.isKerberos() ).thenReturn( false );

    new KafkaFactory( consumerFun, producerFun ).producer( outputMeta, Function.identity() );
    Map<String, Object> expectedMap = new HashMap<>();
    expectedMap.put( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "server:1234" );
    expectedMap.put( ProducerConfig.CLIENT_ID_CONFIG, "client" );
    expectedMap.put( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class );
    expectedMap.put( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class );
    expectedMap.put( "advanced.config1", "advancedPropertyValue1" );
    expectedMap.put( "advanced.config2", "advancedPropertyValue2" );

    Mockito.verify( producerFun ).apply( expectedMap  );
  }

  @Test
  public void testMapsProducersWithSerilalizer() throws Exception {
    outputMeta.setTopic( "topic" );
    outputMeta.setClientId( "client" );
    outputMeta.setKeyField( "key" );
    outputMeta.setMessageField( "msg" );
    outputMeta.setNamedClusterServiceLocator( namedClusterServiceLocator );
    when( jaasConfigService.isKerberos() ).thenReturn( false );

    new KafkaFactory( consumerFun, producerFun ).producer( outputMeta, Function.identity(),
      KafkaConsumerField.Type.Integer, KafkaConsumerField.Type.Number );
    Map<String, Object> expectedMap = new HashMap<>();
    expectedMap.put( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "server:1234" );
    expectedMap.put( ProducerConfig.CLIENT_ID_CONFIG, "client" );
    expectedMap.put( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DoubleSerializer.class );
    expectedMap.put( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class );
    Mockito.verify( producerFun ).apply( expectedMap  );
  }

  @Test
  public void testMapsProducersWithVariables() throws Exception {
    outputMeta.setClientId( "${client}" );
    outputMeta.setNamedClusterServiceLocator( namedClusterServiceLocator );

    Map<String, String> advancedConfig = new LinkedHashMap<>();
    advancedConfig.put( "advanced.variable", "${advanced.var}" );
    outputMeta.setConfig( advancedConfig );

    when( jaasConfigService.isKerberos() ).thenReturn( false );

    Variables variables = new Variables();
    variables.setVariable( "server", "server:1234" );
    variables.setVariable( "client", "myclient" );
    variables.setVariable( "advanced.var", "advancedVarValue" );

    new KafkaFactory( consumerFun, producerFun ).producer( outputMeta, variables::environmentSubstitute );
    Map<String, Object> expectedMap = new HashMap<>();
    expectedMap.put( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "server:1234" );
    expectedMap.put( ProducerConfig.CLIENT_ID_CONFIG, "myclient" );
    expectedMap.put( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class );
    expectedMap.put( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class );
    expectedMap.put( "advanced.variable", "advancedVarValue" );

    Mockito.verify( producerFun ).apply( expectedMap  );
  }

  @Test
  public void testNullMetaPropertiesResultInEmptyString() throws Exception {
    outputMeta.setClusterName( null );
    outputMeta.setNamedClusterServiceLocator( namedClusterServiceLocator );
    when( jaasConfigService.isKerberos() ).thenReturn( false );
    Variables variables = new Variables();

    new KafkaFactory( consumerFun, producerFun ).producer( outputMeta, variables::environmentSubstitute );
    Map<String, Object> expectedMap = new HashMap<>();
    expectedMap.put( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "" );
    expectedMap.put( ProducerConfig.CLIENT_ID_CONFIG, "" );
    expectedMap.put( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class );
    expectedMap.put( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class );
    Mockito.verify( producerFun ).apply( expectedMap  );
  }

  @Test
  public void testProvidesJaasConfig() throws Exception {
    ArrayList<String> topicList = new ArrayList<>();
    topicList.add( "topic" );
    inputMeta.setTopics( topicList );
    inputMeta.setConsumerGroup( "cg" );

    inputMeta.setKeyField( new KafkaConsumerField( KafkaConsumerField.Name.KEY, "key" ) );
    inputMeta.setMessageField( new KafkaConsumerField( KafkaConsumerField.Name.MESSAGE, "msg" ) );
    inputMeta.setNamedClusterServiceLocator( namedClusterServiceLocator );
    when( jaasConfigService.isKerberos() ).thenReturn( true );
    when( jaasConfigService.getJaasConfig() ).thenReturn( "some jaas config" );

    new KafkaFactory( consumerFun, producerFun ).consumer( inputMeta, Function.identity() );
    Map<String, Object> expectedMap = new HashMap<>();
    expectedMap.put( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "server:1234" );
    expectedMap.put( ConsumerConfig.GROUP_ID_CONFIG, "cg" );
    expectedMap.put( ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class );
    expectedMap.put( ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class );
    expectedMap.put( SaslConfigs.SASL_JAAS_CONFIG, "some jaas config" );
    expectedMap.put( "security.protocol", "SASL_PLAINTEXT" );
    Mockito.verify( consumerFun ).apply( expectedMap  );
  }
}
