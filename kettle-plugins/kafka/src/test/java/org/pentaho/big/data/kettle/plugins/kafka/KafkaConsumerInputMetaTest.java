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
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceLocator;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.bigdata.api.jaas.JaasConfigService;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.resource.ResourceEntry;
import org.pentaho.di.resource.ResourceReference;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.named.cluster.NamedClusterEmbedManager;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.osgi.metastore.locator.api.MetastoreLocator;
import org.w3c.dom.Node;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.pentaho.big.data.kettle.plugins.kafka.KafkaConsumerInputMeta.BATCH_DURATION;
import static org.pentaho.big.data.kettle.plugins.kafka.KafkaConsumerInputMeta.BATCH_SIZE;
import static org.pentaho.big.data.kettle.plugins.kafka.KafkaConsumerInputMeta.CLUSTER_NAME;
import static org.pentaho.big.data.kettle.plugins.kafka.KafkaConsumerInputMeta.CONNECTION_TYPE;
import static org.pentaho.big.data.kettle.plugins.kafka.KafkaConsumerInputMeta.CONSUMER_GROUP;
import static org.pentaho.big.data.kettle.plugins.kafka.KafkaConsumerInputMeta.ConnectionType.CLUSTER;
import static org.pentaho.big.data.kettle.plugins.kafka.KafkaConsumerInputMeta.ConnectionType.DIRECT;
import static org.pentaho.big.data.kettle.plugins.kafka.KafkaConsumerInputMeta.DIRECT_BOOTSTRAP_SERVERS;
import static org.pentaho.big.data.kettle.plugins.kafka.KafkaConsumerInputMeta.TOPIC;
import static org.pentaho.big.data.kettle.plugins.kafka.KafkaConsumerInputMeta.TRANSFORMATION_PATH;

@RunWith( MockitoJUnitRunner.class )
public class KafkaConsumerInputMetaTest {
  @Mock IMetaStore metastore;
  @Mock Repository rep;
  @Mock MetastoreLocator metastoreLocator;

  @Test
  public void testLoadsFieldsFromXml() throws Exception {
    KafkaConsumerInputMeta meta = new KafkaConsumerInputMeta();
    String inputXml =
      "  <step>\n"
        + "    <name>Kafka Consumer</name>\n"
        + "    <type>KafkaConsumerInput</type>\n"
        + "    <description />\n"
        + "    <distribute>Y</distribute>\n"
        + "    <custom_distribution />\n"
        + "    <copies>1</copies>\n"
        + "    <partitioning>\n"
        + "      <method>none</method>\n"
        + "      <schema_name />\n"
        + "    </partitioning>\n"
        + "    <clusterName>some_cluster</clusterName>\n"
        + "    <directBootstrapServers>some_host:123,some_other_host:456</directBootstrapServers>\n"
        + "    <connectionType>CLUSTER</connectionType>\n"
        + "    <topic>one</topic>\n"
        + "    <consumerGroup>two</consumerGroup>\n"
        + "    <transformationPath>/home/pentaho/myKafkaTransformation.ktr</transformationPath>\n"
        + "    <batchSize>12345</batchSize>\n"
        + "    <batchDuration>999</batchDuration>\n"
        + "    <OutputField kafkaName=\"key\" type=\"String\">three</OutputField>\n"
        + "    <OutputField kafkaName=\"message\" type=\"String\">four</OutputField>\n"
        + "    <OutputField kafkaName=\"topic\" type=\"String\">five</OutputField>\n"
        + "    <OutputField kafkaName=\"partition\" type=\"Integer\">six</OutputField>\n"
        + "    <OutputField kafkaName=\"offset\" type=\"Integer\">seven</OutputField>\n"
        + "    <OutputField kafkaName=\"timestamp\" type=\"Integer\">eight</OutputField>\n"
        + "    <advancedConfig>\n"
        + "        <option property=\"advanced.property1\" value=\"advancedPropertyValue1\"></option>\n"
        + "        <option property=\"advanced.property2\" value=\"advancedPropertyValue2\"></option>\n"
        + "    </advancedConfig>\n"
        + "    <cluster_schema />\n"
        + "    <remotesteps>\n"
        + "      <input>\n"
        + "      </input>\n"
        + "      <output>\n"
        + "      </output>\n"
        + "    </remotesteps>\n"
        + "    <GUI>\n"
        + "      <xloc>208</xloc>\n"
        + "      <yloc>80</yloc>\n"
        + "      <draw>Y</draw>\n"
        + "    </GUI>\n"
        + "  </step>\n";
    Node node = XMLHandler.loadXMLString( inputXml ).getFirstChild();
    meta.loadXML( node, Collections.emptyList(), metastore );
    assertEquals( "some_cluster", meta.getClusterName() );
    assertEquals( "one", meta.getTopics().get( 0 ) );
    assertEquals( "two", meta.getConsumerGroup() );
    assertEquals( "/home/pentaho/myKafkaTransformation.ktr", meta.getTransformationPath() );
    assertEquals( "12345", meta.getBatchSize() );
    assertEquals( "999", meta.getBatchDuration() );
    assertEquals( CLUSTER, meta.getConnectionType() );
    assertEquals( "some_host:123,some_other_host:456", meta.getDirectBootstrapServers() );

    assertEquals( "three", meta.getKeyField().getOutputName() );
    assertEquals( KafkaConsumerField.Type.String, meta.getKeyField().getOutputType() );
    assertEquals( KafkaConsumerField.Name.KEY, meta.getKeyField().getKafkaName() );

    assertEquals( "four", meta.getMessageField().getOutputName() );
    assertEquals( KafkaConsumerField.Type.String, meta.getMessageField().getOutputType() );
    assertEquals( KafkaConsumerField.Name.MESSAGE, meta.getMessageField().getKafkaName() );

    assertEquals( "five", meta.getTopicField().getOutputName() );
    assertEquals( KafkaConsumerField.Type.String, meta.getTopicField().getOutputType() );
    assertEquals( KafkaConsumerField.Name.TOPIC, meta.getTopicField().getKafkaName() );

    assertEquals( "six", meta.getPartitionField().getOutputName() );
    assertEquals( KafkaConsumerField.Type.Integer, meta.getPartitionField().getOutputType() );
    assertEquals( KafkaConsumerField.Name.PARTITION, meta.getPartitionField().getKafkaName() );

    assertEquals( "seven", meta.getOffsetField().getOutputName() );
    assertEquals( KafkaConsumerField.Type.Integer, meta.getOffsetField().getOutputType() );
    assertEquals( KafkaConsumerField.Name.OFFSET, meta.getOffsetField().getKafkaName() );

    assertEquals( "eight", meta.getTimestampField().getOutputName() );
    assertEquals( KafkaConsumerField.Type.Integer, meta.getTimestampField().getOutputType() );
    assertEquals( KafkaConsumerField.Name.TIMESTAMP, meta.getTimestampField().getKafkaName() );

    assertEquals( 2, meta.getConfig().size() );
    assertTrue( meta.getConfig().containsKey( "advanced.property1" ) );
    assertEquals( "advancedPropertyValue1", meta.getConfig().get( "advanced.property1" ) );
    assertTrue( meta.getConfig().containsKey( "advanced.property2" ) );
    assertEquals( "advancedPropertyValue2", meta.getConfig().get( "advanced.property2" ) );
  }

  @Test
  public void testXmlHasAllFields() throws Exception {
    String clusterName = "some_cluster";
    KafkaConsumerInputMeta meta = new KafkaConsumerInputMeta();
    meta.setClusterName( clusterName );

    ArrayList<String> topicList = new ArrayList<String>();
    topicList.add( "temperature" );
    meta.setTopics( topicList );

    meta.setConsumerGroup( "alert" );

    meta.setKeyField( new KafkaConsumerField( KafkaConsumerField.Name.KEY, "kafkaKey" ) );
    meta.setMessageField( new KafkaConsumerField( KafkaConsumerField.Name.MESSAGE, "kafkaMessage" ) );
    meta.setTopicField( new KafkaConsumerField( KafkaConsumerField.Name.TOPIC, "topic" ) );
    meta.setPartitionField( new KafkaConsumerField( KafkaConsumerField.Name.PARTITION, "part",
      KafkaConsumerField.Type.Integer ) );
    meta.setOffsetField( new KafkaConsumerField( KafkaConsumerField.Name.OFFSET, "off",
      KafkaConsumerField.Type.Integer ) );
    meta.setTimestampField( new KafkaConsumerField( KafkaConsumerField.Name.TIMESTAMP, "time",
      KafkaConsumerField.Type.Integer ) );
    meta.setTransformationPath( "/home/pentaho/myKafkaTransformation.ktr" );
    meta.setBatchSize( "54321" );
    meta.setBatchDuration( "987" );
    meta.setConnectionType( DIRECT );
    meta.setDirectBootstrapServers( "localhost:888" );

    Map<String, String> advancedConfig = new LinkedHashMap<>();
    advancedConfig.put( "advanced.property1", "advancedPropertyValue1" );
    advancedConfig.put( "advanced.property2", "advancedPropertyValue2" );
    meta.setConfig( advancedConfig );

    NamedClusterEmbedManager namedClusterEmbedManager = mock( NamedClusterEmbedManager.class );
    TransMeta transMeta = mock( TransMeta.class );
    when( transMeta.getNamedClusterEmbedManager() ).thenReturn( namedClusterEmbedManager );
    StepMeta stepMeta = new StepMeta();
    stepMeta.setParentTransMeta( transMeta );
    meta.setParentStepMeta( stepMeta );

    assertEquals(
        "    <clusterName>some_cluster</clusterName>" + Const.CR
      + "    <topic>temperature</topic>" + Const.CR
      + "    <consumerGroup>alert</consumerGroup>" + Const.CR
      + "    <transformationPath>/home/pentaho/myKafkaTransformation.ktr</transformationPath>" + Const.CR
      + "    <batchSize>54321</batchSize>" + Const.CR
      + "    <batchDuration>987</batchDuration>" + Const.CR
      + "    <connectionType>DIRECT</connectionType>" + Const.CR
      + "    <directBootstrapServers>localhost:888</directBootstrapServers>" + Const.CR
      + "    <OutputField kafkaName=\"key\"  type=\"String\" >kafkaKey</OutputField>" + Const.CR
      + "    <OutputField kafkaName=\"message\"  type=\"String\" >kafkaMessage</OutputField>" + Const.CR
      + "    <OutputField kafkaName=\"topic\"  type=\"String\" >topic</OutputField>" + Const.CR
      + "    <OutputField kafkaName=\"partition\"  type=\"Integer\" >part</OutputField>" + Const.CR
      + "    <OutputField kafkaName=\"offset\"  type=\"Integer\" >off</OutputField>" + Const.CR
      + "    <OutputField kafkaName=\"timestamp\"  type=\"Integer\" >time</OutputField>" + Const.CR
      + "    <advancedConfig>" + Const.CR
      + "        <option property=\"advanced.property1\"  value=\"advancedPropertyValue1\" />" + Const.CR
      + "        <option property=\"advanced.property2\"  value=\"advancedPropertyValue2\" />" + Const.CR
      + "    </advancedConfig>" + Const.CR,
      meta.getXML() );

    verify( namedClusterEmbedManager ).registerUrl( "hc://" + clusterName );
  }

  @Test
  public void testReadsFromRepository() throws Exception {
    KafkaConsumerInputMeta meta = new KafkaConsumerInputMeta();
    StringObjectId stepId = new StringObjectId( "stepId" );
    when( rep.getStepAttributeString( stepId, CLUSTER_NAME ) ).thenReturn( "some_cluster" );
    when( rep.getStepAttributeString( stepId, 0, TOPIC ) ).thenReturn( "readings" );
    when( rep.countNrStepAttributes( stepId, TOPIC ) ).thenReturn( 1 );
    when( rep.getStepAttributeString( stepId, CONSUMER_GROUP ) ).thenReturn( "hooligans" );
    when( rep.getStepAttributeString( stepId, TRANSFORMATION_PATH ) ).thenReturn( "/home/pentaho/atrans.ktr" );
    when( rep.getStepAttributeString( stepId, BATCH_SIZE ) ).thenReturn( "999" );
    when( rep.getStepAttributeString( stepId, BATCH_DURATION ) ).thenReturn( "111" );
    when( rep.getStepAttributeString( stepId, CONNECTION_TYPE ) ).thenReturn( "CLUSTER" );
    when( rep.getStepAttributeString( stepId, DIRECT_BOOTSTRAP_SERVERS ) ).thenReturn( "unused" );

    when( rep.getStepAttributeString( stepId, "OutputField_key" ) ).thenReturn( "machineId" );
    when( rep.getStepAttributeString( stepId, "OutputField_key_type" ) ).thenReturn( "String" );

    when( rep.getStepAttributeString( stepId, "OutputField_message" ) ).thenReturn( "reading" );
    when( rep.getStepAttributeString( stepId, "OutputField_message_type" ) ).thenReturn( "String" );

    when( rep.getStepAttributeString( stepId, "OutputField_topic" ) ).thenReturn( "readings" );
    when( rep.getStepAttributeString( stepId, "OutputField_topic_type" ) ).thenReturn( "String" );

    when( rep.getStepAttributeString( stepId, "OutputField_partition" ) ).thenReturn( "0" );
    when( rep.getStepAttributeString( stepId, "OutputField_partition_type" ) ).thenReturn( "Integer" );

    when( rep.getStepAttributeString( stepId, "OutputField_offset" ) ).thenReturn( "999" );
    when( rep.getStepAttributeString( stepId, "OutputField_offset_type" ) ).thenReturn( "Integer" );

    Date now = new Date();
    when( rep.getStepAttributeString( stepId, "OutputField_timestamp" ) ).thenReturn( String.valueOf( now.getTime() ) );
    when( rep.getStepAttributeString( stepId, "OutputField_timestamp_type" ) ).thenReturn( "Integer" );

    when( rep.getStepAttributeInteger( stepId, meta.ADVANCED_CONFIG + "_COUNT" ) ).thenReturn( 2L );
    when( rep.getStepAttributeString( stepId, 0, meta.ADVANCED_CONFIG + "_NAME" ) ).thenReturn( "advanced.config1" );
    when( rep.getStepAttributeString( stepId, 0, meta.ADVANCED_CONFIG + "_VALUE" ) ).thenReturn( "advancedPropertyValue1" );
    when( rep.getStepAttributeString( stepId, 1, meta.ADVANCED_CONFIG + "_NAME" ) ).thenReturn( "advanced.config2" );
    when( rep.getStepAttributeString( stepId, 1, meta.ADVANCED_CONFIG + "_VALUE" ) ).thenReturn( "advancedPropertyValue2" );

    meta.readRep( rep, metastore, stepId, Collections.emptyList() );
    assertEquals( "some_cluster", meta.getClusterName() );
    assertEquals( "readings", meta.getTopics().get( 0 ) );
    assertEquals( "hooligans", meta.getConsumerGroup() );
    assertEquals( "/home/pentaho/atrans.ktr", meta.getTransformationPath() );
    assertEquals( 999L, Long.parseLong( meta.getBatchSize() ) );
    assertEquals( 111L, Long.parseLong( meta.getBatchDuration() ) );
    assertEquals( CLUSTER, meta.getConnectionType() );
    assertEquals( "unused", meta.getDirectBootstrapServers() );

    assertEquals( KafkaConsumerField.Name.KEY, meta.getKeyField().getKafkaName() );
    assertEquals( "machineId", meta.getKeyField().getOutputName() );
    assertEquals( KafkaConsumerField.Type.String, meta.getKeyField().getOutputType() );

    assertEquals( KafkaConsumerField.Name.MESSAGE, meta.getMessageField().getKafkaName() );
    assertEquals( "reading", meta.getMessageField().getOutputName() );
    assertEquals( KafkaConsumerField.Type.String, meta.getMessageField().getOutputType() );

    assertEquals( KafkaConsumerField.Name.TOPIC, meta.getTopicField().getKafkaName() );
    assertEquals( "readings", meta.getTopicField().getOutputName() );
    assertEquals( KafkaConsumerField.Type.String, meta.getTopicField().getOutputType() );

    assertEquals( KafkaConsumerField.Name.PARTITION, meta.getPartitionField().getKafkaName() );
    assertEquals( "0", meta.getPartitionField().getOutputName() );
    assertEquals( KafkaConsumerField.Type.Integer, meta.getPartitionField().getOutputType() );

    assertEquals( KafkaConsumerField.Name.OFFSET, meta.getOffsetField().getKafkaName() );
    assertEquals( "999", meta.getOffsetField().getOutputName() );
    assertEquals( KafkaConsumerField.Type.Integer, meta.getOffsetField().getOutputType() );

    assertEquals( KafkaConsumerField.Name.TIMESTAMP, meta.getTimestampField().getKafkaName() );
    assertEquals( String.valueOf( now.getTime() ), meta.getTimestampField().getOutputName() );
    assertEquals( KafkaConsumerField.Type.Integer, meta.getTimestampField().getOutputType() );

    assertThat( meta.getConfig().size(), is( 2 ) );
    assertThat( meta.getConfig(), Matchers.hasEntry( "advanced.config1", "advancedPropertyValue1" ) );
    assertThat( meta.getConfig(), Matchers.hasEntry( "advanced.config2", "advancedPropertyValue2" ) );
  }

  @Test
  public void testSavesToRepository() throws Exception {
    KafkaConsumerInputMeta meta = new KafkaConsumerInputMeta();
    StringObjectId stepId = new StringObjectId( "step1" );
    StringObjectId transId = new StringObjectId( "trans1" );
    meta.setClusterName( "some_cluster" );
    ArrayList<String> topicList = new ArrayList<>();
    topicList.add( "temperature" );
    meta.setTopics( topicList );
    meta.setConsumerGroup( "alert" );
    meta.setTransformationPath( "/home/Pentaho/btrans.ktr" );
    meta.setBatchSize( "33" );
    meta.setBatchDuration( "10000" );
    meta.setConnectionType( DIRECT );
    meta.setDirectBootstrapServers( "kafkaServer:9092" );

    meta.setKeyField( new KafkaConsumerField( KafkaConsumerField.Name.KEY, "kafkaKey" ) );
    meta.setMessageField( new KafkaConsumerField( KafkaConsumerField.Name.MESSAGE, "kafkaMessage" ) );

    Map<String, String> advancedConfig = new LinkedHashMap<>();
    advancedConfig.put( "advanced.property1", "advancedPropertyValue1" );
    advancedConfig.put( "advanced.property2", "advancedPropertyValue2" );
    meta.setConfig( advancedConfig );

    meta.saveRep( rep, metastore, transId, stepId );
    verify( rep ).saveStepAttribute( transId, stepId, CLUSTER_NAME, "some_cluster" );
    verify( rep ).saveStepAttribute( transId, stepId, 0, TOPIC, "temperature" );
    verify( rep ).saveStepAttribute( transId, stepId, CONSUMER_GROUP, "alert" );
    verify( rep ).saveStepAttribute( transId, stepId, TRANSFORMATION_PATH, "/home/Pentaho/btrans.ktr" );
    verify( rep ).saveStepAttribute( transId, stepId, BATCH_SIZE, "33" );
    verify( rep ).saveStepAttribute( transId, stepId, BATCH_DURATION, "10000" );
    verify( rep ).saveStepAttribute( transId, stepId, CONNECTION_TYPE, "DIRECT" );
    verify( rep ).saveStepAttribute( transId, stepId, DIRECT_BOOTSTRAP_SERVERS, "kafkaServer:9092" );

    verify( rep ).saveStepAttribute( transId, stepId, "OutputField_key", meta.getKeyField().getOutputName() );
    verify( rep ).saveStepAttribute( transId, stepId, "OutputField_key_type", meta.getKeyField().getOutputType().toString() );

    verify( rep ).saveStepAttribute( transId, stepId, "OutputField_message", meta.getMessageField().getOutputName() );
    verify( rep ).saveStepAttribute( transId, stepId, "OutputField_message_type", meta.getMessageField().getOutputType().toString() );

    verify( rep ).saveStepAttribute( transId, stepId, "OutputField_topic", meta.getTopicField().getOutputName() );
    verify( rep ).saveStepAttribute( transId, stepId, "OutputField_topic_type", meta.getTopicField().getOutputType().toString() );

    verify( rep ).saveStepAttribute( transId, stepId, "OutputField_partition", meta.getPartitionField().getOutputName() );
    verify( rep ).saveStepAttribute( transId, stepId, "OutputField_partition_type", meta.getPartitionField().getOutputType().toString() );

    verify( rep ).saveStepAttribute( transId, stepId, "OutputField_offset", meta.getOffsetField().getOutputName() );
    verify( rep ).saveStepAttribute( transId, stepId, "OutputField_offset_type", meta.getOffsetField().getOutputType().toString() );

    verify( rep ).saveStepAttribute( transId, stepId, "OutputField_timestamp", meta.getTimestampField().getOutputName() );
    verify( rep ).saveStepAttribute( transId, stepId, "OutputField_timestamp_type", meta.getTimestampField().getOutputType().toString() );

    verify( rep, times( 1 ) ).saveStepAttribute( transId, stepId, meta.ADVANCED_CONFIG + "_COUNT", 2 );
    verify( rep ).saveStepAttribute( transId, stepId, 0, meta.ADVANCED_CONFIG + "_NAME", "advanced.property1" );
    verify( rep ).saveStepAttribute( transId, stepId, 0, meta.ADVANCED_CONFIG + "_VALUE", "advancedPropertyValue1" );
    verify( rep ).saveStepAttribute( transId, stepId, 1, meta.ADVANCED_CONFIG + "_NAME", "advanced.property2" );
    verify( rep ).saveStepAttribute( transId, stepId, 1, meta.ADVANCED_CONFIG + "_VALUE", "advancedPropertyValue2" );
  }

  @Test
  public void testReadsBootstrapServersFromNamedCluster() {
    NamedCluster namedCluster = mock( NamedCluster.class );
    when( namedCluster.getKafkaBootstrapServers() ).thenReturn( "server:11111" );

    NamedClusterService namedClusterService = mock( NamedClusterService.class );
    when( namedClusterService.getNamedClusterByName( eq( "my_cluster" ), any( IMetaStore.class ) ) )
        .thenReturn( namedCluster );

    KafkaConsumerInputMeta meta = new KafkaConsumerInputMeta();
    meta.setNamedClusterService( namedClusterService );
    meta.setMetastoreLocator( metastoreLocator );
    meta.setClusterName( "${clusterName}" );
    meta.setConnectionType( CLUSTER );
    meta.setDirectBootstrapServers( "directHost:123" );

    TransMeta transMeta = mock( TransMeta.class );
    when( transMeta.environmentSubstitute( "${clusterName}" ) ).thenReturn( "my_cluster" );
    StepMeta stepMeta = new StepMeta();
    stepMeta.setParentTransMeta( transMeta );
    meta.setParentStepMeta( stepMeta );

    assertEquals( "server:11111", meta.getBootstrapServers() );
    meta.setConnectionType( DIRECT );
    assertEquals( "directHost:123", meta.getBootstrapServers() );
  }

  @Test
  public void testGetJaasConfig() throws Exception {
    NamedClusterServiceLocator namedClusterLocator = mock( NamedClusterServiceLocator.class );
    NamedClusterService namedClusterService = mock( NamedClusterService.class );
    JaasConfigService jaasConfigService = mock( JaasConfigService.class );
    NamedCluster namedCluster =  mock( NamedCluster.class );
    when( metastoreLocator.getMetastore() ).thenReturn( metastore );
    when( namedClusterService.getNamedClusterByName( "kurtsCluster", metastore ) ).thenReturn( namedCluster );
    when( namedClusterLocator.getService( namedCluster, JaasConfigService.class ) ).thenReturn( jaasConfigService );
    KafkaConsumerInputMeta inputMeta = new KafkaConsumerInputMeta();
    inputMeta.setNamedClusterServiceLocator( namedClusterLocator );
    inputMeta.setNamedClusterService( namedClusterService );
    inputMeta.setClusterName( "${clusterName}" );
    inputMeta.setMetastoreLocator( metastoreLocator );
    inputMeta.setConnectionType( CLUSTER );

    TransMeta transMeta = mock( TransMeta.class );
    when( transMeta.environmentSubstitute( "${clusterName}" ) ).thenReturn( "kurtsCluster" );
    StepMeta stepMeta = new StepMeta();
    stepMeta.setParentTransMeta( transMeta );
    inputMeta.setParentStepMeta( stepMeta );

    assertEquals( jaasConfigService, inputMeta.getJaasConfigService().get() );
    inputMeta.setConnectionType( DIRECT );
    assertFalse( inputMeta.getJaasConfigService().isPresent() );
  }

  @Test
  public void testGetJaasConfigException() throws Exception {
    NamedClusterServiceLocator namedClusterLocator = mock( NamedClusterServiceLocator.class );
    NamedClusterService namedClusterService = mock( NamedClusterService.class );
    NamedCluster namedCluster =  mock( NamedCluster.class );
    when( metastoreLocator.getMetastore() ).thenReturn( metastore );
    when( namedClusterService.getNamedClusterByName( "kurtsCluster", metastore ) ).thenReturn( namedCluster );
    when( namedClusterLocator.getService( namedCluster, JaasConfigService.class ) )
      .thenThrow( new ClusterInitializationException( new Exception( "oops" ) ) );
    KafkaConsumerInputMeta inputMeta = new KafkaConsumerInputMeta();
    inputMeta.setNamedClusterServiceLocator( namedClusterLocator );
    inputMeta.setNamedClusterService( namedClusterService );
    inputMeta.setClusterName( "kurtsCluster" );
    inputMeta.setMetastoreLocator( metastoreLocator );
    inputMeta.setConnectionType( CLUSTER );
    assertFalse( inputMeta.getJaasConfigService().isPresent() );
  }

  @Test
  public void testGetResourceDependencies() {
    String stepId = "KafkConsumerInput";
    String path = "/home/bgroves/fake.ktr";

    StepMeta stepMeta = new StepMeta();
    stepMeta.setStepID( stepId );
    KafkaConsumerInputMeta inputMeta = new KafkaConsumerInputMeta();
    List<ResourceReference> resourceDependencies = inputMeta.getResourceDependencies( new TransMeta(), stepMeta );
    assertEquals( 0, resourceDependencies.get( 0 ).getEntries().size() );

    inputMeta.setTransformationPath( path );
    resourceDependencies = inputMeta.getResourceDependencies( new TransMeta(), stepMeta );
    assertEquals( 1, resourceDependencies.get( 0 ).getEntries().size() );
    assertEquals( path, resourceDependencies.get( 0 ).getEntries().get( 0 ).getResource() );
    assertEquals( ResourceEntry.ResourceType.ACTIONFILE, resourceDependencies.get( 0 ).getEntries().get( 0 ).getResourcetype() );
  }

  @Test
  public void testDirecIsDefault() throws Exception {
    assertEquals( DIRECT, new KafkaConsumerInputMeta().getConnectionType() );
  }

  @Test
  public void testMDI() {
    KafkaConsumerInputMeta meta = new KafkaConsumerInputMeta();
    meta.injectedConfigNames = Arrays.asList( "injectedName" );
    meta.injectedConfigValues = Arrays.asList( "injectedValue" );
    meta.applyInjectedProperties();
    assertThat( meta.getConfig().size(), Matchers.is( 1 ) );
    assertThat( meta.getConfig(), hasEntry( "injectedName", "injectedValue" ) );
  }
}
