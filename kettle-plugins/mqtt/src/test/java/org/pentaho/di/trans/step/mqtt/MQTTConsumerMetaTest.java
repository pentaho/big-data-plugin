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

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.encryption.TwoWayPasswordEncoderPluginType;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.streaming.common.BaseStreamStepMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.pentaho.di.trans.step.mqtt.MQTTConsumerMeta.MQTT_SERVER;
import static org.pentaho.di.trans.step.mqtt.MQTTConsumerMeta.QOS;
import static org.pentaho.di.trans.step.mqtt.MQTTConsumerMeta.TOPICS;
import static org.pentaho.di.trans.step.mqtt.MQTTConsumerMeta.USERNAME;
import static org.pentaho.di.trans.streaming.common.BaseStreamStepMeta.DURATION;
import static org.pentaho.di.trans.streaming.common.BaseStreamStepMeta.NUM_MESSAGES;
import static org.pentaho.di.trans.streaming.common.BaseStreamStepMeta.PASSWORD;
import static org.pentaho.di.trans.streaming.common.BaseStreamStepMeta.TRANSFORMATION_PATH;

@RunWith( MockitoJUnitRunner.class )
public class MQTTConsumerMetaTest {

  @Mock private IMetaStore metastore;
  @Mock Repository rep;

  @BeforeClass
  public static void setUpBeforeClass() throws KettleException {
    PluginRegistry.addPluginType( TwoWayPasswordEncoderPluginType.getInstance() );
    PluginRegistry.init( true );
    String passwordEncoderPluginID =
      Const.NVL( EnvUtil.getSystemProperty( Const.KETTLE_PASSWORD_ENCODER_PLUGIN ), "Kettle" );
    Encr.init( passwordEncoderPluginID );
  }

  @Test
  public void testLoadAndSave() throws KettleXMLException {
    MQTTConsumerMeta meta = new MQTTConsumerMeta();
    String inputXml = "  <step>\n"
      + "    <name>MQTT Consumer</name>\n"
      + "    <type>MQTTConsumer</type>\n"
      + "    <description />\n"
      + "    <distribute>Y</distribute>\n"
      + "    <custom_distribution />\n"
      + "    <copies>1</copies>\n"
      + "    <partitioning>\n"
      + "      <method>none</method>\n"
      + "      <schema_name />\n"
      + "    </partitioning>\n"
      + "    <TOPICS>one</TOPICS>\n"
      + "    <TOPICS>two</TOPICS>\n"
      + "    <QOS>1</QOS>\n"
      + "    <PASSWORD>Encrypted 2be98afc86aa7f2e4cb79ce10ca97bcce</PASSWORD>\n"
      + "    <USERNAME>testuser</USERNAME>\n"
      + "    <MSG_OUTPUT_NAME>Message</MSG_OUTPUT_NAME>\n"
      + "    <NUM_MESSAGES>5</NUM_MESSAGES>\n"
      + "    <MQTT_SERVER>mqttHost:1883</MQTT_SERVER>\n"
      + "    <TOPIC_OUTPUT_NAME>Topic</TOPIC_OUTPUT_NAME>\n"
      + "    <DURATION>60000</DURATION>\n"
      + "    <TRANSFORMATION_PATH>${Internal.Entry.Current.Directory}/write-to-log.ktr</TRANSFORMATION_PATH>\n"
      + "    <cluster_schema />\n"
      + "    <remotesteps>\n"
      + "      <input>\n"
      + "      </input>\n"
      + "      <output>\n"
      + "      </output>\n"
      + "    </remotesteps>\n"
      + "    <GUI>\n"
      + "      <xloc>80</xloc>\n"
      + "      <yloc>64</yloc>\n"
      + "      <draw>Y</draw>\n"
      + "    </GUI>\n"
      + "  </step>\n";
    Node node = XMLHandler.loadXMLString( inputXml ).getFirstChild();
    meta.loadXML( node, Collections.emptyList(), metastore );
    assertEquals( "one", meta.getTopics().get( 0 ) );
    assertEquals( "two", meta.getTopics().get( 1 ) );
    assertEquals( "1", meta.getQos() );
    assertEquals( "${Internal.Entry.Current.Directory}/write-to-log.ktr", meta.getTransformationPath() );
    assertEquals( "${Internal.Entry.Current.Directory}/write-to-log.ktr", meta.getFileName() );
    assertEquals( "5", meta.getBatchSize() );
    assertEquals( "60000", meta.getBatchDuration() );
    assertEquals( "mqttHost:1883", meta.getMqttServer() );
    assertEquals( "testuser", meta.getUsername() );
    assertEquals( "test", meta.getPassword() );
  }

  @Test
  public void testXmlHasAllFields() {
    String serverName = "some_cluster";
    MQTTConsumerMeta meta = new MQTTConsumerMeta();
    meta.setMqttServer( serverName );

    ArrayList<String> topicList = new ArrayList<>();
    topicList.add( "temperature" );
    meta.setTopics( topicList );
    meta.setQos( "1" );
    meta.setUsername( "testuser" );
    meta.setPassword( "test" );

    meta.setTransformationPath( "/home/pentaho/myKafkaTransformation.ktr" );
    meta.setBatchSize( "54321" );
    meta.setBatchDuration( "987" );

    StepMeta stepMeta = new StepMeta();
    TransMeta transMeta = mock( TransMeta.class );
    stepMeta.setParentTransMeta( transMeta );
    meta.setParentStepMeta( stepMeta );

    assertEquals(
      "<TOPICS>temperature</TOPICS>" + Const.CR
        + "<MSG_OUTPUT_NAME>Message</MSG_OUTPUT_NAME>" + Const.CR
        + "<NUM_MESSAGES>54321</NUM_MESSAGES>" + Const.CR
        + "<QOS>1</QOS>" + Const.CR
        + "<PASSWORD>Encrypted 2be98afc86aa7f2e4cb79ce10ca97bcce</PASSWORD>" + Const.CR
        + "<USERNAME>testuser</USERNAME>" + Const.CR
        + "<MQTT_SERVER>some_cluster</MQTT_SERVER>" + Const.CR
        + "<TOPIC_OUTPUT_NAME>Topic</TOPIC_OUTPUT_NAME>" + Const.CR
        + "<DURATION>987</DURATION>" + Const.CR
        + "<TRANSFORMATION_PATH>/home/pentaho/myKafkaTransformation.ktr</TRANSFORMATION_PATH>" + Const.CR,
      meta.getXML() );
  }

  @Test
  public void testReadsFromRepository() throws Exception {
    MQTTConsumerMeta meta = new MQTTConsumerMeta();
    StringObjectId stepId = new StringObjectId( "stepId" );
    when( rep.getStepAttributeString( stepId, 0, TOPICS ) ).thenReturn( "readings" );
    when( rep.countNrStepAttributes( stepId, TOPICS ) ).thenReturn( 1 );
    when( rep.getStepAttributeString( stepId, TRANSFORMATION_PATH ) ).thenReturn( "/home/pentaho/atrans.ktr" );
    when( rep.getStepAttributeString( stepId, NUM_MESSAGES ) ).thenReturn( "999" );
    when( rep.getStepAttributeString( stepId, DURATION ) ).thenReturn( "111" );
    when( rep.getStepAttributeString( stepId, MQTT_SERVER ) ).thenReturn( "host111" );
    when( rep.getStepAttributeString( stepId, QOS ) ).thenReturn( "2" );
    when( rep.getStepAttributeString( stepId, USERNAME ) ).thenReturn( "testuser" );
    when( rep.getStepAttributeString( stepId, PASSWORD ) ).thenReturn( "Encrypted 2be98afc86aa7f2e4cb79ce10ca97bcce" );

    meta.readRep( rep, metastore, stepId, Collections.emptyList() );
    assertEquals( "readings", meta.getTopics().get( 0 ) );
    assertEquals( "/home/pentaho/atrans.ktr", meta.getTransformationPath() );
    assertEquals( "/home/pentaho/atrans.ktr", meta.getFileName() );
    assertEquals( 999L, Long.parseLong( meta.getBatchSize() ) );
    assertEquals( 111L, Long.parseLong( meta.getBatchDuration() ) );
    assertEquals( "host111", meta.getMqttServer() );
    assertEquals( "testuser", meta.getUsername() );
    assertEquals( "test", meta.getPassword() );
  }

  @Test
  public void testSavesToRepository() throws Exception {
    MQTTConsumerMeta meta = new MQTTConsumerMeta();
    StringObjectId stepId = new StringObjectId( "step1" );
    StringObjectId transId = new StringObjectId( "trans1" );
    ArrayList<String> topicList = new ArrayList<>();
    topicList.add( "temperature" );
    meta.setTopics( topicList );
    meta.setQos( "1" );
    meta.setTransformationPath( "/home/Pentaho/btrans.ktr" );
    meta.setBatchSize( "33" );
    meta.setBatchDuration( "10000" );
    meta.setMqttServer( "mqttServer:1883" );
    meta.setUsername( "testuser" );
    meta.setPassword( "test" );
    meta.saveRep( rep, metastore, transId, stepId );
    verify( rep ).saveStepAttribute( transId, stepId, MQTTConsumerMeta.MQTT_SERVER, "mqttServer:1883" );
    verify( rep ).saveStepAttribute( transId, stepId, 0, TOPICS, "temperature" );
    verify( rep ).saveStepAttribute( transId, stepId, TRANSFORMATION_PATH, "/home/Pentaho/btrans.ktr" );
    verify( rep ).saveStepAttribute( transId, stepId, BaseStreamStepMeta.NUM_MESSAGES, "33" );
    verify( rep ).saveStepAttribute( transId, stepId, BaseStreamStepMeta.DURATION, "10000" );
    verify( rep ).saveStepAttribute( transId, stepId, QOS, "1" );
    verify( rep ).saveStepAttribute( transId, stepId, USERNAME, "testuser" );
    verify( rep ).saveStepAttribute( transId, stepId, PASSWORD, "Encrypted 2be98afc86aa7f2e4cb79ce10ca97bcce" );
  }

  @Test
  public void testSaveDefaultEmptyConnection() {
    MQTTConsumerMeta meta = new MQTTConsumerMeta();
    assertEquals(
      "<MSG_OUTPUT_NAME>Message</MSG_OUTPUT_NAME>" + Const.CR
        + "<NUM_MESSAGES>1000</NUM_MESSAGES>" + Const.CR
        + "<QOS>0</QOS>" + Const.CR
        + "<PASSWORD>Encrypted </PASSWORD>" + Const.CR
        + "<USERNAME/>" + Const.CR
        + "<MQTT_SERVER/>" + Const.CR
        + "<TOPIC_OUTPUT_NAME>Topic</TOPIC_OUTPUT_NAME>" + Const.CR
        + "<DURATION>1000</DURATION>" + Const.CR
        + "<TRANSFORMATION_PATH/>" + Const.CR, meta.getXML() );
  }
}
