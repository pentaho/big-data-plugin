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

import com.google.common.collect.ImmutableMap;
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
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.MQTT_SERVER;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.QOS;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.SSL_KEYS;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.SSL_VALUES;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.TOPICS;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.USERNAME;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.USE_SSL;
import static org.pentaho.di.trans.streaming.common.BaseStreamStepMeta.DURATION;
import static org.pentaho.di.trans.streaming.common.BaseStreamStepMeta.NUM_MESSAGES;
import static org.pentaho.di.trans.streaming.common.BaseStreamStepMeta.PASSWORD;
import static org.pentaho.di.trans.streaming.common.BaseStreamStepMeta.TRANSFORMATION_PATH;

@RunWith ( MockitoJUnitRunner.class )
public class MQTTConsumerMetaTest {

  @Mock private IMetaStore metastore;
  @Mock private Repository rep;

  @BeforeClass
  public static void setUpBeforeClass() throws KettleException {
    PluginRegistry.addPluginType( TwoWayPasswordEncoderPluginType.getInstance() );
    PluginRegistry.init( true );
    String passwordEncoderPluginID =
      Const.NVL( EnvUtil.getSystemProperty( Const.KETTLE_PASSWORD_ENCODER_PLUGIN ), "Kettle" );
    Encr.init( passwordEncoderPluginID );
  }

  private MQTTConsumerMeta meta = new MQTTConsumerMeta();

  @Test
  public void testLoadAndSave() throws KettleXMLException {

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
    meta.setDefault();
    meta.setMqttServer( serverName );

    ArrayList<String> topicList = new ArrayList<>();
    topicList.add( "temperature" );
    meta.setTopics( topicList );
    meta.setQos( "1" );
    meta.setUsername( "testuser" );
    meta.setPassword( "test" );

    meta.setUseSsl( true );
    meta.setTransformationPath( "/home/pentaho/myKafkaTransformation.ktr" );
    meta.setBatchSize( "54321" );
    meta.setBatchDuration( "987" );

    StepMeta stepMeta = new StepMeta();
    TransMeta transMeta = mock( TransMeta.class );
    stepMeta.setParentTransMeta( transMeta );
    meta.setParentStepMeta( stepMeta );

    // tests serialization/deserialization round trip
    assertTrue( meta.equals( fromXml( meta.getXML() ) ) );
  }

  @Test
  public void testReadsFromRepository() throws Exception {
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
    when( rep.getStepAttributeBoolean( stepId, USE_SSL ) ).thenReturn( true );
    when( rep.countNrStepAttributes( stepId, SSL_KEYS ) ).thenReturn( 2 );
    when( rep.countNrStepAttributes( stepId, SSL_VALUES ) ).thenReturn( 2 );
    when( rep.getStepAttributeString( stepId, 0, SSL_KEYS ) ).thenReturn( "key1" );
    when( rep.getStepAttributeString( stepId, 1, SSL_KEYS ) ).thenReturn( "password" );
    when( rep.getStepAttributeString( stepId, 0, SSL_VALUES ) ).thenReturn( "val1" );
    when( rep.getStepAttributeString( stepId, 1, SSL_VALUES ) )
      .thenReturn( Encr.encryptPasswordIfNotUsingVariables( "foobarbaz" ) );

    meta.readRep( rep, metastore, stepId, Collections.emptyList() );
    assertEquals( "readings", meta.getTopics().get( 0 ) );
    assertEquals( "/home/pentaho/atrans.ktr", meta.getTransformationPath() );
    assertEquals( "/home/pentaho/atrans.ktr", meta.getFileName() );
    assertEquals( 999L, Long.parseLong( meta.getBatchSize() ) );
    assertEquals( 111L, Long.parseLong( meta.getBatchDuration() ) );
    assertEquals( "host111", meta.getMqttServer() );
    assertEquals( "testuser", meta.getUsername() );
    assertEquals( "test", meta.getPassword() );
    assertEquals( true, meta.isUseSsl() );
    assertThat( meta.getSslConfig().size(), equalTo( 2 ) );
    assertThat( meta.getSslConfig().get( "key1" ), equalTo( "val1" ) );
    assertThat( meta.getSslConfig().get( "password" ), equalTo( "foobarbaz" ) );

  }

  @Test
  public void testSavesToRepository() throws Exception {
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
    meta.setUseSsl( true );
    meta.setSslConfig( ImmutableMap.of( "key1", "val1",
      "key2", "val2",
      "password", "foobarbaz" ) );
    meta.saveRep( rep, metastore, transId, stepId );
    verify( rep ).saveStepAttribute( transId, stepId, MQTT_SERVER, "mqttServer:1883" );
    verify( rep ).saveStepAttribute( transId, stepId, 0, TOPICS, "temperature" );
    verify( rep ).saveStepAttribute( transId, stepId, TRANSFORMATION_PATH, "/home/Pentaho/btrans.ktr" );
    verify( rep ).saveStepAttribute( transId, stepId, BaseStreamStepMeta.NUM_MESSAGES, "33" );
    verify( rep ).saveStepAttribute( transId, stepId, BaseStreamStepMeta.DURATION, "10000" );
    verify( rep ).saveStepAttribute( transId, stepId, QOS, "1" );
    verify( rep ).saveStepAttribute( transId, stepId, USERNAME, "testuser" );
    verify( rep ).saveStepAttribute( transId, stepId, PASSWORD, "Encrypted 2be98afc86aa7f2e4cb79ce10ca97bcce" );
    verify( rep ).saveStepAttribute( transId, stepId, USE_SSL, true );
    verify( rep ).saveStepAttribute( transId, stepId, SSL_KEYS, "key1" );
    verify( rep ).saveStepAttribute( transId, stepId, SSL_KEYS, "key2" );
    verify( rep ).saveStepAttribute( transId, stepId, SSL_KEYS, "password" );
    verify( rep ).saveStepAttribute( transId, stepId, SSL_VALUES, "val1" );
    verify( rep ).saveStepAttribute( transId, stepId, SSL_VALUES, "val2" );
    verify( rep )
      .saveStepAttribute( transId, stepId, SSL_VALUES, Encr.encryptPasswordIfNotUsingVariables( "foobarbaz" ) );
  }

  @Test
  public void testSaveDefaultEmptyConnection() {
    assertEquals(
      "<MSG_OUTPUT_NAME>Message</MSG_OUTPUT_NAME>" + Const.CR
        + "<NUM_MESSAGES>1000</NUM_MESSAGES>" + Const.CR
        + "<PASSWORD>Encrypted </PASSWORD>" + Const.CR
        + "<QOS>0</QOS>" + Const.CR
        + "<USERNAME/>" + Const.CR
        + "<MQTT_SERVER/>" + Const.CR
        + "<DURATION>1000</DURATION>" + Const.CR
        + "<TOPIC_OUTPUT_NAME>Topic</TOPIC_OUTPUT_NAME>" + Const.CR
        + "<TRANSFORMATION_PATH/>" + Const.CR
        + "<SSL>" + Const.CR
        + "<USE_SSL>false</USE_SSL>" + Const.CR
        + "</SSL>" + Const.CR, meta.getXML() );
  }

  @Test
  public void testGetSslConfig() {
    meta.setDefault();

    meta.setTopics( Collections.singletonList( "mytopic" ) );
    Map<String, String> config = meta.getSslConfig();
    assertTrue( config.size() > 0 );
    assertThat( meta.sslKeys.size(), equalTo( config.size() ) );
    assertTrue( config.keySet().containsAll( meta.sslKeys ) );
    for ( int i = 0; i < meta.sslKeys.size(); i++ ) {
      assertThat( config.get( meta.sslKeys.get( i ) ), equalTo( meta.sslValues.get( i ) ) );
    }
    MQTTConsumerMeta roundTrip = fromXml( meta.getXML() );

    assertTrue( meta.equals( roundTrip ) );

  }

  @Test
  public void testSetSslConfig() {
    meta.setDefault();
    meta.setTopics( asList( "foo", "bar", "bop" ) );
    Map<String, String> fakeConfig = ImmutableMap.of(
      "key1", "value1",
      "key2", "value2",
      "key3", "value3",
      "key4", "value4"
    );
    meta.setSslConfig( fakeConfig );

    MQTTConsumerMeta deserMeta = fromXml( meta.getXML() );
    assertThat( fakeConfig, equalTo( deserMeta.getSslConfig() ) );
    assertTrue( meta.equals( deserMeta ) );


  }

  public static MQTTConsumerMeta fromXml( String metaXml ) {
    Document doc;
    try {
      doc = XMLHandler.loadXMLString( "<step>" + metaXml + "</step>" );
      Node stepNode = XMLHandler.getSubNode( doc, "step" );
      MQTTConsumerMeta mqttConsumerMeta = new MQTTConsumerMeta();
      mqttConsumerMeta.loadXML( stepNode, Collections.emptyList(), (IMetaStore) null );
      return mqttConsumerMeta;
    } catch ( KettleXMLException e ) {
      throw new RuntimeException( e );
    }
  }
}
