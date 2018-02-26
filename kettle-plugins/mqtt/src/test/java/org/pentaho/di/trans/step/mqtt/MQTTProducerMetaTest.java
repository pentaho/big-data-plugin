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
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.util.Collections;

import static com.google.common.collect.ImmutableMap.of;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.AUTOMATIC_RECONNECT;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.CLEAN_SESSION;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.CLIENT_ID;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.CONNECTION_TIMEOUT;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.KEEP_ALIVE_INTERVAL;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.MAX_INFLIGHT;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.MESSAGE_FIELD;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.MQTT_SERVER;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.MQTT_VERSION;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.PASSWORD;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.QOS;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.SERVER_URIS;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.STORAGE_LEVEL;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.TOPIC;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.USERNAME;

@RunWith ( MockitoJUnitRunner.class )
public class MQTTProducerMetaTest {

  @Mock private IMetaStore metaStore;
  @Mock private Repository rep;

  @BeforeClass
  public static void setUpBeforeClass() throws KettleException {
    PluginRegistry.addPluginType( TwoWayPasswordEncoderPluginType.getInstance() );
    PluginRegistry.init( true );
    String passwordEncoderPluginID =
      Const.NVL( EnvUtil.getSystemProperty( Const.KETTLE_PASSWORD_ENCODER_PLUGIN ), "Kettle" );
    Encr.init( passwordEncoderPluginID );
  }

  @Test
  public void testLoadAndSave() throws KettleException {
    MQTTProducerMeta meta = new MQTTProducerMeta();
    String inputXml = "  <step>\n"
      + "    <name>The MQTT Consumer</name>\n"
      + "    <type>MQTTConsumer</type>\n"
      + "    <description />\n"
      + "    <distribute>Y</distribute>\n"
      + "    <custom_distribution />\n"
      + "    <copies>1</copies>\n"
      + "    <partitioning>\n"
      + "      <method>none</method>\n"
      + "      <schema_name />\n"
      + "    </partitioning>\n"
      + "    <MQTT_SERVER>mqtthost:1883</MQTT_SERVER>\n"
      + "    <KEEP_ALIVE_INTERVAL>1000</KEEP_ALIVE_INTERVAL>\n"
      + "    <MAX_INFLIGHT>2000</MAX_INFLIGHT>\n"
      + "    <CONNECTION_TIMEOUT>3000</CONNECTION_TIMEOUT>\n"
      + "    <CLEAN_SESSION>true</CLEAN_SESSION>\n"
      + "    <STORAGE_LEVEL>/Users/noname/temp</STORAGE_LEVEL>\n"
      + "    <SERVER_URIS>mqttHost2:1883</SERVER_URIS>\n"
      + "    <MQTT_VERSION>3</MQTT_VERSION>\n"
      + "    <AUTOMATIC_RECONNECT>true</AUTOMATIC_RECONNECT>\n"
      + "    <PASSWORD>Encrypted 2be98afc86aa7f2e4cb79ce10ca97bcce</PASSWORD>\n"
      + "    <USERNAME>testuser</USERNAME>\n"
      + "    <CLIENT_ID>client1</CLIENT_ID>\n"
      + "    <TOPIC>test-topic</TOPIC>\n"
      + "    <QOS>1</QOS>\n"
      + "    <MESSAGE_FIELD>tempvalue</MESSAGE_FIELD>\n"
      + "  </step>";

    Node node = XMLHandler.loadXMLString( inputXml ).getFirstChild();
    meta.loadXML( node, Collections.emptyList(), metaStore );
    assertEquals( "mqtthost:1883", meta.getMqttServer() );
    assertEquals( "client1", meta.getClientId() );
    assertEquals( "test-topic", meta.getTopic() );
    assertEquals( "1", meta.getQOS() );
    assertEquals( "tempvalue", meta.getMessageField() );
    assertEquals( "testuser", meta.getUsername() );
    assertEquals( "test", meta.getPassword() );
    assertEquals( "1000", meta.getKeepAliveInterval() );
    assertEquals( "2000", meta.getMaxInflight() );
    assertEquals( "3000", meta.getConnectionTimeout() );
    assertEquals( "true", meta.getCleanSession() );
    assertEquals( "/Users/noname/temp", meta.getStorageLevel() );
    assertEquals( "mqttHost2:1883", meta.getServerUris() );
    assertEquals( "3", meta.getMqttVersion() );
    assertEquals( "true", meta.getAutomaticReconnect() );
  }

  @Test
  public void testXmlHasAllFields() {
    MQTTProducerMeta meta = new MQTTProducerMeta();
    meta.setMqttServer( "mqtthost:1883" );
    meta.setClientId( "client1" );
    meta.setTopic( "test-topic" );
    meta.setQOS( "2" );
    meta.setMessageField( "temp-message" );
    meta.setUsername( "testuser" );
    meta.setPassword( "test" );

    StepMeta stepMeta = new StepMeta();
    TransMeta transMeta = mock( TransMeta.class );
    stepMeta.setParentTransMeta( transMeta );
    meta.setParentStepMeta( stepMeta );

    assertEquals( "    <MQTT_SERVER>mqtthost:1883</MQTT_SERVER>" + Const.CR
        + "    <CLIENT_ID>client1</CLIENT_ID>" + Const.CR
        + "    <TOPIC>test-topic</TOPIC>" + Const.CR
        + "    <QOS>2</QOS>" + Const.CR
        + "    <MESSAGE_FIELD>temp-message</MESSAGE_FIELD>" + Const.CR
        + "    <USERNAME>testuser</USERNAME>" + Const.CR
        + "    <PASSWORD>Encrypted 2be98afc86aa7f2e4cb79ce10ca97bcce</PASSWORD>\n"
        + "<SSL>\n"
        + "<USE_SSL>false</USE_SSL>\n"
        + "</SSL>" + Const.CR
        + "    <KEEP_ALIVE_INTERVAL/>" + Const.CR
        + "    <MAX_INFLIGHT/>" + Const.CR
        + "    <CONNECTION_TIMEOUT/>" + Const.CR
        + "    <CLEAN_SESSION/>" + Const.CR
        + "    <STORAGE_LEVEL/>" + Const.CR
        + "    <SERVER_URIS/>" + Const.CR
        + "    <MQTT_VERSION/>" + Const.CR
        + "    <AUTOMATIC_RECONNECT/>" + Const.CR,
      meta.getXML() );
  }

  @Test
  public void testRoundTripWithSSLStuff() {
    MQTTProducerMeta meta = new MQTTProducerMeta();
    meta.setMqttServer( "mqtthost:1883" );
    meta.setTopic( "test-topic" );
    meta.setQOS( "2" );
    meta.setMessageField( "temp-message" );
    meta.setSslConfig( of(
      "sslKey", "sslVal",
      "sslKey2", "sslVal2",
      "sslKey3", "sslVal3"
    ) );
    meta.setUseSsl( true );

    MQTTProducerMeta rehydrated = fromXml( meta.getXML() );

    assertThat( true, is( rehydrated.isUseSsl() ) );
    meta.getSslConfig().keySet().forEach( key ->
      assertThat( meta.getSslConfig().get( key ), is( rehydrated.getSslConfig().get( key ) ) ) );

  }

  @Test
  public void testReadFromRepository() throws Exception {
    MQTTProducerMeta meta = new MQTTProducerMeta();
    StringObjectId stepId = new StringObjectId( "stepId" );
    when( rep.getStepAttributeString( stepId, MQTT_SERVER ) ).thenReturn( "mqttserver:1883" );
    when( rep.getStepAttributeString( stepId, CLIENT_ID ) ).thenReturn( "client2" );
    when( rep.getStepAttributeString( stepId, TOPIC ) ).thenReturn( "test-topic" );
    when( rep.getStepAttributeString( stepId, QOS ) ).thenReturn( "0" );
    when( rep.getStepAttributeString( stepId, MESSAGE_FIELD ) ).thenReturn( "the-field" );
    when( rep.getStepAttributeString( stepId, PASSWORD ) ).thenReturn( "Encrypted 2be98afc86aa7f2e4cb79ce10ca97bcce" );
    when( rep.getStepAttributeString( stepId, USERNAME ) ).thenReturn( "testuser" );
    when( rep.getStepAttributeString( stepId, KEEP_ALIVE_INTERVAL ) ).thenReturn( "1000" );
    when( rep.getStepAttributeString( stepId, MAX_INFLIGHT ) ).thenReturn( "2000" );
    when( rep.getStepAttributeString( stepId, CONNECTION_TIMEOUT ) ).thenReturn( "3000" );
    when( rep.getStepAttributeString( stepId, CLEAN_SESSION ) ).thenReturn( "true" );
    when( rep.getStepAttributeString( stepId, STORAGE_LEVEL ) ).thenReturn( "/Users/noname/temp" );
    when( rep.getStepAttributeString( stepId, SERVER_URIS ) ).thenReturn( "mqttHost2:1883" );
    when( rep.getStepAttributeString( stepId, MQTT_VERSION ) ).thenReturn( "3" );
    when( rep.getStepAttributeString( stepId, AUTOMATIC_RECONNECT ) ).thenReturn( "true" );

    meta.readRep( rep, metaStore, stepId, Collections.emptyList() );
    assertEquals( "mqttserver:1883", meta.getMqttServer() );
    assertEquals( "client2", meta.getClientId() );
    assertEquals( "test-topic", meta.getTopic() );
    assertEquals( "0", meta.getQOS() );
    assertEquals( "the-field", meta.getMessageField() );
    assertEquals( "testuser", meta.getUsername() );
    assertEquals( "test", meta.getPassword() );
    assertEquals( "1000", meta.getKeepAliveInterval() );
    assertEquals( "2000", meta.getMaxInflight() );
    assertEquals( "3000", meta.getConnectionTimeout() );
    assertEquals( "true", meta.getCleanSession() );
    assertEquals( "/Users/noname/temp", meta.getStorageLevel() );
    assertEquals( "mqttHost2:1883", meta.getServerUris() );
    assertEquals( "3", meta.getMqttVersion() );
    assertEquals( "true", meta.getAutomaticReconnect() );
  }

  @Test
  public void testSavingToRepository() throws Exception {
    MQTTProducerMeta meta = new MQTTProducerMeta();
    StringObjectId stepId = new StringObjectId( "step1" );
    StringObjectId transId = new StringObjectId( "trans1" );
    meta.setMqttServer( "theserver:1883" );
    meta.setClientId( "client100" );
    meta.setTopic( "newtopic" );
    meta.setQOS( "2" );
    meta.setMessageField( "Messages" );
    meta.setUsername( "testuser" );
    meta.setPassword( "test" ); meta.setKeepAliveInterval( "1000" );
    meta.setMaxInflight( "2000" );
    meta.setConnectionTimeout( "3000" );
    meta.setCleanSession( "true" );
    meta.setStorageLevel( "/Users/noname/temp" );
    meta.setServerUris( "mqttHost2:1883" );
    meta.setMqttVersion( "3" );
    meta.setAutomaticReconnect( "true" );
    meta.saveRep( rep, metaStore, transId, stepId );
    verify( rep ).saveStepAttribute( transId, stepId, MQTT_SERVER, "theserver:1883" );
    verify( rep ).saveStepAttribute( transId, stepId, CLIENT_ID, "client100" );
    verify( rep ).saveStepAttribute( transId, stepId, TOPIC, "newtopic" );
    verify( rep ).saveStepAttribute( transId, stepId, QOS, "2" );
    verify( rep ).saveStepAttribute( transId, stepId, MESSAGE_FIELD, "Messages" );
    verify( rep ).saveStepAttribute( transId, stepId, USERNAME, "testuser" );
    verify( rep ).saveStepAttribute( transId, stepId, PASSWORD, "Encrypted 2be98afc86aa7f2e4cb79ce10ca97bcce" );
    verify( rep ).saveStepAttribute( transId, stepId, KEEP_ALIVE_INTERVAL, "1000" );
    verify( rep ).saveStepAttribute( transId, stepId, MAX_INFLIGHT, "2000" );
    verify( rep ).saveStepAttribute( transId, stepId, CONNECTION_TIMEOUT, "3000" );
    verify( rep ).saveStepAttribute( transId, stepId, CLEAN_SESSION, "true" );
    verify( rep ).saveStepAttribute( transId, stepId, STORAGE_LEVEL, "/Users/noname/temp" );
    verify( rep ).saveStepAttribute( transId, stepId, SERVER_URIS, "mqttHost2:1883" );
    verify( rep ).saveStepAttribute( transId, stepId, MQTT_VERSION, "3" );
    verify( rep ).saveStepAttribute( transId, stepId, AUTOMATIC_RECONNECT, "true" );
  }

  @Test
  public void testSaveDefaultEmpty() {
    MQTTProducerMeta meta = new MQTTProducerMeta();
    assertEquals( "    <MQTT_SERVER/>" + Const.CR
      + "    <CLIENT_ID/>" + Const.CR
      + "    <TOPIC/>" + Const.CR
      + "    <QOS/>" + Const.CR
      + "    <MESSAGE_FIELD/>" + Const.CR
      + "    <USERNAME/>" + Const.CR
      + "    <PASSWORD>Encrypted </PASSWORD>\n"
      + "<SSL>\n"
      + "<USE_SSL>false</USE_SSL>\n"
      + "</SSL>" + Const.CR
      + "    <KEEP_ALIVE_INTERVAL/>" + Const.CR
      + "    <MAX_INFLIGHT/>" + Const.CR
      + "    <CONNECTION_TIMEOUT/>" + Const.CR
      + "    <CLEAN_SESSION/>" + Const.CR
      + "    <STORAGE_LEVEL/>" + Const.CR
      + "    <SERVER_URIS/>" + Const.CR
      + "    <MQTT_VERSION/>" + Const.CR
      + "    <AUTOMATIC_RECONNECT/>" + Const.CR, meta.getXML() );
  }

  public static MQTTProducerMeta fromXml( String metaXml ) {
    Document doc;
    try {
      doc = XMLHandler.loadXMLString( "<step>" + metaXml + "</step>" );
      Node stepNode = XMLHandler.getSubNode( doc, "step" );
      MQTTProducerMeta mqttProducerMeta = new MQTTProducerMeta();
      mqttProducerMeta.loadXML( stepNode, Collections.emptyList(), (IMetaStore) null );
      return mqttProducerMeta;
    } catch ( KettleXMLException e ) {
      throw new RuntimeException( e );
    }
  }
}
