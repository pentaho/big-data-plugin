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
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.pentaho.di.trans.step.mqtt.MQTTProducerMeta.CLIENT_ID;
import static org.pentaho.di.trans.step.mqtt.MQTTProducerMeta.MESSAGE_FIELD;
import static org.pentaho.di.trans.step.mqtt.MQTTProducerMeta.MQTT_SERVER;
import static org.pentaho.di.trans.step.mqtt.MQTTProducerMeta.PASSWORD;
import static org.pentaho.di.trans.step.mqtt.MQTTProducerMeta.QOS;
import static org.pentaho.di.trans.step.mqtt.MQTTProducerMeta.TOPIC;
import static org.pentaho.di.trans.step.mqtt.MQTTProducerMeta.USERNAME;

@RunWith( MockitoJUnitRunner.class )
public class MQTTProducerMetaTest {

  @Mock private IMetaStore metaStore;
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
        + "    <PASSWORD>Encrypted 2be98afc86aa7f2e4cb79ce10ca97bcce</PASSWORD>" + Const.CR,
      meta.getXML() );
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

    meta.readRep( rep, metaStore, stepId, Collections.emptyList() );
    assertEquals( "mqttserver:1883", meta.getMqttServer() );
    assertEquals( "client2", meta.getClientId() );
    assertEquals( "test-topic", meta.getTopic() );
    assertEquals( "0", meta.getQOS() );
    assertEquals( "the-field", meta.getMessageField() );
    assertEquals( "testuser", meta.getUsername() );
    assertEquals( "test", meta.getPassword() );
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
    meta.setPassword( "test" );
    meta.saveRep( rep, metaStore, transId, stepId );
    verify( rep ).saveStepAttribute( transId, stepId, MQTT_SERVER, "theserver:1883" );
    verify( rep ).saveStepAttribute( transId, stepId, CLIENT_ID, "client100" );
    verify( rep ).saveStepAttribute( transId, stepId, TOPIC, "newtopic" );
    verify( rep ).saveStepAttribute( transId, stepId, QOS, "2" );
    verify( rep ).saveStepAttribute( transId, stepId, MESSAGE_FIELD, "Messages" );
    verify( rep ).saveStepAttribute( transId, stepId, USERNAME, "testuser" );
    verify( rep ).saveStepAttribute( transId, stepId, PASSWORD, "Encrypted 2be98afc86aa7f2e4cb79ce10ca97bcce" );
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
      + "    <PASSWORD>Encrypted </PASSWORD>" + Const.CR, meta.getXML() );
  }
}
