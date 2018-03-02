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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.encryption.TwoWayPasswordEncoderPluginType;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.streaming.common.BaseStreamStepMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.AUTOMATIC_RECONNECT;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.CLEAN_SESSION;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.CONNECTION_TIMEOUT;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.KEEP_ALIVE_INTERVAL;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.MAX_INFLIGHT;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.MQTT_SERVER;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.MQTT_VERSION;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.QOS;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.SERVER_URIS;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.SSL_KEYS;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.SSL_VALUES;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.STORAGE_LEVEL;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.TOPICS;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.USERNAME;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.USE_SSL;
import static org.pentaho.di.trans.streaming.common.BaseStreamStepMeta.DURATION;
import static org.pentaho.di.trans.streaming.common.BaseStreamStepMeta.NUM_MESSAGES;
import static org.pentaho.di.trans.streaming.common.BaseStreamStepMeta.PASSWORD;
import static org.pentaho.di.trans.streaming.common.BaseStreamStepMeta.TRANSFORMATION_PATH;

@RunWith ( MockitoJUnitRunner.class )
public class MQTTConsumerMetaTest {
  private static Class PKG = MQTTConsumerMetaTest.class;

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
      + "    <KEEP_ALIVE_INTERVAL>1000</KEEP_ALIVE_INTERVAL>\n"
      + "    <MAX_INFLIGHT>2000</MAX_INFLIGHT>\n"
      + "    <CONNECTION_TIMEOUT>3000</CONNECTION_TIMEOUT>\n"
      + "    <CLEAN_SESSION>true</CLEAN_SESSION>\n"
      + "    <STORAGE_LEVEL>/Users/noname/temp</STORAGE_LEVEL>\n"
      + "    <SERVER_URIS>mqttHost2:1883</SERVER_URIS>\n"
      + "    <MQTT_VERSION>3</MQTT_VERSION>\n"
      + "    <AUTOMATIC_RECONNECT>true</AUTOMATIC_RECONNECT>\n"
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
    when( rep.getStepAttributeString( stepId, KEEP_ALIVE_INTERVAL ) ).thenReturn( "1000" );
    when( rep.getStepAttributeString( stepId, MAX_INFLIGHT ) ).thenReturn( "2000" );
    when( rep.getStepAttributeString( stepId, CONNECTION_TIMEOUT ) ).thenReturn( "3000" );
    when( rep.getStepAttributeString( stepId, CLEAN_SESSION ) ).thenReturn( "true" );
    when( rep.getStepAttributeString( stepId, STORAGE_LEVEL ) ).thenReturn( "/Users/noname/temp" );
    when( rep.getStepAttributeString( stepId, SERVER_URIS ) ).thenReturn( "mqttHost2:1883" );
    when( rep.getStepAttributeString( stepId, MQTT_VERSION ) ).thenReturn( "3" );
    when( rep.getStepAttributeString( stepId, AUTOMATIC_RECONNECT ) ).thenReturn( "true" );

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
    meta.setKeepAliveInterval( "1000" );
    meta.setMaxInflight( "2000" );
    meta.setConnectionTimeout( "3000" );
    meta.setCleanSession( "true" );
    meta.setStorageLevel( "/Users/noname/temp" );
    meta.setServerUris( "mqttHost2:1883" );
    meta.setMqttVersion( "3" );
    meta.setAutomaticReconnect( "true" );
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
  public void testSaveDefaultEmptyConnection() {
    assertEquals(
      "<KEEP_ALIVE_INTERVAL/>" + Const.CR
        + "<AUTOMATIC_RECONNECT/>" + Const.CR
        + "<NUM_MESSAGES>1000</NUM_MESSAGES>" + Const.CR
        + "<SERVER_URIS/>" + Const.CR
        + "<CONNECTION_TIMEOUT/>" + Const.CR
        + "<STORAGE_LEVEL/>" + Const.CR
        + "<TOPIC_OUTPUT_NAME>Topic</TOPIC_OUTPUT_NAME>" + Const.CR
        + "<TRANSFORMATION_PATH/>" + Const.CR
        + "<MSG_OUTPUT_NAME>Message</MSG_OUTPUT_NAME>" + Const.CR
        + "<MQTT_VERSION/>" + Const.CR
        + "<MAX_INFLIGHT/>" + Const.CR
        + "<PASSWORD>Encrypted </PASSWORD>" + Const.CR
        + "<QOS>0</QOS>" + Const.CR
        + "<CLEAN_SESSION/>" + Const.CR
        + "<USERNAME/>" + Const.CR
        + "<MQTT_SERVER/>" + Const.CR
        + "<DURATION>1000</DURATION>" + Const.CR
        + "<SSL>" + Const.CR
        + "<USE_SSL>false</USE_SSL>" + Const.CR
        + "</SSL>" + Const.CR,
      meta.getXML() );
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

  @Test
  public void testRetrieveOptions() {
    List<String> keys = Arrays
      .asList( KEEP_ALIVE_INTERVAL, MAX_INFLIGHT, CONNECTION_TIMEOUT, CLEAN_SESSION, STORAGE_LEVEL, SERVER_URIS,
        MQTT_VERSION, AUTOMATIC_RECONNECT );

    MQTTConsumerMeta meta = new MQTTConsumerMeta();
    meta.setDefault();
    List<MqttOption> options = meta.retrieveOptions();
    assertEquals( 8, options.size() );
    for ( MqttOption option : options ) {
      assertEquals( "", option.getValue() );
      assertNotNull( option.getText() );
      Assert.assertTrue( keys.contains( option.getKey() ) );
    }
  }

  @Test
  public void testCheckDefaults() {
    List<CheckResultInterface> remarks = new ArrayList<>();
    meta.check( remarks, null, null, null, null, null, null, new Variables(), null, null );

    assertEquals( 0, remarks.size() );
  }

  @Test
  public void testCheckFailAll() {
    List<CheckResultInterface> remarks = new ArrayList<>();
    meta.setKeepAliveInterval( "asdf" );
    meta.setMaxInflight( "asdf" );
    meta.setConnectionTimeout( "asdf" );
    meta.setCleanSession( "asdf" );
    meta.setAutomaticReconnect( "adsf" );
    meta.setMqttVersion( "9" );
    meta.check( remarks, null, null, null, null, null, null, new Variables(), null, null );

    assertEquals( 6, remarks.size() );
    assertEquals( BaseMessages
        .getString( PKG, "MQTTMeta.CheckResult.NotANumber",
          BaseMessages.getString( PKG, "MQTTDialog.Options." + KEEP_ALIVE_INTERVAL ) ),
      remarks.get( 0 ).getText() );
    assertEquals( BaseMessages
        .getString( PKG, "MQTTMeta.CheckResult.NotANumber",
          BaseMessages.getString( PKG, "MQTTDialog.Options." + MAX_INFLIGHT ) ),
      remarks.get( 1 ).getText() );
    assertEquals( BaseMessages
        .getString( PKG, "MQTTMeta.CheckResult.NotANumber",
          BaseMessages.getString( PKG, "MQTTDialog.Options." + CONNECTION_TIMEOUT ) ),
      remarks.get( 2 ).getText() );
    assertEquals( BaseMessages
        .getString( PKG, "MQTTMeta.CheckResult.NotABoolean",
          BaseMessages.getString( PKG, "MQTTDialog.Options." + CLEAN_SESSION ) ),
      remarks.get( 3 ).getText() );
    assertEquals( BaseMessages
        .getString( PKG, "MQTTMeta.CheckResult.NotCorrectVersion",
          BaseMessages.getString( PKG, "MQTTDialog.Options." + MQTT_VERSION ) ),
      remarks.get( 4 ).getText() );
    assertEquals(
      BaseMessages
        .getString( PKG, "MQTTMeta.CheckResult.NotABoolean",
          BaseMessages.getString( PKG, "MQTTDialog.Options." + AUTOMATIC_RECONNECT ) ),
      remarks.get( 5 ).getText() );
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
