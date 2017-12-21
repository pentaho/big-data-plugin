package org.pentaho.di.trans.step.mqtt;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleXMLException;
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
import static org.pentaho.di.trans.step.mqtt.MQTTConsumerMeta.TOPICS;
import static org.pentaho.di.trans.streaming.common.BaseStreamStepMeta.DURATION;
import static org.pentaho.di.trans.streaming.common.BaseStreamStepMeta.NUM_MESSAGES;
import static org.pentaho.di.trans.streaming.common.BaseStreamStepMeta.TRANSFORMATION_PATH;

@RunWith( MockitoJUnitRunner.class )
public class MQTTConsumerMetaTest {

  @Mock private IMetaStore metastore;
  @Mock Repository rep;

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
    assertEquals( "${Internal.Entry.Current.Directory}/write-to-log.ktr", meta.getTransformationPath() );
    assertEquals( "${Internal.Entry.Current.Directory}/write-to-log.ktr", meta.getFileName() );
    assertEquals( "5", meta.getBatchSize() );
    assertEquals( "60000", meta.getBatchDuration() );
    assertEquals( "mqttHost:1883", meta.getMqttServer() );
  }

  @Test
  public void testXmlHasAllFields() {
    String serverName = "some_cluster";
    MQTTConsumerMeta meta = new MQTTConsumerMeta();
    meta.setMqttServer( serverName );

    ArrayList<String> topicList = new ArrayList<>();
    topicList.add( "temperature" );
    meta.setTopics( topicList );

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

    meta.readRep( rep, metastore, stepId, Collections.emptyList() );
    assertEquals( "readings", meta.getTopics().get( 0 ) );
    assertEquals( "/home/pentaho/atrans.ktr", meta.getTransformationPath() );
    assertEquals( "/home/pentaho/atrans.ktr", meta.getFileName() );
    assertEquals( 999L, Long.parseLong( meta.getBatchSize() ) );
    assertEquals( 111L, Long.parseLong( meta.getBatchDuration() ) );
    assertEquals( "host111", meta.getMqttServer() );
  }

  @Test
  public void testSavesToRepository() throws Exception {
    MQTTConsumerMeta meta = new MQTTConsumerMeta();
    StringObjectId stepId = new StringObjectId( "step1" );
    StringObjectId transId = new StringObjectId( "trans1" );
    ArrayList<String> topicList = new ArrayList<>();
    topicList.add( "temperature" );
    meta.setTopics( topicList );
    meta.setTransformationPath( "/home/Pentaho/btrans.ktr" );
    meta.setBatchSize( "33" );
    meta.setBatchDuration( "10000" );
    meta.setMqttServer( "mqttServer:1883" );
    meta.saveRep( rep, metastore, transId, stepId );
    verify( rep ).saveStepAttribute( transId, stepId, MQTTConsumerMeta.MQTT_SERVER, "mqttServer:1883" );
    verify( rep ).saveStepAttribute( transId, stepId, 0, TOPICS, "temperature" );
    verify( rep ).saveStepAttribute( transId, stepId, TRANSFORMATION_PATH, "/home/Pentaho/btrans.ktr" );
    verify( rep ).saveStepAttribute( transId, stepId, BaseStreamStepMeta.NUM_MESSAGES, "33" );
    verify( rep ).saveStepAttribute( transId, stepId, BaseStreamStepMeta.DURATION, "10000" );
  }
}
