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

import java.util.Collections;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceLocator;
import org.pentaho.bigdata.api.jaas.JaasConfigService;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.osgi.metastore.locator.api.MetastoreLocator;
import org.w3c.dom.Node;

import static org.pentaho.big.data.kettle.plugins.kafka.KafkaProducerOutputMeta.CLIENT_ID;
import static org.pentaho.big.data.kettle.plugins.kafka.KafkaProducerOutputMeta.CLUSTER_NAME;
import static org.pentaho.big.data.kettle.plugins.kafka.KafkaProducerOutputMeta.KEY_FIELD;
import static org.pentaho.big.data.kettle.plugins.kafka.KafkaProducerOutputMeta.MESSAGE_FIELD;
import static org.pentaho.big.data.kettle.plugins.kafka.KafkaProducerOutputMeta.TOPIC;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class KafkaProducerOutputMetaTest {
  @Mock IMetaStore metastore;
  @Mock Repository rep;
  @Mock NamedClusterService namedClusterService;
  @Mock MetastoreLocator metastoreLocator;

  @Test
  public void testLoadsFieldsFromXml() throws Exception {
    KafkaProducerOutputMeta meta = new KafkaProducerOutputMeta();
    String inputXml =
      "  <step>\n"
        + "    <name>Kafka Producer</name>\n"
        + "    <type>KafkaProducerOutput</type>\n"
        + "    <description />\n"
        + "    <distribute>Y</distribute>\n"
        + "    <custom_distribution />\n"
        + "    <copies>1</copies>\n"
        + "    <partitioning>\n"
        + "      <method>none</method>\n"
        + "      <schema_name />\n"
        + "    </partitioning>\n"
        + "    <clusterName>some_cluster</clusterName>\n"
        + "    <clientId>clientid01</clientId>\n"
        + "    <topic>one</topic>\n"
        + "    <keyField>three</keyField>\n"
        + "    <messageField>four</messageField>\n"
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
    assertEquals( "clientid01", meta.getClientId() );
    assertEquals( "one", meta.getTopic() );
    assertEquals( "three", meta.getKeyField() );
    assertEquals( "four", meta.getMessageField() );
  }

  @Test
  public void testXmlHasAllFields() throws Exception {
    KafkaProducerOutputMeta meta = new KafkaProducerOutputMeta();
    meta.setClusterName( "some_cluster" );
    meta.setClientId( "id1" );
    meta.setTopic( "myTopic" );
    meta.setKeyField( "fieldOne" );
    meta.setMessageField( "message" );
    assertEquals(
      "    <clusterName>some_cluster</clusterName>" + Const.CR
        + "    <topic>myTopic</topic>" + Const.CR
        + "    <clientId>id1</clientId>" + Const.CR
        + "    <keyField>fieldOne</keyField>" + Const.CR
        + "    <messageField>message</messageField>" + Const.CR, meta.getXML()
    );
  }

  @Test
  public void testReadsFromRepository() throws Exception {
    KafkaProducerOutputMeta meta = new KafkaProducerOutputMeta();
    StringObjectId stepId = new StringObjectId( "stepId" );
    when( rep.getStepAttributeString( stepId, CLUSTER_NAME ) ).thenReturn( "some_cluster" );
    when( rep.getStepAttributeString( stepId, CLIENT_ID ) ).thenReturn( "client01" );
    when( rep.getStepAttributeString( stepId, TOPIC ) ).thenReturn( "readings" );
    when( rep.getStepAttributeString( stepId, KEY_FIELD ) ).thenReturn( "machineId" );
    when( rep.getStepAttributeString( stepId, MESSAGE_FIELD ) ).thenReturn( "reading" );
    meta.readRep( rep, metastore, stepId, Collections.emptyList() );
    assertEquals( "some_cluster", meta.getClusterName() );
    assertEquals( "client01", meta.getClientId() );
    assertEquals( "readings", meta.getTopic() );
    assertEquals( "machineId", meta.getKeyField() );
    assertEquals( "reading", meta.getMessageField() );
  }

  @Test
  public void testSavesToRepository() throws Exception {
    KafkaProducerOutputMeta meta = new KafkaProducerOutputMeta();
    StringObjectId stepId = new StringObjectId( "step1" );
    StringObjectId transId = new StringObjectId( "trans1" );
    meta.setClusterName( "some_cluster" );
    meta.setClientId( "client01" );
    meta.setTopic( "temperature" );
    meta.setKeyField( "kafkaKey" );
    meta.setMessageField( "kafkaMessage" );
    meta.saveRep( rep, metastore, transId, stepId );
    verify( rep ).saveStepAttribute( transId, stepId, CLUSTER_NAME, "some_cluster" );
    verify( rep ).saveStepAttribute( transId, stepId, CLIENT_ID, "client01" );
    verify( rep ).saveStepAttribute( transId, stepId, TOPIC, "temperature" );
    verify( rep ).saveStepAttribute( transId, stepId, KEY_FIELD, "kafkaKey" );
    verify( rep ).saveStepAttribute( transId, stepId, MESSAGE_FIELD, "kafkaMessage" );
  }

  @Test
  public void testReadsBootstrapServersFromNamedCluster() {
    NamedCluster namedCluster = mock( NamedCluster.class );
    when( namedCluster.getKafkaBootstrapServers() ).thenReturn( "server:11111" );

    NamedClusterService namedClusterService = mock( NamedClusterService.class );
    when( namedClusterService.getNamedClusterByName( eq( "my_cluster" ), any( IMetaStore.class ) ) )
        .thenReturn( namedCluster );

    KafkaProducerOutputMeta meta = new KafkaProducerOutputMeta();
    meta.setNamedClusterService( namedClusterService );
    meta.setMetastoreLocator( metastoreLocator );
    meta.setClusterName( "my_cluster" );

    assertThat( meta.getBootstrapServers(), is( "server:11111" ) );
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
    KafkaProducerOutputMeta inputMeta = new KafkaProducerOutputMeta();
    inputMeta.setNamedClusterServiceLocator( namedClusterLocator );
    inputMeta.setNamedClusterService( namedClusterService );
    inputMeta.setClusterName( "kurtsCluster" );
    inputMeta.setMetastoreLocator( metastoreLocator );
    assertEquals( jaasConfigService, inputMeta.getJaasConfigService().get() );
  }
}
