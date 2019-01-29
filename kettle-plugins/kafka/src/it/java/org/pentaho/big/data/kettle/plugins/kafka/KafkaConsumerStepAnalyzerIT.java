/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

import org.apache.commons.collections4.IteratorUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.pentaho.di.core.KettleClientEnvironment;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.metaverse.frames.Concept;
import org.pentaho.metaverse.frames.FramedMetaverseNode;
import org.pentaho.metaverse.frames.TransformationNode;
import org.pentaho.metaverse.frames.TransformationStepNode;
import org.pentaho.metaverse.impl.MetaverseConfig;
import org.pentaho.metaverse.step.StepAnalyzerValidationIT;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.pentaho.big.data.kettle.plugins.kafka.KafkaStepAnalyzer.NODE_TYPE_KAFKA_TOPIC;
import static org.pentaho.dictionary.DictionaryConst.LINK_CONTAINS;
import static org.pentaho.dictionary.DictionaryConst.LINK_EXECUTES;
import static org.pentaho.dictionary.DictionaryConst.LINK_INPUTS;
import static org.pentaho.dictionary.DictionaryConst.LINK_OUTPUTS;
import static org.pentaho.dictionary.DictionaryConst.LINK_READBY;

@RunWith( PowerMockRunner.class )
@PrepareForTest( MetaverseConfig.class )
public class KafkaConsumerStepAnalyzerIT extends StepAnalyzerValidationIT {

  @BeforeClass
  public static void setUp() throws Exception {
    KettleClientEnvironment.init();
    PluginRegistry.addPluginType( StepPluginType.getInstance() );
    PluginRegistry.init();
    if ( !Props.isInitialized() ) {
      Props.init( 0 );
    }
    StepPluginType.getInstance().handlePluginAnnotation(
      KafkaConsumerInputMeta.class,
      KafkaConsumerInputMeta.class.getAnnotation( org.pentaho.di.core.annotations.Step.class ),
      Collections.emptyList(), false, null );
  }

  @Test
  public void analyzerConsumer() throws Exception {
    final String transNodeName = "consumeMaxOffset";
    initTest( transNodeName );

    final TransformationNode consumerMaxOffset = verifyTransformationNode( transNodeName, false );
    assertEquals( "Unexpected number of nodes", 37, getIterableSize( framedGraph.getVertices() ) );
    assertEquals( "Unexpected number of edges", 76, getIterableSize( framedGraph.getEdges() ) );


    final Map<String, FramedMetaverseNode> parentSteps = verifyTransformationSteps( consumerMaxOffset,
      new String[] { "Kafka consumer", "Write to log" },  false );
    TransformationStepNode kafkaConsumer = (TransformationStepNode) parentSteps.get( "Kafka consumer" );

    List<Concept> consumerReadby = IteratorUtils.toList( kafkaConsumer.getInNodes( LINK_READBY ).iterator() );
    assertEquals( 1, consumerReadby.size() );
    Concept itTopic = consumerReadby.get( 0 );
    assertEquals( NODE_TYPE_KAFKA_TOPIC, itTopic.getType() );
    assertEquals( "it-topic", itTopic.getName() );
    for ( KafkaConsumerField.Name kafkaField : KafkaConsumerField.Name.values() ) {
      FramedMetaverseNode fieldNode = verifyLinkedNode( itTopic, LINK_CONTAINS, kafkaField.toString() );
      verifyLinkedNode( fieldNode, LINK_INPUTS, "Kafka consumer" );
    }

    verifyLinkedNode( kafkaConsumer, LINK_EXECUTES, "maxOffset" );

    final TransformationNode maxOffset = verifyTransformationNode( "maxOffset", true );
    final Map<String, FramedMetaverseNode> subSteps = verifyTransformationSteps( maxOffset,
      new String[] { "Get records from stream", "Group by" },  false );
    TransformationStepNode recordsFromStream = (TransformationStepNode) subSteps.get( "Get records from stream" );
    verifyLinkedNode( recordsFromStream, LINK_OUTPUTS, "aKey" );
    verifyLinkedNode( recordsFromStream, LINK_OUTPUTS, "aMessage" );
    verifyLinkedNode( recordsFromStream, LINK_OUTPUTS, "aTopic" );
    verifyLinkedNode( recordsFromStream, LINK_OUTPUTS, "aPartition" );
    verifyLinkedNode( recordsFromStream, LINK_OUTPUTS, "aTimestamp" );
    verifyLinkedNode( recordsFromStream, LINK_OUTPUTS, "anOffset" );
  }
}
