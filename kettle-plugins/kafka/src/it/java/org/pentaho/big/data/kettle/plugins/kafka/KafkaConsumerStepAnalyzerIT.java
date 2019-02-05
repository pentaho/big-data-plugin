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
import org.pentaho.di.trans.steps.groupby.GroupBy;
import org.pentaho.di.trans.steps.groupby.GroupByMeta;
import org.pentaho.di.trans.steps.rowsfromresult.RowsFromResultMeta;
import org.pentaho.metaverse.analyzer.kettle.step.StepAnalyzerProvider;
import org.pentaho.metaverse.frames.Concept;
import org.pentaho.metaverse.frames.FramedMetaverseNode;
import org.pentaho.metaverse.frames.TransformationNode;
import org.pentaho.metaverse.frames.TransformationStepNode;
import org.pentaho.metaverse.impl.MetaverseConfig;
import org.pentaho.metaverse.step.StepAnalyzerValidationIT;
import org.pentaho.metaverse.util.MetaverseBeanUtil;
import org.pentaho.metaverse.util.MetaverseBundleActivator;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.pentaho.big.data.kettle.plugins.kafka.KafkaStepAnalyzer.NODE_TYPE_KAFKA_SERVER;
import static org.pentaho.big.data.kettle.plugins.kafka.KafkaStepAnalyzer.NODE_TYPE_KAFKA_TOPIC;
import static org.pentaho.dictionary.DictionaryConst.*;

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
    assertEquals( "Unexpected number of edges", 91, getIterableSize( framedGraph.getEdges() ) );


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
      // verify kafka fields are consumer inputs
      verifyLinkedNode( fieldNode, LINK_INPUTS, "Kafka consumer" );

      // verify kafka fields derive inputs to subtrans
      List<FramedMetaverseNode> fieldChildren = IteratorUtils.toList( fieldNode.getOutNodes( LINK_DERIVES ).iterator() );
      assertEquals( 1, fieldChildren.size() );
      assertEquals( kafkaFieldToInputName( kafkaField.toString() ), fieldChildren.get( 0 ).getName() );
      verifyLinkedNode( fieldChildren.get( 0 ), LINK_INPUTS, "Get records from stream" );
    }

    List<Concept> consumerDependsOn = IteratorUtils.toList( kafkaConsumer.getInNodes( LINK_DEPENDENCYOF ).iterator() );
    assertEquals( 1, consumerDependsOn.size() );
    Concept kafkaServer = consumerDependsOn.get( 0 );
    assertEquals( NODE_TYPE_KAFKA_SERVER, kafkaServer.getType() );
    assertEquals( "10.177.178.135:9092", kafkaServer.getName() );

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

    // Check output of Group By step
    TransformationStepNode groupBy = (TransformationStepNode) subSteps.get( "Group by" );
    List<Concept> groupByOutput = IteratorUtils.toList( groupBy.getOutNodes( LINK_OUTPUTS ).iterator() );
    assertEquals( 1, groupByOutput.size() );
    assertEquals( "maxOffset", groupByOutput.get( 0 ).getName() );
    // Verify Group By output derives consumer output
    List<Concept> consumerStepOutput = IteratorUtils.toList(
            groupByOutput.get( 0 ).getOutNodes( LINK_DERIVES ).iterator() );
    // maxOffset output of Group By step derives another field called maxOffset
    assertEquals( 1, consumerStepOutput.size() );
    assertEquals( "maxOffset", consumerStepOutput.get( 0 ).getName() );
    // second maxOffset field is also the output of the Kafka consumer
    List<Concept> consumerStep = IteratorUtils.toList(
            consumerStepOutput.get( 0 ).getInNodes( LINK_OUTPUTS ).iterator() );
    assertEquals( 1, consumerStep.size() );
    assertEquals( "Kafka consumer", consumerStep.get( 0 ).getName() );
  }

  private String kafkaFieldToInputName( String s ) {
    return ( s.startsWith("o") ? "an" : "a" ) + s.substring( 0, 1 ).toUpperCase() + s.substring( 1 );
  }
}
