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

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.steps.transexecutor.TransExecutorMeta;
import org.pentaho.metaverse.api.IMetaverseNode;
import org.pentaho.metaverse.api.MetaverseAnalyzerException;
import org.pentaho.metaverse.api.MetaverseComponentDescriptor;
import org.pentaho.metaverse.api.StepField;
import org.pentaho.metaverse.api.analyzer.kettle.ComponentDerivationRecord;
import org.pentaho.metaverse.api.analyzer.kettle.KettleAnalyzerUtil;
import org.pentaho.metaverse.api.analyzer.kettle.step.ConnectionExternalResourceStepAnalyzer;
import org.pentaho.metaverse.api.analyzer.kettle.step.StepNodes;
import org.pentaho.metaverse.api.analyzer.kettle.step.SubtransAnalyzer;
import org.pentaho.metaverse.api.model.IExternalResourceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.pentaho.big.data.kettle.plugins.kafka.KafkaStepAnalyzer.NODE_TYPE_KAFKA_SERVER;
import static org.pentaho.big.data.kettle.plugins.kafka.KafkaStepAnalyzer.NODE_TYPE_KAFKA_TOPIC;

public class KafkaConsumerStepAnalyzer extends ConnectionExternalResourceStepAnalyzer<KafkaConsumerInputMeta> {
  private final transient Logger log = LoggerFactory.getLogger( KafkaConsumerStepAnalyzer.class );
  public KafkaConsumerStepAnalyzer() {
    super();
    KafkaStepAnalyzer.registerEntityTypes();
  }

  @Override
  protected void customAnalyze( KafkaConsumerInputMeta meta, IMetaverseNode node ) {
    try {
      super.customAnalyze( meta, node );

      TransMeta subTransMeta = TransExecutorMeta
        .loadMappingMeta( baseStepMeta, parentTransMeta.getRepository(), parentTransMeta.getMetaStore(),
          parentTransMeta );
      SubtransAnalyzer<KafkaConsumerInputMeta> subtransAnalyzer = new SubtransAnalyzer<>( this, log );
      HashSet<StepField> stepFields = new HashSet<>();
      for ( KafkaConsumerField.Name kafkaField : KafkaConsumerField.Name.values() ) {
        stepFields.add( new StepField( RESOURCE, kafkaField.toString() ) );
      }

      // Generate graph for subtrans
      IMetaverseNode subTransNode = KettleAnalyzerUtil.analyze( this, parentTransMeta, baseStepMeta, rootNode );

      // Link fields from topic(s) to fields in subtrans
      for ( StepField stepField : stepFields ) {
        IMetaverseNode inputNode = this.getInputs().findNode( stepField );
        String outputName = baseStepMeta.getFieldDefinitions().stream()
          .filter( kcf -> kcf.getKafkaName().toString().equals( stepField.getFieldName() ) )
          .map( KafkaConsumerField::getOutputName )
          .findFirst().orElse( "" );
        subtransAnalyzer.linkUsedFieldToSubTrans( inputNode, subTransMeta, subTransNode, descriptor,
          fieldName -> fieldName.equals( outputName ) );
      }

      // Link subtrans output back to consumer step output
      String resultFieldsStep = baseStepMeta.getSubStep();
      StepNodes outputNodes = this.getOutputs();
      for ( String stepName : outputNodes.getStepNames() ) {
        // for each step after the consumer
        for ( String fieldName : outputNodes.getFieldNames( stepName ) ) {
          // for each field going to that step, link it to the field returned from the subtrans
          subtransAnalyzer.linkResultFieldToSubTrans( outputNodes.findNode( stepName, fieldName ), subTransMeta,
            subTransNode, descriptor, resultFieldsStep );
        }
      }

    } catch ( MetaverseAnalyzerException | KettleException e ) {
      log.warn( e.getMessage(), e );
    }
  }

  @Override protected IMetaverseNode createTableNode( IExternalResourceInfo resource ) {

    KafkaResourceInfo kafkaResourceInfo = (KafkaResourceInfo) resource;
    MetaverseComponentDescriptor topicDescriptor = new MetaverseComponentDescriptor(
      kafkaResourceInfo.getTopic(),
      NODE_TYPE_KAFKA_TOPIC,
      getConnectionNode(),
      getDescriptor().getContext() );

    return createNodeFromDescriptor( topicDescriptor );
  }

  @Override protected Map<String, RowMetaInterface> getInputRowMetaInterfaces( KafkaConsumerInputMeta meta ) {

    Map<String, RowMetaInterface> inputRows = new HashMap<>();
    try {
      RowMeta rowMeta = new RowMeta();
      for ( KafkaConsumerField.Name kafkaField : KafkaConsumerField.Name.values() ) {
        baseStepMeta.putFieldOnRowMeta( new KafkaConsumerField( kafkaField, kafkaField.toString()  ), rowMeta, "", parentTransMeta );
      }
      inputRows.put( RESOURCE, rowMeta );
    } catch ( KettleStepException e ) {
      log.warn( e.getMessage(), e );
    }
    return inputRows;
  }

  @SuppressWarnings( "Duplicates" )
  @Override
  public IMetaverseNode getConnectionNode() {
    if ( connectionNode == null ) {
      MetaverseComponentDescriptor connectionDescriptor = new MetaverseComponentDescriptor(
        baseStepMeta.getBootstrapServers(),
        NODE_TYPE_KAFKA_SERVER,
        getDescriptor().getNamespace(),
        getDescriptor().getContext() );
      connectionNode = createNodeFromDescriptor( connectionDescriptor );
    }
    return connectionNode;
  }

  @Override protected boolean isPassthrough( StepField originalFieldName ) {
    return false;
  }

  @Override protected Set<ComponentDerivationRecord> getChanges() {
    return Collections.emptySet();
  }

  @Override public String getResourceInputNodeType() {
    return null;
  }

  @Override public String getResourceOutputNodeType() {
    return null;
  }

  @Override public boolean isOutput() {
    return false;
  }

  @Override public boolean isInput() {
    return true;
  }

  @Override public Set<Class<? extends BaseStepMeta>> getSupportedSteps() {
    return Collections.singleton( KafkaConsumerInputMeta.class );
  }

  @Override protected Set<StepField> getUsedFields( KafkaConsumerInputMeta meta ) {
    return Collections.emptySet();
  }

}
