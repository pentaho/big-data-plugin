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

import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaNone;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.metaverse.api.IMetaverseNode;
import org.pentaho.metaverse.api.MetaverseComponentDescriptor;
import org.pentaho.metaverse.api.StepField;
import org.pentaho.metaverse.api.analyzer.kettle.ComponentDerivationRecord;
import org.pentaho.metaverse.api.analyzer.kettle.step.ConnectionExternalResourceStepAnalyzer;
import org.pentaho.metaverse.api.model.IExternalResourceInfo;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static org.pentaho.big.data.kettle.plugins.kafka.KafkaStepAnalyzer.KEY;
import static org.pentaho.big.data.kettle.plugins.kafka.KafkaStepAnalyzer.MESSAGE;
import static org.pentaho.big.data.kettle.plugins.kafka.KafkaStepAnalyzer.NODE_TYPE_KAFKA_SERVER;
import static org.pentaho.big.data.kettle.plugins.kafka.KafkaStepAnalyzer.NODE_TYPE_KAFKA_TOPIC;

public class KafkaProducerStepAnalyzer  extends ConnectionExternalResourceStepAnalyzer<KafkaProducerOutputMeta> {

  public KafkaProducerStepAnalyzer() {
    super();
    KafkaStepAnalyzer.registerEntityTypes();
  }

  @Override protected IMetaverseNode createTableNode( IExternalResourceInfo resource ) {

    MetaverseComponentDescriptor topicDescriptor = new MetaverseComponentDescriptor(
      ( (KafkaResourceInfo) resource ).getTopic(),
      NODE_TYPE_KAFKA_TOPIC,
      getConnectionNode(),
      getDescriptor().getContext() );

    return createNodeFromDescriptor( topicDescriptor );
  }

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

  @Override
  public Set<String> getOutputResourceFields( KafkaProducerOutputMeta meta ) {
    HashSet<String> outputResourceFields = new LinkedHashSet<>();
    if ( meta.getKeyField() != null ) {
      outputResourceFields.add( KEY );
    }
    if ( meta.getMessageField() != null ) {
      outputResourceFields.add( MESSAGE );
    }
    return outputResourceFields;
  }

  @Override
  protected Map<String, RowMetaInterface> getOutputRowMetaInterfaces( KafkaProducerOutputMeta meta ) {
    Map<String, RowMetaInterface> outputRows = super.getOutputRowMetaInterfaces( meta );
    RowMetaInterface topicFields = new RowMeta();
    if ( meta.getKeyField() != null ) {
      topicFields.addValueMeta( new ValueMetaNone( KEY ) );
    }
    if ( meta.getMessageField() != null ) {
      topicFields.addValueMeta( new ValueMetaNone( MESSAGE ) );
    }
    outputRows.put( RESOURCE, topicFields );

    return outputRows;
  }

  @Override public Set<ComponentDerivationRecord> getChangeRecords( KafkaProducerOutputMeta meta ) {
    LinkedHashSet<ComponentDerivationRecord> changes = new LinkedHashSet<>();
    Set<String> stepNames = getInputs().getStepNames();
    for ( String stepName : stepNames ) {
      changes.add(
        new ComponentDerivationRecord(
          new StepField( stepName, meta.getKeyField() ), new StepField( RESOURCE, KEY ) ) );
      changes.add(
        new ComponentDerivationRecord(
          new StepField( stepName, meta.getMessageField() ), new StepField( RESOURCE, MESSAGE ) ) );
    }
    return changes;
  }

  @Override public String getResourceInputNodeType() {
    return null;
  }

  @Override public String getResourceOutputNodeType() {
    return null;
  }

  @Override public boolean isOutput() {
    return true;
  }

  @Override public boolean isInput() {
    return false;
  }

  @Override protected Set<StepField> getUsedFields( KafkaProducerOutputMeta meta ) {
    LinkedHashSet<StepField> usedFields = new LinkedHashSet<>();
    Set<String> stepNames = getInputs().getStepNames();
    for ( String stepName : stepNames ) {
      if ( meta.getKeyField() != null ) {
        usedFields.add( new StepField( stepName, meta.getKeyField() ) );
      }
      if ( meta.getMessageField() != null ) {
        usedFields.add( new StepField( stepName, meta.getMessageField() ) );
      }
    }
    return usedFields;
  }

  @Override public Set<Class<? extends BaseStepMeta>> getSupportedSteps() {
    return Collections.singleton( KafkaProducerOutputMeta.class );
  }
}
