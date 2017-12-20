/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

import com.google.common.collect.Sets;
import org.apache.kafka.clients.consumer.Consumer;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.SubtransExecutor;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.step.StepStatus;
import org.pentaho.di.trans.steps.transexecutor.TransExecutorMeta;
import org.pentaho.di.trans.steps.transexecutor.TransExecutorParameters;
import org.pentaho.di.trans.streaming.common.BaseStreamStep;
import org.pentaho.di.trans.streaming.common.FixedTimeStreamWindow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Consume messages from a Kafka topic
 */
public class KafkaConsumerInput extends BaseStreamStep implements StepInterface {

  private static Class<?> PKG = KafkaConsumerInputMeta.class;
  // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  private KafkaConsumerInputMeta kafkaConsumerInputMeta;
  private KafkaConsumerInputData kafkaConsumerInputData;

  public KafkaConsumerInput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                             Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  /**
   * Initialize and do work where other steps need to wait for...
   *
   * @param stepMetaInterface The metadata to work with
   * @param stepDataInterface The data to initialize
   */
  public boolean init( StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface ) {
    kafkaConsumerInputMeta = (KafkaConsumerInputMeta) stepMetaInterface;
    kafkaConsumerInputData = (KafkaConsumerInputData) stepDataInterface;

    try {
      kafkaConsumerInputMeta.setParentStepMeta( getStepMeta() );
      kafkaConsumerInputMeta.setFileName( kafkaConsumerInputMeta.getTransformationPath() );
      TransMeta transMeta = TransExecutorMeta
        .loadMappingMeta( kafkaConsumerInputMeta, getTransMeta().getRepository(), getTransMeta().getMetaStore(),
          getParentVariableSpace() );
      kafkaConsumerInputData.subtransExecutor =
        new SubtransExecutor( getTrans(), transMeta, true, kafkaConsumerInputData, new TransExecutorParameters() );
    } catch ( KettleException e ) {
      logError( BaseMessages.getString( PKG, "KafkaConsumerInput.Error.InitFailed" ), e );
      return false;
    }
    boolean superInit = super.init( kafkaConsumerInputMeta, kafkaConsumerInputData );
    if ( !superInit ) {
      return false;
    }
    List<CheckResultInterface> remarks = new ArrayList<>();
    kafkaConsumerInputMeta.check(
      remarks, getTransMeta(), kafkaConsumerInputMeta.getParentStepMeta(),
      null, null, null, null, //these parameters are not used inside the method
      variables, getRepository(), getMetaStore() );
    boolean errorsPresent =
      remarks.stream().filter( result -> result.getType() == CheckResultInterface.TYPE_RESULT_ERROR )
        .peek( result -> logError( result.getText() ) )
        .count() > 0;
    if ( errorsPresent ) {
      return false;
    }

    kafkaConsumerInputData.outputRowMeta = new RowMeta();
    try {
      kafkaConsumerInputMeta.getFields( kafkaConsumerInputData.outputRowMeta,
        getStepname(), null, null, this, repository, metaStore );
    } catch ( KettleStepException e ) {
      // TODO: just log this exception and return false?
    }

    Consumer consumer =
      kafkaConsumerInputMeta.getKafkaFactory().consumer( kafkaConsumerInputMeta, this::environmentSubstitute,
        kafkaConsumerInputMeta.getKeyField().getOutputType(),
        kafkaConsumerInputMeta.getMessageField().getOutputType() );

    ArrayList topicList = new ArrayList<String>();
    for ( String topic : kafkaConsumerInputMeta.getTopics() ) {
      topicList.add( environmentSubstitute( topic ) );
    }
    consumer.subscribe( Sets.newHashSet( topicList ) );

    window = new FixedTimeStreamWindow<>( subtransExecutor, kafkaConsumerInputData.outputRowMeta, getDuration(),
      getBatchSize() );
    source = new KafkaStreamSource( consumer, kafkaConsumerInputMeta, kafkaConsumerInputData, variables );

    return true;
  }

  @Override public Collection<StepStatus> subStatuses() {
    return kafkaConsumerInputData.subtransExecutor.getStatuses().values();
  }
}
