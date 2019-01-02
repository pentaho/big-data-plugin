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

import org.apache.kafka.clients.consumer.Consumer;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.streaming.common.BaseStreamStep;
import org.pentaho.di.trans.streaming.common.FixedTimeStreamWindow;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Consume messages from a Kafka topic
 */
public class KafkaConsumerInput extends BaseStreamStep implements StepInterface {

  private static final Class<?> PKG = KafkaConsumerInputMeta.class;
  // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

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
  @Override public boolean init( StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface ) {
    KafkaConsumerInputMeta kafkaConsumerInputMeta = (KafkaConsumerInputMeta) stepMetaInterface;
    KafkaConsumerInputData kafkaConsumerInputData = (KafkaConsumerInputData) stepDataInterface;

    boolean superInit = super.init( kafkaConsumerInputMeta, kafkaConsumerInputData );
    if ( !superInit ) {
      logError( BaseMessages.getString( PKG, "KafkaConsumerInput.Error.InitFailed" ) );
      return false;
    }

    try {
      kafkaConsumerInputData.outputRowMeta = kafkaConsumerInputMeta.getRowMeta( getStepname(), this );
    } catch ( KettleStepException e ) {
      log.logError( e.getMessage(), e );
    }

    Consumer consumer =
      kafkaConsumerInputMeta.getKafkaFactory().consumer( kafkaConsumerInputMeta, this::environmentSubstitute,
        kafkaConsumerInputMeta.getKeyField().getOutputType(),
        kafkaConsumerInputMeta.getMessageField().getOutputType() );

    Set<String> topics =
      kafkaConsumerInputMeta.getTopics().stream().map( this::environmentSubstitute ).collect( Collectors.toSet() );
    consumer.subscribe( topics );

    source = new KafkaStreamSource( consumer, kafkaConsumerInputMeta, kafkaConsumerInputData, variables, this );
    window = new FixedTimeStreamWindow<>( subtransExecutor, kafkaConsumerInputData.outputRowMeta, getDuration(),
      getBatchSize(), getParallelism(), kafkaConsumerInputMeta.isAutoCommit() ? p -> {
      } : this::commitOffsets );

    return true;
  }

  private void commitOffsets( Map.Entry<List<List<Object>>, Result> rowsAndResult ) {
    ( (KafkaStreamSource) source ).commitOffsets( rowsAndResult.getKey() );
  }
}
