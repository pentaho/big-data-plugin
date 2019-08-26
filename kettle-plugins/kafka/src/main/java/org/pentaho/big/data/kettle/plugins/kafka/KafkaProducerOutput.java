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

import org.apache.kafka.clients.producer.ProducerRecord;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;


public class KafkaProducerOutput extends BaseStep implements StepInterface {

  private static final Class<?> PKG = KafkaConsumerInputMeta.class;
  private KafkaProducerOutputMeta meta;
  private KafkaProducerOutputData data;
  private KafkaFactory kafkaFactory;
  // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  public KafkaProducerOutput( StepMeta stepMeta,
                              StepDataInterface stepDataInterface, int copyNr,
                              TransMeta transMeta, Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
    setKafkaFactory( KafkaFactory.defaultFactory() );
  }

  void setKafkaFactory( KafkaFactory factory ) {
    this.kafkaFactory = factory;
  }

  /**
   * Initialize and do work where other steps need to wait for...
   *
   * @param stepMetaInterface The metadata to work with
   * @param stepDataInterface The data to initialize
   */
  @Override public boolean init( StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface ) {
    super.init( stepMetaInterface, stepDataInterface );
    meta = ( (KafkaProducerOutputMeta) stepMetaInterface );
    data = ( (KafkaProducerOutputData) stepDataInterface );

    return true;
  }

  @Override public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    Object[] r = getRow(); // get row, set busy!
    if ( r == null ) {
      // no more input to be expected...
      setOutputDone();
      if ( data.kafkaProducer != null ) {
        data.kafkaProducer.close();
      }
      return false;
    }
    if ( first ) {
      data.keyFieldIndex = getInputRowMeta().indexOfValue( environmentSubstitute( meta.getKeyField() ) );
      data.messageFieldIndex = getInputRowMeta().indexOfValue( environmentSubstitute( meta.getMessageField() ) );
      ValueMetaInterface keyValueMeta = getInputRowMeta().getValueMeta( data.keyFieldIndex );
      ValueMetaInterface msgValueMeta = getInputRowMeta().getValueMeta( data.messageFieldIndex );

      data.kafkaProducer = kafkaFactory.producer( meta, this::environmentSubstitute,
        KafkaConsumerField.Type.fromValueMetaInterface( keyValueMeta ),
        KafkaConsumerField.Type.fromValueMetaInterface( msgValueMeta ) );

      data.isOpen = true;

      first = false;
    }

    if ( !data.isOpen ) {
      return false;
    }
    ProducerRecord<Object, Object> producerRecord;
    // allow for null keys
    if ( data.keyFieldIndex < 0 || r[ data.keyFieldIndex ] == null || StringUtil
      .isEmpty( r[ data.keyFieldIndex ].toString() ) ) {
      producerRecord = new ProducerRecord<>( environmentSubstitute( meta.getTopic() ), r[ data.messageFieldIndex ] );
    } else {
      producerRecord = new ProducerRecord<>( environmentSubstitute( meta.getTopic() ), r[ data.keyFieldIndex ],
        r[ data.messageFieldIndex ] );
    }

    data.kafkaProducer.send( producerRecord );
    incrementLinesOutput();

    putRow( getInputRowMeta(), r ); // copy row to possible alternate rowset(s).

    if ( checkFeedback( getLinesRead() ) && log.isBasic() ) {
      logBasic( BaseMessages.getString( PKG, "KafkaConsumerInput.Log.LineNumber" ) + getLinesRead() );
    }

    return true;
  }

  @Override
  public void stopRunning( StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface ) {
    if ( data.kafkaProducer != null && data.isOpen ) {
      data.isOpen = false;
      data.kafkaProducer.flush();
      data.kafkaProducer.close();
    }
  }
}
