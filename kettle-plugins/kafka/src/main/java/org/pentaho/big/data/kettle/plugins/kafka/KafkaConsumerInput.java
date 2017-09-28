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

import com.google.common.collect.Sets;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.SubtransExecutor;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.transexecutor.TransExecutorMeta;
import org.pentaho.di.trans.steps.transexecutor.TransExecutorParameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

/**
 * Consume messages from a Kafka topic
 */
public class KafkaConsumerInput extends BaseStep implements StepInterface {

  private static Class<?> PKG = KafkaConsumerInputMeta.class;
    // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  private KafkaConsumerInputMeta kafkaConsumerInputMeta;
  private KafkaConsumerInputData kafkaConsumerInputData;
  private KafkaConsumerCallable callable;
  private ExecutorService executorService;
  private Map<KafkaConsumerField.Name, Integer> positions;

  AtomicLong messageOffset = new AtomicLong();
  private final HashMap<TopicPartition, Long> maxOffsets = new HashMap<>();


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
        .loadMappingMeta( kafkaConsumerInputMeta, getTransMeta().getRepository(), getTransMeta().getMetaStore(), getParentVariableSpace() );
      kafkaConsumerInputData.subtransExecutor =
        new SubtransExecutor( getTrans(), transMeta, true, kafkaConsumerInputData, new TransExecutorParameters() );
    } catch ( KettleException e ) {
      logError( BaseMessages.getString( PKG, "KafkaConsumerInput.Error.InitFailed" ),  e );
      return false;
    }
    boolean superInit = super.init( kafkaConsumerInputMeta, kafkaConsumerInputData );
    if ( !superInit ) {
      return false;
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
    callable = new KafkaConsumerCallable( consumer );
    startBatchDurationTimer();
    return true;
  }

  public void startBatchDurationTimer() {
    kafkaConsumerInputData.timer = new Timer();
    if ( Long.parseLong( environmentSubstitute( kafkaConsumerInputMeta.getBatchDuration() ) ) <= 0 ) {
      return;
    }
    kafkaConsumerInputData.timer.schedule( new TimerTask() {
      @Override public void run() {
        try {
          sendBufferToSubtrans( true );
        } catch ( KettleException e ) {
          logError( e.getMessage() );
        }
      }
    }, Long.parseLong( environmentSubstitute( kafkaConsumerInputMeta.getBatchDuration() ) ) );
  }

  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    kafkaConsumerInputMeta = (KafkaConsumerInputMeta) smi;
    kafkaConsumerInputData = (KafkaConsumerInputData) sdi;

    Object[] r = getRow(); // get row, set busy!
    if ( r == null ) {
      if ( !first ) {
        // no more input to be expected...
        setOutputDone();
        return false;
      }
    }

    if ( first ) {
      // go get messages from the kafka topic
      first = false;
      kafkaConsumerInputData.outputRowMeta = new RowMeta();
      kafkaConsumerInputMeta.getFields( kafkaConsumerInputData.outputRowMeta,
        getStepname(), null, null, this, repository, metaStore );

      if ( executorService == null ) {
        executorService = Executors.newSingleThreadExecutor();
      }

      List<ValueMetaInterface> valueMetas = kafkaConsumerInputData.outputRowMeta.getValueMetaList();
      positions = new HashMap<>( valueMetas.size() );

      IntStream.range( 0, valueMetas.size() )
        .forEach( idx -> {
          Optional<KafkaConsumerField.Name> match = Arrays.stream( KafkaConsumerField.Name.values() )
            .filter( name -> {
              KafkaConsumerField f = name.getFieldFromMeta( kafkaConsumerInputMeta );
              String fieldName = environmentSubstitute( f.getOutputName() );
              return fieldName != null && fieldName.equals( valueMetas.get( idx ).getName() );
            } )
            .findFirst();

          match.ifPresent( name -> positions.put( name, idx ) );
        } );

      Future<Void> future = executorService.submit( callable );
      try {
        future.get();
      } catch ( InterruptedException | ExecutionException e ) {
        throw new KettleException(
          BaseMessages.getString( PKG, "KafkaConsumerInput.Error.WaitingForMessages" ), e );
      }
    }

    return true;
  }

  void processMessageAsRow( ConsumerRecord<String, String> record ) throws KettleException {
    Object[] rowData = RowDataUtil.allocateRowData( kafkaConsumerInputData.outputRowMeta.size() );

    if ( positions.get( KafkaConsumerField.Name.KEY ) != null ) {
      rowData[ positions.get( KafkaConsumerField.Name.KEY ) ] = record.key();
    }

    if ( positions.get( KafkaConsumerField.Name.MESSAGE ) != null ) {
      rowData[ positions.get( KafkaConsumerField.Name.MESSAGE ) ] = record.value();
    }

    if ( positions.get( KafkaConsumerField.Name.TOPIC ) != null ) {
      rowData[ positions.get( KafkaConsumerField.Name.TOPIC ) ] = record.topic();
    }

    if ( positions.get( KafkaConsumerField.Name.PARTITION ) != null ) {
      rowData[ positions.get( KafkaConsumerField.Name.PARTITION ) ] = (long) record.partition();
    }

    if ( positions.get( KafkaConsumerField.Name.OFFSET ) != null ) {
      rowData[ positions.get( KafkaConsumerField.Name.OFFSET ) ] = record.offset();
    }

    if ( positions.get( KafkaConsumerField.Name.TIMESTAMP ) != null ) {
      rowData[ positions.get( KafkaConsumerField.Name.TIMESTAMP ) ] = record.timestamp();
    }


    collectRow( kafkaConsumerInputData.outputRowMeta, rowData );
    sendBufferToSubtrans( false );
    incrementLinesInput();
    if ( record.offset() > messageOffset.get() ) {
      messageOffset.set( record.offset() );
    }
  }

  private synchronized void sendBufferToSubtrans( boolean forcedByTimer ) throws KettleException {
    if ( forcedByTimer || kafkaConsumerInputData.buffer.size() == Long.parseLong( environmentSubstitute( kafkaConsumerInputMeta.getBatchSize() ) ) ) {
      Optional<Result> result = kafkaConsumerInputData.subtransExecutor.execute( kafkaConsumerInputData.buffer );
      kafkaConsumerInputData.buffer.clear();
      if ( Long.parseLong( environmentSubstitute( kafkaConsumerInputMeta.getBatchDuration() ) ) >= 0 ) {
        kafkaConsumerInputData.timer.cancel();
        startBatchDurationTimer();
      }
      if ( result.isPresent() && result.get().getNrErrors() > 0 ) {
        stopAll();
      }
    }
  }

  synchronized void collectRow( RowMetaInterface rowMeta, Object[] rowData ) {
    kafkaConsumerInputData.buffer.add( new RowMetaAndData( rowMeta, rowData ) );
  }


  class KafkaConsumerCallable implements Callable<Void> {
    private final AtomicBoolean closed = new AtomicBoolean( false );
    private final Consumer consumer;
    private CountDownLatch pauseLatch = new CountDownLatch( 0 );

    public KafkaConsumerCallable( Consumer consumer ) {
      this.consumer = consumer;
    }

    public Void call() {
      try {
        while ( !closed.get() ) {
          waitIfPaused();
          ConsumerRecords<String, String> records = consumer.poll( 1000 );

          waitIfPaused();
          for ( ConsumerRecord<String, String> record : records ) {
            maxOffsets.put( new TopicPartition( record.topic(), record.partition() ), record.offset() );
            if ( closed.get() ) {
              for ( TopicPartition topicPartition : maxOffsets.keySet() ) {
                consumer.seek( topicPartition, maxOffsets.get( topicPartition ) );
              }
              consumer.commitSync();
              break;
            }
            try {
              processMessageAsRow( record );
            } catch ( KettleException e ) {
              logError( BaseMessages.getString(
                PKG, "KafkaConsumerInput.Error.ProcessingMessage", record.key(), record.value() ), e );
            }
          }
        }
        return null;
      } catch ( WakeupException e ) {
        // Ignore exception if closing
        if ( !closed.get() ) {
          throw e;
        }
        return null;
      } finally {
        consumer.close();
      }
    }

    public void waitIfPaused() {
      try {
        pauseLatch.await();
      } catch ( InterruptedException e ) {
        logError( BaseMessages.getString( PKG, "KafkaConsumerInput.Error.Polling" ), e );
      }
    }

    // Shutdown hook which can be called from a separate thread
    public void shutdown() {
      closed.set( true );
      consumer.wakeup();
    }
  }

  @Override
  public void stopRunning( StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface )
    throws KettleException {

    callable.shutdown();
    kafkaConsumerInputData.timer.cancel();
  }

  @Override public void resumeRunning() {
    callable.pauseLatch.countDown();
    super.resumeRunning();
  }

  @Override public void pauseRunning() {
    callable.pauseLatch = new CountDownLatch( 1 );
    super.pauseRunning();
  }
}
