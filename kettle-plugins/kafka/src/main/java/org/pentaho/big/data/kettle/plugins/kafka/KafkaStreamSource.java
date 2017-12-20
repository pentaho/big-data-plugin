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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.streaming.common.BlockingQueueStreamSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

public class KafkaStreamSource extends BlockingQueueStreamSource {

  private static Class<?> PKG = KafkaConsumerInputMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
  private final Logger logger = LoggerFactory.getLogger( getClass() );

  private final VariableSpace variables;
  private KafkaConsumerInputMeta kafkaConsumerInputMeta;
  private KafkaConsumerInputData kafkaConsumerInputData;
  private Map<KafkaConsumerField.Name, Integer> positions;
  private final HashMap<TopicPartition, Long> maxOffsets = new HashMap<>();

  private Consumer consumer;
  private final ExecutorService executorService = Executors.newCachedThreadPool();
  private KafkaConsumerCallable callable;
  private Future<Void> future;

  public KafkaStreamSource( Consumer consumer, KafkaConsumerInputMeta inputMeta,
                            KafkaConsumerInputData kafkaConsumerInputData, VariableSpace variables ) {
    positions = new HashMap<>();
    this.consumer = consumer;
    this.variables = variables;
    this.kafkaConsumerInputData = kafkaConsumerInputData;
    this.kafkaConsumerInputMeta = inputMeta;
  }

  @Override public void close() {
    super.close();
    kafkaConsumerInputData.subtransExecutor.stop();
    callable.shutdown();
    future.cancel( true );
  }

  @Override public void open() {
    if ( future != null ) {
      // TODO: create message property
      logger.warn( "open() called more than once" );
      return;
    }

    List<ValueMetaInterface> valueMetas = kafkaConsumerInputData.outputRowMeta.getValueMetaList();
    positions = new HashMap<>( valueMetas.size() );

    IntStream.range( 0, valueMetas.size() )
      .forEach( idx -> {
        Optional<KafkaConsumerField.Name> match = Arrays.stream( KafkaConsumerField.Name.values() )
          .filter( name -> {
            KafkaConsumerField f = name.getFieldFromMeta( kafkaConsumerInputMeta );
            String fieldName = variables.environmentSubstitute( f.getOutputName() );
            return fieldName != null && fieldName.equals( valueMetas.get( idx ).getName() );
          } )
          .findFirst();

        match.ifPresent( name -> positions.put( name, idx ) );
      } );

    callable = new KafkaConsumerCallable( consumer );
    future = executorService.submit( callable );
  }

  class KafkaConsumerCallable implements Callable<Void> {
    private final AtomicBoolean closed = new AtomicBoolean( false );
    private final Consumer consumer;

    public KafkaConsumerCallable( Consumer consumer ) {
      this.consumer = consumer;
    }

    public Void call() {
      try {
        while ( !closed.get() ) {
          ConsumerRecords<String, String> records = consumer.poll( 1000 );

          List<List<Object>> rows = new ArrayList<>();
          for ( ConsumerRecord<String, String> record : records ) {
            maxOffsets.put( new TopicPartition( record.topic(), record.partition() ), record.offset() );
            if ( closed.get() ) {
              for ( TopicPartition topicPartition : maxOffsets.keySet() ) {
                consumer.seek( topicPartition, maxOffsets.get( topicPartition ) );
              }
              consumer.commitSync();
              break;
            }
            rows.add( processMessageAsRow( record ) );
          }

          acceptRows( rows );
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

    // Shutdown hook which can be called from a separate thread
    public void shutdown() {
      closed.set( true );
      consumer.wakeup();
    }
  }

  List<Object> processMessageAsRow( ConsumerRecord<String, String> record ) {
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

    return Arrays.asList( rowData );
  }
}
