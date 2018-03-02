/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.verification.VerificationMode;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceLocator;
import org.pentaho.di.core.KettleClientEnvironment;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogChannelInterfaceFactory;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepStatus;
import org.pentaho.di.trans.steps.abort.AbortMeta;
import org.pentaho.osgi.metastore.locator.api.MetastoreLocator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.pentaho.big.data.kettle.plugins.kafka.KafkaConsumerField.Type.String;
import static org.pentaho.di.core.util.Assert.assertFalse;
import static org.pentaho.di.core.util.Assert.assertNull;

@RunWith( MockitoJUnitRunner.class )
public class KafkaConsumerInputTest {
  StepMeta stepMeta;
  KafkaConsumerInputMeta meta;
  KafkaConsumerInputData data;
  KafkaConsumerInput step;

  TransMeta transMeta;
  Trans trans;

  TopicPartition topic = new TopicPartition( "pentaho", 0 );
  Map<TopicPartition, List<ConsumerRecord<String, String>>> messages = Maps.newHashMap();
  ConsumerRecords records;
  ArrayList<String> topicList;

  @Mock KafkaFactory factory;
  @Mock Consumer consumer;
  @Mock LogChannelInterfaceFactory logChannelFactory;
  @Mock LogChannelInterface logChannel;

  @BeforeClass
  public static void init() throws Exception {
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
    StepPluginType.getInstance().handlePluginAnnotation(
      AbortMeta.class,
      AbortMeta.class.getAnnotation( org.pentaho.di.core.annotations.Step.class ),
      Collections.emptyList(), false, null );
  }

  @Before
  public void setUp() {
    KettleLogStore.setLogChannelInterfaceFactory( logChannelFactory );
    when( logChannelFactory.create( any(), any() ) ).thenReturn( logChannel );

    NamedClusterService namedClusterService = mock( NamedClusterService.class );
    NamedClusterServiceLocator namedClusterServiceLocator = mock( NamedClusterServiceLocator.class );
    MetastoreLocator metastoreLocator = mock( MetastoreLocator.class );

    meta = new KafkaConsumerInputMeta();
    topicList = new ArrayList<>();
    topicList.add( topic.topic() );
    meta.setTopics( topicList );
    meta.setConsumerGroup( "" );
    meta.setTransformationPath( getClass().getResource( "/consumerSub.ktr" ).getPath() );
    meta.setBatchSize( "10" );
    meta.setNamedClusterService( namedClusterService );
    meta.setNamedClusterServiceLocator( namedClusterServiceLocator );
    meta.setMetastoreLocator( metastoreLocator );

    data = new KafkaConsumerInputData();
    stepMeta = new StepMeta( "KafkaConsumer", meta );
    transMeta = new TransMeta();
    transMeta.addStep( stepMeta );
    trans = new Trans( transMeta );
  }

  @Test( expected = KafkaException.class )
  public void testInit_kafkaConfigIssue() {
    step = new KafkaConsumerInput( stepMeta, data, 1, transMeta, trans );
    meta.setBatchSize( "100" );
    meta.setBatchDuration( "1000" );

    step.init( meta, data );
  }

  @Test
  public void testInit_happyPath() {
    meta.setConsumerGroup( "testGroup" );
    meta.setKafkaFactory( factory );
    meta.setBatchDuration( "0" );

    Collection<String> topics = new HashSet<>();
    topics.add( topic.topic() );

    step = new KafkaConsumerInput( stepMeta, data, 1, transMeta, trans );

    when( factory.consumer( eq( meta ), any(), eq( meta.getKeyField().getOutputType() ),
      eq( meta.getMessageField().getOutputType() ) ) ).thenReturn( consumer );

    topicList = new ArrayList<>();
    topicList.add( topics.iterator().next() );
    meta.setTopics( topicList );

    step.init( meta, data );

    verify( consumer ).subscribe( topics );
  }

  @Test
  public void testInitWithRepository() throws Exception {
    final Repository repository = mock( Repository.class );
    transMeta.setRepository( repository );
    meta.setConsumerGroup( "testGroup" );
    meta.setKafkaFactory( factory );
    meta.setBatchDuration( "0" );

    Collection<String> topics = new HashSet<>();
    topics.add( topic.topic() );

    step = new KafkaConsumerInput( stepMeta, data, 1, transMeta, trans );

    when( factory.consumer( eq( meta ), any(), eq( meta.getKeyField().getOutputType() ),
      eq( meta.getMessageField().getOutputType() ) ) ).thenReturn( consumer );

    topicList = new ArrayList<>();
    topicList.add( topics.iterator().next() );
    meta.setTopics( topicList );

    step.init( meta, data );

    verify( consumer ).subscribe( topics );
    verify( repository ).loadTransformation( "consumerSub.ktr", null, null, true, null );
  }

  @Test
  public void testInitFailsOnZeroBatchAndDuration() {
    meta.setConsumerGroup( "testGroup" );
    meta.setKafkaFactory( factory );
    meta.setBatchDuration( "0" );
    meta.setBatchSize( "0" );

    step = new KafkaConsumerInput( stepMeta, data, 1, transMeta, trans );

    assertFalse( step.init( meta, data ) );
    verify( logChannel ).logError( "The \"Number of records\" and \"Duration\" fields can’t both be set to 0. Please "
      + "set a value of 1 or higher for one of the fields." );
  }

  @Test
  public void testInitFailsOnNaNBatchAndDuration() {
    meta.setConsumerGroup( "testGroup" );
    meta.setKafkaFactory( factory );
    meta.setBatchDuration( "one" );
    meta.setBatchSize( "two" );

    step = new KafkaConsumerInput( stepMeta, data, 1, transMeta, trans );

    assertFalse( step.init( meta, data ) );
    verify( logChannel ).logError( "The \"Duration\" field is using a non-numeric value. Please set a numeric value." );
    verify( logChannel ).logError( "The \"Number of records\" field is using a non-numeric value. Please set a numeric value." );
  }

  @Test
  public void testErrorLoadingSubtrans() {
    meta.setTransformationPath( "garbage" );
    meta.setBatchDuration( "1000" );
    meta.setBatchSize( "1000" );
    step = new KafkaConsumerInput( stepMeta, data, 1, transMeta, trans );

    assertFalse( step.init( meta, data ) );
    verify( logChannel ).logError( eq( "Unable to initialize Kafka Consumer" ) );
  }

  @Test
  public void testProcessRow_first() throws Exception {
    meta.setConsumerGroup( "testGroup" );

    meta.setKeyField( new KafkaConsumerField( KafkaConsumerField.Name.KEY, "key" ) );
    meta.setMessageField( new KafkaConsumerField( KafkaConsumerField.Name.MESSAGE, "message" ) );

    // set the topic output field name to not include it in the output fields
    meta.setTopicField( new KafkaConsumerField( KafkaConsumerField.Name.TOPIC, null ) );

    meta.setBatchDuration( "0" );

    meta.setKafkaFactory( factory );
    Collection<String> topics = Sets.newHashSet( topic.topic() );

    // spy on the step meta so we can verify things were called as we expect
    // and we can provide values normally provided by things we aren't testing
    step = spy( new KafkaConsumerInput( stepMeta, data, 1, transMeta, trans ) );
    doReturn( null ).when( step ).getRow();
    doNothing().when( step ).putRow( any( RowMetaInterface.class ), any( Object[].class ) );
    // we control the consumer creation since the usual constructor tries to connect to kafka
    when( factory.consumer( eq( meta ), any(), eq( meta.getKeyField().getOutputType() ),
      eq( meta.getMessageField().getOutputType() ) ) ).thenReturn( consumer );

    int messageCount = 5;
    messages.put( topic, createRecords( topic.topic(), messageCount ) );
    records = new ConsumerRecords<>( messages );
    // provide some data when we try to poll for kafka messages
    CountDownLatch latch = new CountDownLatch( 1 );
    when( consumer.poll( anyLong() ) ).thenReturn( records ).then( invocationOnMock -> {
      latch.countDown();
      return Collections.emptyList();
    } );

    topicList = new ArrayList<>();
    topicList.add( topics.iterator().next() );
    meta.setTopics( topicList );
    step.init( meta, data );

    KafkaStreamSource kafkaStreamSource = (KafkaStreamSource) spy( step.getSource() );
    step.setSource( kafkaStreamSource );
    Iterable rows = kafkaStreamSource.observable().blockingIterable();

    Runnable processRowRunnable = () -> {
      try {
        step.processRow( meta, data );
      } catch ( KettleException e ) {
        fail( e.getMessage() );
      }
    };

    ExecutorService service = Executors.newSingleThreadExecutor();
    service.submit( processRowRunnable );
    latch.await();
    step.stopRunning( meta, data );
    service.shutdown();

    verify( kafkaStreamSource ).open();
    verify( kafkaStreamSource, times( 2 ) ).observable();
    assertEquals( 5, Iterables.size( rows ) );

    // make sure all of the appropriate columns are in the output row meta
    assertNotNull( data.outputRowMeta.searchValueMeta( meta.getMessageField().getOutputName() ) );
    assertNotNull( data.outputRowMeta.searchValueMeta( meta.getKeyField().getOutputName() ) );
    assertNotNull( data.outputRowMeta.searchValueMeta( KafkaConsumerInputMeta.PARTITION_FIELD_NAME ) );
    assertNotNull( data.outputRowMeta.searchValueMeta( KafkaConsumerInputMeta.OFFSET_FIELD_NAME ) );
    assertNotNull( data.outputRowMeta.searchValueMeta( KafkaConsumerInputMeta.TIMESTAMP_FIELD_NAME ) );

    // we deliberately set the topic field name to null so it would NOT be included, make sure it's not there
    assertNull( data.outputRowMeta.searchValueMeta( KafkaConsumerInputMeta.TOPIC_FIELD_NAME ) );
  }

  private List<ConsumerRecord<String, String>> createRecords( String topic, int count ) {
    ArrayList<ConsumerRecord<String, String>> records = Lists.newArrayList();

    for ( int i = 0; i < count; i++ ) {
      ConsumerRecord<String, String> r =
        new ConsumerRecord<>( topic, 0, i, "key_" + i, "value_" + i );
      records.add( r );
    }
    return records;
  }


  @Test
  public void testRunsSubtransWhenPresent() throws Exception {
    String path = getClass().getResource( "/consumerParent.ktr" ).getPath();
    TransMeta consumerParent = new TransMeta( path, new Variables() );
    Trans trans = new Trans( consumerParent );
    KafkaConsumerInputMeta kafkaMeta =
      (KafkaConsumerInputMeta) consumerParent.getStep( 0 ).getStepMetaInterface();
    kafkaMeta.setTransformationPath( getClass().getResource( "/consumerSub.ktr" ).getPath() );
    kafkaMeta.setBatchSize( "2" );
    kafkaMeta.setKafkaFactory( factory );
    int messageCount = 4;
    messages.put( topic, createRecords( topic.topic(), messageCount ) );
    records = new ConsumerRecords<>( messages );
    // provide some data when we try to poll for kafka messages
    when( consumer.poll( 1000 ) ).thenReturn( records )
      .then( invocationOnMock -> {
        while ( trans.getSteps().get( 0 ).step.getLinesWritten() < 4 ) {
          //noinspection UnnecessaryContinue
          continue;  //here to fool checkstyle
        }
        trans.stopAll();
        return new ConsumerRecords<>( Collections.emptyMap() );
      } );
    when( factory.consumer( eq( kafkaMeta ), any(), eq( String ), eq( String ) ) )
      .thenReturn( consumer );
    trans.prepareExecution( new String[]{} );
    trans.startThreads();
    trans.waitUntilFinished();
    verifyRow( "key_0", "value_0", "0", "1", times( 1 ) );
    verifyRow( "key_1", "value_1", "1", "2", times( 1 ) );
    verifyRow( "key_2", "value_2", "2", "1", times( 1 ) );
    verifyRow( "key_3", "value_3", "3", "2", times( 1 ) );
  }

  @Test
  public void testExecutesWhenDurationIsReached() throws Exception {
    String path = getClass().getResource( "/consumerParent.ktr" ).getPath();
    TransMeta consumerParent = new TransMeta( path, new Variables() );
    Trans trans = new Trans( consumerParent );
    KafkaConsumerInputMeta kafkaMeta =
      (KafkaConsumerInputMeta) consumerParent.getStep( 0 ).getStepMetaInterface();
    kafkaMeta.setTransformationPath( getClass().getResource( "/consumerSub.ktr" ).getPath() );
    kafkaMeta.setBatchSize( "200" );
    kafkaMeta.setBatchDuration( "50" );
    kafkaMeta.setKafkaFactory( factory );
    int messageCount = 4;
    messages.put( topic, createRecords( topic.topic(), messageCount ) );
    records = new ConsumerRecords<>( messages );
    // provide some data when we try to poll for kafka messages
    when( consumer.poll( 1000 ) ).thenReturn( records )
      .thenReturn( new ConsumerRecords<>( Collections.emptyMap() ) );
    when( factory.consumer( eq( kafkaMeta ), any(), eq( String ), eq( String ) ) )
      .thenReturn( consumer );
    trans.prepareExecution( new String[]{} );
    trans.startThreads();
    waitForOneSubTrans( trans );
    verifyRow( "key_0", "value_0", "0", "1", times( 1 ) );
    verifyRow( "key_1", "value_1", "1", "2", times( 1 ) );
    verifyRow( "key_2", "value_2", "2", "3", times( 1 ) );
    verifyRow( "key_3", "value_3", "3", "4", times( 1 ) );
    trans.stopAll();
  }

  @Test
  public void testStopsPollingWhenPaused() throws Exception {
    String path = getClass().getResource( "/consumerParent.ktr" ).getPath();
    TransMeta consumerParent = new TransMeta( path, new Variables() );
    Trans trans = new Trans( consumerParent );
    KafkaConsumerInputMeta kafkaMeta =
      (KafkaConsumerInputMeta) consumerParent.getStep( 0 ).getStepMetaInterface();
    kafkaMeta.setTransformationPath( getClass().getResource( "/consumerSub.ktr" ).getPath() );
    kafkaMeta.setBatchSize( "4" );
    kafkaMeta.setBatchDuration( "0" );
    kafkaMeta.setKafkaFactory( factory );
    int messageCount = 4;
    messages.put( topic, createRecords( topic.topic(), messageCount ) );
    records = new ConsumerRecords<>( messages );
    CountDownLatch latch = new CountDownLatch( 1 );
    // provide some data when we try to poll for kafka messages
    when( consumer.poll( 1000 ) )
      .then( invocationOnMock -> {
        trans.pauseRunning();
        latch.countDown();
        return records;
      } )
      .thenReturn( new ConsumerRecords<>( Collections.emptyMap() ) );
    when( factory.consumer( eq( kafkaMeta ), any(), eq( String ), eq( String ) ) )
      .thenReturn( consumer );
    trans.prepareExecution( new String[]{} );
    trans.startThreads();
    latch.await();
    verifyRow( "key_0", "value_0", "0", "1", never() );
    trans.resumeRunning();
    waitForOneSubTrans( trans );
    verifyRow( "key_0", "value_0", "0", "1", times( 1 ) );
    verifyRow( "key_1", "value_1", "1", "2", times( 1 ) );
    verifyRow( "key_2", "value_2", "2", "3", times( 1 ) );
    verifyRow( "key_3", "value_3", "3", "4", times( 1 ) );
    trans.stopAll();
  }

  private void waitForOneSubTrans( Trans trans ) throws InterruptedException {
    while ( trans.getSteps().get( 0 ).step.subStatuses().isEmpty() ) {
      Thread.sleep( 10 );
      //noinspection UnnecessaryContinue
      continue; //checkstyle complains without this
    }
  }

  @Test
  public void testParentAbortsWithChild() throws Exception {
    String path = getClass().getResource( "/abortParent.ktr" ).getPath();
    TransMeta consumerParent = new TransMeta( path, new Variables() );
    Trans trans = new Trans( consumerParent );
    KafkaConsumerInputMeta kafkaMeta =
      (KafkaConsumerInputMeta) consumerParent.getStep( 0 ).getStepMetaInterface();
    kafkaMeta.setTransformationPath( getClass().getResource( "/abortSub.ktr" ).getPath() );
    kafkaMeta.setKafkaFactory( factory );
    int messageCount = 4;
    messages.put( topic, createRecords( topic.topic(), messageCount ) );
    records = new ConsumerRecords<>( messages );
    // provide some data when we try to poll for kafka messages
    when( consumer.poll( 1000 ) ).thenReturn( records );
    when( factory.consumer( eq( kafkaMeta ), any(), eq( String ), eq( String ) ) ).thenReturn( consumer );
    trans.prepareExecution( new String[]{} );
    trans.startThreads();
    trans.waitUntilFinished();
    StepInterface kafkaStep = trans.getSteps().get( 0 ).step;
    Collection<StepStatus> stepStatuses = kafkaStep.subStatuses();
    StepStatus recordsFromStream =
      stepStatuses.stream().filter( stepStatus -> stepStatus.getStepname().equals( "Get records from stream" ) )
        .findFirst().orElseThrow( RuntimeException::new );
    StepStatus abort =
      stepStatuses.stream().filter( stepStatus -> stepStatus.getStepname().equals( "Abort" ) )
        .findFirst().orElseThrow( RuntimeException::new );
    assertEquals( 3, recordsFromStream.getLinesRead() );
    assertEquals( 2, abort.getLinesRead() );

    //I know this seems weird.  It proves the Abort stops kafka from reading new rows
    Thread.sleep( 10 );
    long linesInput = kafkaStep.getLinesInput();
    Thread.sleep( 10 );
    assertEquals( linesInput,  kafkaStep.getLinesInput() );
  }

  @Test
  public void testSubTransStatuses() throws Exception {
    String path = getClass().getResource( "/consumerParent.ktr" ).getPath();
    TransMeta consumerParent = new TransMeta( path, new Variables() );
    Trans trans = new Trans( consumerParent );
    KafkaConsumerInputMeta kafkaMeta =
      (KafkaConsumerInputMeta) consumerParent.getStep( 0 ).getStepMetaInterface();
    kafkaMeta.setTransformationPath( getClass().getResource( "/consumerSub.ktr" ).getPath() );
    kafkaMeta.setBatchSize( "4" );
    kafkaMeta.setBatchDuration( "0" );
    kafkaMeta.setKafkaFactory( factory );
    int messageCount = 4;
    messages.put( topic, createRecords( topic.topic(), messageCount ) );
    records = new ConsumerRecords<>( messages );
    // provide some data when we try to poll for kafka messages
    when( consumer.poll( 1000 ) )
      .thenReturn( records )
      .then( invocationOnMock -> {
        for ( StepStatus stepStatus : trans.getSteps().get( 0 ).step.subStatuses() ) {
          assertEquals( BaseStepData.StepExecutionStatus.STATUS_RUNNING.getDescription(),
                        stepStatus.getStatusDescription() );
        }
        return new ConsumerRecords<>( Collections.emptyMap() );
      } );
    when( factory.consumer( eq( kafkaMeta ), any(), eq( String ), eq( String ) ) )
      .thenReturn( consumer );
    trans.prepareExecution( new String[]{} );
    KafkaConsumerInput kafkaStep = (KafkaConsumerInput) trans.getSteps().get( 0 ).step;
    Collection<StepStatus> stepStatuses = kafkaStep.subStatuses();
    assertEquals( 0, stepStatuses.size() );
    trans.startThreads();
    waitForOneSubTrans( trans );
    trans.stopAll();
    for ( StepStatus stepStatus : trans.getSteps().get( 0 ).step.subStatuses() ) {
      assertEquals( BaseStepData.StepExecutionStatus.STATUS_STOPPED.getDescription(),
                    stepStatus.getStatusDescription() );
    }
  }

  public void verifyRow( String key, String message, String offset, String lineNr, final VerificationMode mode ) {
    verify( logChannel, mode ).logBasic(
      "\n"
      + "------------> Linenr " + lineNr + "------------------------------\n"
      + "Key = " + key + "\n"
      + "Message = " + message + "\n"
      + "Topic = pentaho\n"
      + "Partition = 0\n"
      + "Offset = " + offset + "\n"
      + "Timestamp = -1\n"
      + "\n"
      + "====================" );
  }
}
