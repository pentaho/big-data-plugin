/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/

package org.pentaho.big.data.kettle.plugins.logging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.junit.After;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.pentaho.di.core.logging.LogChannelInterface;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HadoopExecutionLoggingTest {
  private static final Logger HADOOP_LOGGER = LogManager.getLogger( HadoopExecutionLogging.HADOOP_LOGGER_NAME );
  private static final Logger SQOOP_LOGGER = LogManager.getLogger( "org.apache.sqoop" );

  @After
  public void clearThreadContext() {
    ThreadContext.clearMap();
  }

  @Test
  public void routesApplicationIdsToTheOwningLogChannelOnly() {
    LogChannelInterface firstLogChannel = logChannel( "first" );
    LogChannelInterface secondLogChannel = logChannel( "second" );
    CountDownLatch appendersAttached = new CountDownLatch( 2 );
    CountDownLatch emitEvents = new CountDownLatch( 1 );
    CountDownLatch eventsEmitted = new CountDownLatch( 2 );
    CountDownLatch executionsComplete = new CountDownLatch( 2 );
    AtomicReference<Throwable> failure = new AtomicReference<>();

    Thread firstExecution = startExecution( firstLogChannel, "application_1", appendersAttached, emitEvents,
      eventsEmitted, executionsComplete, failure );
    Thread secondExecution = startExecution( secondLogChannel, "application_2", appendersAttached, emitEvents,
      eventsEmitted, executionsComplete, failure );
    firstExecution.start();
    secondExecution.start();

    await( appendersAttached );
    emitEvents.countDown();
    await( eventsEmitted );
    await( executionsComplete );
    assertEquals( null, failure.get() );

    ArgumentCaptor<String> firstMessages = ArgumentCaptor.forClass( String.class );
    verify( firstLogChannel ).logBasic( firstMessages.capture() );
    assertMessagesContain( firstMessages.getAllValues(), "application_1" );

    ArgumentCaptor<String> secondMessages = ArgumentCaptor.forClass( String.class );
    verify( secondLogChannel ).logBasic( secondMessages.capture() );
    assertMessagesContain( secondMessages.getAllValues(), "application_2" );
  }

  @Test
  public void ignoresDuplicateLoggerCategoriesAndDetachesOnClose() {
    LogChannelInterface logChannel = logChannel( "single" );

    try ( HadoopExecutionLogging ignored = HadoopExecutionLogging.start( logChannel,
      HadoopExecutionLogging.HADOOP_LOGGER_NAME, HadoopExecutionLogging.HADOOP_LOGGER_NAME ) ) {
      HADOOP_LOGGER.info( "application_4" );
    }
    HADOOP_LOGGER.info( "application_5" );

    ArgumentCaptor<String> messages = ArgumentCaptor.forClass( String.class );
    verify( logChannel ).logBasic( messages.capture() );
    assertMessagesContain( messages.getAllValues(), "application_4" );
  }

  @Test
  public void capturesEveryConfiguredHadoopLoggerCategory() {
    LogChannelInterface logChannel = logChannel( "multiple-categories" );

    try ( HadoopExecutionLogging ignored = HadoopExecutionLogging.start( logChannel,
      HadoopExecutionLogging.HADOOP_LOGGER_NAME, "org.apache.sqoop" ) ) {
      HADOOP_LOGGER.info( "application_6" );
      SQOOP_LOGGER.info( "application_7" );
    }

    ArgumentCaptor<String> messages = ArgumentCaptor.forClass( String.class );
    verify( logChannel, times( 2 ) ).logBasic( messages.capture() );
    assertMessagesContain( messages.getAllValues(), "application_6", "application_7" );
  }

  private LogChannelInterface logChannel( String id ) {
    LogChannelInterface logChannel = mock( LogChannelInterface.class );
    when( logChannel.getLogChannelId() ).thenReturn( id );
    return logChannel;
  }

  private Thread startExecution( final LogChannelInterface logChannel, final String applicationId,
                                 final CountDownLatch appendersAttached, final CountDownLatch emitEvents,
                                 final CountDownLatch eventsEmitted, final CountDownLatch executionsComplete,
                                 final AtomicReference<Throwable> failure ) {
    return new Thread( new Runnable() {
      @Override
      public void run() {
        try ( HadoopExecutionLogging ignored = HadoopExecutionLogging.start( logChannel ) ) {
          appendersAttached.countDown();
          await( emitEvents );
          HADOOP_LOGGER.info( applicationId );
          eventsEmitted.countDown();
          await( eventsEmitted );
        } catch ( Throwable throwable ) {
          failure.compareAndSet( null, throwable );
        } finally {
          executionsComplete.countDown();
        }
      }
    } );
  }

  private void await( CountDownLatch latch ) {
    try {
      assertTrue( latch.await( 10, TimeUnit.SECONDS ) );
    } catch ( InterruptedException exception ) {
      Thread.currentThread().interrupt();
      throw new AssertionError( exception );
    }
  }

  private void assertMessagesContain( List<String> messages, String... expectedMessages ) {
    assertEquals( expectedMessages.length, messages.size() );
    for ( int index = 0; index < expectedMessages.length; index++ ) {
      assertTrue( messages.get( index ).contains( expectedMessages[ index ] ) );
    }
  }
}