/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/

package org.pentaho.big.data.services.bootstrap;

import org.apache.logging.log4j.ThreadContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ThreadContextPropagatingExecutorServiceTest {
  private static final String KEY = "logChannelId";
  private static final long TIMEOUT_SECONDS = 10;

  private ExecutorService pool;
  private ExecutorService executor;

  @Before
  public void setUp() {
    ThreadContext.clearMap();
    // A single worker thread guarantees that tasks reuse the same thread.
    pool = Executors.newSingleThreadExecutor();
    executor = new ThreadContextPropagatingExecutorService( pool );
  }

  @After
  public void tearDown() {
    pool.shutdownNow();
    ThreadContext.clearMap();
  }

  @Test
  public void submitterContextIsVisibleOnTheWorkerThread() throws Exception {
    ThreadContext.put( KEY, "channel-A" );

    assertEquals( "channel-A", executor.submit( read( KEY ) ).get( TIMEOUT_SECONDS, TimeUnit.SECONDS ) );
  }

  @Test
  public void executeRunnableSeesTheSubmitterContext() throws Exception {
    ThreadContext.put( KEY, "channel-A" );
    String[] seen = new String[ 1 ];

    executor.submit( () -> seen[ 0 ] = ThreadContext.get( KEY ) ).get( TIMEOUT_SECONDS, TimeUnit.SECONDS );

    assertEquals( "channel-A", seen[ 0 ] );
  }

  @Test
  public void contextDoesNotLeakIntoLaterTasksOnTheSameThread() throws Exception {
    ThreadContext.put( KEY, "channel-A" );
    executor.submit( read( KEY ) ).get( TIMEOUT_SECONDS, TimeUnit.SECONDS );

    ThreadContext.clearMap();

    assertNull( executor.submit( read( KEY ) ).get( TIMEOUT_SECONDS, TimeUnit.SECONDS ) );
    assertNull( pool.submit( read( KEY ) ).get( TIMEOUT_SECONDS, TimeUnit.SECONDS ) );
  }

  @Test
  public void workerOwnContextIsRestoredAfterTheTask() throws Exception {
    pool.submit( () -> ThreadContext.put( "workerOnly", "w" ) ).get( TIMEOUT_SECONDS, TimeUnit.SECONDS );
    ThreadContext.put( KEY, "channel-A" );

    Callable<String> readBoth = () -> ThreadContext.get( KEY ) + "/" + ThreadContext.get( "workerOnly" );

    assertEquals( "channel-A/null", executor.submit( readBoth ).get( TIMEOUT_SECONDS, TimeUnit.SECONDS ) );
    assertEquals( "w", pool.submit( read( "workerOnly" ) ).get( TIMEOUT_SECONDS, TimeUnit.SECONDS ) );
    assertNull( pool.submit( read( KEY ) ).get( TIMEOUT_SECONDS, TimeUnit.SECONDS ) );
  }

  @Test
  public void submitterContextIsNotAffectedByTheTask() throws Exception {
    ThreadContext.put( KEY, "channel-A" );

    executor.submit( () -> ThreadContext.put( KEY, "changed-by-task" ) ).get( TIMEOUT_SECONDS, TimeUnit.SECONDS );

    assertEquals( "channel-A", ThreadContext.get( KEY ) );
  }

  @Test
  public void concurrentTasksKeepTheContextCapturedAtSubmitTime() throws Exception {
    ExecutorService widePool = Executors.newFixedThreadPool( 2 );
    try {
      ExecutorService wrapped = new ThreadContextPropagatingExecutorService( widePool );
      CyclicBarrier bothRunning = new CyclicBarrier( 2 );
      Callable<String> readWhileOverlapping = () -> {
        bothRunning.await( TIMEOUT_SECONDS, TimeUnit.SECONDS );
        return ThreadContext.get( KEY );
      };

      ThreadContext.put( KEY, "channel-A" );
      Future<String> first = wrapped.submit( readWhileOverlapping );
      ThreadContext.put( KEY, "channel-B" );
      Future<String> second = wrapped.submit( readWhileOverlapping );

      assertEquals( "channel-A", first.get( TIMEOUT_SECONDS, TimeUnit.SECONDS ) );
      assertEquals( "channel-B", second.get( TIMEOUT_SECONDS, TimeUnit.SECONDS ) );
    } finally {
      widePool.shutdownNow();
    }
  }

  @Test
  public void lifecycleCallsAreDelegated() throws Exception {
    assertFalse( executor.isShutdown() );

    executor.shutdown();

    assertTrue( pool.isShutdown() );
    assertTrue( executor.isShutdown() );
    assertTrue( executor.awaitTermination( TIMEOUT_SECONDS, TimeUnit.SECONDS ) );
    assertTrue( executor.isTerminated() );
  }

  private static Callable<String> read( String key ) {
    return () -> ThreadContext.get( key );
  }
}
