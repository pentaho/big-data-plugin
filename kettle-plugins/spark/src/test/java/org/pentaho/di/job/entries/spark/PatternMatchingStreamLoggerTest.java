/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package org.pentaho.di.job.entries.spark;

import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.util.Assert;

public class PatternMatchingStreamLoggerTest {
  private static final String log = "Line1\nSome other line\nOne more line\n";
  private static final String[] matchingPatterns = new String[] { "other" };
  private static final String[] nonMatchingPatterns = new String[] { "non matching pattern" };
  private InputStream input;
  private AtomicBoolean stop;

  @Before
  public void setUp() {
    input = new ByteArrayInputStream( log.getBytes() );
    stop = new AtomicBoolean( false );
  }

  private PatternMatchingStreamLogger createTestee( String[] patterns, final AtomicBoolean listenerNotified ) {
    PatternMatchingStreamLogger testee = new PatternMatchingStreamLogger( mock( LogChannelInterface.class ), input, patterns, stop );
    testee.addPatternMatchedListener( new PatternMatchingStreamLogger.PatternMatchedListener() {
      @Override public void onPatternFound( String pattern ) {
        listenerNotified.set( true );
      }
    } );

    return testee;
  }

  private void doTest( String[] patterns, boolean listenerShouldBeNotified )
      throws InterruptedException, TimeoutException, ExecutionException {
    AtomicBoolean listenerNotified = new AtomicBoolean( false );

    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future f = executor.submit( createTestee( patterns, listenerNotified ), true );

    Assert.assertTrue( (Boolean) f.get( 1, TimeUnit.SECONDS ) );
    Assert.assertTrue( listenerNotified.get() == listenerShouldBeNotified );

    executor.shutdown();
    executor.awaitTermination( 1, TimeUnit.SECONDS );
    Assert.assertTrue( executor.isTerminated() );
  }

  @Test
  public void positiveTest() throws InterruptedException, TimeoutException, ExecutionException {
    doTest( matchingPatterns, true );
  }

  @Test
  public void negativeTest() throws InterruptedException, TimeoutException, ExecutionException {
    doTest( nonMatchingPatterns, false );
  }
}
