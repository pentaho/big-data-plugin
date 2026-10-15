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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * An {@link ExecutorService} that runs every task with the Log4j {@link ThreadContext} map of the thread that
 * submitted it.
 * <p>
 * Hadoop execution logging tags the job entry thread with the owning Kettle log channel id (see
 * {@code HadoopExecutionLogging}) and rejects events that carry no tag. Work handed to a pool thread, such as the
 * driver of a Hadoop Job Executor entry in Simple mode, would otherwise run without that tag and its log events
 * would be dropped. The context is captured when the task is submitted and the pool thread's own context is
 * restored when the task ends, so reused threads never leak one execution's tag into another.
 */
public final class ThreadContextPropagatingExecutorService extends AbstractExecutorService {
  private final ExecutorService delegate;

  public ThreadContextPropagatingExecutorService( ExecutorService delegate ) {
    this.delegate = Objects.requireNonNull( delegate, "delegate" );
  }

  @Override
  public void execute( Runnable command ) {
    Objects.requireNonNull( command, "command" );
    final Map<String, String> submitterContext = ThreadContext.getImmutableContext();
    delegate.execute( () -> {
      final Map<String, String> workerContext = ThreadContext.getContext();
      ThreadContext.clearMap();
      ThreadContext.putAll( submitterContext );
      try {
        command.run();
      } finally {
        ThreadContext.clearMap();
        ThreadContext.putAll( workerContext );
      }
    } );
  }

  @Override
  public void shutdown() {
    delegate.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow() {
    return delegate.shutdownNow();
  }

  @Override
  public boolean isShutdown() {
    return delegate.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return delegate.isTerminated();
  }

  @Override
  public boolean awaitTermination( long timeout, TimeUnit unit ) throws InterruptedException {
    return delegate.awaitTermination( timeout, unit );
  }
}
