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



package org.pentaho.big.data.kettle.plugins.sqoop;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;

import java.io.PrintStream;
import java.util.function.Supplier;

/**
 * Redirect all String-based logging for a {@link PrintStream} to a Log4j logger at a specified logging level.
 */
public class LoggingProxy extends PrintStream {
  private final PrintStream wrappedStream;
  private final Supplier<Logger> loggerSupplier;
  private final Level level;

  /**
   * Create a new Logging proxy that will log all {@link String}s printed with {@link #print(String)} to the logger
   * using the level provided.
   * 
   * @param stream
   *          Stream to redirect output for
   * @param logger
   *          Logger to log to
   * @param level
   *          Level to log messages at
   */
  public LoggingProxy( PrintStream stream, Logger logger, Level level ) {
    this( stream, () -> logger, level );
  }

  /**
   * Create a new Logging proxy that resolves its target logger on every write, so that a single JVM-wide proxy can
   * follow the execution that owns the calling thread even when those loggers live in different logger contexts.
   *
   * @param stream
   *          Stream to redirect output for
   * @param loggerSupplier
   *          Supplies the logger to log to, may return {@code null} when the calling thread owns no logger
   * @param level
   *          Level to log messages at
   */
  public LoggingProxy( PrintStream stream, Supplier<Logger> loggerSupplier, Level level ) {
    super( stream );
    this.wrappedStream = stream;
    this.loggerSupplier = loggerSupplier;
    this.level = level;
  }

  @Override
  public void print( String s ) {
    Logger logger = loggerSupplier.get();
    if ( logger == null ) {
      wrappedStream.print( s );
    } else {
      logger.log( level, s );
    }
  }

  /**
   * @return the steam this proxy wraps
   */
  public PrintStream getWrappedStream() {
    return wrappedStream;
  }
}
