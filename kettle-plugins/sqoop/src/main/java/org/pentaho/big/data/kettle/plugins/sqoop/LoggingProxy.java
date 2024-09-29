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


package org.pentaho.big.data.kettle.plugins.sqoop;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;

import java.io.PrintStream;

/**
 * Redirect all String-based logging for a {@link PrintStream} to a Log4j logger at a specified logging level.
 */
public class LoggingProxy extends PrintStream {
  private PrintStream wrappedStream;
  private Logger logger;
  private Level level;

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
    super( stream );
    wrappedStream = stream;
    this.logger = logger;
    this.level = level;
  }

  @Override
  public void print( String s ) {
    logger.log( level, s );
  }

  /**
   * @return the steam this proxy wraps
   */
  public PrintStream getWrappedStream() {
    return wrappedStream;
  }
}
