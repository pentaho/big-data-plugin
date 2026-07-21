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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.log4j.KettleLogChannelAppender;
import org.pentaho.di.core.logging.log4j.Log4jKettleLayout;
import org.pentaho.platform.api.util.LogUtil;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Routes third-party Hadoop Log4j events to the Kettle log channel that owns an execution.
 */
public final class HadoopExecutionLogging implements AutoCloseable {
  public static final String HADOOP_LOGGER_NAME = "org.apache.hadoop";
  private static final String LOG_CHANNEL_ID_CONTEXT_KEY = "logChannelId";
  private static final AtomicLong APPENDER_SEQUENCE = new AtomicLong();

  private final String previousLogChannelId;
  private final List<LogUtil.AppenderRegistration> registrations = new ArrayList<>();
  private boolean closed;

  public static HadoopExecutionLogging start( LogChannelInterface logChannel ) {
    return start( logChannel, HADOOP_LOGGER_NAME );
  }

  public static HadoopExecutionLogging start( LogChannelInterface logChannel, String... loggerNames ) {
    return new HadoopExecutionLogging( logChannel, loggerNames );
  }

  private HadoopExecutionLogging( LogChannelInterface logChannel, String... loggerNames ) {
    if ( logChannel == null ) {
      throw new IllegalArgumentException( "A Kettle log channel is required" );
    }
    String logChannelId = logChannel.getLogChannelId();
    if ( logChannelId == null ) {
      logChannelId = "log-channel-" + System.identityHashCode( logChannel );
    }
    previousLogChannelId = ThreadContext.get( LOG_CHANNEL_ID_CONTEXT_KEY );
    ThreadContext.put( LOG_CHANNEL_ID_CONTEXT_KEY, logChannelId );

    try {
      for ( String loggerName : uniqueLoggerNames( loggerNames ) ) {
        Logger logger = LogManager.getLogger( loggerName );
        Appender appender = new NamedKettleLogChannelAppender( logChannel,
          loggerName + "." + logChannelId + "." + APPENDER_SEQUENCE.incrementAndGet() );
        Filter filter = new LogChannelFilter( logChannelId );
        registrations.add( LogUtil.addAppenderWithLevel( appender, logger, Level.INFO, filter ) );
      }
    } catch ( RuntimeException ex ) {
      close();
      throw ex;
    }
  }

  @Override
  public void close() {
    if ( closed ) {
      return;
    }
    closed = true;
    for ( int index = registrations.size() - 1; index >= 0; index-- ) {
      registrations.get( index ).close();
    }
    if ( previousLogChannelId == null ) {
      ThreadContext.remove( LOG_CHANNEL_ID_CONTEXT_KEY );
    } else {
      ThreadContext.put( LOG_CHANNEL_ID_CONTEXT_KEY, previousLogChannelId );
    }
  }

  private static Set<String> uniqueLoggerNames( String... loggerNames ) {
    Set<String> names = new LinkedHashSet<>();
    if ( loggerNames == null ) {
      throw new IllegalArgumentException( "At least one logger name is required" );
    }
    for ( String loggerName : loggerNames ) {
      if ( loggerName == null || loggerName.isEmpty() ) {
        throw new IllegalArgumentException( "Logger names must not be empty" );
      }
      names.add( loggerName );
    }
    if ( names.isEmpty() ) {
      throw new IllegalArgumentException( "At least one logger name is required" );
    }
    return names;
  }

  private static final class NamedKettleLogChannelAppender extends KettleLogChannelAppender {
    private final String name;

    private NamedKettleLogChannelAppender( LogChannelInterface logChannel, String name ) {
      super( logChannel, new Log4jKettleLayout( StandardCharsets.UTF_8, true ) );
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }
  }

  private static final class LogChannelFilter extends AbstractFilter {
    private final String logChannelId;

    private LogChannelFilter( String logChannelId ) {
      this.logChannelId = logChannelId;
    }

    @Override
    public Result filter( LogEvent event ) {
      return logChannelId.equals( event.getContextData().getValue( LOG_CHANNEL_ID_CONTEXT_KEY ) )
        ? Result.NEUTRAL : Result.DENY;
    }
  }
}