/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2022 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.impl.shim.pig;

import org.apache.commons.vfs2.FileObject;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Filter;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.logging.log4j.KettleLogChannelAppender;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.logging.log4j.Log4jKettleLayout;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.platform.api.util.LogUtil;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by bryan on 10/1/15.
 */
public class WriterAppenderManager implements Closeable {
  private static final Class<?> PKG = WriterAppenderManager.class;
  private Appender textFileAppender;
  private final Appender kettleLogChannelAppender;
  private FileObject file;
  private String[] loggersToWatch;

  /**
   * Maps Kettle LogLevels to Log4j Levels
   */
  public static Map<LogLevel, Level> LOG_LEVEL_MAP;

  static {
    Map<LogLevel, Level> map = new HashMap<LogLevel, Level>();
    map.put( LogLevel.BASIC, Level.INFO );
    map.put( LogLevel.MINIMAL, Level.INFO );
    map.put( LogLevel.DEBUG, Level.DEBUG );
    map.put( LogLevel.ERROR, Level.ERROR );
    map.put( LogLevel.DETAILED, Level.INFO );
    map.put( LogLevel.ROWLEVEL, Level.DEBUG );
    map.put( LogLevel.NOTHING, Level.OFF );
    LOG_LEVEL_MAP = Collections.unmodifiableMap( map );
  }


  public WriterAppenderManager( LogChannelInterface logChannelInterface, LogLevel logLevel, String name,
                                String[] logs ) {
    // Set up an appender that will send all pig log messages to Kettle's log
    // via logBasic().
    String logFileName = "pdi-" + name; //$NON-NLS-1$
    loggersToWatch = logs;
    try {
      file = KettleVFS.createTempFile( logFileName, ".log", System.getProperty( "java.io.tmpdir" ) );
      textFileAppender = LogUtil.makeAppender( logFileName,
        new OutputStreamWriter( KettleVFS.getOutputStream( file, true ),
          Charset.forName( "utf-8" ) ), new Log4jKettleLayout( Charset.forName( "utf-8" ), true ) );
    } catch ( Exception e ) {
      logChannelInterface.logError( BaseMessages
        .getString( PKG, "JobEntryPigScriptExecutor.FailedToOpenLogFile", logFileName, e.toString() ) ); //$NON-NLS-1$
      logChannelInterface.logError( Const.getStackTracker( e ) );
    }
    ThreadContext.put( "logChannelId", logChannelInterface.getLogChannelId() );
    kettleLogChannelAppender =
      new KettleLogChannelAppender( logChannelInterface, new Log4jKettleLayout( Charset.forName( "utf-8" ), true ) );
    Filter pigLogFilter = new KettleLogChannelFilter( logChannelInterface.getLogChannelId() );
    Level log4jLevel = getLog4jLevel( logLevel );
    for ( String logName : loggersToWatch ) {
      Logger logger = LogManager.getLogger( logName );
      LogUtil.addAppender( kettleLogChannelAppender, logger, log4jLevel, pigLogFilter );
      LogUtil.addAppender( textFileAppender, logger, log4jLevel, pigLogFilter );
      LogUtil.setLevel( logger, log4jLevel );
    }
  }

  private Level getLog4jLevel( LogLevel level ) {
    Level log4jLevel = LOG_LEVEL_MAP.get( level );
    return log4jLevel != null ? log4jLevel : Level.INFO;
  }

  @Override public void close() throws IOException {
    // remove the file appender from kettle logging
    for ( String logName : loggersToWatch ) {
      Logger logger = LogManager.getLogger( logName );
      if ( textFileAppender != null ) {
        LogUtil.removeAppender( textFileAppender, logger );
      }
      if ( kettleLogChannelAppender != null ) {
        LogUtil.removeAppender( kettleLogChannelAppender, logger );
      }
    }
  }

  public FileObject getFile() {
    return file;
  }

  public static class Factory {
    public WriterAppenderManager create( LogChannelInterface logChannelInterface, LogLevel logLevel, String name,
                                         String[] logs ) {
      return new WriterAppenderManager( logChannelInterface, logLevel, name, logs );
    }
  }
}
