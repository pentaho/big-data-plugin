/*******************************************************************************
 *
 * Pentaho Big Data
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

package org.pentaho.big.data.impl.shim.pig;

import org.apache.commons.vfs2.FileObject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.WriterAppender;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.logging.KettleLogChannelAppender;
import org.pentaho.di.core.logging.Log4jFileAppender;
import org.pentaho.di.core.logging.Log4jKettleLayout;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.logging.LogWriter;
import org.pentaho.di.i18n.BaseMessages;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by bryan on 10/1/15.
 */
public class WriterAppenderManager implements Closeable {
  private static final Class<?> PKG = WriterAppenderManager.class;
  private final Log4jFileAppender appender;
  private final WriterAppender pigToKettleAppender;
  private final LogWriter logWriter;

  public WriterAppenderManager( LogChannelInterface logChannelInterface, LogLevel logLevel, String name ) {
    this( logChannelInterface, logLevel, name, LogWriter.getInstance() );
  }

  public WriterAppenderManager( LogChannelInterface logChannelInterface, LogLevel logLevel, String name,
                                LogWriter logWriter ) {
    // Set up an appender that will send all pig log messages to Kettle's log
    // via logBasic().
    KettleLoggingPrintWriter klps = new KettleLoggingPrintWriter( logChannelInterface );
    pigToKettleAppender = new WriterAppender( new Log4jKettleLayout( true ), klps );

    Logger pigLogger = Logger.getLogger( "org.apache.pig" );
    Level log4jLevel = getLog4jLevel( logLevel );
    pigLogger.setLevel( log4jLevel );
    String logFileName = "pdi-" + name; //$NON-NLS-1$
    Log4jFileAppender appender = null;
    this.logWriter = logWriter;
    try {
      appender = LogWriter.createFileAppender( logFileName, true, false );
      logWriter.addAppender( appender );
      logChannelInterface.setLogLevel( logLevel );
      if ( pigLogger != null ) {
        pigLogger.addAppender( pigToKettleAppender );
      }
    } catch ( Exception e ) {
      logChannelInterface.logError( BaseMessages
        .getString( PKG, "JobEntryPigScriptExecutor.FailedToOpenLogFile", logFileName, e.toString() ) ); //$NON-NLS-1$
      logChannelInterface.logError( Const.getStackTracker( e ) );
    }
    this.appender = appender;
  }

  private Level getLog4jLevel( LogLevel level ) {
    // KettleLogChannelAppender does not exists in Kette core, so we'll use it from kettle5-log4j-core.
    Level log4jLevel = KettleLogChannelAppender.LOG_LEVEL_MAP.get( level );
    return log4jLevel != null ? log4jLevel : Level.INFO;
  }

  @Override public void close() throws IOException {
    // remove the file appender from kettle logging
    if ( appender != null ) {
      logWriter.removeAppender( appender );
      appender.close();
    }

    Logger pigLogger = Logger.getLogger( "org.apache.pig" );
    if ( pigLogger != null && pigToKettleAppender != null ) {
      pigLogger.removeAppender( pigToKettleAppender );
      pigToKettleAppender.close();
    }
  }

  public FileObject getFile() {
    return appender == null ? null : appender.getFile();
  }

  public static class Factory {
    public WriterAppenderManager create( LogChannelInterface logChannelInterface, LogLevel logLevel, String name ) {
      return new WriterAppenderManager( logChannelInterface, logLevel, name );
    }
  }
}
