/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package org.pentaho.big.data.services.bootstrap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;

/**
 * Configuration class for Big Data plugin logging.
 * The actual logging configuration is defined in log4j2.xml in the plugin resources,
 * which configures Big Data loggers to write to the existing Pentaho logs (pdi.log and console).
 * This class ensures the Log4j2 configuration is properly initialized and provides utility methods.
 */
public class BigDataLogConfig {

  private static boolean initialized = false;
  private static final Logger logger = LogManager.getLogger( BigDataLogConfig.class );

  /**
   * Initializes the Big Data logging configuration.
   * This method triggers Log4j2 to reconfigure if needed and verifies the configuration is loaded.
   * The actual logger configuration is defined in the log4j2.xml file in the plugin resources,
   * which ensures Big Data logs are written to existing Pentaho log files (pdi.log).
   */
  public static synchronized void initializeBigDataLogging() {
    if ( initialized ) {
      logger.debug( "Big Data logging already initialized" );
      return;
    }

    try {
      // Get the Log4j2 context and trigger reconfiguration to pick up our log4j2.xml
      LoggerContext context = ( LoggerContext ) LogManager.getContext( false );

      // Log the configuration source
      logger.info( "Big Data Plugin logging initialized. Configuration source: " +
        context.getConfiguration().getName() );
      logger.info( "Big Data logs will be written to logs/pdi.log and console output" );

      initialized = true;

    } catch ( Exception e ) {
      logger.error( "Failed to initialize Big Data logging configuration", e );
    }
  }

  /**
   * Gets a logger for the Big Data plugin with the configured appender.
   *
   * @param clazz the class requesting the logger
   * @return configured Logger instance
   */
  public static Logger getBigDataLogger( Class<?> clazz ) {
    // Ensure initialization before returning logger
    if ( !initialized ) {
      initializeBigDataLogging();
    }
    return LogManager.getLogger( clazz );
  }

  /**
   * Gets a logger for the Big Data plugin with the configured appender.
   *
   * @param name the name for the logger
   * @return configured Logger instance
   */
  public static Logger getBigDataLogger( String name ) {
    // Ensure initialization before returning logger
    if ( !initialized ) {
      initializeBigDataLogging();
    }
    return LogManager.getLogger( name );
  }

  /**
   * Checks if the Big Data logging has been initialized.
   *
   * @return true if initialized, false otherwise
   */
  public static boolean isInitialized() {
    return initialized;
  }

  /**
   * Resets the initialization flag (mainly for testing purposes).
   */
  protected static void resetInitialization() {
    initialized = false;
  }

  /**
   * Registers a logger with a specific log level.
   * This method dynamically configures a logger at runtime.
   *
   * @param loggerName the name of the logger to register
   * @param level      the Log4j2 Level to set for this logger
   * @return true if the logger was successfully registered, false otherwise
   */
  public static boolean registerLogger( String loggerName, org.apache.logging.log4j.Level level ) {
    try {
      LoggerContext context = ( LoggerContext ) LogManager.getContext( false );
      org.apache.logging.log4j.core.config.Configuration config = context.getConfiguration();
      org.apache.logging.log4j.core.config.LoggerConfig loggerConfig = config.getLoggerConfig( loggerName );

      // Create a new logger config if it doesn't exist or if it's the root logger
      if ( !loggerName.equals( loggerConfig.getName() ) ) {
        // Create a new logger config
        logger.debug( "Logger : " + loggerName + " does not exist. Registering and adding of the logger" );
        loggerConfig = new org.apache.logging.log4j.core.config.LoggerConfig( loggerName, level, true );
        config.addLogger( loggerName, loggerConfig );
        context.updateLoggers();
      } else {
        logger.debug( "Logger : " + loggerName + " does exist. Skipping the registering and adding of the logger" );
      }
      return true;
    } catch ( Exception e ) {
      logger.error( "Failed to register logger: " + loggerName, e );
      return false;
    }
  }
}




