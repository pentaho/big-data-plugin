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


package org.pentaho.runtime.test.action.impl;

import org.pentaho.runtime.test.action.RuntimeTestAction;
import org.pentaho.runtime.test.action.RuntimeTestActionHandler;
import org.pentaho.runtime.test.i18n.MessageGetter;
import org.pentaho.runtime.test.i18n.MessageGetterFactory;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by bryan on 9/8/15.
 */
public class LoggingRuntimeTestActionHandlerImpl implements RuntimeTestActionHandler {
  public static final String LOGGING_RUNTIME_TEST_ACTION_HANDLER_IMPL = "LoggingRuntimeTestActionHandlerImpl.Action";
  public static final String LOGGING_RUNTIME_TEST_ACTION_HANDLER_IMPL_MISSING_SEVERITY = "LoggingRuntimeTestActionHandlerImpl.MissingSeverity";
  private final Logger logger;
  private final MessageGetter messageGetter;

  public LoggingRuntimeTestActionHandlerImpl( MessageGetterFactory messageGetterFactory ) {
    this( messageGetterFactory, LoggerFactory.getLogger( LoggingRuntimeTestActionHandlerImpl.class ) );
  }

  public LoggingRuntimeTestActionHandlerImpl( MessageGetterFactory messageGetterFactory, Logger logger ) {
    this.messageGetter = messageGetterFactory.create( LoggingRuntimeTestActionHandlerImpl.class );
    this.logger = logger;
  }

  @Override public boolean canHandle( RuntimeTestAction runtimeTestAction ) {
    return true;
  }

  private String getMessage( RuntimeTestAction runtimeTestAction ) {
    return messageGetter
      .getMessage( LOGGING_RUNTIME_TEST_ACTION_HANDLER_IMPL,
        runtimeTestAction.getName(), runtimeTestAction.getDescription(),
        String.valueOf( runtimeTestAction.getPayload() ) );
  }

  @Override public void handle( RuntimeTestAction runtimeTestAction ) {
    RuntimeTestEntrySeverity severity = runtimeTestAction.getSeverity();
    if ( severity == null ) {
      logger.warn( messageGetter
        .getMessage( LOGGING_RUNTIME_TEST_ACTION_HANDLER_IMPL_MISSING_SEVERITY, runtimeTestAction.getName(),
          runtimeTestAction.getDescription(), String.valueOf( runtimeTestAction.getPayload() ) ) );
      return;
    }
    switch( severity ) {
      case DEBUG:
        logger.debug( getMessage( runtimeTestAction ) );
        break;
      case SKIPPED:
      case WARNING:
        logger.warn( getMessage( runtimeTestAction ) );
        break;
      case ERROR:
      case FATAL:
        logger.error( getMessage( runtimeTestAction ) );
        break;
      default:
        logger.info( getMessage( runtimeTestAction ) );
        break;
    }
  }
}
