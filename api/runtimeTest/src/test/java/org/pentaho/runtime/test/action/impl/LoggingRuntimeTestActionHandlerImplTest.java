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

import org.junit.Before;
import org.junit.Test;
import org.pentaho.runtime.test.TestMessageGetterFactory;
import org.pentaho.runtime.test.action.RuntimeTestAction;
import org.pentaho.runtime.test.action.RuntimeTestActionPayload;
import org.pentaho.runtime.test.i18n.MessageGetter;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;
import org.slf4j.Logger;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 9/10/15.
 */
public class LoggingRuntimeTestActionHandlerImplTest {
  private MessageGetter messageGetter;
  private Logger logger;
  private LoggingRuntimeTestActionHandlerImpl loggingRuntimeTestActionHandler;
  private RuntimeTestAction runtimeTestAction;
  private String actionDescription;
  private String actionName;
  private RuntimeTestActionPayload runtimeTestActionPayload;

  @Before
  public void setup() {
    TestMessageGetterFactory messageGetterFactory = new TestMessageGetterFactory();
    messageGetter = messageGetterFactory.create( LoggingRuntimeTestActionHandlerImpl.class );
    logger = mock( Logger.class );
    loggingRuntimeTestActionHandler = new LoggingRuntimeTestActionHandlerImpl( messageGetterFactory, logger );
    runtimeTestAction = mock( RuntimeTestAction.class );
    actionName = "actionName";
    actionDescription = "actionDescription";
    runtimeTestActionPayload = mock( RuntimeTestActionPayload.class );
  }

  @Test
  public void testCanHandle() {
    // Should work with least specific payload as it always returns true
    when( runtimeTestAction.getPayload() ).thenReturn( mock( RuntimeTestActionPayload.class ) );
    assertTrue( loggingRuntimeTestActionHandler.canHandle( runtimeTestAction ) );
  }

  private void handleSetup( RuntimeTestEntrySeverity severity ) {
    when( runtimeTestAction.getSeverity() ).thenReturn( severity );
    when( runtimeTestAction.getName() ).thenReturn( actionName );
    when( runtimeTestAction.getDescription() ).thenReturn( actionDescription );
    when( runtimeTestAction.getPayload() ).thenReturn( runtimeTestActionPayload );
    loggingRuntimeTestActionHandler.handle( runtimeTestAction );
  }

  @Test
  public void testHandleNullSeverity() {
    handleSetup( null );
    verify( logger ).warn( messageGetter
      .getMessage( LoggingRuntimeTestActionHandlerImpl.LOGGING_RUNTIME_TEST_ACTION_HANDLER_IMPL_MISSING_SEVERITY,
        actionName, actionDescription, runtimeTestActionPayload.toString() ) );
  }

  @Test
  public void testHandleDebugSeverity() {
    handleSetup( RuntimeTestEntrySeverity.DEBUG );
    verify( logger ).debug( messageGetter
      .getMessage( LoggingRuntimeTestActionHandlerImpl.LOGGING_RUNTIME_TEST_ACTION_HANDLER_IMPL,
        actionName, actionDescription, runtimeTestActionPayload.toString() ) );
  }

  @Test
  public void testHandleInfoSeverity() {
    handleSetup( RuntimeTestEntrySeverity.INFO );
    verify( logger ).info( messageGetter
      .getMessage( LoggingRuntimeTestActionHandlerImpl.LOGGING_RUNTIME_TEST_ACTION_HANDLER_IMPL,
        actionName, actionDescription, runtimeTestActionPayload.toString() ) );
  }

  @Test
  public void testHandleWarningSeverity() {
    handleSetup( RuntimeTestEntrySeverity.WARNING );
    verify( logger ).warn( messageGetter
      .getMessage( LoggingRuntimeTestActionHandlerImpl.LOGGING_RUNTIME_TEST_ACTION_HANDLER_IMPL,
        actionName, actionDescription, runtimeTestActionPayload.toString() ) );
  }

  @Test
  public void testHandleSkippedSeverity() {
    handleSetup( RuntimeTestEntrySeverity.SKIPPED );
    verify( logger ).warn( messageGetter
      .getMessage( LoggingRuntimeTestActionHandlerImpl.LOGGING_RUNTIME_TEST_ACTION_HANDLER_IMPL,
        actionName, actionDescription, runtimeTestActionPayload.toString() ) );
  }

  @Test
  public void testHandleErrorSeverity() {
    handleSetup( RuntimeTestEntrySeverity.ERROR );
    verify( logger ).error( messageGetter
      .getMessage( LoggingRuntimeTestActionHandlerImpl.LOGGING_RUNTIME_TEST_ACTION_HANDLER_IMPL,
        actionName, actionDescription, runtimeTestActionPayload.toString() ) );
  }

  @Test
  public void testHandleFatalSeverity() {
    handleSetup( RuntimeTestEntrySeverity.FATAL );
    verify( logger ).error( messageGetter
      .getMessage( LoggingRuntimeTestActionHandlerImpl.LOGGING_RUNTIME_TEST_ACTION_HANDLER_IMPL,
        actionName, actionDescription, runtimeTestActionPayload.toString() ) );
  }
}
