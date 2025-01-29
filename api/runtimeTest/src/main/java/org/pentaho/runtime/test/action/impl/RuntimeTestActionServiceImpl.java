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


package org.pentaho.runtime.test.action.impl;

import org.pentaho.runtime.test.action.RuntimeTestAction;
import org.pentaho.runtime.test.action.RuntimeTestActionHandler;
import org.pentaho.runtime.test.action.RuntimeTestActionService;

import java.util.List;

/**
 * Created by bryan on 9/8/15.
 */
public class RuntimeTestActionServiceImpl implements RuntimeTestActionService {
  private final List<RuntimeTestActionHandler> runtimeTestActionHandlers;
  private final RuntimeTestActionHandler defaultHandler;

  /**
   * Creates the RuntimeTestActionService
   *
   * @param runtimeTestActionHandlers list of handlers
   * @param defaultHandler fallback handler (MUST BE ABLE TO HANDLE ANY PAYLOAD)
   */
  public RuntimeTestActionServiceImpl( List<RuntimeTestActionHandler> runtimeTestActionHandlers,
                                       RuntimeTestActionHandler defaultHandler ) {
    this.runtimeTestActionHandlers = runtimeTestActionHandlers;
    this.defaultHandler = defaultHandler;
  }

  @Override public void handle( RuntimeTestAction runtimeTestAction ) {
    for ( RuntimeTestActionHandler runtimeTestActionHandler : runtimeTestActionHandlers ) {
      if ( runtimeTestActionHandler.canHandle( runtimeTestAction ) ) {
        runtimeTestActionHandler.handle( runtimeTestAction );
        return;
      }
    }
    defaultHandler.handle( runtimeTestAction );
  }
}
