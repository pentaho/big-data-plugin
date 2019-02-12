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
