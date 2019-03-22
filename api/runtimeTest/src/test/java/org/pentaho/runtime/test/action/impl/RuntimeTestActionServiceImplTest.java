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

import org.junit.Before;
import org.junit.Test;
import org.pentaho.runtime.test.action.RuntimeTestAction;
import org.pentaho.runtime.test.action.RuntimeTestActionHandler;

import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 9/10/15.
 */
public class RuntimeTestActionServiceImplTest {
  private RuntimeTestActionHandler runtimeTestActionHandler;
  private RuntimeTestActionHandler defaultHandler;
  private RuntimeTestActionServiceImpl runtimeTestActionService;
  private RuntimeTestAction runtimeTestAction;

  @Before
  public void setup() {
    runtimeTestActionHandler = mock( RuntimeTestActionHandler.class );
    defaultHandler = mock( RuntimeTestActionHandler.class );
    runtimeTestActionService =
      new RuntimeTestActionServiceImpl( Arrays.asList( runtimeTestActionHandler ), defaultHandler );
    runtimeTestAction = mock( RuntimeTestAction.class );
  }

  @Test
  public void testHandleDefault() {
    when( runtimeTestActionHandler.canHandle( runtimeTestAction ) ).thenReturn( false );
    runtimeTestActionService.handle( runtimeTestAction );
    verify( runtimeTestActionHandler, never() ).handle( runtimeTestAction );
    verify( defaultHandler ).handle( runtimeTestAction );
    verifyNoMoreInteractions( defaultHandler );
  }

  @Test
  public void testHandleNormal() {
    when( runtimeTestActionHandler.canHandle( runtimeTestAction ) ).thenReturn( true );
    runtimeTestActionService.handle( runtimeTestAction );
    verify( runtimeTestActionHandler ).handle( runtimeTestAction );
    verifyNoMoreInteractions( defaultHandler );
  }
}
