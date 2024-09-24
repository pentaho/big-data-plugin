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
