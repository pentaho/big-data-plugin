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


package org.pentaho.runtime.test.impl;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.pentaho.runtime.test.RuntimeTest;
import org.pentaho.runtime.test.RuntimeTestProgressCallback;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 8/20/15.
 */
public class RuntimeTesterImplTest {

  private RuntimeTesterImpl runtimeTester;
  private List<RuntimeTest> runtimeTests;
  private ExecutorService executorService;
  private String orderedModulesString;
  private RuntimeTestRunner.Factory runtimeTestRunnerFactory;

  @Before
  public void setup() {
    runtimeTests = new ArrayList<>( Arrays.asList( mock( RuntimeTest.class ) ) );
    executorService = mock( ExecutorService.class );
    orderedModulesString = "test-modules";
    runtimeTestRunnerFactory = mock( RuntimeTestRunner.Factory.class );
    runtimeTester =
      new RuntimeTesterImpl( runtimeTests, executorService, orderedModulesString, runtimeTestRunnerFactory );
  }

  @Test
  public void testRunTests() {
    Object objectUnderTest = new Object();
    RuntimeTestProgressCallback runtimeTestProgressCallback = mock( RuntimeTestProgressCallback.class );
    runtimeTester.runtimeTest( objectUnderTest, runtimeTestProgressCallback );
    ArgumentCaptor<Runnable> runnableArgumentCaptor = ArgumentCaptor.forClass( Runnable.class );
    verify( executorService ).submit( runnableArgumentCaptor.capture() );
    RuntimeTestRunner runtimeTestRunner = mock( RuntimeTestRunner.class );
    when(
      runtimeTestRunnerFactory.create( runtimeTests, objectUnderTest, runtimeTestProgressCallback, executorService ) )
      .thenReturn(
        runtimeTestRunner );
    runnableArgumentCaptor.getValue().run();
    verify( runtimeTestRunner ).runTests();
  }
}
