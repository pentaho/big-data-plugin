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
