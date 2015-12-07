/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.mapreduce;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceLocator;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;

import java.net.MalformedURLException;
import java.net.URL;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * User: Dzmitry Stsiapanau Date: 9/11/14 Time: 1:36 PM
 */
public class JobEntryHadoopJobExecutorTest {
  private NamedClusterService namedClusterService;
  private RuntimeTestActionService runtimeTestActionService;
  private RuntimeTester runtimeTester;
  private NamedClusterServiceLocator namedClusterServiceLocator;
  private JobEntryHadoopJobExecutor jobExecutor;

  @Before
  public void setup() {
    namedClusterService = mock( NamedClusterService.class );
    runtimeTestActionService = mock( RuntimeTestActionService.class );
    runtimeTester = mock( RuntimeTester.class );
    namedClusterServiceLocator = mock( NamedClusterServiceLocator.class );

    jobExecutor = new JobEntryHadoopJobExecutor( namedClusterService, runtimeTestActionService, runtimeTester,
      namedClusterServiceLocator );
  }

  @Test
  public void testResolveJobUrl() throws MalformedURLException {
    String variableValue = "http://jar.net/url";
    String testvar = "testvar";
    jobExecutor.setVariable( testvar, variableValue );
    assertEquals( new URL( variableValue ), jobExecutor.resolveJarUrl( "${" + testvar + "}" ) );
  }
}
