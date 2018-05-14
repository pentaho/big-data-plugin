/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.impl.shim.tests;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.ConfigurationException;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.runtime.test.TestMessageGetterFactory;
import org.pentaho.runtime.test.i18n.MessageGetter;
import org.pentaho.runtime.test.i18n.MessageGetterFactory;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;
import org.pentaho.runtime.test.result.RuntimeTestResultSummary;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.pentaho.runtime.test.RuntimeTestEntryUtil.verifyRuntimeTestResultEntry;

/**
 * Created by bryan on 8/24/15.
 */
public class TestShimLoadTest {
  private MessageGetterFactory messageGetterFactory;
  private MessageGetter messageGetter;
  private TestShimLoad testShimLoad;
  private NamedCluster namedCluster;

  @Before
  public void setup() {
    messageGetterFactory = new TestMessageGetterFactory();
    messageGetter = messageGetterFactory.create( TestShimLoad.class );
    testShimLoad = new TestShimLoad( messageGetterFactory );
    namedCluster = mock( NamedCluster.class );
  }

  @Test
  public void testGetName() {
    assertEquals( messageGetter.getMessage( TestShimLoad.TEST_SHIM_LOAD_NAME ), testShimLoad.getName() );
  }

  @Test
  public void testConfigurationException() throws ConfigurationException {
    String testMessage = "testMessage";
    when( namedCluster.getShimIdentifier() ).thenThrow( new RuntimeException( testMessage ) );
    RuntimeTestResultSummary runtimeTestResultSummary = testShimLoad.runTest( namedCluster );
    verifyRuntimeTestResultEntry( runtimeTestResultSummary.getOverallStatusEntry(),
      RuntimeTestEntrySeverity.ERROR, messageGetter.getMessage( TestShimLoad.TEST_SHIM_LOAD_NO_SHIM_SPECIFIED_DESC ),
      testMessage, RuntimeException.class );
    assertEquals( 0, runtimeTestResultSummary.getRuntimeTestResultEntries().size() );
  }

  @Test
  public void testSuccess() throws ConfigurationException {
    String testShim = "testShim";
    when( namedCluster.getShimIdentifier() ).thenReturn( testShim );
    RuntimeTestResultSummary runtimeTestResultSummary = testShimLoad.runTest( namedCluster );
    verifyRuntimeTestResultEntry( runtimeTestResultSummary.getOverallStatusEntry(),
      RuntimeTestEntrySeverity.INFO, messageGetter.getMessage( TestShimLoad.TEST_SHIM_LOAD_SHIM_LOADED_DESC, testShim ),
      messageGetter.getMessage( TestShimLoad.TEST_SHIM_LOAD_SHIM_LOADED_MESSAGE, testShim ) );
    assertEquals( 0, runtimeTestResultSummary.getRuntimeTestResultEntries().size() );
  }
}
