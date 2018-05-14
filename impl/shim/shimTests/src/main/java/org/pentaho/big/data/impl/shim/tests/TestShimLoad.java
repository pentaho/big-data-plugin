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

import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.big.data.impl.cluster.tests.ClusterRuntimeTestEntry;
import org.pentaho.runtime.test.i18n.MessageGetter;
import org.pentaho.runtime.test.i18n.MessageGetterFactory;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;
import org.pentaho.runtime.test.result.RuntimeTestResultSummary;
import org.pentaho.runtime.test.result.org.pentaho.runtime.test.result.impl.RuntimeTestResultSummaryImpl;
import org.pentaho.runtime.test.test.impl.BaseRuntimeTest;

import java.util.HashSet;

/**
 * Created by bryan on 8/14/15.
 */
public class TestShimLoad extends BaseRuntimeTest {
  public static final String HADOOP_CONFIGURATION_TEST_SHIM_LOAD = "hadoopConfigurationTestShimLoad";
  public static final String TEST_SHIM_LOAD_NAME = "TestShimLoad.Name";
  public static final String TEST_SHIM_LOAD_SHIM_LOADED_DESC = "TestShimLoad.ShimLoaded.Desc";
  public static final String TEST_SHIM_LOAD_SHIM_LOADED_MESSAGE = "TestShimLoad.ShimLoaded.Message";
  public static final String TEST_SHIM_LOAD_NO_SHIM_SPECIFIED_DESC = "TestShimLoad.NoShimSpecified.Desc";
  public static final String TEST_SHIM_LOAD_UNABLE_TO_LOAD_SHIM_DESC = "TestShimLoad.UnableToLoadShim.Desc";
  public static final String HADOOP_CONFIGURATION_MODULE = "Hadoop Configuration";
  private static final Class<?> PKG = TestShimLoad.class;
  private final MessageGetterFactory messageGetterFactory;
  private final MessageGetter messageGetter;

  public TestShimLoad( MessageGetterFactory messageGetterFactory ) {
    super( NamedCluster.class, HADOOP_CONFIGURATION_MODULE, HADOOP_CONFIGURATION_TEST_SHIM_LOAD,
      messageGetterFactory.create( PKG ).getMessage( TEST_SHIM_LOAD_NAME ), true, new HashSet<String>() );
    this.messageGetterFactory = messageGetterFactory;
    messageGetter = messageGetterFactory.create( PKG );
  }

  @Override public RuntimeTestResultSummary runTest( Object objectUnderTest ) {
    try {
      NamedCluster namedCluster = (NamedCluster) objectUnderTest;
      String shimIdentifier = namedCluster.getShimIdentifier();

      return new RuntimeTestResultSummaryImpl(
        new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.INFO,
          messageGetter.getMessage( TEST_SHIM_LOAD_SHIM_LOADED_DESC, shimIdentifier ),
          messageGetter.getMessage( TEST_SHIM_LOAD_SHIM_LOADED_MESSAGE, shimIdentifier ),
          ClusterRuntimeTestEntry.DocAnchor.SHIM_LOAD ) );
    } catch ( Exception e ) {
      return new RuntimeTestResultSummaryImpl(
        new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.ERROR,
          messageGetter.getMessage( TEST_SHIM_LOAD_NO_SHIM_SPECIFIED_DESC ), e.getMessage(), e,
          ClusterRuntimeTestEntry.DocAnchor.SHIM_LOAD ) );
    }
  }
}
