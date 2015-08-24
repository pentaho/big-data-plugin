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

package org.pentaho.big.data.impl.cluster.tests.mr;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.clusterTest.i18n.MessageGetterFactory;
import org.pentaho.big.data.api.clusterTest.network.ConnectivityTestFactory;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResultEntry;
import org.pentaho.big.data.api.clusterTest.test.impl.BaseClusterTest;
import org.pentaho.big.data.impl.cluster.tests.Constants;

import java.util.HashSet;
import java.util.List;

/**
 * Created by bryan on 8/14/15.
 */
public class PingJobTrackerTest extends BaseClusterTest {
  public static final String JOB_TRACKER_PING_JOB_TRACKER_TEST =
    "jobTrackerPingJobTrackerTest";
  public static final String PING_JOB_TRACKER_TEST_NAME = "PingJobTrackerTest.Name";
  private static final Class<?> PKG = PingJobTrackerTest.class;
  private final MessageGetterFactory messageGetterFactory;
  private final ConnectivityTestFactory connectivityTestFactory;

  public PingJobTrackerTest( MessageGetterFactory messageGetterFactory,
                             ConnectivityTestFactory connectivityTestFactory ) {
    super( Constants.MAP_REDUCE, JOB_TRACKER_PING_JOB_TRACKER_TEST,
      messageGetterFactory.create( PKG ).getMessage( PING_JOB_TRACKER_TEST_NAME ), new HashSet<String>() );
    this.messageGetterFactory = messageGetterFactory;
    this.connectivityTestFactory = connectivityTestFactory;
  }

  @Override public List<ClusterTestResultEntry> runTest( NamedCluster namedCluster ) {
    return connectivityTestFactory
      .create( messageGetterFactory, namedCluster.getJobTrackerHost(), namedCluster.getJobTrackerPort(), true )
      .runTest();
  }
}
