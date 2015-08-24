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

package org.pentaho.big.data.impl.cluster.tests.oozie;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.clusterTest.i18n.MessageGetter;
import org.pentaho.big.data.api.clusterTest.i18n.MessageGetterFactory;
import org.pentaho.big.data.api.clusterTest.network.ConnectivityTestFactory;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestEntrySeverity;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResultEntry;
import org.pentaho.big.data.api.clusterTest.test.impl.BaseClusterTest;
import org.pentaho.big.data.api.clusterTest.test.impl.ClusterTestResultEntryImpl;
import org.pentaho.big.data.impl.cluster.tests.Constants;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * Created by bryan on 8/14/15.
 */
public class PingOozieHostTest extends BaseClusterTest {
  public static final String OOZIE_PING_OOZIE_HOST_TEST =
    "ooziePingOozieHostTest";
  private static final Class<?> PKG = PingOozieHostTest.class;
  public static final String PING_OOZIE_HOST_TEST_NAME = "PingOozieHostTest.Name";
  public static final String PING_OOZIE_HOST_TEST_MALFORMED_URL_DESC = "PingOozieHostTest.MalformedUrl.Desc";
  public static final String PING_OOZIE_HOST_TEST_MALFORMED_URL_MESSAGE = "PingOozieHostTest.MalformedUrl.Message";
  private final MessageGetterFactory messageGetterFactory;
  private final ConnectivityTestFactory connectivityTestFactory;
  private final MessageGetter messageGetter;

  public PingOozieHostTest( MessageGetterFactory messageGetterFactory,
                            ConnectivityTestFactory connectivityTestFactory ) {
    super( Constants.OOZIE, OOZIE_PING_OOZIE_HOST_TEST,
      messageGetterFactory.create( PKG ).getMessage( PING_OOZIE_HOST_TEST_NAME ), new HashSet<String>() );
    this.messageGetterFactory = messageGetterFactory;
    this.messageGetter = messageGetterFactory.create( PKG );
    this.connectivityTestFactory = connectivityTestFactory;
  }

  @Override public List<ClusterTestResultEntry> runTest( NamedCluster namedCluster ) {
    String oozieUrl = namedCluster.getOozieUrl();
    try {
      URL url = new URL( oozieUrl );
      return connectivityTestFactory
        .create( messageGetterFactory, url.getHost(), String.valueOf( url.getPort() ), false ).runTest();
    } catch ( MalformedURLException e ) {
      return new ArrayList<ClusterTestResultEntry>( Arrays.asList(
        new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.FATAL,
          messageGetter.getMessage( PING_OOZIE_HOST_TEST_MALFORMED_URL_DESC ),
          messageGetter.getMessage( PING_OOZIE_HOST_TEST_MALFORMED_URL_MESSAGE, oozieUrl ), e ) ) );
    }
  }
}
