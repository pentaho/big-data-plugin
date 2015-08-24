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

package org.pentaho.big.data.kettle.plugins.pig;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.big.data.api.clusterTest.ClusterTester;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.bigdata.api.pig.PigService;
import org.pentaho.bigdata.api.pig.PigServiceLocator;
import org.pentaho.di.core.xml.XMLHandler;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 7/10/15.
 */
public class JobEntryPigScriptExecutorTest {
  private NamedClusterService namedClusterService;
  private PigServiceLocator pigServiceLocator;
  private PigService pigService;

  private JobEntryPigScriptExecutor jobEntryPigScriptExecutor;
  private NamedCluster namedCluster;
  private String jobEntryName;
  private String namedClusterName;
  private String namedClusterHdfsHost;
  private String namedClusterHdfsPort;
  private String namedClusterJobTrackerPort;
  private String namedClusterJobTrackerHost;
  private ClusterTester clusterTester;

  @Before
  public void setup() throws ClusterInitializationException {
    namedClusterService = mock( NamedClusterService.class );
    pigServiceLocator = mock( PigServiceLocator.class );
    clusterTester = mock( ClusterTester.class );
    jobEntryPigScriptExecutor = new JobEntryPigScriptExecutor( namedClusterService, clusterTester, pigServiceLocator );

    jobEntryName = "jobEntryName";
    namedClusterName = "namedClusterName";
    namedClusterHdfsHost = "namedClusterHdfsHost";
    namedClusterHdfsPort = "namedClusterHdfsPort";
    namedClusterJobTrackerHost = "namedClusterJobTrackerHost";
    namedClusterJobTrackerPort = "namedClusterJobTrackerPort";

    pigService = mock( PigService.class );
    namedCluster = mock( NamedCluster.class );
    when( pigServiceLocator.getPigService( namedCluster ) ).thenReturn( pigService );
    when( namedCluster.getName() ).thenReturn( namedClusterName );
    when( namedCluster.getHdfsHost() ).thenReturn( namedClusterHdfsHost );
    when( namedCluster.getHdfsPort() ).thenReturn( namedClusterHdfsPort );
    when( namedCluster.getJobTrackerHost() ).thenReturn( namedClusterJobTrackerHost );
    when( namedCluster.getJobTrackerPort() ).thenReturn( namedClusterJobTrackerPort );
  }

  @Test
  public void testGetXmlFullyPopulated() throws IOException, SAXException, ParserConfigurationException {
    String scriptFileName = "scriptFileName";

    jobEntryPigScriptExecutor.setName( jobEntryName );
    jobEntryPigScriptExecutor.setNamedCluster( namedCluster );
    jobEntryPigScriptExecutor.setScriptFilename( scriptFileName );
    jobEntryPigScriptExecutor.setEnableBlocking( true );
    jobEntryPigScriptExecutor.setLocalExecution( true );

    NodeTagValueAsserter nodeTagValueAsserter = new NodeTagValueAsserter( jobEntryPigScriptExecutor.getXML() );

    nodeTagValueAsserter.assertEquals( JobEntryPigScriptExecutor.CLUSTER_NAME, namedClusterName );
    nodeTagValueAsserter.assertEquals( JobEntryPigScriptExecutor.HDFS_HOSTNAME, namedClusterHdfsHost );
    nodeTagValueAsserter.assertEquals( JobEntryPigScriptExecutor.HDFS_PORT, namedClusterHdfsPort );
    nodeTagValueAsserter.assertEquals( JobEntryPigScriptExecutor.JOBTRACKER_HOSTNAME, namedClusterJobTrackerHost );
    nodeTagValueAsserter.assertEquals( JobEntryPigScriptExecutor.JOBTRACKER_PORT, namedClusterJobTrackerPort );

    nodeTagValueAsserter.assertEquals( JobEntryPigScriptExecutor.SCRIPT_FILE, scriptFileName );
    nodeTagValueAsserter.assertEquals( JobEntryPigScriptExecutor.ENABLE_BLOCKING, "Y" );
    nodeTagValueAsserter.assertEquals( JobEntryPigScriptExecutor.LOCAL_EXECUTION, "Y" );
  }

  private static class NodeTagValueAsserter {
    private final Node node;

    private NodeTagValueAsserter( String xmlSnippet ) throws ParserConfigurationException,
      IOException, SAXException {
      DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
      Document snippetDocument = builder.parse(
        new ByteArrayInputStream( ( "<jobEntry>" + xmlSnippet + "</jobEntry>" ).getBytes( "UTF-8" ) ) );
      node = snippetDocument.getFirstChild();
    }

    public void assertEquals( String tag, Object value ) {
      org.junit.Assert.assertEquals( XMLHandler.getTagValue( node, tag ), value );
    }
  }
}
