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

package org.pentaho.big.data.kettle.plugins.oozie;

import static junit.framework.Assert.assertEquals;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.big.data.kettle.plugins.job.JobEntryMode;
import org.pentaho.big.data.kettle.plugins.job.PropertyEntry;

import org.pentaho.di.core.KettleEnvironment;

import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;
import org.w3c.dom.Document;

/**
 * User: RFellows Date: 6/5/12
 */
@RunWith( MockitoJUnitRunner.class )
public class OozieJobExecutorJobEntryTest {

  @Mock NamedClusterService namedClusterService;
  @Mock NamedCluster namedCluster;
  @Mock OozieJobExecutorConfig config;
  @Mock RuntimeTestActionService runtimeTestActionService;
  @Mock NamedClusterServiceLocator namedClusterServiceLocator;
  @Mock RuntimeTester runtimeTester;
  @Mock IMetaStore metaStore;
  @InjectMocks OozieJobExecutorJobEntry oozieJobEntry;

  final String OOZIE_URL = "http://the.url";
  final String CLUSTER_NAME = "cluster name";

  @BeforeClass
  public static void init() throws Exception {
    KettleEnvironment.init();
  }

  @Test
  public void testLoadXml() throws Exception {

    OozieJobExecutorJobEntry jobEntry = new OozieJobExecutorJobEntry();
    OozieJobExecutorConfig jobConfig = new OozieJobExecutorConfig();

    jobConfig.setOozieWorkflow( "hdfs://localhost:9000/user/test-user/oozie/workflow.xml" );
    jobConfig.setOozieWorkflowConfig( "file:///User/test-user/oozie/job.properties" );
    jobConfig.setOozieUrl( "http://localhost:11000/oozie" );

    jobEntry.setJobConfig( jobConfig );

    JobEntryCopy jec = new JobEntryCopy( jobEntry );
    jec.setLocation( 0, 0 );
    String xml = jec.getXML();

    Document d = XMLHandler.loadXMLString( xml );

    OozieJobExecutorJobEntry jobEntry2 = new OozieJobExecutorJobEntry();
    jobEntry2.loadXML( d.getDocumentElement(), null, null, null );

    OozieJobExecutorConfig jobConfig2 = jobEntry2.getJobConfig();
    assertEquals( jobConfig.getOozieWorkflow(), jobConfig2.getOozieWorkflow() );
    assertEquals( jobConfig.getOozieWorkflowConfig(), jobConfig2.getOozieWorkflowConfig() );
    assertEquals( jobConfig.getOozieUrl(), jobConfig2.getOozieUrl() );
  }

  @Test
  public void testLoadXml_customProps() throws Exception {

    OozieJobExecutorJobEntry jobEntry = new OozieJobExecutorJobEntry();
    OozieJobExecutorConfig jobConfig = new OozieJobExecutorConfig();

    jobConfig.setOozieWorkflow( "hdfs://localhost:9000/user/test-user/oozie/workflow.xml" );
    jobConfig.setOozieWorkflowConfig( "file:///User/test-user/oozie/job.properties" );
    jobConfig.setOozieUrl( "http://localhost:11000/oozie" );

    ArrayList<PropertyEntry> props = new ArrayList<>();
    props.add( new PropertyEntry( "testProp", "testValue" ) );
    jobConfig.setWorkflowProperties( props );

    jobEntry.setJobConfig( jobConfig );

    JobEntryCopy jec = new JobEntryCopy( jobEntry );
    jec.setLocation( 0, 0 );
    String xml = jec.getXML();

    Document d = XMLHandler.loadXMLString( xml );

    OozieJobExecutorJobEntry jobEntry2 = new OozieJobExecutorJobEntry();
    jobEntry2.loadXML( d.getDocumentElement(), null, null, null );

    OozieJobExecutorConfig jobConfig2 = jobEntry2.getJobConfig();
    assertEquals( jobConfig.getOozieWorkflow(), jobConfig2.getOozieWorkflow() );
    assertEquals( jobConfig.getOozieWorkflowConfig(), jobConfig2.getOozieWorkflowConfig() );
    assertEquals( jobConfig.getOozieUrl(), jobConfig2.getOozieUrl() );

    assertNotNull( jobConfig2.getWorkflowProperties() );
    assertEquals( "testValue", jobConfig2.getWorkflowProperties().get( 0 ).getValue() );
  }

  @Test
  public void testGetValidationWarnings_emptyConfig() throws Exception {
    OozieJobExecutorConfig config = new OozieJobExecutorConfig();

    OozieJobExecutorJobEntry je = new OozieJobExecutorJobEntry();
    List<String> warnings = je.getValidationWarnings( config );

    assertEquals( 3, warnings.size() );
  }

  @Test
  public void testGetProperties() throws Exception {
    OozieJobExecutorConfig config = new OozieJobExecutorConfig();
    config.setOozieWorkflowConfig( "src/test/resources/job.properties" );
    Properties props = OozieJobExecutorJobEntry.getProperties( config, new Variables() );

    assertEquals( 6, props.size() );
  }

  @Test
  public void testGetProperties_VariableizedWorkflowPath() throws Exception {
    OozieJobExecutorConfig config = new OozieJobExecutorConfig();
    config.setOozieWorkflowConfig( "${propertiesFile}" );
    OozieJobExecutorJobEntry je = new OozieJobExecutorJobEntry();
    je.setVariable( "propertiesFile", "src/test/resources/job.properties" );

    Properties props = je.getProperties( config );
    assertEquals( 6, props.size() );
  }

  @Test
  public void testGetProperties_fromAdvancedProperties() throws Exception {
    OozieJobExecutorConfig config = new OozieJobExecutorConfig();

    ArrayList<PropertyEntry> advancedProps = new ArrayList<>();
    advancedProps.add( new PropertyEntry( "prop1", "value1" ) );
    advancedProps.add( new PropertyEntry( "prop2", "value2" ) );
    advancedProps.add( new PropertyEntry( "prop3", "value3" ) );

    config.setOozieWorkflowConfig( "src/test/resources/job.properties" );
    config.setWorkflowProperties( advancedProps );
    config.setMode( JobEntryMode.ADVANCED_LIST );

    // make sure our properties are the advanced ones, not read in from the workflow config file
    Properties props = OozieJobExecutorJobEntry.getProperties( config, new Variables() );

    assertTrue( "Advanced properties were not used", props.containsKey( "prop1" ) );
    assertEquals( 3, props.size() );
  }


  @Test
  public void getEffectiveOozieUrlFromCluster() {
    when( config.getNamedCluster() ).thenReturn( namedCluster );
    when( namedCluster.getOozieUrl() ).thenReturn( OOZIE_URL );

    assertThat( oozieJobEntry.getEffectiveOozieUrl( config ),
      is( OOZIE_URL ) );
  }

  @Test
  public void oozieUrlSubstitutedInVariableSpace() {
    OozieJobExecutorJobEntry jobEntry = getStubbedOozieJobExecutorJobEntry();
    VariableSpace variableSpace = mock( VariableSpace.class );
    String OOZIE_VAR = "${oozie_url}";
    when( jobEntry.getVariableSpace() ).thenReturn( variableSpace );
    when( config.getOozieUrl() ).thenReturn( OOZIE_VAR );
    String SUBSTITUTED_URL = "http://my.url";
    when( variableSpace.environmentSubstitute( OOZIE_VAR ) )
      .thenReturn( SUBSTITUTED_URL );

    assertThat( jobEntry.getEffectiveOozieUrl( config ), is( SUBSTITUTED_URL ) );
  }

  private OozieJobExecutorJobEntry getStubbedOozieJobExecutorJobEntry() {
    OozieJobExecutorJobEntry jobEntry = spy( oozieJobEntry );
    jobEntry.setMetaStore( metaStore );
    jobEntry.setJobConfig( config );
    when( config.getClusterName() ).thenReturn( CLUSTER_NAME );
    return jobEntry;
  }

  private TestOozieClient getFailingTestOozieClient() {
    // return status = FAILED
    // isValidWS = true
    // isValidProtocol = true
    return new TestOozieClient( WorkflowJob.Status.FAILED, true, true );
  }

  private TestOozieClient getSucceedingTestOozieClient() {
    // return status = SUCCEEDED
    // isValidWS = true
    // isValidProtocol = true
    return new TestOozieClient( WorkflowJob.Status.SUCCEEDED, true, true );
  }

  private TestOozieClient getBadConfigTestOozieClient() {
    // return status = SUCCEEDED
    // isValidWS = false
    // isValidProtocol = false
    return new TestOozieClient( WorkflowJob.Status.SUCCEEDED, false, false );
  }

  // //////////////////////////////////////////////////////////
  // Stub classes to help in testing.
  // Oozie doesn't provide much in the way of interfaces,
  // so this is our best solution
  // //////////////////////////////////////////////////////////
  class TestOozieClient extends OozieClient {
    TestWorkflowJob wj = null;
    WorkflowJob.Status returnStatus = null;
    boolean isValidWS = true;
    boolean isValidProtocol = true;

    TestOozieClient( WorkflowJob.Status returnStatus, boolean isValidWS, boolean isValidProtocol ) {
      this.returnStatus = returnStatus;
      this.isValidWS = isValidWS;
      this.isValidProtocol = isValidProtocol;
    }

    @Override
    public synchronized void validateWSVersion() throws OozieClientException {
      if ( isValidWS ) {
        return;
      }
      throw new OozieClientException( "Error", new Exception( "Not compatible" ) );
    }

    @Override
    public String getProtocolUrl() throws OozieClientException {
      if ( isValidProtocol ) {
        return "HTTP";
      }
      return null;
    }

    @Override
    public String run( Properties conf ) throws OozieClientException {
      wj = new TestWorkflowJob( WorkflowJob.Status.RUNNING );
      Thread t = new Thread( new Runnable() {
        @Override
        public void run() {
          // block for a second
          try {
            Thread.sleep( 1000 );
            wj.setStatus( returnStatus );
          } catch ( InterruptedException e ) {
            //expected
          }
        }
      } );
      t.start();
      return "test-job-id";
    }

    @Override
    public String getJobLog( String jobId ) throws OozieClientException {
      return "nothing to log";
    }

    @Override
    public WorkflowJob getJobInfo( String jobId ) throws OozieClientException {
      return wj;
    }
  }


  class TestWorkflowJob implements WorkflowJob {
    private Status status;

    TestWorkflowJob( Status status ) {
      this.status = status;
    }

    public void setStatus( Status status ) {
      this.status = status;
    }

    @Override
    public String getAppPath() {
      return null;
    }

    @Override
    public String getAppName() {
      return null;
    }

    @Override
    public String getId() {
      return null;
    }

    @Override
    public String getConf() {
      return null;
    }

    @Override
    public Status getStatus() {
      return status;
    }

    @Override
    public Date getLastModifiedTime() {
      return null;
    }

    @Override
    public Date getCreatedTime() {
      return null;
    }

    @Override
    public Date getStartTime() {
      return null;
    }

    @Override
    public Date getEndTime() {
      return null;
    }

    @Override
    public String getUser() {
      return null;
    }

    @Override
    public String getGroup() {
      return null;
    }

    @Override
    public String getAcl() {
      return null;
    }

    @Override
    public int getRun() {
      return 0;
    }

    @Override
    public String getConsoleUrl() {
      return null;
    }

    @Override
    public String getParentId() {
      return null;
    }

    @Override
    public List<WorkflowAction> getActions() {
      return null;
    }

    @Override
    public String getExternalId() {
      return null;
    }
  }

}
