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

package org.pentaho.big.data.kettle.plugins.oozie;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.hadoop.shim.api.cluster.ClusterInitializationException;
import org.pentaho.big.data.impl.shim.oozie.FallbackOozieClientImpl;
import org.pentaho.big.data.impl.shim.oozie.OozieServiceImpl;
import org.pentaho.big.data.kettle.plugins.job.JobEntryMode;
import org.pentaho.big.data.kettle.plugins.job.PropertyEntry;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.hadoop.shim.api.oozie.OozieService;
import org.pentaho.hadoop.shim.api.oozie.OozieServiceException;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
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
  public void testGetValidationWarnings_invalidOozieUrl() throws Exception {
    OozieJobExecutorConfig config = new OozieJobExecutorConfig();

    NamedClusterServiceLocator locator = mock( NamedClusterServiceLocator.class );
    OozieJobExecutorJobEntry je = new OozieJobExecutorJobEntry(
      mock( NamedClusterService.class ),
      mock( RuntimeTestActionService.class ),
      mock( RuntimeTester.class ),
      locator );

    OozieService service = mock( OozieService.class );
    OozieServiceException ex = mock( OozieServiceException.class );
    when( locator.getService( any( NamedCluster.class ), any( Class.class ) ) )
      .thenReturn( service );
    when( ex.getErrorCode() ).thenReturn( OozieJobExecutorJobEntry.HTTP_ERROR_CODE_404 );
    when( service.getProtocolUrl() )
      .thenThrow( ex );

    config.setOozieUrl( "bad url" );
    config.setOozieWorkflowConfig( "src/test/resources/job.properties" );
    config.setJobEntryName( "name" );

    NamedCluster cluster = mock( NamedCluster.class );
    when( cluster.getOozieUrl() ).thenReturn( "bad url" );
    when( cluster.clone() ).thenReturn( cluster );
    config.setNamedCluster( cluster );
    //when( cluster.)


    List<String> warnings = je.getValidationWarnings( config );

    assertEquals( 2, warnings.size() );
    assertEquals( BaseMessages.getString( OozieJobExecutorJobEntry.class, "ValidationMessages.Invalid.Oozie.URL" ),
        warnings.get( 0 ) );
    assertEquals( BaseMessages.getString( OozieJobExecutorJobEntry.class, "ValidationMessages.App.Path.Property.Missing" ),
      warnings.get( 1 ) );
  }

  @Test
  public void testGetValidationWarnings_incompatibleVersions() throws Exception {
    OozieJobExecutorConfig config = new OozieJobExecutorConfig();

    OozieClient client = getBadConfigTestOozieClient();
    OozieJobExecutorJobEntry je = new TestOozieJobExecutorJobEntry( client ); // just use this to force an error
                                                                              // condition

    config.setOozieUrl( "http://localhost/oozie" );
    config.setOozieWorkflowConfig( "src/test/resources/job.properties" );
    config.setJobEntryName( "name" );

    List<String> warnings = je.getValidationWarnings( config );

    assertEquals( 1, warnings.size() );
    assertEquals( BaseMessages.getString( OozieJobExecutorJobEntry.class,
        "ValidationMessages.Incompatible.Oozie.Versions", client.getClientBuildVersion() ), warnings.get( 0 ) );
  }

  @Test
  public void testGetValidationWarnings_CantFindPropertiesFile() throws Exception {
    OozieJobExecutorConfig config = new OozieJobExecutorConfig();
    OozieClient client = getSucceedingTestOozieClient();
    OozieJobExecutorJobEntry je = new TestOozieJobExecutorJobEntry( client );

    config.setOozieUrl( "http://localhost:11000/oozie" );
    config.setOozieWorkflowConfig( "test/job.properties" );
    config.setJobEntryName( "name" );

    List<String> warnings = je.getValidationWarnings( config );

    // file is a props file & can be parsed into Properties object
    assertEquals( 1, warnings.size() );
    assertTrue( warnings.get( 0 ).startsWith( "Can not resolve Workflow Properties file" ) );
    // assertEquals("Invalid workflow job configuration file.", warnings.get(0));
  }

  @Test
  public void testGetValidationWarnings_MissingAppPathSetting() throws Exception {
    OozieJobExecutorConfig config = new OozieJobExecutorConfig();
    OozieClient client = getSucceedingTestOozieClient();
    OozieJobExecutorJobEntry je = new TestOozieJobExecutorJobEntry( client );

    config.setOozieUrl( "http://localhost:11000/oozie" );
    config.setOozieWorkflowConfig( "src/test/resources/badJob.properties" );
    config.setJobEntryName( "name" );

    List<String> warnings = je.getValidationWarnings( config );

    // file is a props file & can be parsed into Properties object
    assertEquals( 1, warnings.size() );
    assertTrue( warnings.get( 0 ).startsWith( "App Path setting not found in Workflow Properties" ) );
  }

  @Test
  public void testGetValidationWarnings_invalidPort() throws Exception {
    OozieJobExecutorJobEntry je = new TestOozieJobExecutorJobEntry( getSucceedingTestOozieClient() );

    OozieJobExecutorConfig config = new OozieJobExecutorConfig();
    config.setOozieUrl( "http://localhost:11000/oozie" ); // don't worry if it isn't running, we fake out our test
                                                          // connection to it anyway
    config.setOozieWorkflowConfig( "src/test/resources/job.properties" );
    config.setJobEntryName( "name" );

    config.setBlockingPollingInterval( "-100" );
    List<String> warnings = je.getValidationWarnings( config );
    assertEquals( 1, warnings.size() );
    assertEquals(
        BaseMessages.getString( OozieJobExecutorJobEntry.class, "ValidationMessages.Invalid.PollingInterval" ),
        warnings.get( 0 ) );

    config.setBlockingPollingInterval( "0" );
    warnings = je.getValidationWarnings( config );
    assertEquals( 1, warnings.size() );
    assertEquals(
        BaseMessages.getString( OozieJobExecutorJobEntry.class, "ValidationMessages.Invalid.PollingInterval" ),
        warnings.get( 0 ) );

    config.setBlockingPollingInterval( "asdf" );
    warnings = je.getValidationWarnings( config );
    assertEquals( 1, warnings.size() );
    assertEquals(
        BaseMessages.getString( OozieJobExecutorJobEntry.class, "ValidationMessages.Invalid.PollingInterval" ),
        warnings.get( 0 ) );
  }

  @Test
  public void testGetValidationWarnings_everythingIsGood() throws Exception {
    OozieJobExecutorJobEntry je = new TestOozieJobExecutorJobEntry( getSucceedingTestOozieClient() );

    OozieJobExecutorConfig config = new OozieJobExecutorConfig();
    config.setOozieUrl( "http://localhost:11000/oozie" ); // don't worry if it isn't running, we fake out our test
                                                          // connection to it anyway
    config.setOozieWorkflowConfig( "src/test/resources/job.properties" );
    config.setJobEntryName( "name" );

    List<String> warnings = je.getValidationWarnings( config );

    assertEquals( 0, warnings.size() );
  }

  @Test
  public void execute_blocking() throws KettleException {
    final long waitTime = 200;

    OozieJobExecutorConfig config = new OozieJobExecutorConfig();
    config.setOozieUrl( "http://localhost:11000/oozie" ); // don't worry if it isn't running, we fake out our test
                                                          // connection to it anyway
    config.setOozieWorkflowConfig( "src/test/resources/job.properties" );
    config.setJobEntryName( "name" );
    config.setBlockingPollingInterval( "" + waitTime );

    TestOozieJobExecutorJobEntry je = new TestOozieJobExecutorJobEntry( getSucceedingTestOozieClient() );

    je.setParentJob( new Job( "test", null, null ) );
    je.setJobConfig( config );

    Result result = new Result();
    long start = System.currentTimeMillis();
    je.execute( result, 0 );
    long end = System.currentTimeMillis();
    assertTrue( "Total runtime should be >= the wait time if we are blocking", ( end - start ) >= waitTime );

    Assert.assertEquals( 0, result.getNrErrors() );
    assertTrue( result.getResult() );
  }

  @Test
  public void execute_blocking_FAIL() throws KettleException {
    final long waitTime = 1000;

    OozieJobExecutorConfig config = new OozieJobExecutorConfig();
    config.setOozieUrl( "http://localhost:11000/oozie" ); // don't worry if it isn't running, we fake out our test
                                                          // connection to it anyway
    config.setOozieWorkflowConfig( "src/test/resources/job.properties" );
    config.setJobEntryName( "name" );
    config.setBlockingPollingInterval( "" + waitTime );

    TestOozieJobExecutorJobEntry je = new TestOozieJobExecutorJobEntry( getFailingTestOozieClient() );

    je.setParentJob( new Job( "test", null, null ) );
    je.setJobConfig( config );

    Result result = new Result();
    long start = System.currentTimeMillis();
    je.execute( result, 0 );
    long end = System.currentTimeMillis();
    assertTrue( "Total runtime should be >= the wait time if we are blocking", ( end - start ) >= waitTime );

    Assert.assertEquals( 1, result.getNrErrors() );
    assertFalse( result.getResult() );
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
  public void getOozieServiceNoMetaStoreNoConfigCluster() throws ClusterInitializationException {
    when( namedClusterService.getClusterTemplate() )
      .thenReturn( namedCluster );
    when( namedCluster.clone() ).thenReturn( namedCluster );
    when( config.getOozieUrl() ).thenReturn( OOZIE_URL );
    oozieJobEntry.getOozieService( config );

    verify( namedCluster, atLeastOnce() ).setOozieUrl( OOZIE_URL );
    verify( namedClusterServiceLocator )
      .getService( namedCluster, OozieService.class );
  }

  @Test
  public void getOozieServiceNamedClusterInMetaStore() throws ClusterInitializationException, MetaStoreException {
    oozieJobEntry.setMetaStore( metaStore );
    when( config.getClusterName() ).thenReturn( CLUSTER_NAME );
    when( namedClusterService.contains( CLUSTER_NAME, metaStore ) )
      .thenReturn( true );
    when( namedCluster.clone() ).thenReturn( namedCluster );
    when( namedClusterService.read( CLUSTER_NAME, metaStore ) )
      .thenReturn( namedCluster );

    oozieJobEntry.setJobConfig( config );
    oozieJobEntry.getOozieService();

    verify( namedClusterService, atLeastOnce() ).read( CLUSTER_NAME, metaStore );
    verify( namedClusterServiceLocator )
      .getService( namedCluster, OozieService.class );
  }

  @Test
  public void getOozieServiceClusterInConfig() throws ClusterInitializationException {
    when( config.getNamedCluster() ).thenReturn( namedCluster );
    when( namedCluster.clone() ).thenReturn( namedCluster );
    oozieJobEntry.getOozieService( config );

    verify( namedClusterServiceLocator )
      .getService( namedCluster, OozieService.class );
  }

  @Test
  public void getOozieServiceHandlesMetaStoreException() throws MetaStoreException {
    OozieJobExecutorJobEntry jobEntry = getStubbedOozieJobExecutorJobEntry();
    when( namedClusterService.contains( CLUSTER_NAME, metaStore ) )
      .thenThrow( mock( MetaStoreException.class ) );
    jobEntry.getOozieService();

    verify( jobEntry ).logError( anyString(), any( MetaStoreException.class ) );
  }

  @Test
  public void getOozieServiceClusterInitializationException() throws MetaStoreException,
    ClusterInitializationException {
    OozieJobExecutorJobEntry jobEntry = getStubbedOozieJobExecutorJobEntry();
    when( namedClusterServiceLocator.getService( namedCluster, OozieService.class ) )
      .thenThrow( mock( ClusterInitializationException.class ) );
    when( namedClusterService.contains( CLUSTER_NAME, metaStore ) )
      .thenReturn( true );
    when( namedCluster.clone() ).thenReturn( namedCluster );
    when( namedClusterService.read( CLUSTER_NAME, metaStore ) )
      .thenReturn( namedCluster );

    jobEntry.getOozieService();

    verify( jobEntry ).logError( anyString(), any( ClusterInitializationException.class ) );
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

  public class TestOozieJobExecutorJobEntry extends OozieJobExecutorJobEntry {
    private OozieClient client = null;

    TestOozieJobExecutorJobEntry( OozieClient client ) {
      this.client = client;
    }

    @Override
    public OozieService getOozieService() {
      return new OozieServiceImpl( new FallbackOozieClientImpl( client ) );
    }

    @Override
    public OozieService getOozieService( OozieJobExecutorConfig config ) {
      return getOozieService();
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
