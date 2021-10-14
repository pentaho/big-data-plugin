/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2021 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.amazon.client.impl;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.ActionOnFailure;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsResult;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepSummary;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.pentaho.amazon.hive.job.AmazonHiveJobExecutor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.times;

/**
 * Created by Aliaksandr_Zhuk on 2/8/2018.
 */
@RunWith( PowerMockRunner.class )
@PrepareForTest( EmrClientImpl.class )
@PowerMockIgnore( "jdk.internal.reflect.*" )
public class EmrClientImplTest {

  private EmrClientImpl emrClient;
  private AmazonElasticMapReduce awsEmrClient;
  private AmazonHiveJobExecutor jobEntry;

  @Before
  public void setUp() {
    awsEmrClient = PowerMockito.mock( AmazonElasticMapReduce.class );
    emrClient = PowerMockito.spy( new EmrClientImpl( awsEmrClient ) );
    jobEntry = PowerMockito.spy( new AmazonHiveJobExecutor() );
    setJobEntryFields();
  }

  @Test
  public void testInitEmrCluster_setRequestParamsForHiveStep() throws Exception {
    String stagingS3FileUrl = "s3://bucket/key/test.q";
    String stagingS3BucketUrl = "s3://bucket";
    String stepType = "hive";
    String mainClass = "";
    String bootstrapActions = "";

    RunJobFlowRequest mockJobFlowRequest = Mockito.mock( RunJobFlowRequest.class );

    PowerMockito.doReturn( "s-15PK2NMVIPRPF" ).when( emrClient, "getCurrentlyRunningStepId" );
    PowerMockito.doReturn( new RunJobFlowResult() ).when( awsEmrClient, "runJobFlow", mockJobFlowRequest );

    RunJobFlowRequest jobFlowRequest =
      emrClient.initEmrCluster( stagingS3FileUrl, stagingS3BucketUrl, stepType, mainClass, bootstrapActions, jobEntry );

    Assert.assertEquals( 1, jobFlowRequest.getApplications().size() );
    Assert.assertEquals( 1, jobFlowRequest.getSteps().size() );

    Assert.assertEquals( stagingS3BucketUrl, jobFlowRequest.getLogUri() );
    Assert.assertEquals( jobEntry.getHadoopJobName(), jobFlowRequest.getName() );
    Assert.assertEquals( jobEntry.getEmrRelease(), jobFlowRequest.getReleaseLabel() );
    Assert.assertEquals( jobEntry.getNumInstances(), jobFlowRequest.getInstances().getInstanceCount().toString() );
    Assert.assertEquals( jobEntry.getMasterInstanceType(), jobFlowRequest.getInstances().getMasterInstanceType() );
    Assert.assertEquals( jobEntry.getSlaveInstanceType(), jobFlowRequest.getInstances().getSlaveInstanceType() );
    Assert.assertEquals( jobEntry.getAlive(), jobFlowRequest.getInstances().getKeepJobFlowAliveWhenNoSteps() );
    Assert.assertEquals( jobEntry.getEc2Role(), jobFlowRequest.getJobFlowRole() );
    Assert.assertEquals( jobEntry.getEmrRole(), jobFlowRequest.getServiceRole() );
  }

  @Test
  public void testInitEmrCluster_setRequestParamsForEmrStep() throws Exception {
    String stagingS3FileUrl = "s3://bucket/key/test.jar";
    String stagingS3BucketUrl = "s3://bucket";
    String stepType = "emr";
    String mainClass = "WordCount";
    String bootstrapActions = "";

    RunJobFlowRequest mockJobFlowRequest = Mockito.mock( RunJobFlowRequest.class );

    PowerMockito.doReturn( "s-15PK2NMVIPRPF" ).when( emrClient, "getCurrentlyRunningStepId" );
    PowerMockito.doReturn( new RunJobFlowResult() ).when( awsEmrClient, "runJobFlow", mockJobFlowRequest );

    RunJobFlowRequest jobFlowRequest =
      emrClient.initEmrCluster( stagingS3FileUrl, stagingS3BucketUrl, stepType, mainClass, bootstrapActions, jobEntry );

    Assert.assertEquals( 0, jobFlowRequest.getApplications().size() );
    Assert.assertEquals( 1, jobFlowRequest.getSteps().size() );

    Assert.assertEquals( stagingS3BucketUrl, jobFlowRequest.getLogUri() );
    Assert.assertEquals( jobEntry.getHadoopJobName(), jobFlowRequest.getName() );
    Assert.assertEquals( jobEntry.getEmrRelease(), jobFlowRequest.getReleaseLabel() );
    Assert.assertEquals( jobEntry.getNumInstances(), jobFlowRequest.getInstances().getInstanceCount().toString() );
    Assert.assertEquals( jobEntry.getMasterInstanceType(), jobFlowRequest.getInstances().getMasterInstanceType() );
    Assert.assertEquals( jobEntry.getSlaveInstanceType(), jobFlowRequest.getInstances().getSlaveInstanceType() );
    Assert.assertEquals( jobEntry.getAlive(), jobFlowRequest.getInstances().getKeepJobFlowAliveWhenNoSteps() );
    Assert.assertEquals( jobEntry.getEc2Role(), jobFlowRequest.getJobFlowRole() );
    Assert.assertEquals( jobEntry.getEmrRole(), jobFlowRequest.getServiceRole() );
  }

  @Test
  public void testInitEmrCluster_checkStepParamsForEmrStep() throws Exception {
    String stagingS3FileUrl = "s3://bucket/key/test.jar";
    String stagingS3BucketUrl = "s3://bucket";
    String stepType = "emr";
    String mainClass = "WordCount";
    String bootstrapActions = "";

    List<String> stepArgs = new ArrayList<>();
    stepArgs.add( "--" );
    stepArgs.add( "bucket" );
    stepArgs.add( "s3://test" );

    RunJobFlowRequest mockJobFlowRequest = Mockito.mock( RunJobFlowRequest.class );

    PowerMockito.doReturn( "s-15PK2NMVIPRPF" ).when( emrClient, "getCurrentlyRunningStepId" );
    PowerMockito.doReturn( new RunJobFlowResult() ).when( awsEmrClient, "runJobFlow", mockJobFlowRequest );

    RunJobFlowRequest jobFlowRequest =
      emrClient.initEmrCluster( stagingS3FileUrl, stagingS3BucketUrl, stepType, mainClass, bootstrapActions, jobEntry );

    StepConfig stepConfig = jobFlowRequest.getSteps().get( 0 );
    String hadoopJobName = stepConfig.getName();
    String actionOnFailure = stepConfig.getActionOnFailure();
    HadoopJarStepConfig hadoopJarStepConfig = stepConfig.getHadoopJarStep();

    Assert.assertEquals( 1, jobFlowRequest.getSteps().size() );

    Assert.assertEquals( "custom jar: " + stagingS3FileUrl, hadoopJobName );
    Assert.assertEquals( ActionOnFailure.TERMINATE_JOB_FLOW.name(), actionOnFailure );
    Assert.assertEquals( mainClass, hadoopJarStepConfig.getMainClass() );
    Assert.assertEquals( stepArgs, hadoopJarStepConfig.getArgs() );
    Assert.assertEquals( stagingS3FileUrl, hadoopJarStepConfig.getJar() );
  }

  @Test
  public void testInitEmrCluster_checkStepParamsForHiveStep() throws Exception {
    String stagingS3FileUrl = "s3://bucket/key/test.q";
    String stagingS3BucketUrl = "s3://bucket";
    String stepType = "hive";
    String mainClass = "";
    String bootstrapActions = "";

    RunJobFlowRequest mockJobFlowRequest = Mockito.mock( RunJobFlowRequest.class );

    PowerMockito.doReturn( "s-15PK2NMVIPRPF" ).when( emrClient, "getCurrentlyRunningStepId" );
    PowerMockito.doReturn( new RunJobFlowResult() ).when( awsEmrClient, "runJobFlow", mockJobFlowRequest );

    RunJobFlowRequest jobFlowRequest =
      emrClient.initEmrCluster( stagingS3FileUrl, stagingS3BucketUrl, stepType, mainClass, bootstrapActions, jobEntry );

    StepConfig stepConfig = jobFlowRequest.getSteps().get( 0 );
    String hiveJobName = stepConfig.getName();
    String actionOnFailure = stepConfig.getActionOnFailure();

    Assert.assertEquals( 1, jobFlowRequest.getSteps().size() );

    Assert.assertEquals( "Hive", hiveJobName );
    Assert.assertEquals( ActionOnFailure.TERMINATE_JOB_FLOW.name(), actionOnFailure );
  }

  @Test
  public void testAddStepToExistingJobFlow_getIdOfRunningStep() throws Exception {
    String stagingS3FileUrl = "s3://bucket/key/test.q";
    String stagingS3BucketUrl = "s3://bucket";
    String stepType = "hive";
    String mainClass = "";

    jobEntry.setHadoopJobFlowId( "j-11WRZQW6NIQOA" );

    List<StepSummary> existingSteps = new ArrayList<>();
    List<StepSummary> existingWithNewSteps = new ArrayList<>();

    StepSummary stepSummary1 = new StepSummary();
    stepSummary1.setId( "s-1" );
    StepSummary stepSummary2 = new StepSummary();
    stepSummary2.setId( "s-2" );
    StepSummary stepSummary3 = new StepSummary();
    stepSummary3.setId( "s-3" );

    existingSteps.add( stepSummary1 );
    existingSteps.add( stepSummary2 );

    existingWithNewSteps.addAll( existingSteps );
    existingWithNewSteps.add( stepSummary3 );

    AddJobFlowStepsRequest jobFlowStepsRequest = PowerMockito.mock( AddJobFlowStepsRequest.class );

    PowerMockito.doNothing().when( emrClient, "setStepsFromCluster" );
    PowerMockito.doReturn( new AddJobFlowStepsResult() ).when( awsEmrClient, "addJobFlowSteps", jobFlowStepsRequest );
    PowerMockito.doReturn( existingWithNewSteps ).when( emrClient, "getSteps" );
    Whitebox.setInternalState( emrClient, "stepSummaries", existingSteps );

    emrClient.addStepToExistingJobFlow( stagingS3FileUrl, stagingS3BucketUrl, stepType, mainClass, jobEntry );

    Assert.assertEquals( stepSummary3.getId(), emrClient.getStepId() );
  }

  @Test
  public void testStopSteps_whenLeaveClusterAlive() throws Exception {

    Whitebox.setInternalState( emrClient, "alive", true );

    PowerMockito.doNothing().when( emrClient, "terminateJobFlows" );
    PowerMockito.doNothing().when( emrClient, "cancelStepExecution" );

    boolean stopSteps = emrClient.stopSteps();

    PowerMockito.verifyPrivate( emrClient, times( 0 ) ).invoke( "terminateJobFlows" );
    PowerMockito.verifyPrivate( emrClient, times( 1 ) ).invoke( "cancelStepExecution" );
    Assert.assertEquals( true, stopSteps );
  }

  @Test
  public void testStopSteps_whenNotLeaveClusterAlive() throws Exception {

    Whitebox.setInternalState( emrClient, "alive", false );

    PowerMockito.doNothing().when( emrClient, "terminateJobFlows" );
    PowerMockito.doNothing().when( emrClient, "cancelStepExecution" );

    boolean stopSteps = emrClient.stopSteps();

    PowerMockito.verifyPrivate( emrClient, times( 1 ) ).invoke( "terminateJobFlows" );
    PowerMockito.verifyPrivate( emrClient, times( 0 ) ).invoke( "cancelStepExecution" );
    Assert.assertEquals( false, stopSteps );
  }

  @Test
  public void testRemoveLineBreaks_whenBootstrapActionStringIsNull(){
    String resultBootstrapString = EmrClientImpl.removeLineBreaks( null );
    Assert.assertEquals( null, resultBootstrapString );
  }

  @Test
  public void testRemoveLineBreaks_whenBootstrapActionStringIsSpaces(){

    String bootstrapStringWithBreaks = "  ";
    String expectedString = "";

    String resultBootstrapString = EmrClientImpl.removeLineBreaks( bootstrapStringWithBreaks );

    Assert.assertEquals( expectedString, resultBootstrapString );
  }

  @Test
  public void testRemoveLineBreaks_whenBootstrapActionStringIsEmpty(){

    String bootstrapStringWithBreaks = "";
    String expectedString = "";

    String resultBootstrapString = EmrClientImpl.removeLineBreaks( bootstrapStringWithBreaks );

    Assert.assertEquals( expectedString, resultBootstrapString );
  }

  @Test
  public void testRemoveLineBreaks_whenBootstrapActionStringIsNotNull(){

    String bootstrapStringWithBreaks = " --bootstrap-action\n \"s3://hive-input/copymyfile.sh\" --args\n\n   s3://hive-input/input1/weblogs_small.txt   \n   ";
    String expectedString = "--bootstrap-action \"s3://hive-input/copymyfile.sh\" --args s3://hive-input/input1/weblogs_small.txt";

    String resultBootstrapString = EmrClientImpl.removeLineBreaks( bootstrapStringWithBreaks );

    Assert.assertEquals( expectedString, resultBootstrapString );
  }

  @Test
  public void testRemoveLineBreaks_whenBootstrapActionStringEqualsToExpectedString(){

    String bootstrapStringWithBreaks = "--bootstrap-action \"s3://hive-input/copymyfile.sh\" --args s3://hive-input/input1/weblogs_small.txt";
    String expectedString = "--bootstrap-action \"s3://hive-input/copymyfile.sh\" --args s3://hive-input/input1/weblogs_small.txt";

    String resultBootstrapString = EmrClientImpl.removeLineBreaks( bootstrapStringWithBreaks );

    Assert.assertEquals( expectedString, resultBootstrapString );
  }

  private void setJobEntryFields() {
    jobEntry.setAlive( false );
    jobEntry.setHadoopJobName( "Test Job Executor" );
    jobEntry.setEmrRelease( "emr-5.11.0" );
    jobEntry.setNumInstances( "2" );
    jobEntry.setMasterInstanceType( "c1.medium" );
    jobEntry.setSlaveInstanceType( "c1.medium" );
    jobEntry.setEc2Role( "default_ec2_role" );
    jobEntry.setEmrRole( "default_emr_role" );
    jobEntry.setCmdLineArgs( "-- bucket s3://test" );
  }
}
