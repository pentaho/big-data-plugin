/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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
import com.amazonaws.services.elasticmapreduce.model.Application;
import com.amazonaws.services.elasticmapreduce.model.BootstrapActionConfig;
import com.amazonaws.services.elasticmapreduce.model.CancelStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.ClusterState;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterResult;
import com.amazonaws.services.elasticmapreduce.model.DescribeStepRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeStepResult;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.ListStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.ListStepsResult;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.ScriptBootstrapActionConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepExecutionState;
import com.amazonaws.services.elasticmapreduce.model.StepSummary;
import com.amazonaws.services.elasticmapreduce.model.TerminateJobFlowsRequest;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import com.google.common.annotations.VisibleForTesting;
import org.pentaho.amazon.AbstractAmazonJobEntry;
import org.pentaho.amazon.client.api.EmrClient;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.util.Utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

/**
 * Created by Aliaksandr_Zhuk on 2/5/2018.
 */
public class EmrClientImpl implements EmrClient {
  private static final String EMR_EC2_DEFAULT_ROLE = "EMR_EC2_DefaultRole";
  private static final String EMR_EFAULT_ROLE = "EMR_DefaultRole";

  private static final String STEP_HIVE = "hive";
  private static final String STEP_EMR = "emr";

  private AmazonElasticMapReduce emrClient;
  private String currentClusterState;
  private String currentStepState;
  private RunJobFlowResult runJobFlowResult;
  private String hadoopJobFlowId;
  private String stepId;
  private List<StepSummary> stepSummaries = null;
  private boolean alive;
  private boolean requestClusterShutdown = false;
  private boolean requestStepCancell = false;

  public EmrClientImpl( AmazonElasticMapReduce emrClient ) {
    this.emrClient = emrClient;
  }

  @Override
  public void runJobFlow( String stagingS3FileUrl, String stagingS3BucketUrl, String stepType, String mainClass,
                          String bootstrapActions,
                          AbstractAmazonJobEntry jobEntry
  ) {

    this.alive = jobEntry.getAlive();

    RunJobFlowRequest runJobFlowRequest =
      initEmrCluster( stagingS3FileUrl, stagingS3BucketUrl, stepType, mainClass, bootstrapActions, jobEntry );

    runJobFlowResult = emrClient.runJobFlow( runJobFlowRequest );
    hadoopJobFlowId = runJobFlowResult.getJobFlowId();
    stepId = getCurrentlyRunningStepId();
  }

  @Override
  public String getHadoopJobFlowId() {
    return runJobFlowResult.getJobFlowId();
  }

  @Override
  public String getStepId() {
    return stepId;
  }

  @Override
  public void addStepToExistingJobFlow( String stagingS3FileUrl, String stagingS3BucketUrl, String stepType,
                                        String mainClass,
                                        AbstractAmazonJobEntry jobEntry ) {
    this.alive = jobEntry.getAlive();
    this.hadoopJobFlowId = jobEntry.getHadoopJobFlowId();

    setStepsFromCluster();
    List<StepConfig> steps = initSteps( stagingS3FileUrl, stepType, mainClass, jobEntry );
    AddJobFlowStepsRequest addJobFlowStepsRequest = new AddJobFlowStepsRequest();
    addJobFlowStepsRequest.setJobFlowId( hadoopJobFlowId );
    addJobFlowStepsRequest.setSteps( steps );
    emrClient.addJobFlowSteps( addJobFlowStepsRequest );

    stepId = getSpecifiedRunningStep();
  }

  /**
   * Determine if the step flow is in a running state.
   *
   * @return true if it is not in COMPLETED or CANCELLED or FAILED or INTERRUPTED, and false otherwise.
   */
  @Override
  public boolean isClusterRunning() {
    if ( ClusterState.WAITING.name().equalsIgnoreCase( currentClusterState ) ) {
      return false;
    }
    if ( ClusterState.TERMINATED.name().equalsIgnoreCase( currentClusterState ) ) {
      return false;
    }
    if ( ClusterState.TERMINATED_WITH_ERRORS.name().equalsIgnoreCase( currentClusterState ) ) {
      return false;
    }
    return true;
  }

  @Override
  public boolean isStepRunning() {
    if ( StepExecutionState.CANCELLED.name().equalsIgnoreCase( currentStepState ) ) {
      return false;
    }
    if ( StepExecutionState.INTERRUPTED.name().equalsIgnoreCase( currentStepState ) ) {
      return false;
    }
    if ( StepExecutionState.COMPLETED.name().equalsIgnoreCase( currentStepState ) ) {
      return false;
    }
    if ( StepExecutionState.FAILED.name().equalsIgnoreCase( currentStepState ) ) {
      return false;
    }
    return true;
  }

  @Override
  public boolean isRunning() {
    currentStepState = getActualStepState();
    currentClusterState = getActualClusterState();
    boolean isClusterRunning = isClusterRunning();
    boolean isStepRunning = isStepRunning();

    if ( !alive && !requestClusterShutdown && ClusterState.WAITING.name().equalsIgnoreCase( currentClusterState ) ) {
      if ( !isStepRunning ) {
        terminateJobFlows();
        return isClusterRunning();
      }
    }
    return ( isClusterRunning || isStepRunning );
  }

  @Override
  public String getCurrentClusterState() {
    return currentClusterState;
  }

  @Override
  public String getCurrentStepState() {
    return currentStepState;
  }

  @Override
  public boolean isClusterTerminated() {
    return ClusterState.TERMINATED.name().equalsIgnoreCase( currentClusterState ) || ClusterState.TERMINATED_WITH_ERRORS
      .name().equalsIgnoreCase( currentClusterState );
  }

  @Override
  public boolean isStepFailed() {
    return StepExecutionState.FAILED.name().equalsIgnoreCase( currentStepState );
  }

  @Override
  public boolean isStepNotSuccess() {
    currentStepState = getActualStepState();
    if ( StepExecutionState.CANCELLED.name().equalsIgnoreCase( currentStepState ) ) {
      return true;
    }
    if ( StepExecutionState.INTERRUPTED.name().equalsIgnoreCase( currentStepState ) ) {
      return true;
    }
    if ( StepExecutionState.FAILED.name().equalsIgnoreCase( currentStepState ) ) {
      return true;
    }
    return false;
  }

  private JobFlowInstancesConfig initEC2Instance( Integer numInsts, String masterInstanceType,
                                                  String slaveInstanceType ) {
    JobFlowInstancesConfig instances = new JobFlowInstancesConfig();
    instances.setInstanceCount( numInsts );
    instances.setMasterInstanceType( masterInstanceType );
    instances.setSlaveInstanceType( slaveInstanceType );
    instances.setKeepJobFlowAliveWhenNoSteps( alive );

    return instances;
  }

  @VisibleForTesting
  RunJobFlowRequest initEmrCluster( String stagingS3FileUrl, String stagingS3BucketUrl, String stepType,
                                    String mainClass,
                                    String bootstrapActions,
                                    AbstractAmazonJobEntry jobEntry
  ) {

    RunJobFlowRequest runJobFlowRequest = new RunJobFlowRequest();

    runJobFlowRequest.setName( jobEntry.getHadoopJobName() );
    runJobFlowRequest.setReleaseLabel( jobEntry.getEmrRelease() );
    runJobFlowRequest.setLogUri( stagingS3BucketUrl );

    JobFlowInstancesConfig instances =
      initEC2Instance( Integer.parseInt( jobEntry.getNumInstances() ), jobEntry.getMasterInstanceType(),
        jobEntry.getSlaveInstanceType() );
    runJobFlowRequest.setInstances( instances );

    List<StepConfig> steps = initSteps( stagingS3FileUrl, stepType, mainClass, jobEntry );
    if ( steps.size() > 0 ) {
      runJobFlowRequest.setSteps( steps );
    }

    if ( stepType.equals( STEP_HIVE ) ) {
      List<Application> applications = initApplications();
      if ( applications.size() > 0 ) {
        runJobFlowRequest.setApplications( applications );
      }

      List<BootstrapActionConfig> stepBootstrapActions = initBootstrapActions( bootstrapActions );
      if ( stepBootstrapActions != null && stepBootstrapActions.size() > 0 ) {
        runJobFlowRequest.setBootstrapActions( stepBootstrapActions );
      }
    }

    String ec2Role = jobEntry.getEc2Role();
    if ( ec2Role == null || ec2Role.trim().isEmpty() ) {
      runJobFlowRequest.setJobFlowRole( EMR_EC2_DEFAULT_ROLE );
    } else {
      runJobFlowRequest.setJobFlowRole( ec2Role );
    }

    String emrRole = jobEntry.getEmrRole();
    if ( emrRole == null || emrRole.trim().isEmpty() ) {
      runJobFlowRequest.setServiceRole( EMR_EFAULT_ROLE );
    } else {
      runJobFlowRequest.setServiceRole( emrRole );
    }

    runJobFlowRequest.setVisibleToAllUsers( true );

    return runJobFlowRequest;
  }

  private StepConfig configureHiveStep( String stagingS3qUrl, String cmdLineArgs ) {

    String[] cmdLineArgsArr;
    if ( cmdLineArgs == null ) {
      cmdLineArgsArr = new String[] { "" };
    } else {
      List<String> cmdArgs = Arrays.asList( cmdLineArgs.split( "\\s+" ) );
      List<String> updatedCmdArgs = cmdArgs.stream().map( e -> replaceDoubleS3( e ) ).collect( Collectors.toList() );
      cmdLineArgsArr = updatedCmdArgs.toArray( new String[ updatedCmdArgs.size() ] );
    }

    StepConfig hiveStepConfig =
      new StepConfig( "Hive",
        new StepFactory().newRunHiveScriptStep( stagingS3qUrl, cmdLineArgsArr ) );
    if ( alive ) {
      hiveStepConfig.withActionOnFailure( ActionOnFailure.CANCEL_AND_WAIT );
    } else {
      hiveStepConfig.withActionOnFailure( ActionOnFailure.TERMINATE_JOB_FLOW );
    }
    return hiveStepConfig;
  }

  private StepConfig initHiveStep( String stagingS3qUrl, String cmdLineArgs ) {
    StepConfig hiveStepConfig = configureHiveStep( stagingS3qUrl, cmdLineArgs );
    return hiveStepConfig;
  }

  private static HadoopJarStepConfig configureHadoopStep( String stagingS3Jar, String mainClass,
                                                          List<String> jarStepArgs ) {
    HadoopJarStepConfig hadoopJarStepConfig = new HadoopJarStepConfig();
    hadoopJarStepConfig.setJar( stagingS3Jar );
    hadoopJarStepConfig.setMainClass( mainClass );
    hadoopJarStepConfig.setArgs( jarStepArgs );

    return hadoopJarStepConfig;
  }

  private StepConfig initHadoopStep( String jarUrl, String mainClass, List<String> jarStepArgs ) {
    StepConfig stepConfig = new StepConfig();
    stepConfig.setName( "custom jar: " + jarUrl );

    stepConfig.setHadoopJarStep( configureHadoopStep( jarUrl, mainClass, jarStepArgs ) );
    if ( this.alive ) {
      stepConfig.withActionOnFailure( ActionOnFailure.CANCEL_AND_WAIT );
    } else {
      stepConfig.withActionOnFailure( ActionOnFailure.TERMINATE_JOB_FLOW );
    }
    return stepConfig;
  }

  @VisibleForTesting
  public static String removeLineBreaks( String multiLineFieldValue ) {
    if ( StringUtil.isEmpty( multiLineFieldValue ) ) {
      return multiLineFieldValue;
    }
    return multiLineFieldValue.replaceAll( "\\s+", " " ).trim();
  }

  private List<StepConfig> initSteps( String stagingS3FileUrl, String stepType,
                                      String mainClass,
                                      AbstractAmazonJobEntry jobEntry ) {
    List<StepConfig> steps = new ArrayList<>();
    StepConfig config = null;

    String cmdLineArgs = removeLineBreaks( jobEntry.getCmdLineArgs() );

    if ( stepType.equals( STEP_HIVE ) ) {
      config = initHiveStep( stagingS3FileUrl, cmdLineArgs );
    }

    if ( stepType.equals( STEP_EMR ) ) {
      List<String> jarStepArgs = parseJarStepArgs( cmdLineArgs );
      config = initHadoopStep( stagingS3FileUrl, mainClass, jarStepArgs );
    }

    steps.add( config );
    return steps;
  }

  private List<String> parseJarStepArgs( String cmdLineArgs ) {
    List<String> jarStepArgs = new ArrayList<>();
    if ( !StringUtil.isEmpty( cmdLineArgs ) ) {
      StringTokenizer st = new StringTokenizer( cmdLineArgs, " " );
      while ( st.hasMoreTokens() ) {
        String token = st.nextToken();
        jarStepArgs.add( replaceDoubleS3( token ) );
      }
    }
    return jarStepArgs;
  }

  private static String replaceDoubleS3( String token ) {

    if ( token.contains( "s3://s3/" ) ) {
      token = token.replace( "s3://s3/", "s3://" );
    }
    return token;
  }

  private List<Application> initApplications() {
    List<Application> applications = new ArrayList<>();
    Application hive = new Application().withName( "Hive" );
    applications.add( hive );
    return applications;
  }

  private List<BootstrapActionConfig> initBootstrapActions( String bootstrapActions ) {
    List<BootstrapActionConfig> actionConfigs = configBootstrapActions( removeLineBreaks( bootstrapActions ) );
    return actionConfigs;
  }

  /**
   * Configure the bootstrap actions, which are executed before Hadoop starts.
   *
   * @return List<StepConfig> configuration data for the bootstrap actions
   */
  private static List<BootstrapActionConfig> configBootstrapActions( String bootstrapActions ) {

    List<BootstrapActionConfig> bootstrapActionConfigs = new ArrayList<>();

    if ( !StringUtil.isEmpty( bootstrapActions ) ) {

      StringTokenizer st = new StringTokenizer( bootstrapActions, " " );
      String path = "";
      String name = "";
      List<String> args = null;
      int actionCount = 0;

      while ( st.hasMoreTokens() ) {

        // Take a key/value pair.
        String key = st.nextToken();
        String value = st.nextToken();

        try {
          // If an argument is enclosed by double quote, take the string without double quote.
          if ( value.startsWith( "\"" ) ) {
            while ( !value.endsWith( "\"" ) ) {
              if ( st.hasMoreTokens() ) {
                value += " " + st.nextToken();
              } else {
                throw new RuntimeException( "Argument does not end with a double quote: " + key + " " + value );
              }
            }
            value = value.substring( 1, value.length() - 1 );
          }
          if ( key.equals( "--bootstrap-action" ) ) {
            if ( !Const.isEmpty( path ) ) {
              actionCount++;
              if ( name.equals( "" ) ) {
                name = "Bootstrap Action " + actionCount;
              }
              // Enter data for one bootstrap action.
              BootstrapActionConfig bootstrapActionConfig = configureBootstrapAction( path, name, args );
              bootstrapActionConfigs.add( bootstrapActionConfig );
              name = "";
              args = null;
            }
            if ( value.startsWith( "s3://" ) ) {
              value = replaceDoubleS3( value );
              path = value;
            } else { // The value for a bootstrap action does not start with "s3://".
              throw new RuntimeException( "s3:// path expected for bootstrap action: " + key + " " + value );
            }
          }
        } catch ( RuntimeException e ) {
          e.printStackTrace();
          return null;
        }
        if ( key.equals( "--bootstrap-name" ) ) {
          name = value;
        }
        if ( key.equals( "--args" ) ) {
          args = configArgs( value, "," );
        }
      }

      if ( !Utils.isEmpty( path ) ) {
        actionCount++;
        if ( name.equals( "" ) ) {
          name = "Bootstrap Action " + actionCount;
        }
        // Enter data for the last bootstrap action.
        BootstrapActionConfig bootstrapActionConfig = configureBootstrapAction( path, name, args );
        bootstrapActionConfigs.add( bootstrapActionConfig );
      }
    }

    return bootstrapActionConfigs;
  }

  /**
   * Configure a bootstrap action object, given its name, path and arguments.
   *
   * @param path - path for the bootstrap action program in S3
   * @param name - name of the bootstrap action
   * @param args - arguments for the bootstrap action
   * @return configuration data object for one bootstrap action
   */
  private static BootstrapActionConfig configureBootstrapAction( String path, String name, List<String> args ) {

    ScriptBootstrapActionConfig scriptBootstrapActionConfig = new ScriptBootstrapActionConfig();
    BootstrapActionConfig bootstrapActionConfig = new BootstrapActionConfig();
    scriptBootstrapActionConfig.setPath( path );
    scriptBootstrapActionConfig.setArgs( args );
    bootstrapActionConfig.setName( name );
    bootstrapActionConfig.setScriptBootstrapAction( scriptBootstrapActionConfig );

    return bootstrapActionConfig;
  }

  /**
   * Given a unparsed arguments and a separator, print log for each argument and return a list of arguments.
   *
   * @param args      - unparsed arguments
   * @param separator - separates one argument from another.
   * @return A list of arguments
   */
  private static List<String> configArgs( String args, String separator ) {

    List<String> argList = new ArrayList<String>();
    if ( !StringUtil.isEmpty( args ) ) {
      StringTokenizer st = new StringTokenizer( args, separator );
      while ( st.hasMoreTokens() ) {
        String token = st.nextToken();
        argList.add( token );
      }
    }
    return argList;
  }

  /**
   * Configure a bootstrap action object, given its name, path and arguments.
   *
   * @param path - path for the bootstrap action program in S3
   * @param name - name of the bootstrap action
   * @param args - arguments for the bootstrap action
   * @return configuration data object for one bootstrap action
   */
  private static BootstrapActionConfig createBootstrapAction( String path, String name, List<String> args ) {

    ScriptBootstrapActionConfig scriptBootstrapActionConfig = new ScriptBootstrapActionConfig();
    BootstrapActionConfig bootstrapActionConfig = new BootstrapActionConfig();
    if ( !path.isEmpty() ) {
      scriptBootstrapActionConfig.setPath( path );
      scriptBootstrapActionConfig.setArgs( args );
    }
    bootstrapActionConfig.setName( name );
    bootstrapActionConfig.setScriptBootstrapAction( scriptBootstrapActionConfig );

    return bootstrapActionConfig;
  }

  private List<StepSummary> getSteps() {

    ListStepsRequest listStepsRequest = new ListStepsRequest();
    listStepsRequest.setClusterId( hadoopJobFlowId );
    ListStepsResult listStepsResult = emrClient.listSteps( listStepsRequest );
    List<StepSummary> stepSummaries = listStepsResult.getSteps();

    if ( stepSummaries.isEmpty() ) {
      return null;
    }
    return stepSummaries;
  }

  private void setStepsFromCluster() {
    stepSummaries = getSteps();
  }

  private String getCurrentlyRunningStepId() {
    return getSteps().get( 0 ).getId();
  }

  private String getSpecifiedRunningStep() {

    List<StepSummary> currentSteps = getSteps();

    currentSteps.removeAll( stepSummaries );

    if ( currentSteps.isEmpty() ) {
      return null;
    }
    return currentSteps.get( 0 ).getId();
  }

  private void terminateJobFlows() {
    if ( !requestClusterShutdown ) {
      TerminateJobFlowsRequest terminateJobFlowsRequest = new TerminateJobFlowsRequest();
      terminateJobFlowsRequest.withJobFlowIds( hadoopJobFlowId );
      emrClient.terminateJobFlows( terminateJobFlowsRequest );
      currentClusterState = getActualClusterState();
      requestClusterShutdown = true;
    }
  }

  private void cancelStepExecution() {
    if ( !requestStepCancell ) {
      CancelStepsRequest cancelStepsRequest = new CancelStepsRequest();
      cancelStepsRequest.setClusterId( hadoopJobFlowId );
      Collection<String> stepIds = new ArrayList<>();
      stepIds.add( stepId );
      cancelStepsRequest.setStepIds( stepIds );
      emrClient.cancelSteps( cancelStepsRequest );
      requestStepCancell = true;
    }
  }

  @Override
  public boolean stopSteps() {
    if ( alive ) {
      cancelStepExecution();
      return true;
    } else {
      terminateJobFlows();
    }
    return false;
  }

  private String getActualClusterState() {
    String clusterState = null;
    DescribeClusterRequest describeClusterRequest = new DescribeClusterRequest();
    describeClusterRequest.setClusterId( hadoopJobFlowId );
    DescribeClusterResult describeClusterResult = emrClient.describeCluster( describeClusterRequest );

    if ( describeClusterResult != null ) {
      clusterState = describeClusterResult.getCluster().getStatus().getState();
    }
    return clusterState;
  }

  private String getActualStepState() {
    String stepState = null;
    DescribeStepRequest describeStepRequest = new DescribeStepRequest();
    describeStepRequest.setClusterId( hadoopJobFlowId );
    describeStepRequest.setStepId( stepId );
    DescribeStepResult describeStepResult = emrClient.describeStep( describeStepRequest );

    if ( describeStepResult != null ) {
      stepState = describeStepResult.getStep().getStatus().getState();
    }
    return stepState;
  }

  @Override
  public String getJobFlowLogUri() throws URISyntaxException {
    DescribeClusterRequest clusterRequest = new DescribeClusterRequest();
    clusterRequest.setClusterId( hadoopJobFlowId );

    DescribeClusterResult clusterResult = emrClient.describeCluster( clusterRequest );
    String clusterLogUri = clusterResult.getCluster().getLogUri();
    String clusterLogBucket = new URI( clusterLogUri ).getHost();
    return clusterLogBucket;
  }
}
