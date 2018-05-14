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

package org.pentaho.big.data.kettle.plugins.mapreduce.entry.hadoop;

import com.google.common.annotations.VisibleForTesting;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.big.data.kettle.plugins.mapreduce.DialogClassUtil;
import org.pentaho.big.data.kettle.plugins.mapreduce.entry.NamedClusterLoadSaveUtil;
import org.pentaho.big.data.kettle.plugins.mapreduce.entry.UserDefinedItem;
import org.pentaho.big.data.kettle.plugins.mapreduce.entry.pmr.JobEntryHadoopTransJobExecutor;
import org.pentaho.hadoop.shim.api.mapreduce.MapReduceJobAdvanced;
import org.pentaho.hadoop.shim.api.mapreduce.MapReduceJobBuilder;
import org.pentaho.hadoop.shim.api.mapreduce.MapReduceJobSimple;
import org.pentaho.hadoop.shim.api.mapreduce.MapReduceService;
import org.pentaho.hadoop.shim.api.mapreduce.TaskCompletionEvent;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.ResultFile;
import org.pentaho.di.core.annotations.JobEntry;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.logging.Log4jFileAppender;
import org.pentaho.di.core.logging.LogWriter;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;
import org.w3c.dom.Node;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@JobEntry( id = "HadoopJobExecutorPlugin", image = "HDE.svg", name = "HadoopJobExecutorPlugin.Name",
  description = "HadoopJobExecutorPlugin.Description",
  categoryDescription = "i18n:org.pentaho.di.job:JobCategory.Category.BigData",
  i18nPackageName = "org.pentaho.big.data.kettle.plugins.mapreduce",
  documentationUrl = "http://wiki.pentaho.com/display/EAI/Hadoop+Job+Executor" )
public class JobEntryHadoopJobExecutor extends JobEntryBase implements Cloneable, JobEntryInterface {
  private static final String DEFAULT_LOGGING_INTERVAL = "60";
  public static final String CLUSTER_NAME = "cluster_name";
  public static final String HDFS_HOSTNAME = "hdfs_hostname";
  public static final String HDFS_PORT = "hdfs_port";
  public static final String JOB_TRACKER_HOSTNAME = "job_tracker_hostname";
  public static final String JOB_TRACKER_PORT = "job_tracker_port";
  private static Class<?> PKG = JobEntryHadoopJobExecutor.class; // for i18n purposes, needed by Translator2!!
  public static final String DIALOG_NAME = DialogClassUtil.getDialogClassName( PKG );
  // $NON-NLS-1$
  private final NamedClusterService namedClusterService;
  private final RuntimeTestActionService runtimeTestActionService;
  private final RuntimeTester runtimeTester;
  private final NamedClusterServiceLocator namedClusterServiceLocator;
  private final NamedClusterLoadSaveUtil namedClusterLoadSaveUtil = new NamedClusterLoadSaveUtil();
  private String hadoopJobName;
  private String jarUrl = "";
  private String driverClass = "";
  private boolean isSimple = true;
  private String cmdLineArgs;
  private String outputKeyClass;
  private String outputValueClass;
  private String mapperClass;
  private String combinerClass;
  private String reducerClass;
  private String inputFormatClass;
  private String outputFormatClass;
  private NamedCluster namedCluster;
  private String inputPath;
  private String outputPath;
  private boolean blocking;
  private String loggingInterval = DEFAULT_LOGGING_INTERVAL; // 60 seconds default
  private boolean simpleBlocking;
  private String simpleLoggingInterval = loggingInterval;
  private String numMapTasks = "1";
  private String numReduceTasks = "1";
  private List<UserDefinedItem> userDefined = new ArrayList<UserDefinedItem>();

  public JobEntryHadoopJobExecutor( NamedClusterService namedClusterService,
                                    RuntimeTestActionService runtimeTestActionService, RuntimeTester runtimeTester,
                                    NamedClusterServiceLocator namedClusterServiceLocator ) {
    this.namedClusterService = namedClusterService;
    this.runtimeTestActionService = runtimeTestActionService;
    this.runtimeTester = runtimeTester;
    this.namedClusterServiceLocator = namedClusterServiceLocator;
  }

  public NamedClusterService getNamedClusterService() {
    return namedClusterService;
  }

  public RuntimeTestActionService getRuntimeTestActionService() {
    return runtimeTestActionService;
  }

  public RuntimeTester getRuntimeTester() {
    return runtimeTester;
  }

  public NamedClusterServiceLocator getNamedClusterServiceLocator() {
    return namedClusterServiceLocator;
  }

  public String getHadoopJobName() {
    return hadoopJobName;
  }

  public void setHadoopJobName( String hadoopJobName ) {
    this.hadoopJobName = hadoopJobName;
  }

  public String getJarUrl() {
    return jarUrl;
  }

  public void setJarUrl( String jarUrl ) {
    this.jarUrl = jarUrl;
  }

  public String getDriverClass() {
    return driverClass;
  }

  public void setDriverClass( String driverClass ) {
    this.driverClass = driverClass;
  }

  public boolean isSimple() {
    return isSimple;
  }

  public void setSimple( boolean isSimple ) {
    this.isSimple = isSimple;
  }

  public String getCmdLineArgs() {
    return cmdLineArgs;
  }

  public void setCmdLineArgs( String cmdLineArgs ) {
    this.cmdLineArgs = cmdLineArgs;
  }

  public String getOutputKeyClass() {
    return outputKeyClass;
  }

  public void setOutputKeyClass( String outputKeyClass ) {
    this.outputKeyClass = outputKeyClass;
  }

  public String getOutputValueClass() {
    return outputValueClass;
  }

  public void setOutputValueClass( String outputValueClass ) {
    this.outputValueClass = outputValueClass;
  }

  public String getMapperClass() {
    return mapperClass;
  }

  public void setMapperClass( String mapperClass ) {
    this.mapperClass = mapperClass;
  }

  public String getCombinerClass() {
    return combinerClass;
  }

  public void setCombinerClass( String combinerClass ) {
    this.combinerClass = combinerClass;
  }

  public String getReducerClass() {
    return reducerClass;
  }

  public void setReducerClass( String reducerClass ) {
    this.reducerClass = reducerClass;
  }

  public String getInputFormatClass() {
    return inputFormatClass;
  }

  public void setInputFormatClass( String inputFormatClass ) {
    this.inputFormatClass = inputFormatClass;
  }

  public String getOutputFormatClass() {
    return outputFormatClass;
  }

  public void setOutputFormatClass( String outputFormatClass ) {
    this.outputFormatClass = outputFormatClass;
  }

  public NamedCluster getNamedCluster() {
    return namedCluster;
  }

  public void setNamedCluster( NamedCluster namedCluster ) {
    this.namedCluster = namedCluster;
  }

  public String getInputPath() {
    return inputPath;
  }

  public void setInputPath( String inputPath ) {
    this.inputPath = inputPath;
  }

  public String getOutputPath() {
    return outputPath;
  }

  public void setOutputPath( String outputPath ) {
    this.outputPath = outputPath;
  }

  public boolean isBlocking() {
    return blocking;
  }

  public void setBlocking( boolean blocking ) {
    this.blocking = blocking;
  }

  public String getLoggingInterval() {
    return loggingInterval == null ? DEFAULT_LOGGING_INTERVAL : loggingInterval;
  }

  public void setLoggingInterval( String loggingInterval ) {
    this.loggingInterval = loggingInterval;
  }

  public List<UserDefinedItem> getUserDefined() {
    return userDefined;
  }

  public void setUserDefined( List<UserDefinedItem> userDefined ) {
    this.userDefined = userDefined;
  }

  public String getNumMapTasks() {
    return numMapTasks;
  }

  public void setNumMapTasks( String numMapTasks ) {
    this.numMapTasks = numMapTasks;
  }

  public String getNumReduceTasks() {
    return numReduceTasks;
  }

  public void setNumReduceTasks( String numReduceTasks ) {
    this.numReduceTasks = numReduceTasks;
  }

  public Result execute( final Result result, int arg1 ) throws KettleException {
    result.setNrErrors( 0 );

    Log4jFileAppender appender = null;
    String logFileName = "pdi-" + this.getName(); //$NON-NLS-1$

    try {
      appender = LogWriter.createFileAppender( logFileName, true, false );
      LogWriter.getInstance().addAppender( appender );
      log.setLogLevel( parentJob.getLogLevel() );
    } catch ( Exception e ) {
      logError( BaseMessages
        .getString( PKG, "JobEntryHadoopJobExecutor.FailedToOpenLogFile", logFileName, e.toString() ) ); //$NON-NLS-1$
      logError( Const.getStackTracker( e ) );
    }

    try {
      URL resolvedJarUrl = resolveJarUrl( jarUrl );
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "JobEntryHadoopJobExecutor.ResolvedJar", resolvedJarUrl
          .toExternalForm() ) );
      }

      MapReduceService mapReduceService = namedClusterServiceLocator.getService( namedCluster, MapReduceService.class );
      if ( isSimple ) {
        String simpleLoggingIntervalS = environmentSubstitute( getSimpleLoggingInterval() );
        int simpleLogInt = 60;
        try {
          simpleLogInt = Integer.parseInt( simpleLoggingIntervalS, 10 );
        } catch ( NumberFormatException e ) {
          logError( BaseMessages.getString( PKG, "ErrorParsingLogInterval", simpleLoggingIntervalS, simpleLogInt ) );
        }

        MapReduceJobSimple mapReduceJobSimple =
          mapReduceService.executeSimple( resolvedJarUrl, environmentSubstitute( driverClass ),
            environmentSubstitute( cmdLineArgs ) );

        String mainClass = mapReduceJobSimple.getMainClass();
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString(
            PKG, "JobEntryHadoopJobExecutor.UsingDriverClass", mainClass == null ? "null" : mainClass ) );
          logDetailed( BaseMessages.getString( PKG, "JobEntryHadoopJobExecutor.SimpleMode" ) );
        }
        if ( simpleBlocking ) {
          boolean done = false;
          do {
            done =
              mapReduceJobSimple.waitOnCompletion( simpleLogInt, TimeUnit.SECONDS, new MapReduceService.Stoppable() {
                @Override public boolean isStopped() {
                  return parentJob.isStopped();
                }
              } );
            logDetailed( BaseMessages
              .getString( JobEntryHadoopJobExecutor.class, "JobEntryHadoopJobExecutor.Blocking", mainClass ) );
          } while ( !parentJob.isStopped() && !done );
          if ( !done ) {
            mapReduceJobSimple.killJob();
          }
          if ( !mapReduceJobSimple.isSuccessful() ) {
            result.setStopped( true );
            result.setNrErrors( 1 );
            result.setResult( false );
            log.logError(
              BaseMessages.getString( PKG, "JobEntryHadoopJobExecutor.FailedToExecuteClass", mainClass,
                mapReduceJobSimple.getStatus() ) );
          }
        }
      } else {
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "JobEntryHadoopJobExecutor.AdvancedMode" ) );
        }
        MapReduceJobBuilder jobBuilder = mapReduceService.createJobBuilder( log, variables );

        jobBuilder.setResolvedJarUrl( resolvedJarUrl );
        jobBuilder.setJarUrl( environmentSubstitute( jarUrl ) );
        jobBuilder.setHadoopJobName( environmentSubstitute( hadoopJobName ) );

        jobBuilder.setOutputKeyClass( environmentSubstitute( outputKeyClass ) );
        jobBuilder.setOutputValueClass( environmentSubstitute( outputValueClass ) );

        if ( mapperClass != null ) {
          jobBuilder.setMapperClass( environmentSubstitute( mapperClass ) );
        }
        if ( combinerClass != null ) {
          jobBuilder.setCombinerClass( environmentSubstitute( combinerClass ) );
        }
        if ( reducerClass != null ) {
          jobBuilder.setReducerClass( environmentSubstitute( reducerClass ) );
        }

        if ( inputFormatClass != null ) {
          jobBuilder.setInputFormatClass( environmentSubstitute( inputFormatClass ) );
        }
        if ( outputFormatClass != null ) {
          jobBuilder.setOutputFormatClass( environmentSubstitute( outputFormatClass ) );
        }

        jobBuilder.setInputPaths( JobEntryHadoopTransJobExecutor.splitInputPaths( inputPath, variables ) );
        jobBuilder.setOutputPath( environmentSubstitute( outputPath ) );

        // process user defined values
        for ( UserDefinedItem item : userDefined ) {
          if ( item.getName() != null && !"".equals( item.getName() ) && item.getValue() != null
            && !"".equals( item.getValue() ) ) {
            String nameS = environmentSubstitute( item.getName() );
            String valueS = environmentSubstitute( item.getValue() );
            jobBuilder.set( nameS, valueS );
          }
        }

        String numMapTasksS = environmentSubstitute( numMapTasks );
        String numReduceTasksS = environmentSubstitute( numReduceTasks );
        int numM = 1;
        try {
          numM = Integer.parseInt( numMapTasksS );
        } catch ( NumberFormatException e ) {
          logError( "Can't parse number of map tasks '" + numMapTasksS + "'. Setting num" + "map tasks to 1" );
        }
        int numR = 1;
        try {
          numR = Integer.parseInt( numReduceTasksS );
        } catch ( NumberFormatException e ) {
          logError( "Can't parse number of reduce tasks '" + numReduceTasksS + "'. Setting num" + "reduce tasks to 1" );
        }

        jobBuilder.setNumMapTasks( numM );
        jobBuilder.setNumReduceTasks( numR );

        MapReduceJobAdvanced mapReduceJobAdvanced = jobBuilder.submit();

        String loggingIntervalS = environmentSubstitute( getLoggingInterval() );
        int logIntv = 60;
        try {
          logIntv = Integer.parseInt( loggingIntervalS );
        } catch ( NumberFormatException e ) {
          logError( BaseMessages.getString( PKG, "ErrorParsingLogInterval", loggingIntervalS, logIntv ) );
        }
        if ( blocking ) {
          try {
            int taskCompletionEventIndex = 0;
            while ( !mapReduceJobAdvanced
              .waitOnCompletion( logIntv >= 1 ? logIntv : 60, TimeUnit.SECONDS, new MapReduceService.Stoppable() {
                @Override public boolean isStopped() {
                  return parentJob.isStopped();
                }
              } ) ) {
              if ( logIntv >= 1 ) {
                printJobStatus( mapReduceJobAdvanced );
                taskCompletionEventIndex = logTaskMessages( mapReduceJobAdvanced, taskCompletionEventIndex );
              }
            }

            if ( parentJob.isStopped() && !mapReduceJobAdvanced.isComplete() ) {
              // We must stop the job running on Hadoop
              mapReduceJobAdvanced.killJob();
              // Indicate this job entry did not complete
              result.setResult( false );
            }

            printJobStatus( mapReduceJobAdvanced );
            // Log any messages we may have missed while polling
            logTaskMessages( mapReduceJobAdvanced, taskCompletionEventIndex );
          } catch ( InterruptedException ie ) {
            logError( ie.getMessage(), ie );
          }

          // Entry is successful if the MR job is successful overall
          result.setResult( mapReduceJobAdvanced.isSuccessful() );
        }

      }
    } catch ( Throwable t ) {
      t.printStackTrace();
      result.setStopped( true );
      result.setNrErrors( 1 );
      result.setResult( false );
      logError( t.getMessage(), t );
    }

    if ( appender != null ) {
      LogWriter.getInstance().removeAppender( appender );
      appender.close();

      ResultFile resultFile =
        new ResultFile( ResultFile.FILE_TYPE_LOG, appender.getFile(), parentJob.getJobname(), getName() );
      result.getResultFiles().put( resultFile.getFile().toString(), resultFile );
    }

    return result;
  }

  @VisibleForTesting
  URL resolveJarUrl( final String jarUrl ) throws MalformedURLException {
    return resolveJarUrl( jarUrl, this );
  }

  public static URL resolveJarUrl( final String jarUrl, VariableSpace variableSpace ) throws MalformedURLException {
    String jarUrlS = variableSpace.environmentSubstitute( jarUrl );
    if ( jarUrlS.indexOf( "://" ) == -1 ) {
      // default to file://
      File jarFile = new File( jarUrlS );
      return jarFile.toURI().toURL();
    } else {
      return new URL( jarUrlS );
    }
  }

  /**
   * Log messages indicating completion (success/failure) of component tasks for the provided running job.
   *
   * @param runningJob Running job to poll for completion events
   * @param startIndex Start at this event index to poll from
   * @return Total events consumed
   * @throws IOException Error fetching events
   */
  private int logTaskMessages( MapReduceJobAdvanced runningJob, int startIndex ) throws IOException {
    TaskCompletionEvent[] tcEvents = runningJob.getTaskCompletionEvents( startIndex );
    for ( int i = 0; i < tcEvents.length; i++ ) {
      String[] diags = runningJob.getTaskDiagnostics( tcEvents[ i ].getTaskAttemptId() );
      StringBuilder diagsOutput = new StringBuilder();

      if ( diags != null && diags.length > 0 ) {
        diagsOutput.append( Const.CR );
        for ( String s : diags ) {
          diagsOutput.append( s );
          diagsOutput.append( Const.CR );
        }
      }

      switch ( tcEvents[ i ].getTaskStatus() ) {
        case KILLED:
          logError( BaseMessages
            .getString(
              PKG,
              "JobEntryHadoopJobExecutor.TaskDetails", TaskCompletionEvent.Status.KILLED,
              tcEvents[ i ].getTaskAttemptId(), tcEvents[ i ].getTaskAttemptId(), tcEvents[ i ].getEventId(),
              diagsOutput ) ); //$NON-NLS-1$

          break;
        case FAILED:
          logError( BaseMessages
            .getString(
              PKG,
              "JobEntryHadoopJobExecutor.TaskDetails", TaskCompletionEvent.Status.FAILED,
              tcEvents[ i ].getTaskAttemptId(), tcEvents[ i ].getTaskAttemptId(), tcEvents[ i ].getEventId(),
              diagsOutput ) ); //$NON-NLS-1$

          break;
        case SUCCEEDED:
          logDetailed( BaseMessages
            .getString(
              PKG,
              "JobEntryHadoopJobExecutor.TaskDetails", TaskCompletionEvent.Status.SUCCEEDED,
              tcEvents[ i ].getTaskAttemptId(), tcEvents[ i ].getTaskAttemptId(), tcEvents[ i ].getEventId(),
              diagsOutput ) ); //$NON-NLS-1$

          break;
      }
    }
    return tcEvents.length;
  }

  /**
   * Execute the main method of the provided class with the current command line arguments.
   *
   * @param clazz Class with main method to execute
   * @throws NoSuchMethodException
   * @throws IllegalAccessException
   * @throws InvocationTargetException
   */
  protected void executeMainMethod( Class<?> clazz ) throws NoSuchMethodException, IllegalAccessException,
    InvocationTargetException {
    final ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader( clazz.getClassLoader() );
      Method mainMethod = clazz.getMethod( "main", new Class[] { String[].class } );
      String commandLineArgs = environmentSubstitute( cmdLineArgs );
      Object[] args = ( commandLineArgs != null ) ? new Object[] { commandLineArgs.split( " " ) } : new Object[ 0 ];
      mainMethod.invoke( null, args );
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }

  public void printJobStatus( MapReduceJobAdvanced runningJob ) throws IOException {
    if ( log.isBasic() ) {
      double setupPercent = runningJob.getSetupProgress() * 100f;
      double mapPercent = runningJob.getMapProgress() * 100f;
      double reducePercent = runningJob.getReduceProgress() * 100f;
      logBasic( BaseMessages.getString( PKG, "JobEntryHadoopJobExecutor.RunningPercent", setupPercent, mapPercent,
        reducePercent ) );
    }
  }

  @Override
  public void loadXML( Node entrynode, List<DatabaseMeta> databases, List<SlaveServer> slaveServers, Repository rep,
                       IMetaStore metaStore )
    throws KettleXMLException {
    super.loadXML( entrynode, databases, slaveServers );
    hadoopJobName = XMLHandler.getTagValue( entrynode, "hadoop_job_name" );

    isSimple = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "simple" ) );
    jarUrl = XMLHandler.getTagValue( entrynode, "jar_url" );
    driverClass = XMLHandler.getTagValue( entrynode, "driver_class" );
    cmdLineArgs = XMLHandler.getTagValue( entrynode, "command_line_args" );
    simpleBlocking = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "simple_blocking" ) );
    blocking = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "blocking" ) );
    simpleLoggingInterval = XMLHandler.getTagValue( entrynode, "simple_logging_interval" );
    loggingInterval = XMLHandler.getTagValue( entrynode, "logging_interval" );

    mapperClass = XMLHandler.getTagValue( entrynode, "mapper_class" );
    combinerClass = XMLHandler.getTagValue( entrynode, "combiner_class" );
    reducerClass = XMLHandler.getTagValue( entrynode, "reducer_class" );
    inputPath = XMLHandler.getTagValue( entrynode, "input_path" );
    inputFormatClass = XMLHandler.getTagValue( entrynode, "input_format_class" );
    outputPath = XMLHandler.getTagValue( entrynode, "output_path" );
    outputKeyClass = XMLHandler.getTagValue( entrynode, "output_key_class" );
    outputValueClass = XMLHandler.getTagValue( entrynode, "output_value_class" );
    outputFormatClass = XMLHandler.getTagValue( entrynode, "output_format_class" );

    namedCluster = namedClusterLoadSaveUtil.loadClusterConfig( namedClusterService, null, rep, metaStore, entrynode, log );

    setRepository( rep );

    // numMapTasks = Integer.parseInt(XMLHandler.getTagValue(entrynode, "num_map_tasks"));
    numMapTasks = XMLHandler.getTagValue( entrynode, "num_map_tasks" );
    // numReduceTasks = Integer.parseInt(XMLHandler.getTagValue(entrynode, "num_reduce_tasks"));
    numReduceTasks = XMLHandler.getTagValue( entrynode, "num_reduce_tasks" );

    // How many user defined elements?
    userDefined = new ArrayList<UserDefinedItem>();
    Node userDefinedList = XMLHandler.getSubNode( entrynode, "user_defined_list" );
    int nrUserDefined = XMLHandler.countNodes( userDefinedList, "user_defined" );
    for ( int i = 0; i < nrUserDefined; i++ ) {
      Node userDefinedNode = XMLHandler.getSubNodeByNr( userDefinedList, "user_defined", i );
      String name = XMLHandler.getTagValue( userDefinedNode, "name" );
      String value = XMLHandler.getTagValue( userDefinedNode, "value" );
      UserDefinedItem item = new UserDefinedItem();
      item.setName( name );
      item.setValue( value );
      userDefined.add( item );
    }
  }

  @Override public String getXML() {
    StringBuilder retval = new StringBuilder( 1024 );
    retval.append( super.getXML() );
    retval.append( "      " ).append( XMLHandler.addTagValue( "hadoop_job_name", hadoopJobName ) );

    retval.append( "      " ).append( XMLHandler.addTagValue( "simple", isSimple ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "jar_url", jarUrl ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "driver_class", driverClass ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "command_line_args", cmdLineArgs ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "simple_blocking", simpleBlocking ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "blocking", blocking ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "logging_interval", loggingInterval ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "simple_logging_interval", simpleLoggingInterval ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "hadoop_job_name", hadoopJobName ) );

    retval.append( "      " ).append( XMLHandler.addTagValue( "mapper_class", mapperClass ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "combiner_class", combinerClass ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "reducer_class", reducerClass ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "input_path", inputPath ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "input_format_class", inputFormatClass ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "output_path", outputPath ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "output_key_class", outputKeyClass ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "output_value_class", outputValueClass ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "output_format_class", outputFormatClass ) );

    namedClusterLoadSaveUtil.getXmlNamedCluster( namedCluster, namedClusterService, metaStore, log, retval );

    retval.append( "      " ).append( XMLHandler.addTagValue( "num_map_tasks", numMapTasks ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "num_reduce_tasks", numReduceTasks ) );

    retval.append( "      <user_defined_list>" ).append( Const.CR );
    if ( userDefined != null ) {
      for ( UserDefinedItem item : userDefined ) {
        if ( item.getName() != null && !"".equals( item.getName() ) && item.getValue() != null
          && !"".equals( item.getValue() ) ) {
          retval.append( "        <user_defined>" ).append( Const.CR );
          retval.append( "          " ).append( XMLHandler.addTagValue( "name", item.getName() ) );
          retval.append( "          " ).append( XMLHandler.addTagValue( "value", item.getValue() ) );
          retval.append( "        </user_defined>" ).append( Const.CR );
        }
      }
    }
    retval.append( "      </user_defined_list>" ).append( Const.CR );
    return retval.toString();
  }

  @Override
  public void loadRep( Repository rep, IMetaStore metaStore, ObjectId id_jobentry, List<DatabaseMeta> databases,
                       List<SlaveServer> slaveServers ) throws KettleException {
    if ( rep != null ) {
      super.loadRep( rep, metaStore, id_jobentry, databases, slaveServers );

      setHadoopJobName( rep.getJobEntryAttributeString( id_jobentry, "hadoop_job_name" ) );

      setSimple( rep.getJobEntryAttributeBoolean( id_jobentry, "simple" ) );

      setJarUrl( rep.getJobEntryAttributeString( id_jobentry, "jar_url" ) );
      setDriverClass( rep.getJobEntryAttributeString( id_jobentry, "driver_class" ) );
      setCmdLineArgs( rep.getJobEntryAttributeString( id_jobentry, "command_line_args" ) );
      setSimpleBlocking( rep.getJobEntryAttributeBoolean( id_jobentry, "simple_blocking" ) );
      setBlocking( rep.getJobEntryAttributeBoolean( id_jobentry, "blocking" ) );
      setSimpleLoggingInterval( rep.getJobEntryAttributeString( id_jobentry, "simple_logging_interval" ) );
      setLoggingInterval( rep.getJobEntryAttributeString( id_jobentry, "logging_interval" ) );

      setMapperClass( rep.getJobEntryAttributeString( id_jobentry, "mapper_class" ) );
      setCombinerClass( rep.getJobEntryAttributeString( id_jobentry, "combiner_class" ) );
      setReducerClass( rep.getJobEntryAttributeString( id_jobentry, "reducer_class" ) );
      setInputPath( rep.getJobEntryAttributeString( id_jobentry, "input_path" ) );
      setInputFormatClass( rep.getJobEntryAttributeString( id_jobentry, "input_format_class" ) );
      setOutputPath( rep.getJobEntryAttributeString( id_jobentry, "output_path" ) );
      setOutputKeyClass( rep.getJobEntryAttributeString( id_jobentry, "output_key_class" ) );
      setOutputValueClass( rep.getJobEntryAttributeString( id_jobentry, "output_value_class" ) );
      setOutputFormatClass( rep.getJobEntryAttributeString( id_jobentry, "output_format_class" ) );

      namedCluster = namedClusterLoadSaveUtil.loadClusterConfig( namedClusterService, id_jobentry, rep, metaStore, null, log );

      setRepository( rep );

      // setNumMapTasks(new Long(rep.getJobEntryAttributeInteger(id_jobentry, "num_map_tasks")).intValue());
      setNumMapTasks( rep.getJobEntryAttributeString( id_jobentry, "num_map_tasks" ) );
      // setNumReduceTasks(new Long(rep.getJobEntryAttributeInteger(id_jobentry, "num_reduce_tasks")).intValue());
      setNumReduceTasks( rep.getJobEntryAttributeString( id_jobentry, "num_reduce_tasks" ) );

      int argnr = rep.countNrJobEntryAttributes( id_jobentry, "user_defined_name" ); //$NON-NLS-1$
      if ( argnr > 0 ) {
        userDefined = new ArrayList<UserDefinedItem>();

        UserDefinedItem item = null;
        for ( int i = 0; i < argnr; i++ ) {
          item = new UserDefinedItem();
          item.setName( rep.getJobEntryAttributeString( id_jobentry, i, "user_defined_name" ) ); //$NON-NLS-1$
          item.setValue( rep.getJobEntryAttributeString( id_jobentry, i, "user_defined_value" ) ); //$NON-NLS-1$
          userDefined.add( item );
        }
      }
    } else {
      throw new KettleException( "Unable to save to a repository. The repository is null." ); //$NON-NLS-1$
    }
  }

  @Override public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_job ) throws KettleException {
    if ( rep != null ) {
      super.saveRep( rep, id_job );

      rep.saveJobEntryAttribute( id_job, getObjectId(), "hadoop_job_name", hadoopJobName ); //$NON-NLS-1$

      rep.saveJobEntryAttribute( id_job, getObjectId(), "simple", isSimple ); //$NON-NLS-1$

      rep.saveJobEntryAttribute( id_job, getObjectId(), "jar_url", jarUrl ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "driver_class", driverClass ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "command_line_args", cmdLineArgs ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "simple_blocking", simpleBlocking ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "blocking", blocking ); //$NON-NLS-1$
      rep
        .saveJobEntryAttribute( id_job, getObjectId(), "simple_logging_interval", simpleLoggingInterval ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "logging_interval", loggingInterval ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "hadoop_job_name", hadoopJobName ); //$NON-NLS-1$

      rep.saveJobEntryAttribute( id_job, getObjectId(), "mapper_class", mapperClass ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "combiner_class", combinerClass ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "reducer_class", reducerClass ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "input_path", inputPath ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "input_format_class", inputFormatClass ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "output_path", outputPath ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "output_key_class", outputKeyClass ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "output_value_class", outputValueClass ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "output_format_class", outputFormatClass ); //$NON-NLS-1$

      namedClusterLoadSaveUtil
        .saveNamedClusterRep( namedCluster, namedClusterService, rep, metaStore, id_job, getObjectId(), log );

      rep.saveJobEntryAttribute( id_job, getObjectId(), "num_map_tasks", numMapTasks ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "num_reduce_tasks", numReduceTasks ); //$NON-NLS-1$

      if ( userDefined != null ) {
        for ( int i = 0; i < userDefined.size(); i++ ) {
          UserDefinedItem item = userDefined.get( i );
          if ( item.getName() != null
            && !"".equals( item.getName() ) && item.getValue() != null && !""
            .equals( item.getValue() ) ) { //$NON-NLS-1$ //$NON-NLS-2$
            rep.saveJobEntryAttribute( id_job, getObjectId(), i, "user_defined_name", item.getName() ); //$NON-NLS-1$
            rep.saveJobEntryAttribute( id_job, getObjectId(), i, "user_defined_value", item.getValue() ); //$NON-NLS-1$
          }
        }
      }

    } else {
      throw new KettleException( "Unable to save to a repository. The repository is null." ); //$NON-NLS-1$
    }
  }

  public boolean evaluates() {
    return true;
  }

  public boolean isUnconditional() {
    return true;
  }

  public String getSimpleLoggingInterval() {
    return simpleLoggingInterval == null ? DEFAULT_LOGGING_INTERVAL : simpleLoggingInterval;
  }

  public void setSimpleLoggingInterval( String simpleLoggingInterval ) {
    this.simpleLoggingInterval = simpleLoggingInterval;
  }

  public boolean isSimpleBlocking() {
    return simpleBlocking;
  }

  public void setSimpleBlocking( boolean simpleBlocking ) {
    this.simpleBlocking = simpleBlocking;
  }

  @Override public String getDialogClassName() {
    return  DIALOG_NAME;
  }
}
