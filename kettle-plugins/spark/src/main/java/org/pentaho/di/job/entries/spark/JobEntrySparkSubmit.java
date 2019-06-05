/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.job.entries.spark;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;

import java.io.File;
import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.sun.jna.Platform;
import org.apache.commons.vfs2.FileObject;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.annotations.JobEntry;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.vfs.AliasedFileObject;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobEntryListener;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.dictionary.MetaverseAnalyzers;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import static org.pentaho.di.job.entry.validator.AndValidator.putValidators;
import static org.pentaho.di.job.entry.validator.JobEntryValidatorUtils.andValidator;
import static org.pentaho.di.job.entry.validator.JobEntryValidatorUtils.fileExistsValidator;
import static org.pentaho.di.job.entry.validator.JobEntryValidatorUtils.notBlankValidator;

/**
 * This job entry submits a JAR to Spark and executes a class. It uses the spark-submit script to submit a command like
 * this: spark-submit --class org.pentaho.spark.SparkExecTest --master yarn-cluster my-spark-job.jar arg1 arg2
 * <p>
 * More information on the options is here: http://spark.apache.org/docs/1.2.0/submitting-applications.html
 */

@JobEntry( image = "org/pentaho/di/ui/job/entries/spark/img/spark.svg",
  id = MetaverseAnalyzers.JobEntrySparkSubmitAnalyzer.ID,
  name = "JobEntrySparkSubmit.Title", description = "JobEntrySparkSubmit.Description",
  categoryDescription = "i18n:org.pentaho.di.job:JobCategory.Category.BigData",
  i18nPackageName = "org.pentaho.di.job.entries.spark",
  documentationUrl = "Products/Spark_Submit" )
public class JobEntrySparkSubmit extends JobEntryBase implements Cloneable, JobEntryInterface, JobEntryListener {
  public static final String JOB_TYPE_JAVA_SCALA = "Java or Scala";
  public static final String JOB_TYPE_PYTHON = "Python";
  public static final String HADOOP_CLUSTER_PREFIX = "hc://";

  private static Class<?> PKG = JobEntrySparkSubmit.class; // for i18n purposes, needed by Translator2!!

  private String jobType = JOB_TYPE_JAVA_SCALA;
  private String scriptPath; // the path for the spark-submit utility
  private String master = "yarn-cluster"; // the URL for the Spark master
  private Map<String, String> libs = new LinkedHashMap<>();
  // supporting documents options, "path->environment"
  private List<String> configParams = new ArrayList<String>(); // configuration options, "key=value"
  private String jar; // the path for the jar containing the Spark code to run
  private String pyFile; // path to python file for python jobs
  private String className; // the name of the class to run
  private String args; // arguments for the Spark code
  private boolean blockExecution = true; // wait for job to complete
  private String executorMemory; // memory allocation config param for the executor
  private String driverMemory; // memory allocation config param for the driver

  protected Process proc; // the process for the spark-submit command

  public JobEntrySparkSubmit( String n ) {
    super( n, "" );
  }

  public JobEntrySparkSubmit() {
    this( "" );
  }

  public Object clone() {
    JobEntrySparkSubmit je = (JobEntrySparkSubmit) super.clone();
    return je;
  }

  /**
   * Converts the state into XML and returns it
   *
   * @return The XML for the current state
   */
  public String getXML() {
    StringBuffer retval = new StringBuffer( 200 );

    retval.append( super.getXML() );
    retval.append( "      " ).append( XMLHandler.addTagValue( "scriptPath", scriptPath ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "jobType", jobType ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "master", master ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "jar", jar ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "pyFile", pyFile ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "className", className ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "args", args ) );
    retval.append( "      " ).append( XMLHandler.openTag( "configParams" ) ).append( Const.CR );
    for ( String param : configParams ) {
      retval.append( "            " ).append( XMLHandler.addTagValue( "param", param ) );
    }
    retval.append( "      " ).append( XMLHandler.closeTag( "configParams" ) ).append( Const.CR );
    retval.append( "      " ).append( XMLHandler.openTag( "libs" ) ).append( Const.CR );

    for ( String key : libs.keySet() ) {
      retval.append( "            " ).append( XMLHandler.addTagValue( "env", libs.get( key ) ) );
      retval.append( "            " ).append( XMLHandler.addTagValue( "path", key ) );
    }
    retval.append( "      " ).append( XMLHandler.closeTag( "libs" ) ).append( Const.CR );
    retval.append( "      " ).append( XMLHandler.addTagValue( "driverMemory", driverMemory ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "executorMemory", executorMemory ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "blockExecution", blockExecution ) );
    return retval.toString();
  }

  /**
   * Parses XML and recreates the state
   */
  public void loadXML( Node entrynode, List<DatabaseMeta> databases, List<SlaveServer> slaveServers, Repository rep,
                       IMetaStore metaStore ) throws KettleXMLException {
    try {
      super.loadXML( entrynode, databases, slaveServers );

      scriptPath = XMLHandler.getTagValue( entrynode, "scriptPath" );
      master = XMLHandler.getTagValue( entrynode, "master" );
      jobType = XMLHandler.getTagValue( entrynode, "jobType" );
      if ( jobType == null ) {
        this.jobType = JOB_TYPE_JAVA_SCALA;
      }
      jar = XMLHandler.getTagValue( entrynode, "jar" );
      pyFile = XMLHandler.getTagValue( entrynode, "pyFile" );
      className = XMLHandler.getTagValue( entrynode, "className" );
      args = XMLHandler.getTagValue( entrynode, "args" );
      Node configParamsNode = XMLHandler.getSubNode( entrynode, "configParams" );
      List<Node> paramNodes = XMLHandler.getNodes( configParamsNode, "param" );
      for ( Node paramNode : paramNodes ) {
        configParams.add( paramNode.getTextContent() );
      }
      Node libsNode = XMLHandler.getSubNode( entrynode, "libs" );
      if ( libsNode != null ) {
        List<Node> envNodes = XMLHandler.getNodes( libsNode, "env" );
        List<Node> pathNodes = XMLHandler.getNodes( libsNode, "path" );
        for ( int i = 0; i < envNodes.size(); i++ ) {
          libs.put( pathNodes.get( i ).getTextContent(), envNodes.get( i ).getTextContent() );
        }
      }
      driverMemory = XMLHandler.getTagValue( entrynode, "driverMemory" );
      executorMemory = XMLHandler.getTagValue( entrynode, "executorMemory" );
      blockExecution = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "blockExecution" ) );
    } catch ( KettleXMLException xe ) {
      throw new KettleXMLException( "Unable to load job entry of type 'SparkSubmit' from XML node", xe );
    }
  }

  /**
   * Reads the state from the repository
   */
  public void loadRep( Repository rep, IMetaStore metaStore, ObjectId id_jobentry, List<DatabaseMeta> databases,
                       List<SlaveServer> slaveServers ) throws KettleException {
    try {
      scriptPath = rep.getJobEntryAttributeString( id_jobentry, "scriptPath" );
      master = rep.getJobEntryAttributeString( id_jobentry, "master" );
      jobType = rep.getJobEntryAttributeString( id_jobentry, "jobType" );
      if ( jobType == null ) {
        this.jobType = JOB_TYPE_JAVA_SCALA;
      }
      jar = rep.getJobEntryAttributeString( id_jobentry, "jar" );
      pyFile = rep.getJobEntryAttributeString( id_jobentry, "pyFile" );
      className = rep.getJobEntryAttributeString( id_jobentry, "className" );
      args = rep.getJobEntryAttributeString( id_jobentry, "args" );
      for ( int i = 0; i < rep.countNrJobEntryAttributes( id_jobentry, "param" ); i++ ) {
        configParams.add( rep.getJobEntryAttributeString( id_jobentry, i, "param" ) );
      }
      for ( int i = 0; i < rep.countNrJobEntryAttributes( id_jobentry, "libsEnv" ); i++ ) {
        libs.put( rep.getJobEntryAttributeString( id_jobentry, i, "libsPath" ),
          rep.getJobEntryAttributeString( id_jobentry, i, "libsEnv" ) );
      }
      driverMemory = rep.getJobEntryAttributeString( id_jobentry, "driverMemory" );
      executorMemory = rep.getJobEntryAttributeString( id_jobentry, "executorMemory" );
      blockExecution = rep.getJobEntryAttributeBoolean( id_jobentry, "blockExecution" );
    } catch ( KettleException dbe ) {
      throw new KettleException( "Unable to load job entry of type 'SparkSubmit' from the repository for id_jobentry="
        + id_jobentry, dbe );
    }
  }

  /**
   * Saves the current state into the repository
   */
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_job ) throws KettleException {
    try {
      rep.saveJobEntryAttribute( id_job, getObjectId(), "scriptPath", scriptPath );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "master", master );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "jobType", jobType );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "jar", jar );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "pyFile", pyFile );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "className", className );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "args", args );
      for ( int i = 0; i < configParams.size(); i++ ) {
        rep.saveJobEntryAttribute( id_job, getObjectId(), i, "param", configParams.get( i ) );
      }

      int i = 0;
      for ( String key : libs.keySet() ) {
        rep.saveJobEntryAttribute( id_job, getObjectId(), i, "libsEnv", libs.get( key ) );
        rep.saveJobEntryAttribute( id_job, getObjectId(), i++, "libsPath", key );
      }
      rep.saveJobEntryAttribute( id_job, getObjectId(), "driverMemory", driverMemory );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "executorMemory", executorMemory );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "blockExecution", blockExecution );
    } catch ( KettleDatabaseException dbe ) {
      throw new KettleException( "Unable to save job entry of type 'SparkSubmit' to the repository for id_job="
        + id_job, dbe );
    }
  }

  /**
   * Returns the path for the spark-submit utility
   *
   * @return The script path
   */
  public String getScriptPath() {
    return scriptPath;
  }

  /**
   * Sets the path for the spark-submit utility
   *
   * @param scriptPath path to spark-submit utility
   */
  public void setScriptPath( String scriptPath ) {
    this.scriptPath = scriptPath;
  }

  /**
   * Returns the URL for the Spark master node
   *
   * @return The URL for the Spark master node
   */
  public String getMaster() {
    return master;
  }

  /**
   * Sets the URL for the Spark master node
   *
   * @param master URL for the Spark master node
   */
  public void setMaster( String master ) {
    this.master = master;
  }

  /**
   * Returns map of configuration params
   *
   * @return map of configuration params
   */
  public List<String> getConfigParams() {
    return configParams;
  }

  /**
   * Sets configuration params
   */
  public void setConfigParams( List<String> configParams ) {
    this.configParams = configParams;
  }

  /**
   * Returns list of library-env pairs.
   *
   * @return list of libs.
   */
  public Map<String, String> getLibs() {
    return libs;
  }

  /**
   * Sets path-env pairs for libraries
   */
  public void setLibs( Map<String, String> docs ) {
    this.libs = docs;
  }

  /**
   * Returns the path for the jar containing the Spark code to execute
   *
   * @return The path for the jar
   */
  public String getJar() {
    return jar;
  }

  /**
   * Sets the path for the jar containing the Spark code to execute
   *
   * @param jar path for the jar
   */
  public void setJar( String jar ) {
    this.jar = jar;
  }

  /**
   * Returns the name of the class containing the Spark code to execute
   *
   * @return The name of the class
   */
  public String getClassName() {
    return className;
  }

  /**
   * Sets the name of the class containing the Spark code to execute
   *
   * @param className name of the class
   */
  public void setClassName( String className ) {
    this.className = className;
  }

  /**
   * Returns the arguments for the Spark class. This is a space-separated list of strings, e.g. "http.log 1000"
   *
   * @return The arguments
   */
  public String getArgs() {
    return args;
  }

  /**
   * Sets the arguments for the Spark class. This is a space-separated list of strings, e.g. "http.log 1000"
   *
   * @param args arguments
   */
  public void setArgs( String args ) {
    this.args = args;
  }

  /**
   * Returns executor memory config param's value
   *
   * @return executor memory config param
   */
  public String getExecutorMemory() {
    return executorMemory;
  }

  /**
   * Sets executor memory config param's value
   *
   * @param executorMemory amount of memory executor process is allowed to consume
   */
  public void setExecutorMemory( String executorMemory ) {
    this.executorMemory = executorMemory;
  }

  /**
   * Returns driver memory config param's value
   *
   * @return driver memory config param
   */
  public String getDriverMemory() {
    return driverMemory;
  }

  /**
   * Sets driver memory config param's value
   *
   * @param driverMemory amount of memory driver process is allowed to consume
   */
  public void setDriverMemory( String driverMemory ) {
    this.driverMemory = driverMemory;
  }

  /**
   * Returns if the job entry will wait till job execution completes
   *
   * @return blocking mode
   */
  public boolean isBlockExecution() {
    return blockExecution;
  }

  /**
   * Sets if the job entry will wait for job execution to complete
   *
   * @param blockExecution blocking mode
   */
  public void setBlockExecution( boolean blockExecution ) {
    this.blockExecution = blockExecution;
  }

  /**
   * Returns type of job, valid types are {@link #JOB_TYPE_JAVA_SCALA} and {@link #JOB_TYPE_PYTHON}.
   *
   * @return spark job's type
   */
  public String getJobType() {
    return jobType;
  }

  /**
   * Sets spark job type to be executed, valid types are {@link #JOB_TYPE_JAVA_SCALA} and {@link #JOB_TYPE_PYTHON}..
   *
   * @param jobType to be set
   */
  public void setJobType( String jobType ) {
    this.jobType = jobType;
  }

  /**
   * Returns path to job's python file. Valid for jobs of {@link #JOB_TYPE_PYTHON} type.
   *
   * @return path to python script
   */
  public String getPyFile() {
    return pyFile;
  }

  /**
   * Sets path to python script to be executed. Valid for jobs of {@link #JOB_TYPE_PYTHON} type.
   *
   * @param pyFile path to set
   */
  public void setPyFile( String pyFile ) {
    this.pyFile = pyFile;
  }

  /**
   * Returns the spark-submit command as a list of strings. e.g. <path to spark-submit> --class <main-class> --master
   * <master-url> --deploy-mode <deploy-mode> --conf <key>=<value> <application-jar> \ [application-arguments]
   *
   * @return The spark-submit command
   */
  public List<String> getCmds() throws IOException {
    List<String> cmds = new ArrayList<String>();

    cmds.add( environmentSubstitute( scriptPath ) );
    cmds.add( "--master" );
    cmds.add( environmentSubstitute( master ) );

    for ( String confParam : configParams ) {
      cmds.add( "--conf" );
      cmds.add( environmentSubstitute( confParam ) );
    }

    if ( !Const.isEmpty( driverMemory ) ) {
      cmds.add( "--driver-memory" );
      cmds.add( environmentSubstitute( driverMemory ) );
    }

    if ( !Const.isEmpty( executorMemory ) ) {
      cmds.add( "--executor-memory" );
      cmds.add( environmentSubstitute( executorMemory ) );
    }

    switch ( jobType ) {
      case JOB_TYPE_JAVA_SCALA: {
        if ( !Const.isEmpty( className ) ) {
          cmds.add( "--class" );
          cmds.add( environmentSubstitute( className ) );
        }

        if ( !libs.isEmpty() ) {
          cmds.add( "--jars" );
          cmds.add( environmentSubstitute( Joiner.on( ',' ).join( libs.keySet() ) ) );
        }

        cmds.add( resolvePath( environmentSubstitute( jar ) ) );

        break;
      }
      case JOB_TYPE_PYTHON: {
        if ( !libs.isEmpty() ) {
          cmds.add( "--py-files" );
          cmds.add( environmentSubstitute( Joiner.on( ',' ).join( libs.keySet() ) ) );
        }

        cmds.add( environmentSubstitute( pyFile ) );

        break;
      }
    }

    if ( !Const.isEmpty( args ) ) {
      List<String> argArray = parseCommandLine( args );
      for ( String anArg : argArray ) {
        if ( !Const.isEmpty( anArg ) ) {
          if ( anArg.startsWith( HADOOP_CLUSTER_PREFIX ) ) {
            anArg = resolvePath( environmentSubstitute( anArg ) );
          }
          cmds.add( anArg );
        }
      }
    }

    return cmds;
  }

  @VisibleForTesting
  protected boolean validate() {
    boolean valid = true;
    if ( Const.isEmpty( scriptPath ) || !new File( environmentSubstitute( scriptPath ) ).exists() ) {
      logError( BaseMessages.getString( PKG, "JobEntrySparkSubmit.Error.SparkSubmitPathInvalid" ) );
      valid = false;
    }

    if ( Const.isEmpty( master ) ) {
      logError( BaseMessages.getString( PKG, "JobEntrySparkSubmit.Error.MasterURLEmpty" ) );
      valid = false;
    }

    if ( JOB_TYPE_JAVA_SCALA.equals( getJobType() ) ) {
      if ( Const.isEmpty( jar ) ) {
        logError( BaseMessages.getString( PKG, "JobEntrySparkSubmit.Error.JarPathEmpty" ) );
        valid = false;
      }
    } else {
      if ( Const.isEmpty( pyFile ) ) {
        logError( BaseMessages.getString( PKG, "JobEntrySparkSubmit.Error.PyFilePathEmpty" ) );
        valid = false;
      }
    }

    return valid;
  }


  /**
   * Executes the spark-submit command and returns a Result
   *
   * @return The Result of the operation
   */
  public Result execute( Result result, int nr ) {
    if ( !validate() ) {
      result.setResult( false );
      return result;
    }

    try {
      List<String> cmds = getCmds();

      logBasic( "Submitting Spark Script" );

      if ( log.isDetailed() ) {
        logDetailed( cmds.toString() );
      }

      // Build the environment variable list...
      ProcessBuilder procBuilder = new ProcessBuilder( cmds );
      Map<String, String> env = procBuilder.environment();
      String[] variables = listVariables();
      for ( String variable : variables ) {
        env.put( variable, getVariable( variable ) );
      }
      proc = procBuilder.start();

      String[] jobSubmittedPatterns = new String[] { "tracking URL:" };

      final AtomicBoolean jobSubmitted = new AtomicBoolean( false );

      // any error message?
      PatternMatchingStreamLogger errorLogger =
        new PatternMatchingStreamLogger( log, proc.getErrorStream(), jobSubmittedPatterns, jobSubmitted );

      // any output?
      PatternMatchingStreamLogger outputLogger =
        new PatternMatchingStreamLogger( log, proc.getInputStream(), jobSubmittedPatterns, jobSubmitted );

      if ( !blockExecution ) {
        PatternMatchingStreamLogger.PatternMatchedListener cb =
          new PatternMatchingStreamLogger.PatternMatchedListener() {
            @Override
            public void onPatternFound( String pattern ) {
              log.logDebug( "Found match in output, considering job submitted, stopping spark-submit" );
              jobSubmitted.set( true );
              proc.destroy();
            }
          };
        errorLogger.addPatternMatchedListener( cb );
        outputLogger.addPatternMatchedListener( cb );
      }

      // kick them off
      Thread errorLoggerThread = new Thread( errorLogger );
      errorLoggerThread.start();
      Thread outputLoggerThread = new Thread( outputLogger );
      outputLoggerThread.start();

      // Stop on job stop
      final AtomicBoolean processFinished = new AtomicBoolean( false );
      new Thread( new Runnable() {
        @Override
        public void run() {
          while ( !getParentJob().isStopped() && !processFinished.get() ) {
            try {
              Thread.sleep( 5000 );
            } catch ( InterruptedException e ) {
              e.printStackTrace();
            }
          }
          proc.destroy();
        }
      } ).start();

      proc.waitFor();

      processFinished.set( true );

      prepareProcessThreadsToStop( proc, errorLoggerThread, outputLoggerThread );

      if ( log.isDetailed() ) {
        logDetailed( "Spark submit finished" );
      }

      // What's the exit status?
      int exitCode;
      if ( blockExecution ) {
        exitCode = proc.exitValue();
      } else {
        exitCode = jobSubmitted.get() ? 0 : proc.exitValue();
      }

      result.setExitStatus( exitCode );
      if ( exitCode != 0 ) {
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "JobEntrySparkSubmit.ExitStatus", result.getExitStatus() ) );
        }

        result.setNrErrors( 1 );
      }

      result.setResult( exitCode == 0 );
    } catch ( Exception e ) {
      result.setNrErrors( 1 );
      logError( BaseMessages.getString( PKG, "JobEntrySparkSubmit.Error.SubmittingScript", e.getMessage() ) );
      logError( Const.getStackTracker( e ) );
      result.setResult( false );
    }

    return result;
  }

  private void waitForThreadsFinishToRead( Thread errorLoggerThread, Thread outputLoggerThread )
    throws InterruptedException {
    // wait until loggers read all data from stdout and stderr
    errorLoggerThread.join();
    outputLoggerThread.join();
  }

  private void prepareProcessThreadsToStop( Process proc, Thread errorLoggerThread, Thread outputLoggerThread )
    throws Exception {
    if ( blockExecution ) {
      waitForThreadsFinishToRead( errorLoggerThread, outputLoggerThread );
    } else {
      killChildProcesses();
    }
    // close the streams
    // otherwise you get "Too many open files, java.io.IOException" after a lot of iterations
    proc.getErrorStream().close();
    proc.getOutputStream().close();
  }

  @VisibleForTesting
  void killChildProcesses() {
    if ( Platform.isWindows() ) {
      try {
        WinProcess process = new WinProcess( WinProcess.getPID( proc ) );
        process.killChildProcesses();
      } catch ( IOException e ) {
        if ( log.isDetailed() ) {
          logDetailed(
            ( BaseMessages.getString( PKG, "JobEntrySparkSubmit.Error.KillWindowsChildProcess", e.getMessage() ) ) );
        }
      }
    }
  }

  public boolean evaluates() {
    return true;
  }

  /**
   * Checks that the minimum options have been provided.
   */
  @Override
  public void check( List<CheckResultInterface> remarks, JobMeta jobMeta, VariableSpace space, Repository repository,
                     IMetaStore metaStore ) {
    andValidator().validate( this, "scriptPath", remarks, putValidators( notBlankValidator() ) );
    andValidator().validate( this, "scriptPath", remarks, putValidators( fileExistsValidator() ) );
    andValidator().validate( this, "master", remarks, putValidators( notBlankValidator() ) );
    if ( JOB_TYPE_JAVA_SCALA.equals( getJobType() ) ) {
      andValidator().validate( this, "jar", remarks, putValidators( notBlankValidator() ) );
    } else {
      andValidator().validate( this, "pyFile", remarks, putValidators( notBlankValidator() ) );
    }
  }

  /**
   * Parse a string into arguments as if it were provided on the command line.
   *
   * @param commandLineString A command line string.
   * @return List of parsed arguments
   * @throws IOException when the command line could not be parsed
   */
  public List<String> parseCommandLine( String commandLineString ) throws IOException {
    List<String> args = new ArrayList<String>();
    StringReader reader = new StringReader( commandLineString );
    try {
      StreamTokenizer tokenizer = new StreamTokenizer( reader );
      // Treat a dash as an ordinary character so it gets included in the token
      tokenizer.ordinaryChar( '-' );
      tokenizer.ordinaryChar( '.' );
      tokenizer.ordinaryChars( '0', '9' );
      // Treat all characters as word characters so nothing is parsed out
      tokenizer.wordChars( '\u0000', '\uFFFF' );

      // Re-add whitespace characters
      tokenizer.whitespaceChars( 0, ' ' );

      // Use " and ' as quote characters
      tokenizer.quoteChar( '"' );
      tokenizer.quoteChar( '\'' );

      // Add all non-null string values tokenized from the string to the argument list
      while ( tokenizer.nextToken() != StreamTokenizer.TT_EOF ) {
        if ( tokenizer.sval != null ) {
          String s = tokenizer.sval;
          s = environmentSubstitute( s );
          args.add( s );
        }
      }
    } finally {
      reader.close();
    }

    return args;
  }

  @Override
  public void afterExecution( Job arg0, JobEntryCopy arg1, JobEntryInterface arg2, Result arg3 ) {
    proc.destroy();
  }

  @Override
  public void beforeExecution( Job arg0, JobEntryCopy arg1, JobEntryInterface arg2 ) {
  }

  private String resolvePath( String path ) {
    if ( path != null && !path.isEmpty() ) {
      try {
        FileObject fileObject = KettleVFS.getFileObject( path );
        if ( AliasedFileObject.isAliasedFile( fileObject ) ) {
          return  ( (AliasedFileObject) fileObject ).getOriginalURIString();
        }
      } catch ( KettleFileException e ) {
        throw new RuntimeException( e );
      }
    }
    return path;
  }
}
