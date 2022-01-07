/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2020 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.amazon;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.pentaho.amazon.client.ClientFactoriesManager;
import org.pentaho.amazon.client.ClientType;
import org.pentaho.amazon.client.api.EmrClient;
import org.pentaho.amazon.client.api.S3Client;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.ResultFile;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.logging.log4j.Log4jKettleLayout;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.platform.api.util.LogUtil;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Aliaksandr_Zhuk on 1/31/2018.
 */
public abstract class AbstractAmazonJobExecutor extends AbstractAmazonJobEntry {

  private static Class<?> PKG = AbstractAmazonJobExecutor.class;

  private Appender appender = null;
  private S3Client s3Client;
  protected EmrClient emrClient;
  protected String key;
  protected int numInsts = 2;
  FileObject file;
  /**
   * Maps Kettle LogLevels to Log4j Levels
   */
  public static final Map<LogLevel, Level> LOG_LEVEL_MAP;

  static {
    EnumMap<LogLevel, Level> map = new EnumMap<>( LogLevel.class );
    map.put( LogLevel.BASIC, Level.INFO );
    map.put( LogLevel.MINIMAL, Level.INFO );
    map.put( LogLevel.DEBUG, Level.DEBUG );
    map.put( LogLevel.ERROR, Level.ERROR );
    map.put( LogLevel.DETAILED, Level.INFO );
    map.put( LogLevel.ROWLEVEL, Level.DEBUG );
    map.put( LogLevel.NOTHING, Level.OFF );
    LOG_LEVEL_MAP = Collections.unmodifiableMap( map );
  }

  private Level getLog4jLevel( LogLevel level ) {
    Level log4jLevel = LOG_LEVEL_MAP.get( level );
    return log4jLevel != null ? log4jLevel : Level.INFO;
  }

  public void setupLogFile() {
    String logFileName = "pdi-" + this.getName();
    try {
      file = KettleVFS.createTempFile( logFileName, ".log", System.getProperty( "java.io.tmpdir" ) );
      appender =  LogUtil.makeAppender( logFileName,
              new OutputStreamWriter( KettleVFS.getOutputStream( file, true ),
                StandardCharsets.UTF_8 ), new Log4jKettleLayout( StandardCharsets.UTF_8, true ) );
      LogUtil.addAppender( appender, LogManager.getLogger( "org.pentaho.di.job.Job" ), getLog4jLevel( parentJob.getLogLevel() ) );
    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG,
        "AbstractAmazonJobExecutor.FailedToOpenLogFile", logFileName, e.toString() ) );
      logError( Const.getStackTracker( e ) );
    }
  }

  public String getStagingBucketName() throws FileSystemException, KettleException {

    String bucketName = "";

    String pathToStagingDir = getS3FileObjectPath();
    bucketName = pathToStagingDir.substring( 1, pathToStagingDir.length() ).split( "/" )[ 0 ];

    return bucketName;
  }

  private String getS3FileObjectPath() throws FileSystemException, KettleFileException {
    FileSystemOptions opts = new FileSystemOptions();
    DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator( opts,
      new StaticUserAuthenticator( null, getAWSAccessKeyId(), getAWSSecretKey() ) );
    FileObject stagingDirFileObject = KettleVFS.getFileObject( stagingDir, getVariables(), opts );

    return stagingDirFileObject.getName().getPath();
  }

  private String getKeyFromS3StagingDir() throws KettleFileException, FileSystemException {

    String pathToStagingDir = getS3FileObjectPath();
    StringBuilder sb = new StringBuilder( pathToStagingDir );

    sb.replace( 0, 1, "" );
    if ( sb.indexOf( "/" ) == -1 ) {
      return null;
    }
    sb.replace( 0, sb.indexOf( "/" ) + 1, "" );

    if ( sb.length() > 0 ) {
      return sb.toString();
    } else {
      return null;
    }
  }

  protected void setS3BucketKey( FileObject stagingFile ) throws KettleFileException, FileSystemException {

    String keyFromStagingDir = getKeyFromS3StagingDir();

    if ( keyFromStagingDir == null ) {
      keyFromStagingDir = "";
    }

    StringBuilder sb = new StringBuilder( keyFromStagingDir );
    if ( sb.length() > 0 ) {
      sb.append( "/" );
    }
    sb.append( stagingFile.getName().getBaseName() );
    key = sb.toString();
  }

  public String getStagingS3BucketUrl( String stagingBucketName ) {
    return "s3://" + stagingBucketName;
  }

  public String getStagingS3FileUrl( String stagingBucketName ) {
    return "s3://" + stagingBucketName + "/" + key;
  }

  public String buildFilename( String filename ) {
    filename = environmentSubstitute( filename );
    return filename;
  }

  public abstract File createStagingFile() throws IOException, KettleException;

  public abstract String getStepBootstrapActions();

  public abstract String getMainClass() throws Exception;

  public abstract String getStepType();

  private void runNewJobFlow( String stagingS3FileUrl, String stagingS3BucketUrl ) throws Exception {
    emrClient
      .runJobFlow( stagingS3FileUrl, stagingS3BucketUrl, getStepType(), getMainClass(), getStepBootstrapActions(),
        this );
  }

  private void addStepToExistingJobFlow( String stagingS3FileUrl, String stagingS3BucketUrl ) throws Exception {
    emrClient.addStepToExistingJobFlow( stagingS3FileUrl, stagingS3BucketUrl, getStepType(), getMainClass(), this );
  }

  private void logError( String stagingBucketName, String stepId ) {
    logError( s3Client.readStepLogsFromS3( stagingBucketName, hadoopJobFlowId, stepId ) );
  }

  private void initAmazonClients() {
    ClientFactoriesManager manager = ClientFactoriesManager.getInstance();
    s3Client = manager
      .createClient( getAWSAccessKeyId(), getAWSSecretKey(), getSessionToken(), region, ClientType.S3 );
    emrClient = manager
      .createClient( getAWSAccessKeyId(), getAWSSecretKey(), getSessionToken(), region, ClientType.EMR );
  }

  @Override
  public Result execute( Result result, int arg1 ) throws KettleException {

    setupLogFile();

    try {
      initAmazonClients();

      String stagingBucketName = getStagingBucketName();
      String stagingS3BucketUrl = getStagingS3BucketUrl( stagingBucketName );

      s3Client.createBucketIfNotExists( stagingBucketName );

      File tmpFile = createStagingFile();

      // delete old jar if needed
      try {
        s3Client.deleteObjectFromBucket( stagingBucketName, key );
      } catch ( Exception ex ) {
        logError( Const.getStackTracker( ex ) );
      }

      // put jar in s3 staging bucket
      s3Client.putObjectInBucket( stagingBucketName, key, tmpFile );
      String stagingS3FileUrl = getStagingS3FileUrl( stagingBucketName );

      if ( runOnNewCluster ) {
        // Determine the instances for Hadoop cluster.
        String numInstancesS = environmentSubstitute( numInstances );
        try {
          numInsts = Integer.parseInt( numInstancesS );
        } catch ( NumberFormatException e ) {
          logError( BaseMessages
            .getString( PKG, "AbstractAmazonJobExecutor.InstanceNumber.Error", numInstancesS ) );
        }
        runNewJobFlow( stagingS3FileUrl, stagingS3BucketUrl );
        hadoopJobFlowId = emrClient.getHadoopJobFlowId();
      } else {
        addStepToExistingJobFlow( stagingS3FileUrl, stagingS3BucketUrl );
      }
      // Set a logging interval.
      String loggingIntervalS = environmentSubstitute( loggingInterval );
      int logIntv = 10;
      try {
        logIntv = Integer.parseInt( loggingIntervalS );
      } catch ( NumberFormatException ex ) {
        logError( BaseMessages.getString( PKG,
          "AbstractAmazonJobExecutor.LoggingInterval.Error", loggingIntervalS ) );
      }
      // monitor and log if intended.
      if ( blocking ) {
        try {
          if ( log.isBasic() ) {
            while ( emrClient.isRunning() ) {

              if ( isJobStoppedByUser() ) {
                setResultError( result );
                break;
              }

              if ( emrClient.getCurrentClusterState() == null || emrClient.getCurrentClusterState().isEmpty() ) {
                break;
              }
              logBasic( hadoopJobName
                + " " + BaseMessages
                .getString( PKG, "AbstractAmazonJobExecutor.JobFlowExecutionStatus", hadoopJobFlowId )
                + emrClient.getCurrentClusterState() + " " );

              logBasic( hadoopJobName
                + " " + BaseMessages
                .getString( PKG, "AbstractAmazonJobExecutor.JobFlowStepStatus", emrClient.getStepId() )
                + emrClient.getCurrentStepState() + " " );

              try {
                Thread.sleep( logIntv * 1000 );
              } catch ( InterruptedException ie ) {
                logError( Const.getStackTracker( ie ) );
              }
            }

            if ( emrClient.isClusterTerminated() && emrClient.isStepNotSuccess() ) {
              setResultError( result );
              logError( hadoopJobName
                + " " + BaseMessages
                .getString( PKG, "AbstractAmazonJobExecutor.JobFlowExecutionStatus", hadoopJobFlowId )
                + emrClient.getCurrentClusterState() );
            }

            if ( emrClient.isStepNotSuccess() ) {
              setResultError( result );
              logBasic( hadoopJobName
                + " " + BaseMessages
                .getString( PKG, "AbstractAmazonJobExecutor.JobFlowStepStatus", emrClient.getStepId() )
                + emrClient.getCurrentStepState() + " " );

              if ( emrClient.isStepFailed() ) {
                logError( emrClient.getJobFlowLogUri(), emrClient.getStepId() );
              }
            }
          }
        } catch ( Exception e ) {
          logError( e.getMessage(), e );
        }
      }
    } catch ( Throwable t ) {
      t.printStackTrace();
      setResultError( result );
      logError( t.getMessage(), t );
    }

    if ( appender != null ) {
      LogUtil.removeAppender(appender, LogManager.getLogger());
      ResultFile resultFile =
        new ResultFile( ResultFile.FILE_TYPE_LOG, file, parentJob.getJobname(), getName() );
      result.getResultFiles().put( resultFile.getFile().toString(), resultFile );
    }
    return result;
  }

  private boolean isJobStoppedByUser() {
    if ( getParentJob().isInterrupted() || getParentJob().isStopped() ) {
      return emrClient.stopSteps();
    }
    return false;
  }

  private void setResultError( Result result ) {
    result.setStopped( true );
    result.setNrErrors( 1 );
    result.setResult( false );
  }
}
