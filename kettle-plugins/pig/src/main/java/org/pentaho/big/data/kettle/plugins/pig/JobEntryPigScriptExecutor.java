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

package org.pentaho.big.data.kettle.plugins.pig;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.pentaho.hadoop.shim.api.HadoopClientServices;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.ResultFile;
import org.pentaho.di.core.annotations.JobEntry;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobListener;
import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.hadoop.shim.api.pig.PigResult;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;
import org.w3c.dom.Node;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Job entry that executes a Pig script either on a hadoop cluster or locally.
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision$
 */
@JobEntry( id = "HadoopPigScriptExecutorPlugin", image = "PIG.svg", name = "HadoopPigScriptExecutorPlugin.Name",
  description = "HadoopPigScriptExecutorPlugin.Description",
  categoryDescription = "i18n:org.pentaho.di.job:JobCategory.Category.BigData",
  i18nPackageName = "org.pentaho.di.job.entries.pig",
  documentationUrl = "http://wiki.pentaho.com/display/EAI/Pig+Script+Executor" )
public class JobEntryPigScriptExecutor extends JobEntryBase implements Cloneable, JobEntryInterface {
  public static final Class<?> PKG = JobEntryPigScriptExecutor.class; // for i18n purposes, needed by Translator2!!

  public static final String CLUSTER_NAME = "cluster_name";
  public static final String HDFS_HOSTNAME = "hdfs_hostname";
  public static final String HDFS_PORT = "hdfs_port";
  public static final String JOBTRACKER_HOSTNAME = "jobtracker_hostname";
  public static final String JOBTRACKER_PORT = "jobtracker_port";
  public static final String SCRIPT_FILE = "script_file";
  public static final String ENABLE_BLOCKING = "enable_blocking";

  public static final String LOCAL_EXECUTION = "local_execution";
  public static final String JOB_ENTRY_PIG_SCRIPT_EXECUTOR_ERROR_NO_PIG_SCRIPT_SPECIFIED =
    "JobEntryPigScriptExecutor.Error.NoPigScriptSpecified";
  public static final String JOB_ENTRY_PIG_SCRIPT_EXECUTOR_WARNING_LOCAL_EXECUTION =
    "JobEntryPigScriptExecutor.Warning.LocalExecution";
  // $NON-NLS-1$
  private final NamedClusterService namedClusterService;
  private final RuntimeTestActionService runtimeTestActionService;
  private final RuntimeTester runtimeTester;
  private final NamedClusterServiceLocator namedClusterServiceLocator;
  /**
   * Hostname of the job tracker
   */
  protected NamedCluster namedCluster;
  /**
   * URL to the pig script to execute
   */
  protected String m_scriptFile = "";
  /**
   * True if the job entry should block until the script has executed
   */
  protected boolean m_enableBlocking;
  /**
   * True if the script should execute locally, rather than on a hadoop cluster
   */
  protected boolean m_localExecution;
  /**
   * Parameters for the script
   */
  protected Map<String, String> m_params = new HashMap<String, String>();

  public JobEntryPigScriptExecutor( NamedClusterService namedClusterService,
                                    RuntimeTestActionService runtimeTestActionService, RuntimeTester runtimeTester,
                                    NamedClusterServiceLocator namedClusterServiceLocator ) {
    this.namedClusterService = namedClusterService;
    this.runtimeTestActionService = runtimeTestActionService;
    this.runtimeTester = runtimeTester;
    this.namedClusterServiceLocator = namedClusterServiceLocator;
  }

  private void loadClusterConfig( ObjectId id_jobentry, Repository rep, Node entrynode, IMetaStore metaStore ) {
    boolean configLoaded = false;
    try {
      // attempt to load from named cluster
      String clusterName = null;
      if ( entrynode != null ) {
        clusterName = XMLHandler.getTagValue( entrynode, CLUSTER_NAME ); //$NON-NLS-1$
      } else if ( rep != null ) {
        clusterName = rep.getJobEntryAttributeString( id_jobentry, CLUSTER_NAME ); //$NON-NLS-1$ //$NON-NLS-2$
      }

      // load from system first, then fall back to copy stored with job (AbstractMeta)
      if ( !StringUtils.isEmpty( clusterName ) && namedClusterService.contains( clusterName, metaStore ) ) {
        // pull config from NamedCluster
        namedCluster = namedClusterService.read( clusterName, metaStore );
      }
      if ( namedCluster != null ) {
        configLoaded = true;
      }
    } catch ( Throwable t ) {
      logDebug( t.getMessage(), t );
    }

    if ( !configLoaded ) {
      namedCluster = namedClusterService.getClusterTemplate();
      if ( entrynode != null ) {
        // load default values for cluster & legacy fallback
        namedCluster.setName( XMLHandler.getTagValue( entrynode, CLUSTER_NAME ) );
        namedCluster.setHdfsHost( XMLHandler.getTagValue( entrynode, HDFS_HOSTNAME ) ); //$NON-NLS-1$
        namedCluster.setHdfsPort( XMLHandler.getTagValue( entrynode, HDFS_PORT ) ); //$NON-NLS-1$
        namedCluster.setJobTrackerHost( XMLHandler.getTagValue( entrynode, JOBTRACKER_HOSTNAME ) ); //$NON-NLS-1$
        namedCluster.setJobTrackerPort( XMLHandler.getTagValue( entrynode, JOBTRACKER_PORT ) ); //$NON-NLS-1$
      } else if ( rep != null ) {
        // load default values for cluster & legacy fallback
        try {
          namedCluster.setName( rep.getJobEntryAttributeString( id_jobentry, CLUSTER_NAME ) );
          namedCluster.setHdfsHost( rep.getJobEntryAttributeString( id_jobentry, HDFS_HOSTNAME ) );
          namedCluster.setHdfsPort( rep.getJobEntryAttributeString( id_jobentry, HDFS_PORT ) ); //$NON-NLS-1$
          namedCluster
            .setJobTrackerHost( rep.getJobEntryAttributeString( id_jobentry, JOBTRACKER_HOSTNAME ) ); //$NON-NLS-1$
          namedCluster
            .setJobTrackerPort( rep.getJobEntryAttributeString( id_jobentry, JOBTRACKER_PORT ) ); //$NON-NLS-1$
        } catch ( KettleException ke ) {
          logError( ke.getMessage(), ke );
        }
      }
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.pentaho.di.job.entry.JobEntryBase#getXML()
   */
  public String getXML() {
    StringBuffer retval = new StringBuffer();
    retval.append( super.getXML() );

    if ( namedCluster != null ) {
      String namedClusterName = namedCluster.getName();
      if ( !StringUtils.isEmpty( namedClusterName ) ) {
        retval.append( "      " )
          .append( XMLHandler.addTagValue( CLUSTER_NAME, namedClusterName ) ); //$NON-NLS-1$ //$NON-NLS-2$
      }
      retval.append( "    " ).append( XMLHandler.addTagValue( HDFS_HOSTNAME, namedCluster.getHdfsHost() ) );
      retval.append( "    " ).append( XMLHandler.addTagValue( HDFS_PORT, namedCluster.getHdfsPort() ) );
      retval.append( "    " ).append(
        XMLHandler.addTagValue( JOBTRACKER_HOSTNAME, namedCluster.getJobTrackerHost() ) );
      retval.append( "    " ).append( XMLHandler.addTagValue( JOBTRACKER_PORT, namedCluster.getJobTrackerPort() ) );
    }

    retval.append( "    " ).append( XMLHandler.addTagValue( SCRIPT_FILE, m_scriptFile ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( ENABLE_BLOCKING, m_enableBlocking ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( LOCAL_EXECUTION, m_localExecution ) );

    retval.append( "    <script_parameters>" ).append( Const.CR );
    if ( m_params != null ) {
      for ( String name : m_params.keySet() ) {
        String value = m_params.get( name );
        if ( !Utils.isEmpty( name ) && !Utils.isEmpty( value ) ) {
          retval.append( "      <parameter>" ).append( Const.CR );
          retval.append( "        " ).append( XMLHandler.addTagValue( "name", name ) );
          retval.append( "        " ).append( XMLHandler.addTagValue( "value", value ) );
          retval.append( "      </parameter>" ).append( Const.CR );
        }
      }
    }
    retval.append( "    </script_parameters>" ).append( Const.CR );

    return retval.toString();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.pentaho.di.job.entry.JobEntryInterface#loadXML(org.w3c.dom.Node, java.util.List, java.util.List,
   * org.pentaho.di.repository.Repository)
   */
  @Override
  public void loadXML( Node entrynode, List<DatabaseMeta> databases, List<SlaveServer> slaveServers,
                       Repository repository, IMetaStore metaStore ) throws KettleXMLException {
    super.loadXML( entrynode, databases, slaveServers );

    loadClusterConfig( null, rep, entrynode, metaStore );
    setRepository( repository );

    m_scriptFile = XMLHandler.getTagValue( entrynode, "script_file" );
    m_enableBlocking = XMLHandler.getTagValue( entrynode, "enable_blocking" ).equalsIgnoreCase( "Y" );
    m_localExecution = XMLHandler.getTagValue( entrynode, "local_execution" ).equalsIgnoreCase( "Y" );

    // Script parameters
    m_params = new HashMap<String, String>();
    Node paramList = XMLHandler.getSubNode( entrynode, "script_parameters" );
    if ( paramList != null ) {
      int numParams = XMLHandler.countNodes( paramList, "parameter" );
      for ( int i = 0; i < numParams; i++ ) {
        Node paramNode = XMLHandler.getSubNodeByNr( paramList, "parameter", i );
        String name = XMLHandler.getTagValue( paramNode, "name" );
        String value = XMLHandler.getTagValue( paramNode, "value" );
        m_params.put( name, value );
      }
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.pentaho.di.job.entry.JobEntryBase#loadRep(org.pentaho.di.repository.Repository,
   * org.pentaho.di.repository.ObjectId, java.util.List, java.util.List)
   */
  @Override
  public void loadRep( Repository rep, IMetaStore metaStore, ObjectId id_jobentry, List<DatabaseMeta> databases,
                       List<SlaveServer> slaveServers ) throws KettleException {
    if ( rep != null ) {
      super.loadRep( rep, metaStore, id_jobentry, databases, slaveServers );

      loadClusterConfig( id_jobentry, rep, null, metaStore );
      setRepository( rep );

      setScriptFilename( rep.getJobEntryAttributeString( id_jobentry, "script_file" ) );
      setEnableBlocking( rep.getJobEntryAttributeBoolean( id_jobentry, "enable_blocking" ) );
      setLocalExecution( rep.getJobEntryAttributeBoolean( id_jobentry, "local_execution" ) );

      // Script parameters
      m_params = new HashMap<String, String>();
      int numParams = rep.countNrJobEntryAttributes( id_jobentry, "param_name" );
      if ( numParams > 0 ) {
        for ( int i = 0; i < numParams; i++ ) {
          String name = rep.getJobEntryAttributeString( id_jobentry, i, "param_name" );
          String value = rep.getJobEntryAttributeString( id_jobentry, i, "param_value" );
          m_params.put( name, value );
        }
      }
    } else {
      throw new KettleException( "Unable to load from a repository. The repository is null." );
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.pentaho.di.job.entry.JobEntryBase#saveRep(org.pentaho.di.repository.Repository,
   * org.pentaho.di.repository.ObjectId)
   */
  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_job ) throws KettleException {
    if ( rep != null ) {
      super.saveRep( rep, metaStore, id_job );

      if ( namedCluster != null ) {
        String namedClusterName = namedCluster.getName();
        if ( !StringUtils.isEmpty( namedClusterName ) ) {
          rep.saveJobEntryAttribute( id_job, getObjectId(), "cluster_name", namedClusterName ); //$NON-NLS-1$
        }
        rep.saveJobEntryAttribute( id_job, getObjectId(), "hdfs_hostname", namedCluster.getHdfsHost() );
        rep.saveJobEntryAttribute( id_job, getObjectId(), "hdfs_port", namedCluster.getHdfsPort() );
        rep.saveJobEntryAttribute( id_job, getObjectId(), "jobtracker_hostname", namedCluster.getJobTrackerHost() );
        rep.saveJobEntryAttribute( id_job, getObjectId(), "jobtracker_port", namedCluster.getJobTrackerPort() );
      }
      rep.saveJobEntryAttribute( id_job, getObjectId(), "script_file", m_scriptFile );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "enable_blocking", m_enableBlocking );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "local_execution", m_localExecution );

      if ( m_params != null ) {
        int i = 0;
        for ( String name : m_params.keySet() ) {
          String value = m_params.get( name );
          if ( !Utils.isEmpty( name ) && !Utils.isEmpty( value ) ) {
            rep.saveJobEntryAttribute( id_job, getObjectId(), i, "param_name", name );
            rep.saveJobEntryAttribute( id_job, getObjectId(), i, "param_value", value );
            i++;
          }
        }
      }
    } else {
      throw new KettleException( "Unable to save to a repository. The repository is null." );
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.pentaho.di.job.entry.JobEntryBase#evaluates()
   */
  public boolean evaluates() {
    return true;
  }

  /**
   * Get whether the job entry will block until the script finishes
   *
   * @return true if the job entry will block until the script finishes
   */
  public boolean getEnableBlocking() {
    return m_enableBlocking;
  }

  /**
   * Set whether the job will block until the script finishes
   *
   * @param block true if the job entry is to block until the script finishes
   */
  public void setEnableBlocking( boolean block ) {
    m_enableBlocking = block;
  }

  /**
   * Get whether the script is to run locally rather than on a hadoop cluster
   *
   * @return true if the script is to run locally
   */
  public boolean getLocalExecution() {
    return m_localExecution;
  }

  /**
   * Set whether the script is to be run locally rather than on a hadoop cluster
   *
   * @param l true if the script is to run locally
   */
  public void setLocalExecution( boolean l ) {
    m_localExecution = l;
  }

  /**
   * Get the URL to the pig script to run
   *
   * @return the URL to the pig script to run
   */
  public String getScriptFilename() {
    return m_scriptFile;
  }

  /**
   * Set the URL to the pig script to run
   *
   * @param filename the URL to the pig script
   */
  public void setScriptFilename( String filename ) {
    m_scriptFile = filename;
  }

  /**
   * Get the values of parameters to replace in the script
   *
   * @return a HashMap mapping parameter names to values
   */
  public Map<String, String> getScriptParameters() {
    return m_params;
  }

  /**
   * Set the values of parameters to replace in the script
   *
   * @param params a HashMap mapping parameter names to values
   */
  public void setScriptParameters( Map<String, String> params ) {
    m_params = params;
  }

  public NamedCluster getNamedCluster() {
    return namedCluster;
  }

  public void setNamedCluster( NamedCluster namedCluster ) {
    this.namedCluster = namedCluster;
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

  /*
           * (non-Javadoc)
           *
           * @see org.pentaho.di.job.entry.JobEntryInterface#execute(org.pentaho.di.core.Result, int)
           */
  public Result execute( final Result result, int arg1 ) throws KettleException {
    result.setNrErrors( 0 );
    if ( Utils.isEmpty( m_scriptFile ) ) {
      throw new KettleException( BaseMessages.getString( PKG, JOB_ENTRY_PIG_SCRIPT_EXECUTOR_ERROR_NO_PIG_SCRIPT_SPECIFIED ) );
    }
    try {
      String scriptFileS = m_scriptFile;
      scriptFileS = environmentSubstitute( scriptFileS );

      HadoopClientServices hadoopClientServices = namedClusterServiceLocator.getService( namedCluster, HadoopClientServices.class );

      // transform the map type to list type which can been accepted by ParameterSubstitutionPreprocessor
      final List<String> paramList = new ArrayList<String>();
      if ( m_params != null ) {
        for ( Map.Entry<String, String> entry : m_params.entrySet() ) {
          String name = entry.getKey();
          name = environmentSubstitute( name ); // do environment variable substitution
          String value = entry.getValue();
          value = environmentSubstitute( value ); // do environment variable substitution
          paramList.add( name + "=" + value );
        }
      }

      final HadoopClientServices.PigExecutionMode execMode = ( m_localExecution ? HadoopClientServices.PigExecutionMode.LOCAL : HadoopClientServices.PigExecutionMode.MAPREDUCE );

      if ( m_enableBlocking ) {
        PigResult pigResult = hadoopClientServices.runPig( scriptFileS, execMode, paramList, getName(), getLogChannel(), this, parentJob.getLogLevel() );
        processScriptExecutionResult( pigResult, result );
      } else {
        final String finalScriptFileS = scriptFileS;
        final Thread runThread = new Thread() {
          public void run() {
            PigResult pigResult =
                    hadoopClientServices.runPig( finalScriptFileS, execMode, paramList, getName(), getLogChannel(),
                JobEntryPigScriptExecutor.this, parentJob.getLogLevel() );
            processScriptExecutionResult( pigResult, result );
          }
        };

        runThread.start();
        parentJob.addJobListener( new JobListener() {

          @Override
          public void jobStarted( Job job ) throws KettleException {
          }

          @Override
          public void jobFinished( Job job ) throws KettleException {
            if ( runThread.isAlive() ) {
              logMinimal( BaseMessages.getString( PKG, "JobEntryPigScriptExecutor.Warning.AsynctaskStillRunning", getName(), job.getJobname() ) );
            }
          }
        } );
      }
    } catch ( Exception ex ) {
      ex.printStackTrace();
      result.setStopped( true );
      result.setNrErrors( 1 );
      result.setResult( false );
      logError( ex.getMessage(), ex );
    }

    return result;
  }

  protected void processScriptExecutionResult( PigResult pigResult, Result result ) {
    int[] executionStatus = pigResult.getResult();
    Exception pigResultException = pigResult.getException();
    //we have several execution status
    if ( executionStatus != null && executionStatus.length > 0 ) {
      int countFailedJob = 0;
      if ( executionStatus.length > 1 ) {
        countFailedJob = executionStatus[ 1 ];
      }
      logBasic( BaseMessages.getString( PKG, "JobEntryPigScriptExecutor.JobCompletionStatus",
        String.valueOf( executionStatus[ 0 ] ), String.valueOf( countFailedJob ) ) );

      if ( countFailedJob > 0 ) {
        result.setStopped( true );
        result.setNrErrors( countFailedJob );
        result.setResult( false );
      }
    } else if ( pigResultException != null ) {
      logError( pigResultException.getMessage(), pigResultException );
      result.setStopped( true );
      result.setNrErrors( 1 );
      result.setResult( false );
    }
    FileObject logFile = pigResult.getLogFile();
    if ( logFile != null ) {
      ResultFile resultFile = new ResultFile( ResultFile.FILE_TYPE_LOG, logFile, parentJob.getJobname(), getName() );
      result.getResultFiles().put( resultFile.getFile().toString(), resultFile );
    }
  }

  @VisibleForTesting
  void setLog( LogChannelInterface log ) {
    this.log = log;
  }
}
