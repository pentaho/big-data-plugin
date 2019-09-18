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

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.pentaho.hadoop.shim.api.HadoopClientServices;
import org.pentaho.hadoop.shim.api.HadoopClientServicesException;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.hadoop.shim.api.cluster.ClusterInitializationException;
import org.pentaho.big.data.kettle.plugins.job.AbstractJobEntry;
import org.pentaho.big.data.kettle.plugins.job.JobEntryMode;
import org.pentaho.big.data.kettle.plugins.job.JobEntryUtils;
import org.pentaho.big.data.kettle.plugins.job.PropertyEntry;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.annotations.JobEntry;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.hadoop.shim.api.oozie.OozieJobInfo;
import org.pentaho.hadoop.shim.api.oozie.OozieServiceException;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;

/**
 * User: RFellows Date: 6/4/12
 */
@JobEntry( id = "OozieJobExecutor", name = "Oozie.JobExecutor.PluginName",
  description = "Oozie.JobExecutor.PluginDescription",
  categoryDescription = "i18n:org.pentaho.di.job:JobCategory.Category.BigData", image = "oozie-job-executor.svg",
  documentationUrl = "http://wiki.pentaho.com/display/EAI/Oozie+Job+Executor",
  i18nPackageName = "org.pentaho.di.job.entries.oozie", version = "1" )
public class OozieJobExecutorJobEntry extends AbstractJobEntry<OozieJobExecutorConfig> implements Cloneable,
  JobEntryInterface {

  public static final String HTTP_ERROR_CODE_404 = "HTTP error code: 404";
  public static final String HTTP_ERROR_CODE_401 = "HTTP error code: 401";
  public static final String HTTP_ERROR_CODE_403 = "HTTP error code: 403";
  public static final String USER_NAME = "user.name";
  private final NamedClusterService namedClusterService;
  private final NamedClusterServiceLocator namedClusterServiceLocator;
  private final RuntimeTestActionService runtimeTestActionService;
  private final RuntimeTester runtimeTester;
  private HadoopClientServices hadoopClientServices = null;

  public OozieJobExecutorJobEntry(
    NamedClusterService namedClusterService,
    RuntimeTestActionService runtimeTestActionService, RuntimeTester runtimeTester,
    NamedClusterServiceLocator namedClusterServiceLocator ) {
    this.namedClusterService = namedClusterService;
    this.namedClusterServiceLocator = namedClusterServiceLocator;
    this.runtimeTestActionService = runtimeTestActionService;
    this.runtimeTester = runtimeTester;
  }

  @VisibleForTesting OozieJobExecutorJobEntry() {
    namedClusterService = null;
    namedClusterServiceLocator = null;
    runtimeTestActionService = null;
    runtimeTester = null;
  }

  @Override
  protected OozieJobExecutorConfig createJobConfig() {
    return new OozieJobExecutorConfig();
  }

  public List<String> getValidationWarnings( OozieJobExecutorConfig config, boolean checkOozieConnection ) {
    List<String> messages = new ArrayList<>();

    // verify there is a job name
    if ( StringUtil.isEmpty( config.getJobEntryName() ) ) {
      messages.add( BaseMessages.getString( OozieJobExecutorJobEntry.class, "ValidationMessages.Missing.JobName" ) );
    }

    if ( StringUtils.isEmpty( getEffectiveOozieUrl( config ) ) ) {
      messages
        .add( BaseMessages.getString( OozieJobExecutorJobEntry.class, "ValidationMessages.Missing.Configuration" ) );
    } else {
      try {

        NamedCluster nc = getNamedCluster( config );

        if ( !checkOozieConnection ) {
          if ( nc == null ) {
            messages.add(
              BaseMessages.getString( OozieJobExecutorJobEntry.class, "ValidationMessages.Missing.Configuration" ) );
          } else if ( StringUtils.isEmpty( nc.getOozieUrl() ) ) {
            messages
              .add( BaseMessages.getString( OozieJobExecutorJobEntry.class, "ValidationMessages.Missing.Oozie.URL" ) );
          }
        }
      } catch ( Throwable t ) {
        messages
          .add( BaseMessages.getString( OozieJobExecutorJobEntry.class, "ValidationMessages.Missing.Oozie.URL" ) );
      }
    }

    if ( checkOozieConnection && !StringUtils.isEmpty( getEffectiveOozieUrl( config ) ) ) {
      try {
        hadoopClientServices = getHadoopClientServices( config );
        hadoopClientServices.getOozieProtocolUrl();
        hadoopClientServices.validateOozieWSVersion();
      } catch ( HadoopClientServicesException e ) {
        if ( e.getErrorCode().equals( HTTP_ERROR_CODE_404 )
          || ( e.getCause() != null
          && ( e.getCause() instanceof MalformedURLException || e.getCause() instanceof ConnectException ) ) ) {
          messages
            .add( BaseMessages.getString( OozieJobExecutorJobEntry.class, "ValidationMessages.Invalid.Oozie.URL" ) );
        } else if ( e.getErrorCode().equals( HTTP_ERROR_CODE_401 ) || e.getErrorCode().equals( HTTP_ERROR_CODE_403 ) ) {
          messages.add(
            BaseMessages.getString( OozieJobExecutorJobEntry.class, "ValidationMessages.Unauthorized.Oozie.Access" ) );
        } else {
          messages.add( BaseMessages
            .getString( OozieJobExecutorJobEntry.class, "ValidationMessages.Incompatible.Oozie.Versions" ) );
        }
      }
    }

    // path to oozie workflow properties file
    if ( config.getModeAsEnum() == JobEntryMode.QUICK_SETUP && StringUtil.isEmpty( config.getOozieWorkflowConfig() ) ) {
      messages.add( BaseMessages.getString( OozieJobExecutorJobEntry.class,
        "ValidationMessages.Missing.Workflow.Properties" ) );
    } else {
      // make sure the path to the properties file is valid
      try {
        Properties props = getProperties( config );

        // make sure it has at minimum a workflow definition (need app path)
        if ( checkOozieConnection && !hadoopClientServices.hasOozieAppPath( props ) ) {
          messages.add( BaseMessages.getString( OozieJobExecutorJobEntry.class,
            "ValidationMessages.App.Path.Property.Missing" ) );
        }

      } catch ( KettleFileException e ) {
        // can't find the file specified as the Workflow Properties definition
        messages.add( BaseMessages.getString( OozieJobExecutorJobEntry.class,
          "ValidationMessages.Workflow.Properties.FileNotFound" ) );
      } catch ( IOException e ) {
        // something went wrong with the reading of the properties file
        messages.add( BaseMessages.getString( OozieJobExecutorJobEntry.class,
          "ValidationMessages.Workflow.Properties.ReadError" ) );
      }
    }

    boolean pollingIntervalValid = false;
    try {
      long pollingInterval = JobEntryUtils.asLong( config.getBlockingPollingInterval(), variables );
      pollingIntervalValid = pollingInterval > 0;
    } catch ( Exception ex ) {
      // ignore, polling interval is not valid
    }
    if ( !pollingIntervalValid ) {
      messages.add( BaseMessages.getString( OozieJobExecutorJobEntry.class,
        "ValidationMessages.Invalid.PollingInterval" ) );
    }

    return messages;
  }

  private NamedCluster getNamedCluster( OozieJobExecutorConfig config ) throws MetaStoreException {
    // load from system first, then
    NamedCluster nc = null;
    if ( metaStore != null && !StringUtils.isEmpty( jobConfig.getClusterName() )
      && namedClusterService.contains( jobConfig.getClusterName(), metaStore ) ) {
      // pull config from NamedCluster
      nc = namedClusterService.read( jobConfig.getClusterName(), metaStore );
    }
    // fall back to copy stored with job (AbstractMeta)
    if ( nc == null ) {
      nc = config.getNamedCluster();
    }
    // final fallback, construct cluster based on oozie url from job config
    if ( nc == null && namedClusterService != null ) {
      nc = namedClusterService.getClusterTemplate();
      nc.setOozieUrl( config.getOozieUrl() );
    }
    return nc;
  }

  /**
   * Validates the current configuration of the step.
   * <p/>
   * <strong>To be valid in Quick Setup mode:</strong> <ul> <li>Name is required</li> <li>Oozie URL is required and
   * must be a valid oozie location</li> <li>Workflow Properties file path is required and must be a valid job
   * properties file</li> </ul>
   *
   * @param config Configuration to validate
   * @return
   */
  @Override
  public List<String> getValidationWarnings( OozieJobExecutorConfig config ) {
    return getValidationWarnings( config, true );
  }

  public Properties getPropertiesFromFile( OozieJobExecutorConfig config ) throws IOException, KettleFileException {
    return getPropertiesFromFile( config, getVariableSpace() );
  }

  public static Properties getPropertiesFromFile( OozieJobExecutorConfig config, VariableSpace variableSpace )
    throws IOException, KettleFileException {
    InputStreamReader reader =
      new InputStreamReader( KettleVFS.getInputStream( variableSpace.environmentSubstitute( config
        .getOozieWorkflowConfig() ) ) );

    Properties jobProps = new Properties(); // PropertiesUtils.readProperties(reader, 100*1024);
    jobProps.load( reader );
    return jobProps;
  }

  public Properties getProperties( OozieJobExecutorConfig config ) throws KettleFileException, IOException {
    return getProperties( config, getVariableSpace() );
  }

  public static Properties getProperties( OozieJobExecutorConfig config, VariableSpace variableSpace )
    throws KettleFileException, IOException {
    Properties jobProps;
    if ( config.getModeAsEnum() == JobEntryMode.ADVANCED_LIST && config.getWorkflowProperties() != null ) {
      jobProps = new Properties();
      for ( PropertyEntry propertyEntry : config.getWorkflowProperties() ) {
        if ( propertyEntry.getKey() != null ) {
          String value = propertyEntry.getValue() == null ? "" : propertyEntry.getValue();
          jobProps.setProperty( propertyEntry.getKey(), variableSpace.environmentSubstitute( value ) );
        }
      }
    } else {
      jobProps = getPropertiesFromFile( config, variableSpace );
    }
    return jobProps;
  }

  @Override
  protected Runnable getExecutionRunnable( final Result jobResult ) {
    return new Runnable() {
      @Override
      public void run() {

        HadoopClientServices hadoopClientServices = getHadoopClientServices();

        try {
          hadoopClientServices.validateOozieWSVersion();
        } catch ( HadoopClientServicesException e ) {

          setJobResultFailed( jobResult );

          if ( e.getErrorCode().equals( HTTP_ERROR_CODE_404 )
            || ( e.getCause() != null
            && ( e.getCause() instanceof MalformedURLException || e.getCause() instanceof ConnectException ) ) ) {
            logError( BaseMessages.getString( OozieJobExecutorJobEntry.class, "ValidationMessages.Invalid.Oozie.URL" ),
              e );
          } else if ( e.getErrorCode().equals( HTTP_ERROR_CODE_401 ) || e.getErrorCode()
            .equals( HTTP_ERROR_CODE_403 ) ) {
            logError( BaseMessages
              .getString( OozieJobExecutorJobEntry.class, "ValidationMessages.Unauthorized.Oozie.Access" ) );
          } else {
            logError( BaseMessages.getString( OozieJobExecutorJobEntry.class,
              "ValidationMessages.Incompatible.Oozie.Versions" ), e );
          }
        }

        try {
          Properties jobProps = getProperties( jobConfig );

          // make sure we supply the current user name
          if ( !jobProps.containsKey( USER_NAME ) ) {
            jobProps.setProperty( USER_NAME, getVariableSpace().environmentSubstitute( "${" + USER_NAME + "}" ) );
          }

          OozieJobInfo job = hadoopClientServices.runOozie( jobProps );
          if ( JobEntryUtils.asBoolean( getJobConfig().getBlockingExecution(), variables ) ) {
            while ( job.isRunning() ) {
              // System.out.println("Still running " + jobId + "...");
              long interval = JobEntryUtils.asLong( jobConfig.getBlockingPollingInterval(), variables );
              Thread.sleep( interval );
            }
            String logDetail = job.getJobLog();
            if ( job.didSucceed() ) {
              jobResult.setResult( true );
              logDetailed( logDetail );
            } else {
              // it failed
              setJobResultFailed( jobResult );
              logError( logDetail );
            }
          }

        } catch ( KettleFileException e ) {
          setJobResultFailed( jobResult );
          logError(
            BaseMessages.getString( OozieJobExecutorJobEntry.class, "Oozie.JobExecutor.ERROR.File.Resolution" ), e );
        } catch ( IOException e ) {
          setJobResultFailed( jobResult );
          logError( BaseMessages.getString( OozieJobExecutorJobEntry.class, "Oozie.JobExecutor.ERROR.Props.Loading" ),
            e );
        } catch ( HadoopClientServicesException e ) {
          setJobResultFailed( jobResult );
          logError(
            BaseMessages.getString( OozieJobExecutorJobEntry.class, "Oozie.JobExecutor.ERROR.OozieClient" ), e );
        } catch ( OozieServiceException e ) {
          setJobResultFailed( jobResult );
          logError(
            BaseMessages.getString( OozieJobExecutorJobEntry.class, "Oozie.JobExecutor.ERROR.OozieClient" ), e );
        } catch ( InterruptedException e ) {
          setJobResultFailed( jobResult );
          logError( BaseMessages.getString( OozieJobExecutorJobEntry.class, "Oozie.JobExecutor.ERROR.Threading" ), e );
        }
      }
    };
  }

  @Override
  protected void handleUncaughtThreadException( Thread t, Throwable e, Result jobResult ) {
    logError( BaseMessages.getString( OozieJobExecutorJobEntry.class, "Oozie.JobExecutor.ERROR.Generic" ), e );
    setJobResultFailed( jobResult );
  }

  @VisibleForTesting
  String getEffectiveOozieUrl( OozieJobExecutorConfig config ) {
    String oozieUrl = config.getOozieUrl();
    try {
      NamedCluster nc = getNamedCluster( config );

      if ( nc != null && !StringUtils.isEmpty( nc.getOozieUrl() ) ) {
        oozieUrl = nc.getOozieUrl();
      }
    } catch ( Throwable t ) {
      logDebug( t.getMessage(), t );
    }
    return getVariableSpace().environmentSubstitute( oozieUrl );
  }

  public HadoopClientServices getHadoopClientServices() {
    return getHadoopClientServices( jobConfig );
  }

  public HadoopClientServices getHadoopClientServices( OozieJobExecutorConfig config ) {
    try {
      NamedCluster cluster = getNamedCluster( config ).clone();
      cluster.setOozieUrl( getEffectiveOozieUrl( config ) );
      return namedClusterServiceLocator.getService(
        cluster,
        HadoopClientServices.class );
    } catch ( ClusterInitializationException e ) {
      logError( "Cluster initialization failure on service load", e );
    } catch ( MetaStoreException e ) {
      logError( "Failed to read cluster from metastore", e );
    }
    return null;
  }


  public RuntimeTestActionService getRuntimeTestActionService() {
    return runtimeTestActionService;
  }

  public RuntimeTester getRuntimeTester() {
    return runtimeTester;
  }

  public NamedClusterService getNamedClusterService() {
    return namedClusterService;
  }
}
