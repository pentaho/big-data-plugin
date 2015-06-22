/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.ui.job.entries.hadoopjobexecutor;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.di.core.namedcluster.NamedClusterManager;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.core.plugins.JobEntryPluginType;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.hadoopjobexecutor.JarUtility;
import org.pentaho.di.job.entries.hadoopjobexecutor.JobEntryHadoopJobExecutor;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.database.dialog.tags.ExtTextbox;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.core.namedcluster.NamedClusterUIHelper;
import org.pentaho.di.ui.delegates.HadoopClusterDelegate;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.util.HelpUtils;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.ui.xul.XulDomException;
import org.pentaho.ui.xul.XulEventSourceAdapter;
import org.pentaho.ui.xul.components.XulMenuList;
import org.pentaho.ui.xul.components.XulTextbox;
import org.pentaho.ui.xul.containers.XulDialog;
import org.pentaho.ui.xul.containers.XulVbox;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;
import org.pentaho.ui.xul.jface.tags.JfaceCMenuList;
import org.pentaho.ui.xul.jface.tags.JfaceMenuList;
import org.pentaho.ui.xul.util.AbstractModelList;

public class JobEntryHadoopJobExecutorController extends AbstractXulEventHandler {

  private static final Class<?> PKG = JobEntryHadoopJobExecutor.class;

  public static final String JOB_ENTRY_NAME = "jobEntryName"; //$NON-NLS-1$
  public static final String HADOOP_JOB_NAME = "hadoopJobName"; //$NON-NLS-1$
  public static final String JAR_URL = "jarUrl"; //$NON-NLS-1$
  public static final String DRIVER_CLASS = "driverClass"; //$NON-NLS-1$
  public static final String DRIVER_CLASSES = "driverClasses"; //$NON-NLS-1$
  public static final String IS_SIMPLE = "isSimple"; //$NON-NLS-1$
  public static final String USER_DEFINED = "userDefined"; //$NON-NLS-1$

  private String jobEntryName;
  private String hadoopJobName;
  private String jarUrl = "";
  private String driverClass = "";
  private List<String> driverClasses = new ArrayList<String>();

  private boolean isSimple = true;

  private List<NamedCluster> namedClusters;
  
  private SimpleConfiguration sConf = new SimpleConfiguration();
  private AdvancedConfiguration aConf = new AdvancedConfiguration();

  private JobEntryHadoopJobExecutor jobEntry;

  private JobMeta jobMeta;
  
  private AbstractModelList<UserDefinedItem> userDefined = new AbstractModelList<UserDefinedItem>();

  private HadoopClusterDelegate ncDelegate = new HadoopClusterDelegate( Spoon.getInstance() );;

  protected VariableSpace getVariableSpace() {
    if ( Spoon.getInstance().getActiveTransformation() != null ) {
      return Spoon.getInstance().getActiveTransformation();
    } else if ( Spoon.getInstance().getActiveJob() != null ) {
      return Spoon.getInstance().getActiveJob();
    } else {
      return new Variables();
    }
  }

  public void accept() {
    ExtTextbox tempBox =
        (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "jobentry-hadoopjob-name" );
    this.hadoopJobName = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "jar-url" );
    this.jarUrl = ( (Text) tempBox.getTextControl() ).getText();

    JfaceCMenuList tempList = (JfaceCMenuList) getXulDomContainer().getDocumentRoot().getElementById( "driver-class" );
    this.driverClass = tempList.getValue();

    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "command-line-arguments" );
    sConf.cmdLineArgs = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "classes-output-key-class" );
    aConf.outputKeyClass = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "classes-output-value-class" );
    aConf.outputValueClass = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "classes-mapper-class" );
    aConf.mapperClass = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "classes-reducer-class" );
    aConf.reducerClass = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "input-path" );
    aConf.inputPath = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "output-path" );
    aConf.outputPath = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "classes-input-format" );
    aConf.inputFormatClass = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "classes-output-format" );
    aConf.outputFormatClass = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "num-map-tasks" );
    aConf.numMapTasks = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "num-reduce-tasks" );
    aConf.numReduceTasks = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "logging-interval" );
    aConf.loggingInterval = ( (Text) tempBox.getTextControl() ).getText();

    JfaceMenuList<?> ncBox = (JfaceMenuList<?>) getXulDomContainer().getDocumentRoot().getElementById( "named-clusters" );
    aConf.setClusterName( ncBox.getSelectedItem() );

    if ( !isSimple() && aConf.getClusterName() != null ) {
      NamedCluster nc = null;
      try {
        nc = NamedClusterUIHelper.getNamedCluster( aConf.getClusterName() );
      } catch ( MetaStoreException e ) {
        openErrorDialog( BaseMessages.getString( PKG, "Dialog.Error" ), e.getMessage() );
      }
  
      if ( nc != null ) {
        aConf.setHdfsHostname( nc.getHdfsHost() );
        aConf.setHdfsPort( nc.getHdfsPort() );
        aConf.setJobTrackerHostname( nc.getJobTrackerHost() );
        aConf.setJobTrackerPort( nc.getJobTrackerPort() );
      }    
    }
    
    String validationErrors = "";
    if ( StringUtil.isEmpty( jobEntryName ) ) {
      validationErrors += BaseMessages.getString( PKG, "JobEntryHadoopJobExecutor.JobEntryName.Error" ) + "\n";
    }
    if ( StringUtil.isEmpty( hadoopJobName ) ) {
      validationErrors += BaseMessages.getString( PKG, "JobEntryHadoopJobExecutor.HadoopJobName.Error" ) + "\n";
    }
    
    if ( !StringUtil.isEmpty( validationErrors ) ) {
      openErrorDialog( BaseMessages.getString( PKG, "Dialog.Error" ), validationErrors );
      // show validation errors dialog
      return;
    }    
    
    // common/simple
    jobEntry.setName( jobEntryName );
    jobEntry.setHadoopJobName( hadoopJobName );
    jobEntry.setSimple( isSimple );
    jobEntry.setJarUrl( jarUrl );
    jobEntry.setDriverClass( driverClass );
    jobEntry.setCmdLineArgs( sConf.getCommandLineArgs() );
    jobEntry.setSimpleBlocking( sConf.isSimpleBlocking() );
    jobEntry.setSimpleLoggingInterval( sConf.getSimpleLoggingInterval() );
    // advanced config
    jobEntry.setBlocking( aConf.isBlocking() );
    jobEntry.setLoggingInterval( aConf.getLoggingInterval() );
    jobEntry.setMapperClass( aConf.getMapperClass() );
    jobEntry.setCombinerClass( aConf.getCombinerClass() );
    jobEntry.setReducerClass( aConf.getReducerClass() );
    jobEntry.setInputPath( aConf.getInputPath() );
    jobEntry.setInputFormatClass( aConf.getInputFormatClass() );
    jobEntry.setOutputPath( aConf.getOutputPath() );
    jobEntry.setOutputKeyClass( aConf.getOutputKeyClass() );
    jobEntry.setOutputValueClass( aConf.getOutputValueClass() );
    jobEntry.setOutputFormatClass( aConf.getOutputFormatClass() );
    
    jobEntry.setClusterName( aConf.getClusterName() );
    jobEntry.setHdfsHostname( aConf.getHdfsHostname() );
    jobEntry.setHdfsPort( aConf.getHdfsPort() );
    jobEntry.setJobTrackerHostname( aConf.getJobTrackerHostname() );
    jobEntry.setJobTrackerPort( aConf.getJobTrackerPort() );
    jobEntry.setNumMapTasks( aConf.getNumMapTasks() );
    jobEntry.setNumReduceTasks( aConf.getNumReduceTasks() );
    jobEntry.setUserDefined( userDefined );

    jobEntry.setChanged();

    cancel();
  }

  public void init() throws XulDomException {
    if ( jobEntry != null ) {
      // common/simple
      setName( jobEntry.getName() );
      setJobEntryName( jobEntry.getName() );
      setHadoopJobName( jobEntry.getHadoopJobName() );
      setSimple( jobEntry.isSimple() );
      setJarUrl( jobEntry.getJarUrl() );
      populateDriverMenuList();
      setDriverClass( jobEntry.getDriverClass() );
      sConf.setCommandLineArgs( jobEntry.getCmdLineArgs() );
      sConf.setSimpleBlocking( jobEntry.isSimpleBlocking() );
      sConf.setSimpleLoggingInterval( jobEntry.getSimpleLoggingInterval() );
      // advanced config
      userDefined.clear();
      if ( jobEntry.getUserDefined() != null ) {
        userDefined.addAll( jobEntry.getUserDefined() );
      }

      VariableSpace varSpace = getVariableSpace();
      ExtTextbox tempBox;
      tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "jobentry-hadoopjob-name" );
      tempBox.setVariableSpace( varSpace );
      tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "jar-url" );
      tempBox.setVariableSpace( varSpace );
      tempBox.setVariableSpace( varSpace );
      tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "command-line-arguments" );
      tempBox.setVariableSpace( varSpace );
      tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "classes-output-key-class" );
      tempBox.setVariableSpace( varSpace );
      tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "classes-output-value-class" );
      tempBox.setVariableSpace( varSpace );
      tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "classes-mapper-class" );
      tempBox.setVariableSpace( varSpace );
      tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "classes-combiner-class" );
      tempBox.setVariableSpace( varSpace );
      tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "classes-reducer-class" );
      tempBox.setVariableSpace( varSpace );
      tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "input-path" );
      tempBox.setVariableSpace( varSpace );
      tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "output-path" );
      tempBox.setVariableSpace( varSpace );
      tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "classes-input-format" );
      tempBox.setVariableSpace( varSpace );
      tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "classes-output-format" );
      tempBox.setVariableSpace( varSpace );
      tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "num-map-tasks" );
      tempBox.setVariableSpace( varSpace );
      tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "num-reduce-tasks" );
      tempBox.setVariableSpace( varSpace );
      tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "logging-interval" );
      tempBox.setVariableSpace( varSpace );
      tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "simple-logging-interval" );
      tempBox.setVariableSpace( varSpace );

      aConf.setBlocking( jobEntry.isBlocking() );
      aConf.setLoggingInterval( jobEntry.getLoggingInterval() );
      aConf.setMapperClass( jobEntry.getMapperClass() );
      aConf.setCombinerClass( jobEntry.getCombinerClass() );
      aConf.setReducerClass( jobEntry.getReducerClass() );
      aConf.setInputPath( jobEntry.getInputPath() );
      aConf.setInputFormatClass( jobEntry.getInputFormatClass() );
      aConf.setOutputPath( jobEntry.getOutputPath() );
      aConf.setOutputKeyClass( jobEntry.getOutputKeyClass() );
      aConf.setOutputValueClass( jobEntry.getOutputValueClass() );
      aConf.setOutputFormatClass( jobEntry.getOutputFormatClass() );
      aConf.setClusterName( jobEntry.getClusterName() );
      aConf.setHdfsHostname( jobEntry.getHdfsHostname() );
      aConf.setHdfsPort( jobEntry.getHdfsPort() );
      aConf.setJobTrackerHostname( jobEntry.getJobTrackerHostname() );
      aConf.setJobTrackerPort( jobEntry.getJobTrackerPort() );
      aConf.setNumMapTasks( jobEntry.getNumMapTasks() );
      aConf.setNumReduceTasks( jobEntry.getNumReduceTasks() );
    }
  }

  public void setJobMeta( JobMeta jobMeta ) {
    this.jobMeta = jobMeta;
  }  
  
  public void cancel() {
    XulDialog xulDialog = (XulDialog) getXulDomContainer().getDocumentRoot().getElementById( "job-entry-dialog" );

    Shell shell = (Shell) xulDialog.getRootObject();
    if ( !shell.isDisposed() ) {
      WindowProperty winprop = new WindowProperty( shell );
      PropsUI.getInstance().setScreen( winprop );
      ( (Composite) xulDialog.getManagedObject() ).dispose();
      shell.dispose();
    }
  }

  public void browseJar() {
    XulDialog xulDialog = (XulDialog) getXulDomContainer().getDocumentRoot().getElementById( "job-entry-dialog" );
    Shell shell = (Shell) xulDialog.getRootObject();
    FileDialog dialog = new FileDialog( shell, SWT.OPEN );
    dialog.setFilterExtensions( new String[] { "*.jar;*.zip" } );
    dialog.setFilterNames( new String[] { "Java Archives (jar)" } );
    String prevName = jobEntry.environmentSubstitute( jarUrl );
    String parentFolder = null;
    try {
      parentFolder =
          KettleVFS.getFilename( KettleVFS.getFileObject( jobEntry.environmentSubstitute( jobEntry.getFilename() ) )
              .getParent() );
    } catch ( Exception e ) {
      // not that important
    }
    if ( !Const.isEmpty( prevName ) ) {
      try {
        if ( KettleVFS.fileExists( prevName ) ) {
          dialog.setFilterPath( KettleVFS.getFilename( KettleVFS.getFileObject( prevName ).getParent() ) );
        } else {

          if ( !prevName.endsWith( ".jar" ) && !prevName.endsWith( ".zip" ) ) {
            prevName = "${" + Const.INTERNAL_VARIABLE_JOB_FILENAME_DIRECTORY + "}/" + Const.trim( jarUrl ) + ".jar";
          }
          if ( KettleVFS.fileExists( prevName ) ) {
            setJarUrl( prevName );
            return;
          }
        }
      } catch ( Exception e ) {
        dialog.setFilterPath( parentFolder );
      }
    } else if ( !Const.isEmpty( parentFolder ) ) {
      dialog.setFilterPath( parentFolder );
    }

    String fname = dialog.open();
    if ( fname != null ) {
      File file = new File( fname );
      String name = file.getName();
      String parentFolderSelection = file.getParentFile().toString();

      if ( !Const.isEmpty( parentFolder ) && parentFolder.equals( parentFolderSelection ) ) {
        setJarUrl( "${" + Const.INTERNAL_VARIABLE_JOB_FILENAME_DIRECTORY + "}/" + name );
      } else {
        setJarUrl( fname );
      }

      populateDriverMenuList();
    }
  }

  public void newUserDefinedItem() {
    userDefined.add( new UserDefinedItem() );
  }

  public SimpleConfiguration getSimpleConfiguration() {
    return sConf;
  }

  public AdvancedConfiguration getAdvancedConfiguration() {
    return aConf;
  }

  public AbstractModelList<UserDefinedItem> getUserDefined() {
    return userDefined;
  }

  @Override
  public String getName() {
    return "jobEntryController"; //$NON-NLS-1$
  }

  public String getJobEntryName() {
    return jobEntryName;
  }

  public void setJobEntryName( String jobEntryName ) {
    String previousVal = this.jobEntryName;
    String newVal = jobEntryName;

    this.jobEntryName = jobEntryName;
    firePropertyChange( JobEntryHadoopJobExecutorController.JOB_ENTRY_NAME, previousVal, newVal );
  }

  public String getHadoopJobName() {
    return hadoopJobName;
  }

  public void setHadoopJobName( String hadoopJobName ) {
    String previousVal = this.hadoopJobName;
    String newVal = hadoopJobName;

    this.hadoopJobName = hadoopJobName;
    firePropertyChange( JobEntryHadoopJobExecutorController.HADOOP_JOB_NAME, previousVal, newVal );
  }

  public String getJarUrl() {
    return jarUrl;
  }

  public String getDriverClass() {
    return driverClass;
  }

  public List<String> getDriverClasses() {
    return driverClasses;
  }

  public void setJarUrl( String jarUrl ) {
    String previousVal = this.jarUrl;
    String newVal = jarUrl;

    this.jarUrl = jarUrl;
    firePropertyChange( JobEntryHadoopJobExecutorController.JAR_URL, previousVal, newVal );
  }

  public void setDriverClass( String driverClass ) {
    String previousVal = this.driverClass;
    String newVal = driverClass;

    this.driverClass = driverClass;
    firePropertyChange( JobEntryHadoopJobExecutorController.DRIVER_CLASS, previousVal, newVal );
  }

  public void setDriverClasses( List<String> driverClasses ) {
    List<String> previousVal = this.driverClasses;
    List<String> newVal = driverClasses;

    this.driverClasses = driverClasses;
    firePropertyChange( JobEntryHadoopJobExecutorController.DRIVER_CLASSES, previousVal, newVal );
  }

  public boolean isSimple() {
    return isSimple;
  }

  public void setSimple( boolean isSimple ) {
    ( (XulVbox) getXulDomContainer().getDocumentRoot().getElementById( "advanced-configuration" ) )
        .setVisible( !isSimple ); //$NON-NLS-1$
    ( (XulVbox) getXulDomContainer().getDocumentRoot().getElementById( "simple-configuration" ) ).setVisible( isSimple ); //$NON-NLS-1$

    boolean previousVal = this.isSimple;
    boolean newVal = isSimple;

    this.isSimple = isSimple;
    firePropertyChange( JobEntryHadoopJobExecutorController.IS_SIMPLE, previousVal, newVal );
  }

  public void invertSimpleBlocking() {
    sConf.setSimpleBlocking( !sConf.isSimpleBlocking() );
  }

  public void invertBlocking() {
    aConf.setBlocking( !aConf.isBlocking() );
  }

  public JobEntryHadoopJobExecutor getJobEntry() {
    return jobEntry;
  }

  public void setJobEntry( JobEntryHadoopJobExecutor jobEntry ) {
    this.jobEntry = jobEntry;
  }

  public List<NamedCluster> getNamedClusters() {
    namedClusters = NamedClusterUIHelper.getNamedClusters();
    return namedClusters;
  }
  
  public void setNamedClusters( List <NamedCluster> namedClusters ) {
    this.namedClusters = namedClusters;
  }
  
  public void openErrorDialog( String title, String message ) {
    XulDialog errorDialog = (XulDialog) getXulDomContainer().getDocumentRoot().getElementById( "hadoop-error-dialog" );
    errorDialog.setTitle( title );

    XulTextbox errorMessage =
        (XulTextbox) getXulDomContainer().getDocumentRoot().getElementById( "hadoop-error-message" );
    errorMessage.setValue( message );

    errorDialog.show();
  }  
  
  public void closeErrorDialog() {
    XulDialog errorDialog = (XulDialog) getXulDomContainer().getDocumentRoot().getElementById( "hadoop-error-dialog" );
    errorDialog.hide();
  }
  
  public class SimpleConfiguration extends XulEventSourceAdapter {
    public static final String CMD_LINE_ARGS = "commandLineArgs"; //$NON-NLS-1$
    public static final String BLOCKING = "simpleBlocking"; //$NON-NLS-1$
    public static final String LOGGING_INTERVAL = "simpleLoggingInterval"; //$NON-NLS-1$

    private String cmdLineArgs;
    private boolean simpleBlocking;
    private String simpleLoggingInterval = "60";

    public String getCommandLineArgs() {
      return cmdLineArgs;
    }

    public void setCommandLineArgs( String cmdLineArgs ) {
      String previousVal = this.cmdLineArgs;
      String newVal = cmdLineArgs;

      this.cmdLineArgs = cmdLineArgs;

      firePropertyChange( SimpleConfiguration.CMD_LINE_ARGS, previousVal, newVal );
    }

    public boolean isSimpleBlocking() {
      return simpleBlocking;
    }

    public void setSimpleBlocking( boolean simpleBlocking ) {
      boolean old = this.simpleBlocking;
      this.simpleBlocking = simpleBlocking;
      firePropertyChange( SimpleConfiguration.BLOCKING, old, this.simpleBlocking );
    }

    public String getSimpleLoggingInterval() {
      return simpleLoggingInterval;
    }

    public void setSimpleLoggingInterval( String simpleLoggingInterval ) {
      String old = this.simpleLoggingInterval;
      this.simpleLoggingInterval = simpleLoggingInterval;
      firePropertyChange( SimpleConfiguration.LOGGING_INTERVAL, old, this.simpleLoggingInterval );
    }
  }

  public class AdvancedConfiguration extends XulEventSourceAdapter {
    public static final String OUTPUT_KEY_CLASS = "outputKeyClass"; //$NON-NLS-1$
    public static final String OUTPUT_VALUE_CLASS = "outputValueClass"; //$NON-NLS-1$
    public static final String MAPPER_CLASS = "mapperClass"; //$NON-NLS-1$
    public static final String COMBINER_CLASS = "combinerClass"; //$NON-NLS-1$
    public static final String REDUCER_CLASS = "reducerClass"; //$NON-NLS-1$
    public static final String INPUT_FORMAT_CLASS = "inputFormatClass"; //$NON-NLS-1$
    public static final String OUTPUT_FORMAT_CLASS = "outputFormatClass"; //$NON-NLS-1$
    public static final String INPUT_PATH = "inputPath"; //$NON-NLS-1$
    public static final String OUTPUT_PATH = "outputPath"; //$NON-NLS-1$
    public static final String BLOCKING = "blocking"; //$NON-NLS-1$
    public static final String LOGGING_INTERVAL = "loggingInterval"; //$NON-NLS-1$
    public static final String HDFS_HOSTNAME = "hdfsHostname"; //$NON-NLS-1$
    public static final String HDFS_PORT = "hdfsPort"; //$NON-NLS-1$
    public static final String JOB_TRACKER_HOSTNAME = "jobTrackerHostname"; //$NON-NLS-1$
    public static final String JOB_TRACKER_PORT = "jobTrackerPort"; //$NON-NLS-1$
    public static final String NUM_MAP_TASKS = "numMapTasks"; //$NON-NLS-1$
    public static final String NUM_REDUCE_TASKS = "numReduceTasks"; //$NON-NLS-1$

    private String outputKeyClass;
    private String outputValueClass;
    private String mapperClass;
    private String combinerClass;
    private String reducerClass;
    private String inputFormatClass;
    private String outputFormatClass;

    private NamedCluster selectedNamedCluster;
    
    private String clusterName;
    private String hdfsHostname;
    private String hdfsPort;
    private String jobTrackerHostname;
    private String jobTrackerPort;
    private String inputPath;
    private String outputPath;

    private String numMapTasks = "1";
    private String numReduceTasks = "1";

    private boolean blocking;
    private String loggingInterval = "60"; // 60 seconds

    public String getOutputKeyClass() {
      return outputKeyClass;
    }

    public void setOutputKeyClass( String outputKeyClass ) {
      String previousVal = this.outputKeyClass;
      String newVal = outputKeyClass;

      this.outputKeyClass = outputKeyClass;
      firePropertyChange( AdvancedConfiguration.OUTPUT_KEY_CLASS, previousVal, newVal );
    }

    public String getOutputValueClass() {
      return outputValueClass;
    }

    public void setOutputValueClass( String outputValueClass ) {
      String previousVal = this.outputValueClass;
      String newVal = outputValueClass;

      this.outputValueClass = outputValueClass;
      firePropertyChange( AdvancedConfiguration.OUTPUT_VALUE_CLASS, previousVal, newVal );
    }

    public String getMapperClass() {
      return mapperClass;
    }

    public void setMapperClass( String mapperClass ) {
      String previousVal = this.mapperClass;
      String newVal = mapperClass;

      this.mapperClass = mapperClass;
      firePropertyChange( AdvancedConfiguration.MAPPER_CLASS, previousVal, newVal );
    }

    public String getCombinerClass() {
      return combinerClass;
    }

    public void setCombinerClass( String combinerClass ) {
      String previousVal = this.combinerClass;
      String newVal = combinerClass;

      this.combinerClass = combinerClass;
      firePropertyChange( AdvancedConfiguration.COMBINER_CLASS, previousVal, newVal );
    }

    public String getReducerClass() {
      return reducerClass;
    }

    public void setReducerClass( String reducerClass ) {
      String previousVal = this.reducerClass;
      String newVal = reducerClass;

      this.reducerClass = reducerClass;
      firePropertyChange( AdvancedConfiguration.REDUCER_CLASS, previousVal, newVal );
    }

    public String getInputFormatClass() {
      return inputFormatClass;
    }

    public void setInputFormatClass( String inputFormatClass ) {
      String previousVal = this.inputFormatClass;
      String newVal = inputFormatClass;

      this.inputFormatClass = inputFormatClass;
      firePropertyChange( AdvancedConfiguration.INPUT_FORMAT_CLASS, previousVal, newVal );
    }

    public String getOutputFormatClass() {
      return outputFormatClass;
    }

    public void setOutputFormatClass( String outputFormatClass ) {
      String previousVal = this.outputFormatClass;
      String newVal = outputFormatClass;

      this.outputFormatClass = outputFormatClass;
      firePropertyChange( AdvancedConfiguration.OUTPUT_FORMAT_CLASS, previousVal, newVal );
    }

    public String getClusterName() {
      return clusterName;
    }

    public void setClusterName(String clusterName) {
      this.clusterName = clusterName;
    }    
    
    public String getHdfsHostname() {
      return hdfsHostname;
    }

    public void setHdfsHostname( String hdfsHostname ) {
      String previousVal = this.hdfsHostname;
      String newVal = hdfsHostname;

      this.hdfsHostname = hdfsHostname;
      firePropertyChange( AdvancedConfiguration.HDFS_HOSTNAME, previousVal, newVal );
    }

    public String getHdfsPort() {
      return hdfsPort;
    }

    public void setHdfsPort( String hdfsPort ) {
      String previousVal = this.hdfsPort;
      String newVal = hdfsPort;

      this.hdfsPort = hdfsPort;
      firePropertyChange( AdvancedConfiguration.HDFS_PORT, previousVal, newVal );
    }

    public String getJobTrackerHostname() {
      return jobTrackerHostname;
    }

    public void setJobTrackerHostname( String jobTrackerHostname ) {
      String previousVal = this.jobTrackerHostname;
      String newVal = jobTrackerHostname;

      this.jobTrackerHostname = jobTrackerHostname;
      firePropertyChange( AdvancedConfiguration.JOB_TRACKER_HOSTNAME, previousVal, newVal );
    }

    public String getJobTrackerPort() {
      return jobTrackerPort;
    }

    public void setJobTrackerPort( String jobTrackerPort ) {
      String previousVal = this.jobTrackerPort;
      String newVal = jobTrackerPort;

      this.jobTrackerPort = jobTrackerPort;
      firePropertyChange( AdvancedConfiguration.JOB_TRACKER_PORT, previousVal, newVal );
    }

    public String getInputPath() {
      return inputPath;
    }

    public void setInputPath( String inputPath ) {
      String previousVal = this.inputPath;
      String newVal = inputPath;

      this.inputPath = inputPath;
      firePropertyChange( AdvancedConfiguration.INPUT_PATH, previousVal, newVal );
    }

    public String getOutputPath() {
      return outputPath;
    }

    public void setOutputPath( String outputPath ) {
      String previousVal = this.outputPath;
      String newVal = outputPath;

      this.outputPath = outputPath;
      firePropertyChange( AdvancedConfiguration.OUTPUT_PATH, previousVal, newVal );
    }

    public boolean isBlocking() {
      return blocking;
    }

    public void setBlocking( boolean blocking ) {
      boolean previousVal = this.blocking;
      boolean newVal = blocking;

      this.blocking = blocking;
      firePropertyChange( AdvancedConfiguration.BLOCKING, previousVal, newVal );
    }

    public String getLoggingInterval() {
      return loggingInterval;
    }

    public void setLoggingInterval( String loggingInterval ) {
      String previousVal = this.loggingInterval;
      String newVal = loggingInterval;

      this.loggingInterval = loggingInterval;
      firePropertyChange( AdvancedConfiguration.LOGGING_INTERVAL, previousVal, newVal );
    }

    public String getNumMapTasks() {
      return numMapTasks;
    }

    public void setNumMapTasks( String numMapTasks ) {
      String previousVal = this.numMapTasks;
      String newVal = numMapTasks;

      this.numMapTasks = numMapTasks;
      firePropertyChange( AdvancedConfiguration.NUM_MAP_TASKS, previousVal, newVal );
    }

    public String getNumReduceTasks() {
      return numReduceTasks;
    }

    public void setNumReduceTasks( String numReduceTasks ) {
      String previousVal = this.numReduceTasks;
      String newVal = numReduceTasks;

      this.numReduceTasks = numReduceTasks;
      firePropertyChange( AdvancedConfiguration.NUM_REDUCE_TASKS, previousVal, newVal );
    }
    
    public void editNamedCluster() {
      if ( isSelectedNamedCluster() ) {
        Spoon spoon = Spoon.getInstance();
        XulDialog xulDialog = (XulDialog) getXulDomContainer().getDocumentRoot().getElementById( "job-entry-dialog" );
        Shell shell = (Shell) xulDialog.getRootObject();
        ncDelegate.editNamedCluster( null, this.selectedNamedCluster, shell );
        firePropertyChange( "namedClusters", this.selectedNamedCluster, getNamedClusters() );
      }
    }
    
    public void newNamedCluster() {
      Spoon spoon = Spoon.getInstance();
      XulDialog xulDialog = (XulDialog) getXulDomContainer().getDocumentRoot().getElementById( "job-entry-dialog" );
      Shell shell = (Shell) xulDialog.getRootObject();
      ncDelegate.newNamedCluster( jobMeta, null, shell );
      firePropertyChange( "namedClusters", null, getNamedClusters() );
      selectNamedCluster();
    }
    
    private void selectNamedCluster() {
      @SuppressWarnings("unchecked")
      XulMenuList<NamedCluster> namedClusterMenu = (XulMenuList<NamedCluster>) getXulDomContainer().getDocumentRoot().getElementById( "named-clusters" ); //$NON-NLS-1$
      for ( NamedCluster nc : getNamedClusters() ) {
        String cn = JobEntryHadoopJobExecutorController.this.jobEntry.getClusterName();
        if ( cn != null && cn.equals( nc.getName() ) ) {
          namedClusterMenu.setSelectedItem( nc );
          setSelectedNamedCluster( nc );
        }
      }    
    }  
    
    public void setSelectedNamedCluster( NamedCluster namedCluster ) {
      this.selectedNamedCluster = namedCluster;
    }
    
    public boolean isSelectedNamedCluster() {
      return this.selectedNamedCluster != null;
    }    
    
  }

  public void editNamedCluster() {
    try {
      Spoon spoon = Spoon.getInstance();
      XulDialog xulDialog = (XulDialog) getXulDomContainer().getDocumentRoot().getElementById( "job-entry-dialog" );
      Shell shell = (Shell) xulDialog.getRootObject();
      NamedCluster namedCluster;
      if ( aConf.isSelectedNamedCluster() ) {
        namedCluster = aConf.selectedNamedCluster;
      } else {
        namedCluster = getLocalCluster();
      }
      ncDelegate.editNamedCluster( null, namedCluster, shell );
      firePropertyChange( "namedClusters", namedCluster, getNamedClusters() );
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  public void newNamedCluster() {
    try {
    Spoon spoon = Spoon.getInstance();
    XulDialog xulDialog = (XulDialog) getXulDomContainer().getDocumentRoot().getElementById( "job-entry-dialog" );
    Shell shell = (Shell) xulDialog.getRootObject();
    ncDelegate.newNamedCluster( jobMeta, null, shell );
    firePropertyChange( "namedClusters", null, getNamedClusters() );
    aConf.selectNamedCluster();
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  private NamedCluster getLocalCluster() {
    NamedCluster local = NamedClusterManager.getInstance().getClusterTemplate();
    local.setHdfsHost( aConf.getHdfsHostname() );
    local.setHdfsPort( aConf.getHdfsPort() );
    local.setJobTrackerHost( aConf.getJobTrackerHostname() );
    local.setJobTrackerPort( aConf.getJobTrackerPort() );
    return local;
  }

  public void help() {
    XulDialog xulDialog = (XulDialog) getXulDomContainer().getDocumentRoot().getRootElement().getFirstChild();
    Shell shell = (Shell) xulDialog.getRootObject();
    PluginInterface plugin =
        PluginRegistry.getInstance().findPluginWithId( JobEntryPluginType.class, jobEntry.getPluginId() );
    HelpUtils.openHelpDialog( shell, plugin );
  }

  private void populateDriverMenuList() {
    if ( Const.isEmpty( jarUrl ) ) {
      return;
    }
    try {
      URL resolvedJarUrl = jobEntry.resolveJarUrl( jarUrl );
      HadoopShim shim =
        HadoopConfigurationBootstrap.getHadoopConfigurationProvider().getActiveConfiguration().getHadoopShim();
      JarUtility jarUtil = new JarUtility();
      List<Class<?>> clazzes = jarUtil.getClassesInJarWithMain(
        resolvedJarUrl.toExternalForm(), shim.getClass().getClassLoader() );
      List<String> driverClasses = new ArrayList<String>();
      for ( Class<?> clazz : clazzes ) {
        driverClasses.add( clazz.getName() );
      }
      if ( Const.isEmpty( driverClass ) ) {
        setDriverClasses( driverClasses );
        Class<?> mainClass;
        try {
          mainClass = jarUtil.getMainClassFromManifest( resolvedJarUrl, shim.getClass().getClassLoader() );
        } catch ( Exception e ) {
          mainClass = null;
        }
        if ( mainClass != null ) {
          setDriverClass( mainClass.getName() );
        } else if ( !driverClasses.isEmpty() ) {
          setDriverClass(driverClasses.get( 0 ) );
        }
      } else {
        String saveDriverClass = driverClass;
        setDriverClasses( driverClasses );
        setDriverClass( saveDriverClass );
      }
    } catch ( Throwable e ) {
      XulDialog xulDialog = (XulDialog) getXulDomContainer().getDocumentRoot().getRootElement();
      Shell shell = (Shell) xulDialog.getRootObject();
      new ErrorDialog( shell, "Error", "Unable to populate Driver Class list", e );
      setDriverClasses( Collections.<String>emptyList() );
    }
  }
}
