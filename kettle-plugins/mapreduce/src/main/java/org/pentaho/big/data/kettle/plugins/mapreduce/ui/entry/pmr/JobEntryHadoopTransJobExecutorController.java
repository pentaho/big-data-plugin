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

package org.pentaho.big.data.kettle.plugins.mapreduce.ui.entry.pmr;

import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.big.data.kettle.plugins.mapreduce.entry.pmr.JobEntryHadoopTransJobExecutor;
import org.pentaho.big.data.kettle.plugins.mapreduce.entry.UserDefinedItem;
import org.pentaho.big.data.plugins.common.ui.HadoopClusterDelegateImpl;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.JobEntryPluginType;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.database.dialog.tags.ExtTextbox;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.repository.dialog.SelectObjectDialog;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.util.HelpUtils;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.ui.xul.components.XulTextbox;
import org.pentaho.ui.xul.containers.XulDialog;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;
import org.pentaho.ui.xul.jface.tags.JfaceMenuList;
import org.pentaho.ui.xul.util.AbstractModelList;

import com.google.common.annotations.VisibleForTesting;

import java.io.File;
import java.util.List;


public class JobEntryHadoopTransJobExecutorController extends AbstractXulEventHandler {

  private static final Class<?> PKG = JobEntryHadoopTransJobExecutor.class;

  public static final String JOB_ENTRY_NAME = "jobEntryName"; //$NON-NLS-1$
  public static final String HADOOP_JOB_NAME = "hadoopJobName"; //$NON-NLS-1$
  public static final String MAP_TRANS = "mapTrans"; //$NON-NLS-1$
  public static final String COMBINER_TRANS = "combinerTrans"; //$NON-NLS-1$
  public static final String REDUCE_TRANS = "reduceTrans"; //$NON-NLS-1$

  public static final String MAP_TRANS_INPUT_STEP_NAME = "mapTransInputStepName"; //$NON-NLS-1$
  public static final String MAP_TRANS_OUTPUT_STEP_NAME = "mapTransOutputStepName"; //$NON-NLS-1$
  public static final String COMBINER_TRANS_INPUT_STEP_NAME = "combinerTransInputStepName"; //$NON-NLS-1$
  public static final String COMBINER_TRANS_OUTPUT_STEP_NAME = "combinerTransOutputStepName"; //$NON-NLS-1$
  public static final String COMBINING_SINGLE_THREADED = "combiningSingleThreaded"; //$NON-NLS-1$
  public static final String REDUCE_TRANS_INPUT_STEP_NAME = "reduceTransInputStepName"; //$NON-NLS-1$
  public static final String REDUCE_TRANS_OUTPUT_STEP_NAME = "reduceTransOutputStepName"; //$NON-NLS-1$
  public static final String REDUCING_SINGLE_THREADED = "reducingSingleThreaded"; //$NON-NLS-1$

  public static final String SUPPRESS_OUTPUT_MAP_KEY = "suppressOutputOfMapKey";
  public static final String SUPPRESS_OUTPUT_MAP_VALUE = "suppressOutputOfMapValue";
  public static final String SUPPRESS_OUTPUT_KEY = "suppressOutputOfKey";
  public static final String SUPPRESS_OUTPUT_VALUE = "suppressOutputOfValue";
  public static final String MAP_OUTPUT_KEY_CLASS = "mapOutputKeyClass"; //$NON-NLS-1$
  public static final String MAP_OUTPUT_VALUE_CLASS = "mapOutputValueClass"; //$NON-NLS-1$
  public static final String OUTPUT_KEY_CLASS = "outputKeyClass"; //$NON-NLS-1$
  public static final String OUTPUT_VALUE_CLASS = "outputValueClass"; //$NON-NLS-1$
  public static final String INPUT_FORMAT_CLASS = "inputFormatClass"; //$NON-NLS-1$
  public static final String OUTPUT_FORMAT_CLASS = "outputFormatClass"; //$NON-NLS-1$
  public static final String INPUT_PATH = "inputPath"; //$NON-NLS-1$
  public static final String OUTPUT_PATH = "outputPath"; //$NON-NLS-1$
  public static final String CLEAN_OUTPUT_PATH = "cleanOutputPath"; //$NON-NLS-1$
  public static final String BLOCKING = "blocking"; //$NON-NLS-1$
  public static final String LOGGING_INTERVAL = "loggingInterval"; //$NON-NLS-1$

  public static final String HDFS_HOSTNAME = "hdfsHostname"; //$NON-NLS-1$
  public static final String HDFS_PORT = "hdfsPort"; //$NON-NLS-1$
  public static final String JOB_TRACKER_HOSTNAME = "jobTrackerHostname"; //$NON-NLS-1$
  public static final String JOB_TRACKER_PORT = "jobTrackerPort"; //$NON-NLS-1$

  public static final String NUM_MAP_TASKS = "numMapTasks"; //$NON-NLS-1$
  public static final String NUM_REDUCE_TASKS = "numReduceTasks"; //$NON-NLS-1$

  public static final String USER_DEFINED = "userDefined"; //$NON-NLS-1$

  public static final String LOCAL = "local";
  public static final String REPOSITORY = "repository";


  private final NamedClusterService namedClusterService;

  private final HadoopClusterDelegateImpl ncDelegate;

  private String jobEntryName;
  private String hadoopJobName;

  private boolean suppressOutputMapKey;
  private boolean suppressOutputMapValue;
  private boolean suppressOutputKey;
  private boolean suppressOutputValue;

  private String inputFormatClass;
  private String outputFormatClass;

  private String inputPath;
  private String outputPath;

  private boolean cleanOutputPath;

  private String numMapTasks = "1";
  private String numReduceTasks = "1";

  private boolean blocking;
  private String loggingInterval = "60";

  private String mapTrans = "";

  private String combinerTrans = "";
  private boolean combiningSingleThreaded;

  private String reduceTrans = "";
  private boolean reducingSingleThreaded;

  private String mapTransInputStepName = "";
  private String mapTransOutputStepName = "";
  private String combinerTransInputStepName = "";
  private String combinerTransOutputStepName = "";
  private String reduceTransInputStepName = "";
  private String reduceTransOutputStepName = "";

  private static String storageType;
  private List<NamedCluster> namedClusters;

  protected Shell shell;
  private Repository rep;
  private JobMeta jobMeta;
  private NamedCluster selectedNamedCluster;

  private JobEntryHadoopTransJobExecutor jobEntry;

  private AbstractModelList<UserDefinedItem> userDefined = new AbstractModelList<UserDefinedItem>();

  public JobEntryHadoopTransJobExecutorController( HadoopClusterDelegateImpl ncDelegate,
      NamedClusterService namedClusterService ) throws Throwable {
    this.ncDelegate = ncDelegate;
    this.namedClusterService = namedClusterService;
  }

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
    ExtTextbox tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "jobentry-hadoopjob-name" );
    this.hadoopJobName = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "jobentry-map-transformation" );
    this.mapTrans = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "jobentry-map-input-stepname" );
    this.mapTransInputStepName = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "jobentry-map-output-stepname" );
    this.mapTransOutputStepName = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "jobentry-combiner-transformation" );
    this.combinerTrans = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "jobentry-combiner-input-stepname" );
    this.combinerTransInputStepName = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "jobentry-combiner-output-stepname" );
    this.combinerTransOutputStepName = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "jobentry-reduce-transformation" );
    this.reduceTrans = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "jobentry-reduce-input-stepname" );
    this.reduceTransInputStepName = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "jobentry-reduce-output-stepname" );
    this.reduceTransOutputStepName = ( (Text) tempBox.getTextControl() ).getText();

    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "input-path" );
    this.inputPath = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "output-path" );
    this.outputPath = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "classes-input-format" );
    this.inputFormatClass = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "classes-output-format" );
    this.outputFormatClass = ( (Text) tempBox.getTextControl() ).getText();

    JfaceMenuList<?> ncBox = (JfaceMenuList<?>) getXulDomContainer().getDocumentRoot().getElementById( "named-clusters" );

    try {
      selectedNamedCluster = namedClusterService.read( ncBox.getSelectedItem(), jobMeta.getMetaStore() );
    } catch ( MetaStoreException e ) {
      openErrorDialog( BaseMessages.getString( PKG, "Dialog.Error" ), e.getMessage() );
    }

    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "num-map-tasks" );
    this.numMapTasks = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "num-reduce-tasks" );
    this.numReduceTasks = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "logging-interval" );
    this.loggingInterval = ( (Text) tempBox.getTextControl() ).getText();

    String validationErrors = "";
    if ( StringUtil.isEmpty( jobEntryName ) ) {
      validationErrors += BaseMessages.getString( PKG, "JobEntryHadoopTransJobExecutor.JobEntryName.Error" ) + "\n";
    }
    if ( selectedNamedCluster == null ) {
      validationErrors += BaseMessages.getString( PKG, "JobEntryHadoopTransJobExecutor.NamedClusterNotProvided.Error" ) + "\n";
    }
    if ( StringUtil.isEmpty( hadoopJobName ) ) {
      validationErrors += BaseMessages.getString( PKG, "JobEntryHadoopTransJobExecutor.HadoopJobName.Error" ) + "\n";
    }
    if ( !StringUtils.isEmpty( numReduceTasks ) ) {
      String reduceS = getVariableSpace().environmentSubstitute( numReduceTasks );
      try {
        int numR = Integer.parseInt( reduceS );

        if ( numR < 0 ) {
          validationErrors +=
              BaseMessages.getString( PKG, "JobEntryHadoopTransJobExecutor.NumReduceTasks.Error" ) + "\n";
        }
      } catch ( NumberFormatException e ) {
        // omit
      }
    }
    if ( !StringUtils.isEmpty( numMapTasks ) ) {
      String mapS = getVariableSpace().environmentSubstitute( numMapTasks );

      try {
        int numM = Integer.parseInt( mapS );

        if ( numM < 0 ) {
          validationErrors += BaseMessages.getString( PKG, "JobEntryHadoopTransJobExecutor.NumMapTasks.Error" ) + "\n";
        }
      } catch ( NumberFormatException e ) {
        // omit
      }
    }

    if ( !StringUtil.isEmpty( validationErrors ) ) {
      openErrorDialog( BaseMessages.getString( PKG, "Dialog.Error" ), validationErrors );
      // show validation errors dialog
      return;
    }

    // common/simple
    jobEntry.setName( jobEntryName );
    jobEntry.setHadoopJobName( hadoopJobName );

    jobEntry.setMapTrans( mapTrans );
    jobEntry.setMapInputStepName( mapTransInputStepName );
    jobEntry.setMapOutputStepName( mapTransOutputStepName );

    jobEntry.setCombinerTrans( combinerTrans );
    jobEntry.setCombinerInputStepName( combinerTransInputStepName );
    jobEntry.setCombinerOutputStepName( combinerTransOutputStepName );
    jobEntry.setCombiningSingleThreaded( combiningSingleThreaded );


    jobEntry.setReduceTrans( reduceTrans );
    jobEntry.setReduceInputStepName( reduceTransInputStepName );
    jobEntry.setReduceOutputStepName( reduceTransOutputStepName );
    jobEntry.setReducingSingleThreaded( reducingSingleThreaded );

    // advanced config
    jobEntry.setBlocking( isBlocking() );
    jobEntry.setLoggingInterval( loggingInterval );
    jobEntry.setInputPath( getInputPath() );
    jobEntry.setInputFormatClass( getInputFormatClass() );
    jobEntry.setOutputPath( getOutputPath() );
    jobEntry.setCleanOutputPath( isCleanOutputPath() );

    jobEntry.setSuppressOutputOfMapKey( isSuppressOutputOfMapKey() );
    jobEntry.setSuppressOutputOfMapValue( isSuppressOutputOfMapValue() );

    jobEntry.setSuppressOutputOfKey( isSuppressOutputOfKey() );
    jobEntry.setSuppressOutputOfValue( isSuppressOutputOfValue() );

    jobEntry.setOutputFormatClass( getOutputFormatClass() );
    jobEntry.setNamedCluster( selectedNamedCluster );
    jobEntry.setNumMapTasks( getNumMapTasks() );
    jobEntry.setNumReduceTasks( getNumReduceTasks() );
    jobEntry.setUserDefined( userDefined );

    jobEntry.setChanged();

    cancel();
  }

  @SuppressWarnings( { "rawtypes" } )
  public void init() throws Throwable {
    if ( jobEntry != null ) {
      // common/simple
      setName( jobEntry.getName() );
      setJobEntryName( jobEntry.getName() );
      setHadoopJobName( jobEntry.getHadoopJobName() );

      // set variables
      VariableSpace varSpace = getVariableSpace();
      ExtTextbox tempBox;
      tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "jobentry-hadoopjob-name" );
      tempBox.setVariableSpace( varSpace );
      tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "jobentry-map-transformation" );
      tempBox.setVariableSpace( varSpace );
      tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "jobentry-map-input-stepname" );
      tempBox.setVariableSpace( varSpace );
      tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "jobentry-map-output-stepname" );
      tempBox.setVariableSpace( varSpace );
      tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "jobentry-combiner-transformation" );
      tempBox.setVariableSpace( varSpace );
      tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "jobentry-combiner-input-stepname" );
      tempBox.setVariableSpace( varSpace );
      tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "jobentry-combiner-output-stepname" );
      tempBox.setVariableSpace( varSpace );
      tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "jobentry-reduce-transformation" );
      tempBox.setVariableSpace( varSpace );
      tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "jobentry-reduce-input-stepname" );
      tempBox.setVariableSpace( varSpace );
      tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( "jobentry-reduce-output-stepname" );
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

      setCombinerTransInputStepName( jobEntry.getCombinerInputStepName() );
      setCombinerTransOutputStepName( jobEntry.getCombinerOutputStepName() );
      setCombiningSingleThreaded( jobEntry.isCombiningSingleThreaded() );

      // Load the map transformation into the UI
      if ( jobEntry.getMapTrans() != null || rep == null ) {
        setMapTrans( jobEntry.getMapTrans() );
      } else if ( jobEntry.getMapRepositoryReference() != null ) {
        // Load the repository directory and file for displaying to the user
        try {
          TransMeta transMeta = rep.loadTransformation( jobEntry.getMapRepositoryReference(), null );
          if ( transMeta != null && transMeta.getRepositoryDirectory() != null ) {
            setMapTrans( buildRepositoryPath( transMeta.getRepositoryDirectory().getPath(), transMeta.getName() ) );
          }
        } catch ( KettleException e ) {
          // The transformation cannot be loaded from the repository
          setMapTrans( null );
        }
      } else {
        setMapTrans( buildRepositoryPath( jobEntry.getMapRepositoryDir(), jobEntry.getMapRepositoryFile() ) );
      }
      setMapTransInputStepName( jobEntry.getMapInputStepName() );
      setMapTransOutputStepName( jobEntry.getMapOutputStepName() );

      // Load the combiner transformation into the UI
      if ( jobEntry.getCombinerTrans() != null || rep == null ) {
        setCombinerTrans( jobEntry.getCombinerTrans() );
      } else if ( jobEntry.getCombinerRepositoryReference() != null ) {
        // Load the repository directory and file for displaying to the user
        try {
          TransMeta transMeta = rep.loadTransformation( jobEntry.getCombinerRepositoryReference(), null );
          if ( transMeta != null && transMeta.getRepositoryDirectory() != null ) {
            setCombinerTrans( buildRepositoryPath( transMeta.getRepositoryDirectory().getPath(), transMeta.getName() ) );
          }
        } catch ( KettleException e ) {
          // The transformation cannot be loaded from the repository
          setCombinerTrans( null );
        }
      } else {
        setCombinerTrans( buildRepositoryPath( jobEntry.getCombinerRepositoryDir(), jobEntry.getCombinerRepositoryFile() ) );
      }

      // Load the reduce transformation into the UI
      if ( jobEntry.getReduceTrans() != null || rep == null ) {
        setReduceTrans( jobEntry.getReduceTrans() );
      } else if ( jobEntry.getReduceRepositoryReference() != null ) {
        // Load the repository directory and file for displaying to the user
        try {
          TransMeta transMeta = rep.loadTransformation( jobEntry.getReduceRepositoryReference(), null );
          if ( transMeta != null && transMeta.getRepositoryDirectory() != null ) {
            setReduceTrans( buildRepositoryPath( transMeta.getRepositoryDirectory().getPath(), transMeta.getName() ) );
          }
        } catch ( KettleException e ) {
          // The transformation cannot be loaded from the repository
          setReduceTrans( null );
        }
      } else {
        setReduceTrans( buildRepositoryPath( jobEntry.getReduceRepositoryDir(), jobEntry.getReduceRepositoryFile() ) );
      }

      setReduceTransInputStepName( jobEntry.getReduceInputStepName() );
      setReduceTransOutputStepName( jobEntry.getReduceOutputStepName() );
      setReducingSingleThreaded( jobEntry.isReducingSingleThreaded() );

      userDefined.clear();
      if ( jobEntry.getUserDefined() != null ) {
        userDefined.addAll( jobEntry.getUserDefined() );
      }
      setBlocking( jobEntry.isBlocking() );
      setLoggingInterval( jobEntry.getLoggingInterval() );
      setInputPath( jobEntry.getInputPath() );
      setInputFormatClass( jobEntry.getInputFormatClass() );
      setOutputPath( jobEntry.getOutputPath() );
      setCleanOutputPath( jobEntry.isCleanOutputPath() );

      setSuppressOutputOfMapKey( jobEntry.getSuppressOutputOfMapKey() );
      setSuppressOutputOfMapValue( jobEntry.getSuppressOutputOfMapValue() );

      setSuppressOutputOfKey( jobEntry.getSuppressOutputOfKey() );
      setSuppressOutputOfValue( jobEntry.getSuppressOutputOfValue() );
      setOutputFormatClass( jobEntry.getOutputFormatClass() );

      selectedNamedCluster = jobEntry.getNamedCluster();

      setNumMapTasks( jobEntry.getNumMapTasks() );
      setNumReduceTasks( jobEntry.getNumReduceTasks() );
      if ( Spoon.getInstance().getRepository() != null ) {
        storageType = REPOSITORY;
      } else {
        storageType = LOCAL;
      }
    }
  }

  public void setShell( Shell shell ) {
    this.shell = shell;
  }

  public void closeErrorDialog() {
    XulDialog errorDialog = (XulDialog) getXulDomContainer().getDocumentRoot().getElementById( "hadoop-error-dialog" );
    errorDialog.hide();
  }

  public void setRepository( Repository rep ) {
    this.rep = rep;
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

  private interface StringResultSetter {
    public void set( String val );
  }

  private interface ObjectIdResultSetter {
    public void set( ObjectId val );
  }

  public void mapTransBrowse() {
    if ( storageType.equalsIgnoreCase( LOCAL ) ) { //$NON-NLS-1$
      browseLocalFilesystem( JobEntryHadoopTransJobExecutorController.this::setMapTrans, mapTrans );
    } else if ( storageType.equalsIgnoreCase( REPOSITORY ) ) { //$NON-NLS-1$
      browseRepository( JobEntryHadoopTransJobExecutorController.this::setMapTrans );
    }
  }

  public void combinerTransBrowse() {
    if ( storageType.equalsIgnoreCase( LOCAL ) ) { //$NON-NLS-1$
      browseLocalFilesystem( JobEntryHadoopTransJobExecutorController.this::setCombinerTrans, mapTrans );
    } else if ( storageType.equalsIgnoreCase( REPOSITORY ) ) { //$NON-NLS-1$
      browseRepository( JobEntryHadoopTransJobExecutorController.this::setCombinerTrans );
    }
  }

  public void reduceTransBrowse() {
    if ( storageType.equalsIgnoreCase( LOCAL ) ) { //$NON-NLS-1$
      browseLocalFilesystem( JobEntryHadoopTransJobExecutorController.this::setReduceTrans, mapTrans );
    } else if ( storageType.equalsIgnoreCase( REPOSITORY ) ) { //$NON-NLS-1$
      browseRepository( JobEntryHadoopTransJobExecutorController.this::setReduceTrans );
    }
  }

  public void browseLocalFilesystem( StringResultSetter setter, String originalTransformationName ) {
    Shell shell = getJobEntryDialog();

    FileDialog dialog = new FileDialog( shell, SWT.OPEN );
    dialog.setFilterExtensions( Const.STRING_TRANS_FILTER_EXT );
    dialog.setFilterNames( Const.getTransformationFilterNames() );
    String prevName = jobEntry.environmentSubstitute( originalTransformationName );
    String parentFolder = null;
    try {
      parentFolder =
          KettleVFS.getFilename( KettleVFS.getFileObject( jobEntry.environmentSubstitute( jobEntry.getFilename() ) )
              .getParent() );
    } catch ( Exception e ) {
      // not that important
    }
    if ( !StringUtils.isEmpty( prevName ) ) {
      try {
        if ( KettleVFS.fileExists( prevName ) ) {
          dialog.setFilterPath( KettleVFS.getFilename( KettleVFS.getFileObject( prevName ).getParent() ) );
        } else {

          if ( !prevName.endsWith( ".ktr" ) ) {
            prevName =
                "${" + Const.INTERNAL_VARIABLE_JOB_FILENAME_DIRECTORY + "}/" + Const.trim( originalTransformationName )
                    + ".ktr";
          }
          if ( KettleVFS.fileExists( prevName ) ) {
            setter.set( prevName );
            return;
          }
        }
      } catch ( Exception e ) {
        dialog.setFilterPath( parentFolder );
      }
    } else if ( !StringUtils.isEmpty( parentFolder ) ) {
      dialog.setFilterPath( parentFolder );
    }

    String fname = dialog.open();
    if ( fname != null ) {
      File file = new File( fname );
      String name = file.getName();
      String parentFolderSelection = file.getParentFile().toString();

      if ( !StringUtils.isEmpty( parentFolder ) && parentFolder.equals( parentFolderSelection ) ) {
        setter.set( "${" + Const.INTERNAL_VARIABLE_JOB_FILENAME_DIRECTORY + "}/" + name );
      } else {
        setter.set( fname );
      }
    }
  }

  private void browseRepository( StringResultSetter transSetter ) {
    if ( rep != null ) {
      Shell shell = getJobEntryDialog();
      SelectObjectDialog sod = new SelectObjectDialog( shell, rep, true, false );
      String transname = sod.open();
      if ( transname != null ) {
        if ( transSetter != null ) {
          transSetter.set( buildRepositoryPath( sod.getDirectory().getPath(), sod.getObjectName() ) );
        }
      }
    }
  }

 /**
   * This method exists for consistency
   *
   * @param dir
   *          Null is unacceptable input, a blank string will be returned
   * @param file
   *          Null is unacceptable input, a blank string will be returned
   * @return
   */
  private String buildRepositoryPath( String dir, String file ) {
    if ( dir == null || file == null ) {
      return "";
    }

    if ( dir.endsWith( "/" ) ) {
      return dir + file;
    }

    return dir + "/" + file;
  }

  public void newUserDefinedItem() {
    userDefined.add( new UserDefinedItem() );
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
    firePropertyChange( JobEntryHadoopTransJobExecutorController.JOB_ENTRY_NAME, previousVal, newVal );
  }

  public String getHadoopJobName() {
    return hadoopJobName;
  }

  public void setHadoopJobName( String hadoopJobName ) {
    String previousVal = this.hadoopJobName;
    String newVal = hadoopJobName;

    this.hadoopJobName = hadoopJobName;
    firePropertyChange( JobEntryHadoopTransJobExecutorController.HADOOP_JOB_NAME, previousVal, newVal );
  }

  public String getMapTrans() {
    return mapTrans;
  }

  public void setMapTrans( String mapTrans ) {
    String previousVal = this.mapTrans;
    String newVal = mapTrans;

    this.mapTrans = mapTrans;
    firePropertyChange( JobEntryHadoopTransJobExecutorController.MAP_TRANS, previousVal, newVal );
  }

  public String getCombinerTrans() {
    return combinerTrans;
  }

  public void setCombinerTrans( String combinerTrans ) {
    String previousVal = this.combinerTrans;
    String newVal = combinerTrans;

    this.combinerTrans = combinerTrans;
    firePropertyChange( JobEntryHadoopTransJobExecutorController.COMBINER_TRANS, previousVal, newVal );
  }

  public String getReduceTrans() {
    return reduceTrans;
  }

  public void setReduceTrans( String reduceTrans ) {
    String previousVal = this.reduceTrans;
    String newVal = reduceTrans;

    this.reduceTrans = reduceTrans;
    firePropertyChange( JobEntryHadoopTransJobExecutorController.REDUCE_TRANS, previousVal, newVal );
  }

  public String getMapTransInputStepName() {
    return mapTransInputStepName;
  }

  public void setMapTransInputStepName( String mapTransInputStepName ) {
    String previousVal = this.mapTransInputStepName;
    String newVal = mapTransInputStepName;

    this.mapTransInputStepName = mapTransInputStepName;
    firePropertyChange( JobEntryHadoopTransJobExecutorController.MAP_TRANS_INPUT_STEP_NAME, previousVal, newVal );
  }

  public String getMapTransOutputStepName() {
    return mapTransOutputStepName;
  }

  public void setMapTransOutputStepName( String mapTransOutputStepName ) {
    String previousVal = this.mapTransOutputStepName;
    String newVal = mapTransOutputStepName;

    this.mapTransOutputStepName = mapTransOutputStepName;
    firePropertyChange( JobEntryHadoopTransJobExecutorController.MAP_TRANS_OUTPUT_STEP_NAME, previousVal, newVal );
  }

  public String getCombinerTransInputStepName() {
    return combinerTransInputStepName;
  }

  public void setCombinerTransInputStepName( String combinerTransInputStepName ) {
    String previousVal = this.combinerTransInputStepName;
    String newVal = combinerTransInputStepName;

    this.combinerTransInputStepName = combinerTransInputStepName;
    firePropertyChange( JobEntryHadoopTransJobExecutorController.COMBINER_TRANS_INPUT_STEP_NAME, previousVal, newVal );
  }

  public String getCombinerTransOutputStepName() {
    return combinerTransOutputStepName;
  }

  public void setCombinerTransOutputStepName( String combinerTransOutputStepName ) {
    String previousVal = this.combinerTransOutputStepName;
    String newVal = combinerTransOutputStepName;

    this.combinerTransOutputStepName = combinerTransOutputStepName;
    firePropertyChange( JobEntryHadoopTransJobExecutorController.COMBINER_TRANS_OUTPUT_STEP_NAME, previousVal, newVal );
  }

  public String getReduceTransInputStepName() {
    return reduceTransInputStepName;
  }

  public void setReduceTransInputStepName( String reduceTransInputStepName ) {
    String previousVal = this.reduceTransInputStepName;
    String newVal = reduceTransInputStepName;

    this.reduceTransInputStepName = reduceTransInputStepName;
    firePropertyChange( JobEntryHadoopTransJobExecutorController.REDUCE_TRANS_INPUT_STEP_NAME, previousVal, newVal );
  }

  public String getReduceTransOutputStepName() {
    return reduceTransOutputStepName;
  }

  public void setReduceTransOutputStepName( String reduceTransOutputStepName ) {
    String previousVal = this.reduceTransOutputStepName;
    String newVal = reduceTransOutputStepName;

    this.reduceTransOutputStepName = reduceTransOutputStepName;
    firePropertyChange( JobEntryHadoopTransJobExecutorController.REDUCE_TRANS_OUTPUT_STEP_NAME, previousVal, newVal );
  }

  public void invertBlocking() {
    setBlocking( !isBlocking() );
  }

  public JobEntryHadoopTransJobExecutor getJobEntry() {
    return jobEntry;
  }

  public void setJobEntry( JobEntryHadoopTransJobExecutor jobEntry ) {
    this.jobEntry = jobEntry;
  }

  public void invertSuppressOutputOfMapKey() {
    setSuppressOutputOfMapKey( !isSuppressOutputOfMapKey() );
  }

  public boolean isSuppressOutputOfMapKey() {
    return this.suppressOutputMapKey;
  }

  public void setSuppressOutputOfMapKey( boolean suppress ) {
    boolean previousVal = this.suppressOutputMapKey;
    boolean newVal = suppress;

    this.suppressOutputMapKey = suppress;
    firePropertyChange( SUPPRESS_OUTPUT_MAP_KEY, previousVal, newVal );
  }

  public void invertSuppressOutputOfMapValue() {
    setSuppressOutputOfMapValue( !isSuppressOutputOfMapValue() );
  }

  public boolean isSuppressOutputOfMapValue() {
    return this.suppressOutputMapValue;
  }

  public void setSuppressOutputOfMapValue( boolean suppress ) {
    boolean previousVal = this.suppressOutputMapValue;
    boolean newVal = suppress;

    this.suppressOutputMapValue = suppress;
    firePropertyChange( SUPPRESS_OUTPUT_MAP_VALUE, previousVal, newVal );
  }

  public void invertSuppressOutputOfKey() {
    setSuppressOutputOfKey( !isSuppressOutputOfKey() );
  }

  public boolean isSuppressOutputOfKey() {
    return this.suppressOutputKey;
  }

  public void setSuppressOutputOfKey( boolean suppress ) {
    boolean previousVal = this.suppressOutputKey;
    boolean newVal = suppress;

    this.suppressOutputKey = suppress;
    firePropertyChange( SUPPRESS_OUTPUT_KEY, previousVal, newVal );
  }

  public void invertSuppressOutputOfValue() {
    setSuppressOutputOfValue( !isSuppressOutputOfValue() );
  }

  public boolean isSuppressOutputOfValue() {
    return this.suppressOutputValue;
  }

  public void setSuppressOutputOfValue( boolean suppress ) {
    boolean previousVal = this.suppressOutputValue;
    boolean newVal = suppress;

    this.suppressOutputValue = suppress;
    firePropertyChange( SUPPRESS_OUTPUT_VALUE, previousVal, newVal );
  }

  public String getInputFormatClass() {
    return inputFormatClass;
  }

  public void setInputFormatClass( String inputFormatClass ) {
    String previousVal = this.inputFormatClass;
    String newVal = inputFormatClass;

    this.inputFormatClass = inputFormatClass;
    firePropertyChange( INPUT_FORMAT_CLASS, previousVal, newVal );
  }

  public String getOutputFormatClass() {
    return outputFormatClass;
  }

  public void setOutputFormatClass( String outputFormatClass ) {
    String previousVal = this.outputFormatClass;
    String newVal = outputFormatClass;

    this.outputFormatClass = outputFormatClass;
    firePropertyChange( OUTPUT_FORMAT_CLASS, previousVal, newVal );
  }

  public String getInputPath() {
    return inputPath;
  }

  public void setInputPath( String inputPath ) {
    String previousVal = this.inputPath;
    String newVal = inputPath;

    this.inputPath = inputPath;
    firePropertyChange( INPUT_PATH, previousVal, newVal );
  }

  public String getOutputPath() {
    return outputPath;
  }

  public void setOutputPath( String outputPath ) {
    String previousVal = this.outputPath;
    String newVal = outputPath;

    this.outputPath = outputPath;
    firePropertyChange( OUTPUT_PATH, previousVal, newVal );
  }

  public void invertCleanOutputPath() {
    setCleanOutputPath( !isCleanOutputPath() );
  }

  public boolean isCleanOutputPath() {
    return cleanOutputPath;
  }

  public void setCleanOutputPath( boolean cleanOutputPath ) {
    boolean old = this.cleanOutputPath;
    this.cleanOutputPath = cleanOutputPath;
    firePropertyChange( CLEAN_OUTPUT_PATH, old, this.cleanOutputPath );
  }

  public boolean isBlocking() {
    return blocking;
  }

  public void setBlocking( boolean blocking ) {
    boolean previousVal = this.blocking;
    boolean newVal = blocking;

    this.blocking = blocking;
    firePropertyChange( BLOCKING, previousVal, newVal );
  }

  public void setReducingSingleThreaded( boolean reducingSingleThreaded ) {
    boolean previousVal = this.reducingSingleThreaded;
    boolean newVal = reducingSingleThreaded;

    this.reducingSingleThreaded = reducingSingleThreaded;
    firePropertyChange( REDUCING_SINGLE_THREADED, previousVal, newVal );
  }

  public String getLoggingInterval() {
    return loggingInterval;
  }

  public void setLoggingInterval( String loggingInterval ) {
    String previousVal = this.loggingInterval;
    String newVal = loggingInterval;

    this.loggingInterval = loggingInterval;
    firePropertyChange( LOGGING_INTERVAL, previousVal, newVal );
  }

  public String getNumMapTasks() {
    return numMapTasks;
  }

  public void setNumMapTasks( String numMapTasks ) {
    String previousVal = this.numMapTasks;
    String newVal = numMapTasks;

    this.numMapTasks = numMapTasks;
    firePropertyChange( NUM_MAP_TASKS, previousVal, newVal );
  }

  public String getNumReduceTasks() {
    return numReduceTasks;
  }

  public void setNumReduceTasks( String numReduceTasks ) {
    String previousVal = this.numReduceTasks;
    String newVal = numReduceTasks;

    this.numReduceTasks = numReduceTasks;
    firePropertyChange( NUM_REDUCE_TASKS, previousVal, newVal );
  }

  public List<NamedCluster> getNamedClusters() throws MetaStoreException {
    return namedClusterService.list( jobMeta.getMetaStore() );
  }

  public void setNamedClusters( List<NamedCluster> namedClusters ) {
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

  public void invertReducingSingleThreaded() {
    setReducingSingleThreaded( !isReducingSingleThreaded() );
  }

  public boolean isReducingSingleThreaded() {
    return reducingSingleThreaded;
  }

  public void invertCombiningSingleThreaded() {
    setCombiningSingleThreaded( !isCombiningSingleThreaded() );
  }

  public boolean isCombiningSingleThreaded() {
    return combiningSingleThreaded;
  }

  public void setCombiningSingleThreaded( boolean combiningSingleThreaded ) {
    boolean old = this.combiningSingleThreaded;
    this.combiningSingleThreaded = combiningSingleThreaded;
    firePropertyChange( COMBINING_SINGLE_THREADED, old, this.combiningSingleThreaded );
  }

  public void help() {
    Shell shell = getJobEntryDialog();
    PluginInterface plugin =
        PluginRegistry.getInstance().findPluginWithId( JobEntryPluginType.class, jobEntry.getPluginId() );
    HelpUtils.openHelpDialog( shell, plugin );
  }

  public void editNamedCluster() throws MetaStoreException {
    if ( isSelectedNamedCluster() ) {
      String newNcName = ncDelegate.editNamedCluster( null, getSelectedNamedCluster(), getJobEntryDialog() );
      if ( newNcName != null ) {
        //cancel button on editing pressed, clusters not changed
        namedClustersChanged();
        selectedNamedClusterChanged( getNamedClusterName( getSelectedNamedCluster() ), newNcName );
      }
    }
  }

  public void newNamedCluster() throws MetaStoreException {
    String newNcName = ncDelegate.newNamedCluster( jobMeta, null, getJobEntryDialog() );
    if ( newNcName != null ) {
      //cancel button on editing pressed, clusters not changed
      namedClustersChanged();
      selectedNamedClusterChanged( getNamedClusterName( getSelectedNamedCluster() ), newNcName );
    }
  }

  private Shell getJobEntryDialog() {
    XulDialog xulDialog = (XulDialog) getXulDomContainer().getDocumentRoot().getElementById( "job-entry-dialog" );
    Shell shell = (Shell) xulDialog.getRootObject();
    return shell;
  }

  private String getNamedClusterName( NamedCluster namedCluster ) {
    return namedCluster != null ? namedCluster.getName() : null;
  }

  /**
   * Reports the named clusters list has been changed.
   *
   * @throws MetaStoreException
   *           if the exception occurs
   */
  @VisibleForTesting
    void namedClustersChanged() throws MetaStoreException {
    firePropertyChange( "namedClusters", null, getNamedClusters() );
  }

  /**
   * Reports that the selected named cluster has been changed.
   *
   * @param ncVal
   *          the old value of the selected named cluster
   * @param newNcVal
   *          the new value of the selected named cluster
   * @throws MetaStoreException
   *           if the exception occurs
   */
  @VisibleForTesting
    void selectedNamedClusterChanged( String ncVal, String newNcVal ) throws MetaStoreException {
    if ( newNcVal != null ) {
      ncVal = newNcVal;
    }
    if ( ncVal != null ) {
      for ( NamedCluster nc : getNamedClusters() ) {
        if ( nc.getName().equals( ncVal ) ) {
          firePropertyChange( "selectedNamedCluster", null, nc );
          return;
        }
      }
    }
  }

  public void setSelectedNamedCluster( NamedCluster namedCluster ) {
    this.selectedNamedCluster = namedCluster;
  }

  public NamedCluster getSelectedNamedCluster() {
    return this.selectedNamedCluster;
  }

  public boolean isSelectedNamedCluster() {
    return this.selectedNamedCluster != null;
  }
}
