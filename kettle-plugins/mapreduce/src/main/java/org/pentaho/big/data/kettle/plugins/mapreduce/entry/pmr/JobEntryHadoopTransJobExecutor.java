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

package org.pentaho.big.data.kettle.plugins.mapreduce.entry.pmr;

import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.big.data.kettle.plugins.mapreduce.DialogClassUtil;
import org.pentaho.big.data.kettle.plugins.mapreduce.entry.NamedClusterLoadSaveUtil;
import org.pentaho.big.data.kettle.plugins.mapreduce.entry.UserDefinedItem;
import org.pentaho.big.data.kettle.plugins.mapreduce.step.exit.HadoopExitMeta;
import org.pentaho.hadoop.shim.api.mapreduce.MapReduceJobAdvanced;
import org.pentaho.hadoop.shim.api.mapreduce.MapReduceJobBuilder;
import org.pentaho.hadoop.shim.api.mapreduce.MapReduceService;
import org.pentaho.hadoop.shim.api.mapreduce.PentahoMapReduceJobBuilder;
import org.pentaho.hadoop.shim.api.mapreduce.TaskCompletionEvent;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.ObjectLocationSpecificationMethod;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.ResultFile;
import org.pentaho.di.core.annotations.JobEntry;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.logging.Log4jFileAppender;
import org.pentaho.di.core.logging.LogWriter;
import org.pentaho.di.core.plugins.JobEntryPluginType;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.CurrentDirectoryResolver;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.HasRepositoryDirectories;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectory;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.resource.ResourceDefinition;
import org.pentaho.di.resource.ResourceNamingInterface;
import org.pentaho.di.trans.TransConfiguration;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransMeta.TransformationType;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;
import org.w3c.dom.Node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuppressWarnings( "deprecation" )
@JobEntry( id = "HadoopTransJobExecutorPlugin", image = "HDT.svg", name = "HadoopTransJobExecutorPlugin.Name",
  description = "HadoopTransJobExecutorPlugin.Description",
  categoryDescription = "i18n:org.pentaho.di.job:JobCategory.Category.BigData",
  i18nPackageName = "org.pentaho.di.job.entries.hadooptransjobexecutor",
  documentationUrl = "Products/Pentaho_MapReduce" )
public class JobEntryHadoopTransJobExecutor extends JobEntryBase implements Cloneable, JobEntryInterface,
  HasRepositoryDirectories {
  public static final String MAPREDUCE_APPLICATION_CLASSPATH = "mapreduce.application.classpath";
  public static final String DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH =
    "$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*,$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*";
  public static final String PENTAHO_MAPREDUCE_PROPERTY_USE_DISTRIBUTED_CACHE = "pmr.use.distributed.cache";
  // $NON-NLS-1$
  public static final String PENTAHO_MAPREDUCE_PROPERTY_PMR_LIBRARIES_ARCHIVE_FILE = "pmr.libraries.archive.file";
  public static final String PENTAHO_MAPREDUCE_PROPERTY_KETTLE_HDFS_INSTALL_DIR = "pmr.kettle.dfs.install.dir";
  public static final String PENTAHO_MAPREDUCE_PROPERTY_KETTLE_INSTALLATION_ID = "pmr.kettle.installation.id";
  public static final String PENTAHO_MAPREDUCE_PROPERTY_ADDITIONAL_PLUGINS = "pmr.kettle.additional.plugins";
  private static Class<?> PKG = JobEntryHadoopTransJobExecutor.class; // for i18n purposes, needed by Translator2!!
  public static final String DIALOG_NAME = DialogClassUtil.getDialogClassName( PKG );
  private final NamedClusterService namedClusterService;
  private final NamedClusterServiceLocator namedClusterServiceLocator;
  private final NamedClusterLoadSaveUtil namedClusterLoadSaveUtil = new NamedClusterLoadSaveUtil();
  private String hadoopJobName;
  private String mapRepositoryDir;
  private String mapRepositoryFile;
  private ObjectId mapRepositoryReference;
  private String mapTrans;
  private String combinerRepositoryDir;
  private String combinerRepositoryFile;
  private ObjectId combinerRepositoryReference;
  private String combinerTrans;
  private boolean combiningSingleThreaded;
  private String reduceRepositoryDir;
  private String reduceRepositoryFile;
  private ObjectId reduceRepositoryReference;
  private String reduceTrans;
  private boolean reducingSingleThreaded;
  private String mapInputStepName;
  private String mapOutputStepName;
  private String combinerInputStepName;
  private String combinerOutputStepName;
  private String reduceInputStepName;
  private String reduceOutputStepName;
  private boolean suppressOutputMapKey;
  private boolean suppressOutputMapValue;
  private boolean suppressOutputKey;
  private boolean suppressOutputValue;
  private String inputFormatClass;
  private String outputFormatClass;
  private NamedCluster namedCluster;
  private String inputPath;
  private String outputPath;
  private boolean cleanOutputPath;
  private boolean blocking = true;
  private String loggingInterval = "60";
  private String numMapTasks = "1";
  private String numReduceTasks = "1";
  private static final String KTR_EXT = ".ktr";
  private List<UserDefinedItem> userDefined = new ArrayList<UserDefinedItem>();
  private final RuntimeTester runtimeTester;
  private final RuntimeTestActionService runtimeTestActionService;

  public JobEntryHadoopTransJobExecutor( NamedClusterService namedClusterService,
                                         RuntimeTestActionService runtimeTestActionService,
                                         RuntimeTester runtimeTester,
                                         NamedClusterServiceLocator namedClusterServiceLocator ) throws Throwable {
    this.namedClusterService = namedClusterService;
    this.runtimeTestActionService = runtimeTestActionService;
    this.namedClusterServiceLocator = namedClusterServiceLocator;
    this.runtimeTester = runtimeTester;
    reducingSingleThreaded = false;
    combiningSingleThreaded = false;
  }

  protected static final TransMeta loadTransMeta( VariableSpace space, Repository rep, String filename,
                                                ObjectId transformationId, String repositoryDir, String repositoryFile )
          throws KettleException {

    TransMeta transMeta = null;

    if ( rep == null ) {
      if ( !Const.isEmpty( filename ) ) {
        String realFilename = space.environmentSubstitute( filename );
        transMeta = new TransMeta( realFilename );
      }
    } else {
      if ( !Const.isEmpty( filename ) ) {
        transMeta = getTransMetaFromRepo( filename, rep, space );
      } else if ( transformationId != null ) {
        transMeta = rep.loadTransformation( transformationId, null );
      } else if ( !Const.isEmpty( repositoryDir ) && !Const.isEmpty( repositoryFile ) ) {
        transMeta = getTransMetaFromRepo( repositoryDir, repositoryFile, rep, space );
      }
    }

    return transMeta;
  }

  public static TransMeta getTransMetaFromRepo( String fullPath, Repository rep, VariableSpace space ) throws KettleException {
    if ( fullPath == null ) {
      return null;
    }
    String trimPath = fullPath.trim();

    if ( trimPath.isEmpty() || trimPath.endsWith( "/" ) ) {
      return null;
    }

    int index = trimPath.lastIndexOf( '/' );

    if ( index == -1 ) {
      return null;
    }

    String filename = trimPath.substring( index + 1 );
    String repDir = trimPath.substring( 0, index );

    return getTransMetaFromRepo( repDir, filename, rep, space );
  }

  public static TransMeta getTransMetaFromRepo( String repositoryDir, String repositoryFile, Repository rep, VariableSpace space  ) throws KettleException {
    if ( space instanceof JobEntryHadoopTransJobExecutor ) {
      CurrentDirectoryResolver r = new CurrentDirectoryResolver();
      JobEntryHadoopTransJobExecutor jobEntry = (JobEntryHadoopTransJobExecutor) space;
      space = r.resolveCurrentDirectory( jobEntry, jobEntry.getParentJob().getRepositoryDirectory(), null );
    }
    String repositoryDirS = space.environmentSubstitute( repositoryDir );
    if ( repositoryDirS.isEmpty() ) {
      repositoryDirS = "/";
    }
    String repositoryFileS = space.environmentSubstitute( repositoryFile );
    RepositoryDirectoryInterface repositoryDirectory =
            rep.loadRepositoryDirectoryTree().findDirectory( repositoryDirS );
    return  rep.loadTransformation( repositoryFileS, repositoryDirectory, null, true, null );
  }

  public static String[] splitInputPaths( String inputPath, VariableSpace variableSpace ) {
    String inputPathS = variableSpace.environmentSubstitute( inputPath );

    // This is a non-elegant way to split the path on commas unless inside curly braces. There should be
    // a method in Const and/or a fancy regex for this kind of thing. Instead, find the curly-brace groups
    // and temporarily replace the commas with a non-sensical string. Then restore the commas after
    // splitting the input paths.
    Matcher m = Pattern.compile( "[{][^{]*[}]" ).matcher( inputPathS );
    StringBuffer sb = new StringBuffer();
    while ( m.find() ) {
      m.appendReplacement( sb, m.group().replace( ",", "@!@" ) );
    }
    m.appendTail( sb );

    return sb.toString().split( "," );
  }

  public String getHadoopJobName() {
    return hadoopJobName;
  }

  public void setHadoopJobName( String hadoopJobName ) {
    this.hadoopJobName = hadoopJobName;
  }

  /**
   * @return  An array of 3 elements : 0 - specification method for mapper,
   *                                   1 - specification method for combiner,
   *                                   2 - specification method for reducer.
   */
  @Override
  public ObjectLocationSpecificationMethod[] getSpecificationMethods() {
    return new ObjectLocationSpecificationMethod[] {
      defineSpecificationMethod( mapRepositoryDir, mapRepositoryFile, mapRepositoryReference ),
      defineSpecificationMethod( combinerRepositoryDir, combinerRepositoryFile, combinerRepositoryReference ),
      defineSpecificationMethod( reduceRepositoryDir, reduceRepositoryFile, reduceRepositoryReference )
    };
  }

  /**
   * Returns an array of 3 elements : 0 - mapper, 1 - combiner, 2 - reducer directories
   * @return String array[2] of mapper, combiner, reducer repository directories
   */
  @Override
  public String[] getDirectories() {
    return new String[]{
      mapRepositoryDir != null ? mapRepositoryDir : mapTrans,
      combinerRepositoryDir != null ? combinerRepositoryDir : combinerTrans,
      reduceRepositoryDir != null ? reduceRepositoryDir : reduceTrans };
  }

  /**
   * Updates repository directories with values from an array of 3 elements :
   * 0 - mapper, 1 - combiner, 2 - reducer directories
   * @param directory Array[2] of updated mapper, combiner, reducer directories to set
   */
  @Override
  public void setDirectories( String[] directory ) {
    if ( mapRepositoryDir != null ) {
      mapRepositoryDir = directory[0];
    } else {
      mapTrans = directory[0];
    }
    if ( combinerRepositoryDir != null ) {
      combinerRepositoryDir = directory[1];
    } else {
      combinerTrans = directory[1];
    }
    if ( reduceRepositoryDir != null ) {
      reduceRepositoryDir = directory[2];
    } else {
      reduceTrans = directory[2];
    }
  }

  public String getMapTrans() {
    return mapTrans;
  }

  public void setMapTrans( String mapTrans ) {
    this.mapTrans = mapTrans;
  }

  public String getCombinerTrans() {
    return combinerTrans;
  }

  public void setCombinerTrans( String combinerTrans ) {
    this.combinerTrans = combinerTrans;
  }

  public String getReduceTrans() {
    return reduceTrans;
  }

  public void setReduceTrans( String reduceTrans ) {
    this.reduceTrans = reduceTrans;
  }

  public String getMapRepositoryDir() {
    return mapRepositoryDir;
  }

  public void setMapRepositoryDir( String mapRepositoryDir ) {
    this.mapRepositoryDir = mapRepositoryDir;
  }

  public String getMapRepositoryFile() {
    return mapRepositoryFile;
  }

  public void setMapRepositoryFile( String mapRepositoryFile ) {
    this.mapRepositoryFile = mapRepositoryFile;
  }

  public ObjectId getMapRepositoryReference() {
    return mapRepositoryReference;
  }

  public void setMapRepositoryReference( ObjectId mapRepositoryReference ) {
    this.mapRepositoryReference = mapRepositoryReference;
  }

  public String getCombinerRepositoryDir() {
    return combinerRepositoryDir;
  }

  public void setCombinerRepositoryDir( String combinerRepositoryDir ) {
    this.combinerRepositoryDir = combinerRepositoryDir;
  }

  public String getCombinerRepositoryFile() {
    return combinerRepositoryFile;
  }

  public void setCombinerRepositoryFile( String combinerRepositoryFile ) {
    this.combinerRepositoryFile = combinerRepositoryFile;
  }

  public ObjectId getCombinerRepositoryReference() {
    return combinerRepositoryReference;
  }

  public void setCombinerRepositoryReference( ObjectId combinerRepositoryReference ) {
    this.combinerRepositoryReference = combinerRepositoryReference;
  }

  public String getReduceRepositoryDir() {
    return reduceRepositoryDir;
  }

  public void setReduceRepositoryDir( String reduceRepositoryDir ) {
    this.reduceRepositoryDir = reduceRepositoryDir;
  }

  public String getReduceRepositoryFile() {
    return reduceRepositoryFile;
  }

  public void setReduceRepositoryFile( String reduceRepositoryFile ) {
    this.reduceRepositoryFile = reduceRepositoryFile;
  }

  public ObjectId getReduceRepositoryReference() {
    return reduceRepositoryReference;
  }

  public void setReduceRepositoryReference( ObjectId reduceRepositoryReference ) {
    this.reduceRepositoryReference = reduceRepositoryReference;
  }

  public String getMapInputStepName() {
    return mapInputStepName;
  }

  public void setMapInputStepName( String mapInputStepName ) {
    this.mapInputStepName = mapInputStepName;
  }

  public String getMapOutputStepName() {
    return mapOutputStepName;
  }

  public void setMapOutputStepName( String mapOutputStepName ) {
    this.mapOutputStepName = mapOutputStepName;
  }

  public String getCombinerInputStepName() {
    return combinerInputStepName;
  }

  public void setCombinerInputStepName( String combinerInputStepName ) {
    this.combinerInputStepName = combinerInputStepName;
  }

  public String getCombinerOutputStepName() {
    return combinerOutputStepName;
  }

  public void setCombinerOutputStepName( String combinerOutputStepName ) {
    this.combinerOutputStepName = combinerOutputStepName;
  }

  public String getReduceInputStepName() {
    return reduceInputStepName;
  }

  public void setReduceInputStepName( String reduceInputStepName ) {
    this.reduceInputStepName = reduceInputStepName;
  }

  public String getReduceOutputStepName() {
    return reduceOutputStepName;
  }

  public void setReduceOutputStepName( String reduceOutputStepName ) {
    this.reduceOutputStepName = reduceOutputStepName;
  }

  public boolean getSuppressOutputOfMapKey() {
    return suppressOutputMapKey;
  }

  public void setSuppressOutputOfMapKey( boolean suppress ) {
    suppressOutputMapKey = suppress;
  }

  public boolean getSuppressOutputOfMapValue() {
    return suppressOutputMapValue;
  }

  public void setSuppressOutputOfMapValue( boolean suppress ) {
    suppressOutputMapValue = suppress;
  }

  public boolean getSuppressOutputOfKey() {
    return suppressOutputKey;
  }

  public void setSuppressOutputOfKey( boolean suppress ) {
    suppressOutputKey = suppress;
  }

  public boolean getSuppressOutputOfValue() {
    return suppressOutputValue;
  }

  public void setSuppressOutputOfValue( boolean suppress ) {
    suppressOutputValue = suppress;
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

  public boolean isCleanOutputPath() {
    return cleanOutputPath;
  }

  public void setCleanOutputPath( boolean cleanOutputPath ) {
    this.cleanOutputPath = cleanOutputPath;
  }

  public boolean isBlocking() {
    return blocking;
  }

  public void setBlocking( boolean blocking ) {
    this.blocking = blocking;
  }

  public String getLoggingInterval() {
    return loggingInterval;
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

  public Result execute( Result result, int arg1 ) throws KettleException {

    result.setNrErrors( 0 );

    Log4jFileAppender appender = null;
    String logFileName = "pdi-" + this.getName(); //$NON-NLS-1$

    try {
      appender = LogWriter.createFileAppender( logFileName, false, false );
      LogWriter.getInstance().addAppender( appender );
      log.setLogLevel( parentJob.getLogLevel() );
    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG,
        "JobEntryHadoopTransJobExecutor.FailedToOpenLogFile", logFileName, e.toString() ) ); //$NON-NLS-1$
      logError( Const.getStackTracker( e ) );
    }

    try {

      MapReduceService mapReduceService = namedClusterServiceLocator.getService( namedCluster, MapReduceService.class );
      PentahoMapReduceJobBuilder jobBuilder = mapReduceService.createPentahoMapReduceJobBuilder( log, variables );

      String hadoopJobNameS = environmentSubstitute( hadoopJobName );
      jobBuilder.setHadoopJobName( hadoopJobNameS );

      // mapper
      TransExecutionConfiguration transExecConfig = new TransExecutionConfiguration();
      TransMeta transMeta =
        loadTransMeta( this, rep, mapTrans, mapRepositoryReference, mapRepositoryDir, mapRepositoryFile );
      TransConfiguration transConfig = new TransConfiguration( transMeta, transExecConfig );
      String mapInputStepNameS = environmentSubstitute( mapInputStepName );
      String mapOutputStepNameS = environmentSubstitute( mapOutputStepName );

      try {
        jobBuilder.verifyTransMeta( transMeta, mapInputStepNameS, mapOutputStepNameS );
      } catch ( Exception ex ) {
        throw new KettleException( BaseMessages
          .getString( PKG, "JobEntryHadoopTransJobExecutor.MapConfiguration.Error" ), ex );
      }

      jobBuilder.setMapperInfo( transConfig.getXML(), mapInputStepNameS, mapOutputStepNameS );

      jobBuilder.set( MapReduceJobBuilder.STRING_COMBINE_SINGLE_THREADED, combiningSingleThreaded ? "true" : "false" );

      // Pass the single threaded reduction to the configuration...
      //
      jobBuilder.set( MapReduceJobBuilder.STRING_REDUCE_SINGLE_THREADED, reducingSingleThreaded ? "true" : "false" );

      if ( getSuppressOutputOfMapKey() ) {
        jobBuilder.setMapOutputKeyClass( jobBuilder.getHadoopWritableCompatibleClassName( null ) );
      }
      if ( getSuppressOutputOfMapValue() ) {
        jobBuilder.setMapOutputValueClass( jobBuilder.getHadoopWritableCompatibleClassName( null ) );
      }

      // auto configure the output mapper key and value classes
      if ( !getSuppressOutputOfMapKey() || !getSuppressOutputOfMapValue() && transMeta != null ) {
        StepMeta mapOut = transMeta.findStep( mapOutputStepNameS );
        if ( mapOut.getStepMetaInterface() instanceof HadoopExitMeta ) {
          RowMetaInterface prevStepFields = transMeta.getPrevStepFields( mapOut );
          if ( !getSuppressOutputOfMapKey() ) {
            String keyName = ( (HadoopExitMeta) mapOut.getStepMetaInterface() ).getOutKeyFieldname();
            int keyI = prevStepFields.indexOfValue( keyName );
            ValueMetaInterface keyVM = ( keyI >= 0 ) ? prevStepFields.getValueMeta( keyI ) : null;
            if ( keyVM == null ) {
              throw new KettleException( BaseMessages.getString( PKG,
                "JobEntryHadoopTransJobExecutor.NoMapOutputKeyDefined.Error" ) );
            }
            String hadoopWritableKey = jobBuilder.getHadoopWritableCompatibleClassName( keyVM );
            jobBuilder.setMapOutputKeyClass( hadoopWritableKey );
            logDebug( BaseMessages.getString( PKG, "JobEntryHadoopTransJobExecutor.Message.MapOutputKeyMessage",
              hadoopWritableKey ) );
          }

          if ( !getSuppressOutputOfMapValue() ) {
            String valName = ( (HadoopExitMeta) mapOut.getStepMetaInterface() ).getOutValueFieldname();
            int valI = prevStepFields.indexOfValue( valName );
            ValueMetaInterface valueVM = ( valI >= 0 ) ? prevStepFields.getValueMeta( valI ) : null;
            if ( valueVM == null ) {
              throw new KettleException( BaseMessages.getString( PKG,
                "JobEntryHadoopTransJobExecutor.NoMapOutputValueDefined.Error" ) );
            }
            String hadoopWritableValue = jobBuilder.getHadoopWritableCompatibleClassName( valueVM );
            jobBuilder.setMapOutputValueClass( hadoopWritableValue );
            logDebug( BaseMessages.getString( PKG, "JobEntryHadoopTransJobExecutor.Message.MapOutputValueMessage",
              hadoopWritableValue ) );
          }
        }
      }

      // combiner
      transMeta =
        loadTransMeta( this, rep, combinerTrans, combinerRepositoryReference, combinerRepositoryDir,
          combinerRepositoryFile );
      if ( transMeta != null ) {

        if ( combiningSingleThreaded ) {
          verifySingleThreadingValidity( transMeta );
        }

        String combinerInputStepNameS = environmentSubstitute( combinerInputStepName );
        String combinerOutputStepNameS = environmentSubstitute( combinerOutputStepName );
        transConfig = new TransConfiguration( transMeta, transExecConfig );
        jobBuilder.setCombinerInfo( transConfig.getXML(), combinerInputStepNameS, combinerOutputStepNameS );
        try {
          jobBuilder.verifyTransMeta( transMeta, combinerInputStepNameS, combinerOutputStepNameS );
        } catch ( Exception ex ) {
          throw new KettleException( BaseMessages.getString( PKG,
            "JobEntryHadoopTransJobExecutor.CombinerConfiguration.Error" ), ex );
        }
      }

      // reducer
      transMeta =
        loadTransMeta( this, rep, reduceTrans, reduceRepositoryReference, reduceRepositoryDir, reduceRepositoryFile );

      if ( transMeta != null ) {

        // See if this is a valid single threading reducer
        //
        if ( reducingSingleThreaded ) {
          verifySingleThreadingValidity( transMeta );
        }

        String reduceInputStepNameS = environmentSubstitute( reduceInputStepName );
        String reduceOutputStepNameS = environmentSubstitute( reduceOutputStepName );
        transConfig = new TransConfiguration( transMeta, transExecConfig );
        jobBuilder.setReducerInfo( transConfig.getXML(), reduceInputStepNameS, reduceOutputStepNameS );

        try {
          jobBuilder.verifyTransMeta( transMeta, reduceInputStepNameS, reduceOutputStepNameS );
        } catch ( Exception ex ) {
          throw new KettleException( BaseMessages.getString( PKG,
            "JobEntryHadoopTransJobExecutor.ReducerConfiguration.Error" ), ex );
        }

        if ( getSuppressOutputOfKey() ) {
          jobBuilder.setOutputKeyClass( jobBuilder.getHadoopWritableCompatibleClassName( null ) );
        }
        if ( getSuppressOutputOfValue() ) {
          jobBuilder.setOutputValueClass( jobBuilder.getHadoopWritableCompatibleClassName( null ) );
        }

        // auto configure the output reduce key and value classes
        if ( !getSuppressOutputOfKey() || !getSuppressOutputOfValue() ) {
          StepMeta reduceOut = transMeta.findStep( reduceOutputStepNameS );
          RowMetaInterface prevStepFields = transMeta.getPrevStepFields( reduceOut );
          if ( reduceOut.getStepMetaInterface() instanceof HadoopExitMeta ) {
            String keyName = ( (HadoopExitMeta) reduceOut.getStepMetaInterface() ).getOutKeyFieldname();
            String valName = ( (HadoopExitMeta) reduceOut.getStepMetaInterface() ).getOutValueFieldname();
            int keyI = prevStepFields.indexOfValue( keyName );
            ValueMetaInterface keyVM = ( keyI >= 0 ) ? prevStepFields.getValueMeta( keyI ) : null;
            int valI = prevStepFields.indexOfValue( valName );
            ValueMetaInterface valueVM = ( valI >= 0 ) ? prevStepFields.getValueMeta( valI ) : null;

            if ( !getSuppressOutputOfKey() ) {
              if ( keyVM == null ) {
                throw new KettleException( BaseMessages.getString( PKG,
                  "JobEntryHadoopTransJobExecutor.NoOutputKeyDefined.Error" ) );
              }
              String hadoopWritableKey = jobBuilder.getHadoopWritableCompatibleClassName( keyVM );
              jobBuilder.setOutputKeyClass( hadoopWritableKey );
              logDebug( BaseMessages.getString( PKG, "JobEntryHadoopTransJobExecutor.Message.OutputKeyMessage",
                hadoopWritableKey ) );

            }

            if ( !getSuppressOutputOfValue() ) {
              if ( valueVM == null ) {
                throw new KettleException( BaseMessages.getString( PKG,
                  "JobEntryHadoopTransJobExecutor.NoOutputValueDefined.Error" ) );
              }
              String hadoopWritableValue = jobBuilder.getHadoopWritableCompatibleClassName( valueVM );
              jobBuilder.setOutputValueClass( hadoopWritableValue );
              logDebug( BaseMessages.getString( PKG, "JobEntryHadoopTransJobExecutor.Message.OutputValueMessage",
                hadoopWritableValue ) );
            }
          }
        }
      }

      jobBuilder.setInputFormatClass( inputFormatClass );
      jobBuilder.setOutputFormatClass( outputFormatClass );

      jobBuilder.setInputPaths( splitInputPaths( inputPath, variables ) );
      jobBuilder.setOutputPath( environmentSubstitute( outputPath ) );

      // process user defined values
      for ( UserDefinedItem item : userDefined ) {
        if ( item.getName() != null
          && !"".equals( item.getName() ) && item.getValue() != null && !""
          .equals( item.getValue() ) ) { //$NON-NLS-1$ //$NON-NLS-2$
          String nameS = environmentSubstitute( item.getName() );
          String valueS = environmentSubstitute( item.getValue() );
          jobBuilder.set( nameS, valueS );
        }
      }

      String numMapTasksS = environmentSubstitute( numMapTasks );
      try {
        if ( Integer.parseInt( numMapTasksS ) < 0 ) {
          throw new KettleException(
            BaseMessages.getString( PKG, "JobEntryHadoopTransJobExecutor.NumMapTasks.Error" ) );
        }
      } catch ( NumberFormatException e ) {
        if ( log.isDebug() ) {
          logError( Const.getStackTracker( e ) );
        }
      }

      String numReduceTasksS = environmentSubstitute( numReduceTasks );
      try {
        if ( Integer.parseInt( numReduceTasksS ) < 0 ) {
          throw new KettleException( BaseMessages
            .getString( PKG, "JobEntryHadoopTransJobExecutor.NumReduceTasks.Error" ) );
        }
      } catch ( NumberFormatException e ) {
        if ( log.isDebug() ) {
          logError( Const.getStackTracker( e ) );
        }
      }

      jobBuilder.setNumMapTasks( Const.toInt( numMapTasksS, 1 ) );
      jobBuilder.setNumReduceTasks( Const.toInt( numReduceTasksS, 1 ) );

      jobBuilder.setLogLevel( getLogLevel() );

      jobBuilder.setCleanOutputPath( isCleanOutputPath() );

      MapReduceJobAdvanced runningJob = jobBuilder.submit();

      String loggingIntervalS = environmentSubstitute( loggingInterval );
      int logIntv = 60;
      try {
        logIntv = Integer.parseInt( loggingIntervalS );
      } catch ( NumberFormatException e ) {
        logError( "Can't parse logging interval '" + loggingIntervalS + "'. Setting " + "logging interval to 60" );
      }

      if ( blocking ) {
        try {
          int taskCompletionEventIndex = 0;
          while ( !parentJob.isStopped() && !runningJob.isComplete() ) {
            if ( logIntv >= 1 ) {
              printJobStatus( runningJob );
              taskCompletionEventIndex += logTaskMessages( runningJob, taskCompletionEventIndex );
              Thread.sleep( logIntv * 1000 );
            } else {
              Thread.sleep( 60000 );
            }
          }

          if ( parentJob.isStopped() && !runningJob.isComplete() ) {
            // We must stop the job running on Hadoop
            runningJob.killJob();
            // Indicate this job entry did not complete
            result.setResult( false );
          }

          printJobStatus( runningJob );
          // Log any messages we may have missed while polling
          logTaskMessages( runningJob, taskCompletionEventIndex );
        } catch ( InterruptedException ie ) {
          logError( ie.getMessage(), ie );
        }

        // Entry is successful if the MR job is successful overall
        result.setResult( runningJob.isSuccessful() );
      }

    } catch ( Throwable t ) {
      t.printStackTrace();
      result.setStopped( true );
      result.setNrErrors( 1 );
      result.setResult( false );
      logError( Const.NVL( t.getMessage(), "" ), t );
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

  /**
   * Log messages indicating completion (success/failure) of component tasks for the provided running job.
   *
   * @param runningJob
   *          Running job to poll for completion events
   * @param startIndex
   *          Start at this event index to poll from
   * @return Total events consumed
   * @throws IOException
   *           Error fetching events
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

      TaskCompletionEvent.Status status = tcEvents[ i ].getTaskStatus();
      switch ( tcEvents[ i ].getTaskStatus() ) {
        case KILLED:
        case FAILED:
        case TIPFAILED:
          logError( BaseMessages
            .getString(
              PKG,
              "JobEntryHadoopTransJobExecutor.TaskDetails", status, tcEvents[ i ].getTaskAttemptId(),
              tcEvents[ i ].getTaskAttemptId(), tcEvents[ i ].getEventId(), diagsOutput ) ); //$NON-NLS-1$
          break;
        case SUCCEEDED:
        case OBSOLETE:
          logDetailed( BaseMessages
            .getString(
              PKG,
              "JobEntryHadoopTransJobExecutor.TaskDetails", TaskCompletionEvent.Status.SUCCEEDED,
              tcEvents[ i ].getTaskAttemptId(), tcEvents[ i ].getTaskAttemptId(), tcEvents[ i ].getEventId(),
              diagsOutput ) ); //$NON-NLS-1$
          break;
        default:
          logError( BaseMessages
            .getString(
              PKG,
              "JobEntryHadoopTransJobExecutor.TaskDetails", "UNKNOWN", tcEvents[ i ].getTaskAttemptId(),
              tcEvents[ i ].getTaskAttemptId(), tcEvents[ i ].getEventId(), diagsOutput ) ); //$NON-NLS-1$
      }
    }
    return tcEvents.length;
  }

  /**
   * @return the plugin interface for this job entry.
   */
  public PluginInterface getPluginInterface() {
    String pluginId = PluginRegistry.getInstance().getPluginId( this );
    return PluginRegistry.getInstance().findPluginWithId( JobEntryPluginType.class, pluginId );
  }

  private void verifySingleThreadingValidity( TransMeta transMeta ) throws KettleException {
    for ( StepMeta stepMeta : transMeta.getSteps() ) {
      TransformationType[] types = stepMeta.getStepMetaInterface().getSupportedTransformationTypes();
      boolean ok = false;
      for ( TransformationType type : types ) {
        if ( type == TransformationType.SingleThreaded ) {
          ok = true;
        }
      }
      if ( !ok ) {
        throw new KettleException( "Step '" + stepMeta.getName() + "' of type '" + stepMeta.getStepID()
          + "' is not supported in a Single Threaded transformation engine." );
      }
    }
  }

  public void printJobStatus( MapReduceJobAdvanced runningJob ) throws IOException {
    if ( log.isBasic() ) {
      double setupPercent = runningJob.getSetupProgress() * 100f;
      double mapPercent = runningJob.getMapProgress() * 100f;
      double reducePercent = runningJob.getReduceProgress() * 100f;
      logBasic( BaseMessages.getString( PKG,
        "JobEntryHadoopTransJobExecutor.RunningPercent", setupPercent, mapPercent, reducePercent ) ); //$NON-NLS-1$
    }
  }

  @Override
  public void loadXML( Node entrynode, List<DatabaseMeta> databases, List<SlaveServer> slaveServers, Repository rep,
                       IMetaStore metaStore )
    throws KettleXMLException {
    super.loadXML( entrynode, databases, slaveServers );
    hadoopJobName = XMLHandler.getTagValue( entrynode, "hadoop_job_name" ); //$NON-NLS-1$

    mapRepositoryDir = XMLHandler.getTagValue( entrynode, "map_trans_repo_dir" ); //$NON-NLS-1$
    mapRepositoryFile = XMLHandler.getTagValue( entrynode, "map_trans_repo_file" ); //$NON-NLS-1$
    String mapTransId = XMLHandler.getTagValue( entrynode, "map_trans_repo_reference" ); //$NON-NLS-1$
    mapRepositoryReference = Const.isEmpty( mapTransId ) ? null : new StringObjectId( mapTransId );
    mapTrans = XMLHandler.getTagValue( entrynode, "map_trans" ); //$NON-NLS-1$

    combinerRepositoryDir = XMLHandler.getTagValue( entrynode, "combiner_trans_repo_dir" ); //$NON-NLS-1$
    combinerRepositoryFile = XMLHandler.getTagValue( entrynode, "combiner_trans_repo_file" ); //$NON-NLS-1$
    String combinerTransId = XMLHandler.getTagValue( entrynode, "combiner_trans_repo_reference" ); //$NON-NLS-1$
    combinerRepositoryReference = Const.isEmpty( combinerTransId ) ? null : new StringObjectId( combinerTransId );
    combinerTrans = XMLHandler.getTagValue( entrynode, "combiner_trans" ); //$NON-NLS-1$
    final String combinerSingleThreaded = XMLHandler.getTagValue( entrynode, "combiner_single_threaded" ); //$NON-NLS-1$
    if ( !Const.isEmpty( combinerSingleThreaded ) ) {
      setCombiningSingleThreaded( "Y".equalsIgnoreCase( combinerSingleThreaded ) ); //$NON-NLS-1$
    } else {
      setCombiningSingleThreaded( false );
    }

    reduceRepositoryDir = XMLHandler.getTagValue( entrynode, "reduce_trans_repo_dir" ); //$NON-NLS-1$
    reduceRepositoryFile = XMLHandler.getTagValue( entrynode, "reduce_trans_repo_file" ); //$NON-NLS-1$
    String reduceTransId = XMLHandler.getTagValue( entrynode, "reduce_trans_repo_reference" ); //$NON-NLS-1$
    reduceRepositoryReference = Const.isEmpty( reduceTransId ) ? null : new StringObjectId( reduceTransId );
    reduceTrans = XMLHandler.getTagValue( entrynode, "reduce_trans" ); //$NON-NLS-1$
    String single = XMLHandler.getTagValue( entrynode, "reduce_single_threaded" ); //$NON-NLS-1$
    if ( Const.isEmpty( single ) ) {
      setReducingSingleThreaded( false );
    } else {
      setReducingSingleThreaded( "Y".equalsIgnoreCase( single ) ); //$NON-NLS-1$
    }

    mapInputStepName = XMLHandler.getTagValue( entrynode, "map_input_step_name" ); //$NON-NLS-1$
    mapOutputStepName = XMLHandler.getTagValue( entrynode, "map_output_step_name" ); //$NON-NLS-1$
    combinerInputStepName = XMLHandler.getTagValue( entrynode, "combiner_input_step_name" ); //$NON-NLS-1$
    combinerOutputStepName = XMLHandler.getTagValue( entrynode, "combiner_output_step_name" ); //$NON-NLS-1$
    reduceInputStepName = XMLHandler.getTagValue( entrynode, "reduce_input_step_name" ); //$NON-NLS-1$
    reduceOutputStepName = XMLHandler.getTagValue( entrynode, "reduce_output_step_name" ); //$NON-NLS-1$

    blocking = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "blocking" ) ); //$NON-NLS-1$ //$NON-NLS-2$

    loggingInterval = XMLHandler.getTagValue( entrynode, "logging_interval" ); //$NON-NLS-1$
    inputPath = XMLHandler.getTagValue( entrynode, "input_path" ); //$NON-NLS-1$
    inputFormatClass = XMLHandler.getTagValue( entrynode, "input_format_class" ); //$NON-NLS-1$
    outputPath = XMLHandler.getTagValue( entrynode, "output_path" ); //$NON-NLS-1$

    final String cleanOutputPath = XMLHandler.getTagValue( entrynode, "clean_output_path" );
    if ( !Const.isEmpty( cleanOutputPath ) ) { //$NON-NLS-1$
      setCleanOutputPath( cleanOutputPath.equalsIgnoreCase( "Y" ) ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    if ( !Const.isEmpty( XMLHandler.getTagValue( entrynode, "suppress_output_map_key" ) ) ) {
      suppressOutputMapKey = XMLHandler.getTagValue( entrynode, "suppress_output_map_key" ).equalsIgnoreCase( "Y" );
    }
    if ( !Const.isEmpty( XMLHandler.getTagValue( entrynode, "suppress_output_map_value" ) ) ) {
      suppressOutputMapValue = XMLHandler.getTagValue( entrynode, "suppress_output_map_value" ).equalsIgnoreCase( "Y" );
    }

    if ( !Const.isEmpty( XMLHandler.getTagValue( entrynode, "suppress_output_key" ) ) ) {
      suppressOutputKey = XMLHandler.getTagValue( entrynode, "suppress_output_key" ).equalsIgnoreCase( "Y" );
    }
    if ( !Const.isEmpty( XMLHandler.getTagValue( entrynode, "suppress_output_value" ) ) ) {
      suppressOutputValue = XMLHandler.getTagValue( entrynode, "suppress_output_value" ).equalsIgnoreCase( "Y" );
    }
    outputFormatClass = XMLHandler.getTagValue( entrynode, "output_format_class" ); //$NON-NLS-1$

    namedCluster =
      namedClusterLoadSaveUtil.loadClusterConfig( namedClusterService, null, rep, metaStore, entrynode, log );
    setRepository( rep );
    numMapTasks = XMLHandler.getTagValue( entrynode, "num_map_tasks" ); //$NON-NLS-1$
    numReduceTasks = XMLHandler.getTagValue( entrynode, "num_reduce_tasks" ); //$NON-NLS-1$

    // How many user defined elements?
    userDefined = new ArrayList<UserDefinedItem>();
    Node userDefinedList = XMLHandler.getSubNode( entrynode, "user_defined_list" ); //$NON-NLS-1$
    int nrUserDefined = XMLHandler.countNodes( userDefinedList, "user_defined" ); //$NON-NLS-1$
    for ( int i = 0; i < nrUserDefined; i++ ) {
      Node userDefinedNode = XMLHandler.getSubNodeByNr( userDefinedList, "user_defined", i ); //$NON-NLS-1$
      String name = XMLHandler.getTagValue( userDefinedNode, "name" ); //$NON-NLS-1$
      String value = XMLHandler.getTagValue( userDefinedNode, "value" ); //$NON-NLS-1$
      UserDefinedItem item = new UserDefinedItem();
      item.setName( name );
      item.setValue( value );
      userDefined.add( item );
    }
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder( 1024 );
    retval.append( super.getXML() );
    retval.append( "      " )
      .append( XMLHandler.addTagValue( "hadoop_job_name", hadoopJobName ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "      " )
        .append( XMLHandler.addTagValue( "map_trans_repo_dir", mapRepositoryDir ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "      " )
        .append( XMLHandler.addTagValue( "map_trans_repo_file", mapRepositoryFile ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval
        .append( "      " ).append( XMLHandler.addTagValue( "map_trans_repo_reference",
        mapRepositoryReference == null ? null : mapRepositoryReference.toString() ) ); //$NON-NLS-1$ //$NON-NLS-2$

    retval.append( "      " ).append( XMLHandler.addTagValue( "map_trans", mapTrans ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "      " )
      .append( XMLHandler.addTagValue( "combiner_trans_repo_dir", combinerRepositoryDir ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "      " ).append(
        XMLHandler.addTagValue( "combiner_trans_repo_file", combinerRepositoryFile ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval
        .append( "      " ).append( XMLHandler.addTagValue( "combiner_trans_repo_reference",
        combinerRepositoryReference == null ? null : combinerRepositoryReference.toString() ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "      " );

    retval.append( "      " )
      .append( XMLHandler.addTagValue( "combiner_trans", combinerTrans ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "      " ).append(
      XMLHandler.addTagValue( "combiner_single_threaded", combiningSingleThreaded ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "      " )
      .append( XMLHandler.addTagValue( "reduce_trans_repo_dir", reduceRepositoryDir ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "      " )
        .append( XMLHandler.addTagValue( "reduce_trans_repo_file", reduceRepositoryFile ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval
        .append( "      " ).append( XMLHandler.addTagValue( "reduce_trans_repo_reference",
        reduceRepositoryReference == null ? null : reduceRepositoryReference.toString() ) ); //$NON-NLS-1$ //$NON-NLS-2$

    retval.append( "      " )
      .append( XMLHandler.addTagValue( "reduce_trans", reduceTrans ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "      " )
      .append( XMLHandler.addTagValue( "reduce_single_threaded", reducingSingleThreaded ) ); //$NON-NLS-1$ //$NON-NLS-2$

    retval.append( "      " )
      .append( XMLHandler.addTagValue( "map_input_step_name", mapInputStepName ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "      " )
      .append( XMLHandler.addTagValue( "map_output_step_name", mapOutputStepName ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "      " ).append(
      XMLHandler.addTagValue( "combiner_input_step_name", combinerInputStepName ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "      " ).append(
      XMLHandler.addTagValue( "combiner_output_step_name", combinerOutputStepName ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "      " )
      .append( XMLHandler.addTagValue( "reduce_input_step_name", reduceInputStepName ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "      " )
      .append( XMLHandler.addTagValue( "reduce_output_step_name", reduceOutputStepName ) ); //$NON-NLS-1$ //$NON-NLS-2$

    retval.append( "      " ).append( XMLHandler.addTagValue( "blocking", blocking ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "      " )
      .append( XMLHandler.addTagValue( "logging_interval", loggingInterval ) ); //$NON-NLS-1$ //$NON-NLS-2$

    retval.append( "      " ).append( XMLHandler.addTagValue( "input_path", inputPath ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "      " )
      .append( XMLHandler.addTagValue( "input_format_class", inputFormatClass ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "      " ).append( XMLHandler.addTagValue( "output_path", outputPath ) ); //$NON-NLS-1$ //$NON-NLS-2$

    retval.append( "      " )
      .append( XMLHandler.addTagValue( "clean_output_path", cleanOutputPath ) ); //$NON-NLS-1$ //$NON-NLS-2$

    retval.append( "      " )
      .append( XMLHandler.addTagValue( "suppress_output_map_key", suppressOutputMapKey ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "      " ).append(
      XMLHandler.addTagValue( "suppress_output_map_value", suppressOutputMapValue ) ); //$NON-NLS-1$ //$NON-NLS-2$

    retval.append( "      " )
      .append( XMLHandler.addTagValue( "suppress_output_key", suppressOutputKey ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "      " )
      .append( XMLHandler.addTagValue( "suppress_output_value", suppressOutputValue ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "      " )
      .append( XMLHandler.addTagValue( "output_format_class", outputFormatClass ) ); //$NON-NLS-1$ //$NON-NLS-2$

    namedClusterLoadSaveUtil.getXmlNamedCluster( namedCluster, namedClusterService, metaStore, log, retval );

    retval.append( "      " )
      .append( XMLHandler.addTagValue( "num_map_tasks", numMapTasks ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "      " )
      .append( XMLHandler.addTagValue( "num_reduce_tasks", numReduceTasks ) ); //$NON-NLS-1$ //$NON-NLS-2$

    retval.append( "      <user_defined_list>" ).append( Const.CR ); //$NON-NLS-1$
    if ( userDefined != null ) {
      for ( UserDefinedItem item : userDefined ) {
        if ( item.getName() != null
          && !"".equals( item.getName() ) && item.getValue() != null && !""
          .equals( item.getValue() ) ) { //$NON-NLS-1$ //$NON-NLS-2$
          retval.append( "        <user_defined>" ).append( Const.CR ); //$NON-NLS-1$
          retval.append( "          " )
            .append( XMLHandler.addTagValue( "name", item.getName() ) ); //$NON-NLS-1$ //$NON-NLS-2$
          retval.append( "          " )
            .append( XMLHandler.addTagValue( "value", item.getValue() ) ); //$NON-NLS-1$ //$NON-NLS-2$
          retval.append( "        </user_defined>" ).append( Const.CR ); //$NON-NLS-1$
        }
      }
    }
    retval.append( "      </user_defined_list>" ).append( Const.CR ); //$NON-NLS-1$
    return retval.toString();
  }

  @Override
  public void loadRep( Repository rep, IMetaStore metaStore, ObjectId id_jobentry, List<DatabaseMeta> databases,
                       List<SlaveServer> slaveServers ) throws KettleException {
    if ( rep != null ) {
      setHadoopJobName( rep.getJobEntryAttributeString( id_jobentry, "hadoop_job_name" ) ); //$NON-NLS-1$

      setMapRepositoryDir( rep.getJobEntryAttributeString( id_jobentry, "map_trans_repo_dir" ) ); //$NON-NLS-1$
      setMapRepositoryFile( rep.getJobEntryAttributeString( id_jobentry, "map_trans_repo_file" ) ); //$NON-NLS-1$
      String mapTransId = rep.getJobEntryAttributeString( id_jobentry, "map_trans_repo_reference" ); //$NON-NLS-1$
      setMapRepositoryReference( Const.isEmpty( mapTransId ) ? null : new StringObjectId( mapTransId ) );
      setMapTrans( rep.getJobEntryAttributeString( id_jobentry, "map_trans" ) ); //$NON-NLS-1$

      setReduceRepositoryDir( rep.getJobEntryAttributeString( id_jobentry, "reduce_trans_repo_dir" ) ); //$NON-NLS-1$
      setReduceRepositoryFile( rep.getJobEntryAttributeString( id_jobentry, "reduce_trans_repo_file" ) ); //$NON-NLS-1$
      String reduceTransId = rep.getJobEntryAttributeString( id_jobentry, "reduce_trans_repo_reference" ); //$NON-NLS-1$
      setReduceRepositoryReference( Const.isEmpty( reduceTransId ) ? null : new StringObjectId( reduceTransId ) );
      setReduceTrans( rep.getJobEntryAttributeString( id_jobentry, "reduce_trans" ) ); //$NON-NLS-1$
      setReducingSingleThreaded(
        rep.getJobEntryAttributeBoolean( id_jobentry, "reduce_single_threaded", false ) ); //$NON-NLS-1$

      setCombinerRepositoryDir(
        rep.getJobEntryAttributeString( id_jobentry, "combiner_trans_repo_dir" ) ); //$NON-NLS-1$
      setCombinerRepositoryFile(
        rep.getJobEntryAttributeString( id_jobentry, "combiner_trans_repo_file" ) ); //$NON-NLS-1$
      String combinerTransId =
        rep.getJobEntryAttributeString( id_jobentry, "combiner_trans_repo_reference" ); //$NON-NLS-1$
      setCombinerRepositoryReference( Const.isEmpty( combinerTransId ) ? null : new StringObjectId( combinerTransId ) );
      setCombinerTrans( rep.getJobEntryAttributeString( id_jobentry, "combiner_trans" ) ); //$NON-NLS-1$
      setCombiningSingleThreaded(
        rep.getJobEntryAttributeBoolean( id_jobentry, "combiner_single_threaded", false ) ); //$NON-NLS-1$

      setMapInputStepName( rep.getJobEntryAttributeString( id_jobentry, "map_input_step_name" ) ); //$NON-NLS-1$
      setMapOutputStepName( rep.getJobEntryAttributeString( id_jobentry, "map_output_step_name" ) ); //$NON-NLS-1$
      setCombinerInputStepName(
        rep.getJobEntryAttributeString( id_jobentry, "combiner_input_step_name" ) ); //$NON-NLS-1$
      setCombinerOutputStepName(
        rep.getJobEntryAttributeString( id_jobentry, "combiner_output_step_name" ) ); //$NON-NLS-1$
      setReduceInputStepName( rep.getJobEntryAttributeString( id_jobentry, "reduce_input_step_name" ) ); //$NON-NLS-1$
      setReduceOutputStepName( rep.getJobEntryAttributeString( id_jobentry, "reduce_output_step_name" ) ); //$NON-NLS-1$

      setBlocking( rep.getJobEntryAttributeBoolean( id_jobentry, "blocking" ) ); //$NON-NLS-1$
      setLoggingInterval( rep.getJobEntryAttributeString( id_jobentry, "logging_interval" ) ); //$NON-NLS-1$

      setInputPath( rep.getJobEntryAttributeString( id_jobentry, "input_path" ) ); //$NON-NLS-1$
      setInputFormatClass( rep.getJobEntryAttributeString( id_jobentry, "input_format_class" ) ); //$NON-NLS-1$
      setOutputPath( rep.getJobEntryAttributeString( id_jobentry, "output_path" ) ); //$NON-NLS-1$
      setCleanOutputPath( rep.getJobEntryAttributeBoolean( id_jobentry, "clean_output_path" ) ); //$NON-NLS-1$

      setSuppressOutputOfMapKey(
        rep.getJobEntryAttributeBoolean( id_jobentry, "suppress_output_map_key" ) ); //$NON-NLS-1$
      setSuppressOutputOfMapValue(
        rep.getJobEntryAttributeBoolean( id_jobentry, "suppress_output_map_value" ) ); //$NON-NLS-1$

      setSuppressOutputOfKey( rep.getJobEntryAttributeBoolean( id_jobentry, "suppress_output_key" ) ); //$NON-NLS-1$
      setSuppressOutputOfValue( rep.getJobEntryAttributeBoolean( id_jobentry, "suppress_output_value" ) ); //$NON-NLS-1$
      setOutputFormatClass( rep.getJobEntryAttributeString( id_jobentry, "output_format_class" ) ); //$NON-NLS-1$

      namedCluster =
        namedClusterLoadSaveUtil.loadClusterConfig( namedClusterService, id_jobentry, rep, metaStore, null, log );
      setRepository( rep );
      setNumMapTasks( rep.getJobEntryAttributeString( id_jobentry, "num_map_tasks" ) );
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

  public void saveRep( Repository rep, ObjectId id_job ) throws KettleException {
    if ( rep != null ) {
      rep.saveJobEntryAttribute( id_job, getObjectId(), "hadoop_job_name", hadoopJobName ); //$NON-NLS-1$

      rep.saveJobEntryAttribute( id_job, getObjectId(), "map_trans_repo_dir", mapRepositoryDir ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "map_trans_repo_file", mapRepositoryFile ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "map_trans_repo_reference",
          mapRepositoryReference == null ? null : mapRepositoryReference.toString() ); //$NON-NLS-1$
      mapTrans = mapTrans != null && mapTrans.endsWith( KTR_EXT ) ? mapTrans.replace( KTR_EXT, "" ) : mapTrans;
      rep.saveJobEntryAttribute( id_job, getObjectId(), "map_trans", mapTrans ); //$NON-NLS-1$

      rep.saveJobEntryAttribute( id_job, getObjectId(), "reduce_trans_repo_dir", reduceRepositoryDir ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "reduce_trans_repo_file", reduceRepositoryFile ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "reduce_trans_repo_reference",
          reduceRepositoryReference == null ? null : reduceRepositoryReference.toString() ); //$NON-NLS-1$
      reduceTrans = reduceTrans != null && reduceTrans.endsWith( KTR_EXT ) ? reduceTrans.replace( KTR_EXT, "" ) : reduceTrans;
      rep.saveJobEntryAttribute( id_job, getObjectId(), "reduce_trans", reduceTrans ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "reduce_single_threaded", reducingSingleThreaded ); //$NON-NLS-1$

      rep.saveJobEntryAttribute( id_job, getObjectId(), "combiner_trans_repo_dir", combinerRepositoryDir ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "combiner_trans_repo_file", combinerRepositoryFile ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "combiner_trans_repo_reference",
          combinerRepositoryReference == null ? null : combinerRepositoryReference.toString() ); //$NON-NLS-1$
      combinerTrans = combinerTrans != null && combinerTrans.endsWith( KTR_EXT ) ? combinerTrans.replace( KTR_EXT, "" ) : combinerTrans;
      rep.saveJobEntryAttribute( id_job, getObjectId(), "combiner_trans", combinerTrans ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "combiner_single_threaded", combiningSingleThreaded ); //$NON-NLS-1$

      rep.saveJobEntryAttribute( id_job, getObjectId(), "map_input_step_name", mapInputStepName ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "map_output_step_name", mapOutputStepName ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "combiner_input_step_name",
        combinerInputStepName ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "combiner_output_step_name",
        combinerOutputStepName ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "reduce_input_step_name", reduceInputStepName ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "reduce_output_step_name", reduceOutputStepName ); //$NON-NLS-1$

      rep.saveJobEntryAttribute( id_job, getObjectId(), "blocking", blocking ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "logging_interval", loggingInterval ); //$NON-NLS-1$

      rep.saveJobEntryAttribute( id_job, getObjectId(), "input_path", inputPath ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "input_format_class", inputFormatClass ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "output_path", outputPath ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "clean_output_path", cleanOutputPath ); //$NON-NLS-1$

      rep.saveJobEntryAttribute( id_job, getObjectId(), "suppress_output_map_key", suppressOutputMapKey ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "suppress_output_map_value",
        suppressOutputMapValue ); //$NON-NLS-1$

      rep.saveJobEntryAttribute( id_job, getObjectId(), "suppress_output_key", suppressOutputKey ); //$NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, getObjectId(), "suppress_output_value", suppressOutputValue ); //$NON-NLS-1$
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

  /**
   * @return the reduceSingleThreaded
   */
  public boolean isReducingSingleThreaded() {
    return reducingSingleThreaded;
  }

  /**
   * @param reducingSingleThreaded
   *          the reducing single threaded to set
   */
  public void setReducingSingleThreaded( boolean reducingSingleThreaded ) {
    this.reducingSingleThreaded = reducingSingleThreaded;
  }

  public boolean isCombiningSingleThreaded() {
    return combiningSingleThreaded;
  }

  public void setCombiningSingleThreaded( boolean combiningSingleThreaded ) {
    this.combiningSingleThreaded = combiningSingleThreaded;
  }

  private boolean hasMapperDefinition() {
    return !Const.isEmpty( mapTrans ) || mapRepositoryReference != null
      || ( !Const.isEmpty( mapRepositoryDir ) && !Const.isEmpty( mapRepositoryFile ) );
  }

  private boolean hasReducerDefinition() {
    return !Const.isEmpty( reduceTrans ) || reduceRepositoryReference != null
      || ( !Const.isEmpty( reduceRepositoryDir ) && !Const.isEmpty( reduceRepositoryFile ) );
  }

  private boolean hasCombinerDefinition() {
    return !Const.isEmpty( combinerTrans ) || combinerRepositoryReference != null
      || ( !Const.isEmpty( combinerRepositoryDir ) && !Const.isEmpty( combinerRepositoryFile ) );
  }

  private ObjectLocationSpecificationMethod defineSpecificationMethod( String repDir, String repFileName, ObjectId reference ) {
    if ( reference != null ) {
      return ObjectLocationSpecificationMethod.REPOSITORY_BY_REFERENCE;
    }
    return ObjectLocationSpecificationMethod.REPOSITORY_BY_NAME;
  }

  /**
   * @return The objects referenced in the step, like a a transformation, a job, a mapper, a reducer, a combiner, ...
   */
  public String[] getReferencedObjectDescriptions() {
    return new String[] { BaseMessages.getString( PKG, "JobEntryHadoopTransJobExecutor.ReferencedObject.Mapper" ),
      BaseMessages.getString( PKG, "JobEntryHadoopTransJobExecutor.ReferencedObject.Combiner" ),
      BaseMessages.getString( PKG, "JobEntryHadoopTransJobExecutor.ReferencedObject.Reducer" ), };
  }

  /**
   * @return true for each referenced object that is enabled or has a valid reference definition.
   */
  public boolean[] isReferencedObjectEnabled() {
    return new boolean[] { hasMapperDefinition(), hasCombinerDefinition(), hasReducerDefinition(), };
  }

  /**
   * Load the referenced object
   *
   * @param index
   *          the referenced object index to load (in case there are multiple references)
   * @param rep
   *          the repository
   * @param space
   *          the variable space to use
   * @return the referenced object once loaded
   * @throws KettleException
   */
  public Object loadReferencedObject( int index, Repository rep, VariableSpace space ) throws KettleException {
    switch ( index ) {
      case 0:
        return loadTransMeta( space, rep, mapTrans, mapRepositoryReference, mapRepositoryDir, mapRepositoryFile );
      case 1:
        return loadTransMeta( space, rep, combinerTrans, combinerRepositoryReference, combinerRepositoryDir,
          combinerRepositoryFile );
      case 2:
        return loadTransMeta( space, rep, reduceTrans, reduceRepositoryReference, reduceRepositoryDir,
          reduceRepositoryFile );
    }
    return null;

  }

  /**
   * Exports the object to a flat-file system, adding content with filename keys to a set of definitions. The supplied
   * resource naming interface allows the object to name appropriately without worrying about those parts of the
   * implementation specific details.
   *
   * @param space
   *          The variable space to resolve (environment) variables with.
   * @param definitions
   *          The map containing the filenames and content
   * @param namingInterface
   *          The resource naming interface allows the object to be named appropriately
   * @param repository
   *          The repository to load resources from
   * @param metaStore
   *          the metaStore to load external metadata from
   * @return The filename for this object. (also contained in the definitions map)
   * @throws KettleException
   *           in case something goes wrong during the export
   */
  @Override
  public String exportResources( VariableSpace space, Map<String, ResourceDefinition> definitions,
                                 ResourceNamingInterface namingInterface, Repository repository, IMetaStore metaStore )
    throws KettleException {

    // Try to load the transformation from repository or file.

    // Modify this recursively too...
    //
    // AGAIN: there is no need to clone this job entry because the caller is responsible for this.

    copyVariablesFrom( space );

    boolean[] enabled = isReferencedObjectEnabled();
    TransMeta transMeta;
    for ( int i = 0; i < enabled.length; i++ ) {
      if ( enabled[ i ] ) {
        //
        // First load the transformation metadata...
        //
        transMeta = (TransMeta) loadReferencedObject( i, repository, space );
        // Also go down into the transformation and export the files there. (mapping recursively down)
        //
        String proposedNewFilename =
          transMeta.exportResources( transMeta, definitions, namingInterface, repository, metaStore );
        // To get a relative path to it, we inject ${Internal.Job.Filename.Directory}
        //
        String newFilename = "${" + Const.INTERNAL_VARIABLE_JOB_FILENAME_DIRECTORY + "}/" + proposedNewFilename;

        // Set the correct filename inside the XML.
        //
        transMeta.setFilename( newFilename );

        // exports always reside in the root directory, in case we want to turn this into a file repository...
        //
        transMeta.setRepositoryDirectory( new RepositoryDirectory() );

        // export to filename ALWAYS (this allows the exported XML to be executed remotely)
        // change it in the job entry
        setSpecificationMethodAndValue( i, ObjectLocationSpecificationMethod.FILENAME, newFilename, null, null );
      }
    }

    return getHadoopJobName();
  }

  private void setSpecificationMethodAndValue( int i, ObjectLocationSpecificationMethod specification, String filename,
                                               String repositoryDir, ObjectId referrence ) {
    switch ( specification ) {
      case FILENAME: {
        switch ( i ) {
          case 0: {
            setMapTrans( filename );
            break;
          }
          case 1: {
            setCombinerTrans( filename );
            break;
          }
          case 2: {
            setReduceTrans( filename );
            break;
          }
        }
        break;
      }
      case REPOSITORY_BY_NAME: {
        switch ( i ) {
          case 0: {
            setMapRepositoryDir( repositoryDir );
            setMapRepositoryFile( filename );
            break;
          }
          case 1: {
            setCombinerRepositoryDir( repositoryDir );
            setCombinerRepositoryFile( filename );
            break;
          }
          case 2: {
            setReduceRepositoryDir( repositoryDir );
            setReduceRepositoryFile( filename );
            break;
          }
        }
        break;
      }
      case REPOSITORY_BY_REFERENCE: {
        switch ( i ) {
          case 0: {
            setMapRepositoryReference( referrence );
            break;
          }
          case 1: {
            setCombinerRepositoryReference( referrence );
            break;
          }
          case 2: {
            setReduceRepositoryReference( referrence );
            break;
          }
        }
        break;
      }
    }
  }

  public NamedClusterService getNamedClusterService() {
    return namedClusterService;
  }

  public NamedCluster getNamedCluster() {
    return namedCluster;
  }

  public void setNamedCluster( NamedCluster namedCluster ) {
    this.namedCluster = namedCluster;
  }

  public RuntimeTester getRuntimeTester() {
    return runtimeTester;
  }

  public RuntimeTestActionService getRuntimeTestActionService() {
    return runtimeTestActionService;
  }

  @Override public String getDialogClassName() {
    return DIALOG_NAME;
  }
}
