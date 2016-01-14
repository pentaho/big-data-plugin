/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.pentaho.big.data.impl.shim.mapreduce;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.vfs2.FileObject;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.bigdata.api.mapreduce.MapReduceJobAdvanced;
import org.pentaho.bigdata.api.mapreduce.PentahoMapReduceJobBuilder;
import org.pentaho.bigdata.api.mapreduce.PentahoMapReduceOutputStepMetaInterface;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.version.BuildVersion;
import org.pentaho.hadoop.PluginPropertiesUtil;
import org.pentaho.hadoop.mapreduce.InKeyValueOrdinals;
import org.pentaho.hadoop.mapreduce.OutKeyValueOrdinals;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.api.fs.FileSystem;
import org.pentaho.hadoop.shim.api.fs.Path;
import org.pentaho.hadoop.shim.spi.HadoopShim;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by bryan on 1/8/16.
 */
public class PentahoMapReduceJobBuilderImpl extends MapReduceJobBuilderImpl implements PentahoMapReduceJobBuilder {
  public static final Class<?> PKG = PentahoMapReduceJobBuilderImpl.class;
  public static final String MAPREDUCE_APPLICATION_CLASSPATH = "mapreduce.application.classpath";
  public static final String DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH =
    "$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*,$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*";
  public static final String PENTAHO_MAPREDUCE_PROPERTY_USE_DISTRIBUTED_CACHE = "pmr.use.distributed.cache";
  public static final String PENTAHO_MAPREDUCE_PROPERTY_PMR_LIBRARIES_ARCHIVE_FILE = "pmr.libraries.archive.file";
  public static final String PENTAHO_MAPREDUCE_PROPERTY_KETTLE_HDFS_INSTALL_DIR = "pmr.kettle.dfs.install.dir";
  public static final String PENTAHO_MAPREDUCE_PROPERTY_KETTLE_INSTALLATION_ID = "pmr.kettle.installation.id";
  public static final String PENTAHO_MAPREDUCE_PROPERTY_ADDITIONAL_PLUGINS = "pmr.kettle.additional.plugins";
  public static final String PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_INPUT_STEP_NOT_SPECIFIED =
    "PentahoMapReduceJobBuilderImpl.InputStepNotSpecified";
  public static final String PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_INPUT_STEP_NOT_FOUND =
    "PentahoMapReduceJobBuilderImpl.InputStepNotFound";
  public static final String PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_NO_KEY_ORDINAL =
    "PentahoMapReduceJobBuilderImpl.NoKeyOrdinal";
  public static final String PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_NO_VALUE_ORDINAL =
    "PentahoMapReduceJobBuilderImpl.NoValueOrdinal";
  public static final String PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_INPUT_HOP_DISABLED =
    "PentahoMapReduceJobBuilderImpl.InputHopDisabled";
  public static final String PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_OUTPUT_STEP_NOT_SPECIFIED =
    "PentahoMapReduceJobBuilderImpl.OutputStepNotSpecified";
  public static final String PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_OUTPUT_STEP_NOT_FOUND =
    "PentahoMapReduceJobBuilderImpl.OutputStepNotFound";
  public static final String ORG_PENTAHO_BIG_DATA_KETTLE_PLUGINS_MAPREDUCE_STEP_HADOOP_EXIT_META =
    "org.pentaho.big.data.kettle.plugins.mapreduce.step.HadoopExitMeta";
  public static final String PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_VALIDATION_ERROR =
    "PentahoMapReduceJobBuilderImpl.ValidationError";
  public static final String PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_NO_OUTPUT_KEY_ORDINAL =
    "PentahoMapReduceJobBuilderImpl.NoOutputKeyOrdinal";
  public static final String PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_NO_OUTPUT_VALUE_ORDINAL =
    "PentahoMapReduceJobBuilderImpl.NoOutputValueOrdinal";
  public static final String TRANSFORMATION_MAP_XML = "transformation-map-xml";
  public static final String TRANSFORMATION_MAP_INPUT_STEPNAME = "transformation-map-input-stepname";
  public static final String TRANSFORMATION_MAP_OUTPUT_STEPNAME = "transformation-map-output-stepname";
  public static final String LOG_LEVEL = "logLevel";
  public static final String TRANSFORMATION_COMBINER_XML = "transformation-combiner-xml";
  public static final String TRANSFORMATION_COMBINER_INPUT_STEPNAME = "transformation-combiner-input-stepname";
  public static final String TRANSFORMATION_COMBINER_OUTPUT_STEPNAME = "transformation-combiner-output-stepname";
  public static final String TRANSFORMATION_REDUCE_XML = "transformation-reduce-xml";
  public static final String TRANSFORMATION_REDUCE_INPUT_STEPNAME = "transformation-reduce-input-stepname";
  public static final String TRANSFORMATION_REDUCE_OUTPUT_STEPNAME = "transformation-reduce-output-stepname";
  public static final String JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_CLEANING_OUTPUT_PATH =
    "JobEntryHadoopTransJobExecutor.CleaningOutputPath";
  public static final String JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_FAILED_TO_CLEAN_OUTPUT_PATH =
    "JobEntryHadoopTransJobExecutor.FailedToCleanOutputPath";
  public static final String JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_ERROR_CLEANING_OUTPUT_PATH =
    "JobEntryHadoopTransJobExecutor.ErrorCleaningOutputPath";
  public static final String JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_KETTLE_HDFS_INSTALL_DIR_MISSING =
    "JobEntryHadoopTransJobExecutor.KettleHdfsInstallDirMissing";
  public static final String JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_INSTALLATION_OF_KETTLE_FAILED =
    "JobEntryHadoopTransJobExecutor.InstallationOfKettleFailed";
  private final HadoopConfiguration hadoopConfiguration;
  private final HadoopShim hadoopShim;
  private final LogChannelInterface log;
  private final PluginInterface pluginInterface;
  private final Properties pmrProperties;
  private final TransFactory transFactory;
  private boolean cleanOutputPath;
  private LogLevel logLevel;
  private String mapperTransformationXml;
  private String mapperInputStep;
  private String mapperOutputStep;
  private String combinerTransformationXml;
  private String combinerInputStep;
  private String combinerOutputStep;
  private String reducerTransformationXml;
  private String reducerInputStep;
  private String reducerOutputStep;

  public PentahoMapReduceJobBuilderImpl( NamedCluster namedCluster,
                                         HadoopConfiguration hadoopConfiguration,
                                         LogChannelInterface log,
                                         VariableSpace variableSpace, PluginInterface pluginInterface,
                                         Properties pmrProperties ) {
    this( namedCluster, hadoopConfiguration, log, variableSpace, pluginInterface, pmrProperties,
      new TransFactoryImpl() );
  }

  @VisibleForTesting PentahoMapReduceJobBuilderImpl( NamedCluster namedCluster,
                                                     HadoopConfiguration hadoopConfiguration,
                                                     LogChannelInterface log,
                                                     VariableSpace variableSpace, PluginInterface pluginInterface,
                                                     Properties pmrProperties, TransFactory transFactory ) {
    super( namedCluster, hadoopConfiguration.getHadoopShim(), log, variableSpace );
    this.hadoopConfiguration = hadoopConfiguration;
    this.hadoopShim = hadoopConfiguration.getHadoopShim();
    this.log = log;
    this.pluginInterface = pluginInterface;
    this.pmrProperties = pmrProperties;
    this.transFactory = transFactory;
  }

  @Override public String getHadoopWritableCompatibleClassName( ValueMetaInterface valueMetaInterface ) {
    Class<?> hadoopWritableCompatibleClass = hadoopShim.getHadoopWritableCompatibleClass( valueMetaInterface );
    if ( hadoopWritableCompatibleClass == null ) {
      return null;
    }
    return hadoopWritableCompatibleClass.getCanonicalName();
  }

  @Override public void setLogLevel( LogLevel logLevel ) {
    this.logLevel = logLevel;
  }

  @Override public void setCleanOutputPath( boolean cleanOutputPath ) {
    this.cleanOutputPath = cleanOutputPath;
  }

  @Override public void verifyTransMeta( TransMeta transMeta, String inputStepName, String outputStepName )
    throws KettleException {
    // Verify the input step: see that the key/value fields are present...
    //
    if ( Const.isEmpty( inputStepName ) ) {
      throw new KettleException( BaseMessages.getString( PKG,
        PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_INPUT_STEP_NOT_SPECIFIED ) );
    }
    StepMeta inputStepMeta = transMeta.findStep( inputStepName );
    if ( inputStepMeta == null ) {
      throw new KettleException(
        BaseMessages.getString( PKG, PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_INPUT_STEP_NOT_FOUND, inputStepName ) );
    }

    // Get the fields coming out of the input step...
    //
    RowMetaInterface injectorRowMeta = transMeta.getStepFields( inputStepMeta );

    // Verify that the key and value fields are found
    //
    InKeyValueOrdinals inOrdinals = new InKeyValueOrdinals( injectorRowMeta );
    if ( inOrdinals.getKeyOrdinal() < 0 ) {
      throw new KettleException(
        BaseMessages.getString( PKG, PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_NO_KEY_ORDINAL, inputStepName ) );
    }
    if ( inOrdinals.getValueOrdinal() < 0 ) {
      throw new KettleException(
        BaseMessages.getString( PKG, PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_NO_VALUE_ORDINAL, inputStepName ) );
    }

    // make sure that the input step is enabled (i.e. its outgoing hop
    // hasn't been disabled)
    Trans t = transFactory.create( transMeta );
    t.prepareExecution( null );
    if ( t.getStepInterface( inputStepName, 0 ) == null ) {
      throw new KettleException(
        BaseMessages.getString( PKG, PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_INPUT_HOP_DISABLED, inputStepName ) );
    }

    // Now verify the output step output of the reducer...
    //
    if ( Const.isEmpty( outputStepName ) ) {
      throw new KettleException( BaseMessages.getString( PKG,
        PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_OUTPUT_STEP_NOT_SPECIFIED ) );
    }

    StepMeta outputStepMeta = transMeta.findStep( outputStepName );
    if ( outputStepMeta == null ) {
      throw new KettleException(
        BaseMessages.getString( PKG, PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_OUTPUT_STEP_NOT_FOUND, outputStepName ) );
    }

    // It's a special step designed to map the output key/value pair fields...
    //
    if ( outputStepMeta.getStepMetaInterface() instanceof PentahoMapReduceOutputStepMetaInterface ) {
      // Get the row fields entering the output step...
      //
      RowMetaInterface outputRowMeta = transMeta.getPrevStepFields( outputStepMeta );
      StepMetaInterface exitMeta = outputStepMeta.getStepMetaInterface();

      List<CheckResultInterface> remarks = new ArrayList<>();
      ( (PentahoMapReduceOutputStepMetaInterface) exitMeta )
        .checkPmr( remarks, transMeta, outputStepMeta, outputRowMeta );
      StringBuilder message = new StringBuilder();
      for ( CheckResultInterface remark : remarks ) {
        if ( remark.getType() == CheckResultInterface.TYPE_RESULT_ERROR ) {
          message.append( message.toString() ).append( Const.CR );
        }
      }
      if ( message.length() > 0 ) {
        throw new KettleException( BaseMessages.getString( PKG, PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_VALIDATION_ERROR ) + Const.CR + message );
      }
    } else {
      // Any other step: verify that the outKey and outValue fields exist...
      //
      RowMetaInterface outputRowMeta = transMeta.getStepFields( outputStepMeta );
      OutKeyValueOrdinals outOrdinals = new OutKeyValueOrdinals( outputRowMeta );
      if ( outOrdinals.getKeyOrdinal() < 0 ) {
        throw new KettleException( BaseMessages.getString( PKG,
          PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_NO_OUTPUT_KEY_ORDINAL, outputStepName ) );
      }
      if ( outOrdinals.getValueOrdinal() < 0 ) {
        throw new KettleException( BaseMessages.getString( PKG,
          PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_NO_OUTPUT_VALUE_ORDINAL, outputStepName ) );
      }
    }
  }

  @Override
  public void setCombinerInfo( String combinerTransformationXml, String combinerInputStep, String combinerOutputStep ) {
    this.combinerTransformationXml = combinerTransformationXml;
    this.combinerInputStep = combinerInputStep;
    this.combinerOutputStep = combinerOutputStep;
  }

  @Override
  public void setReducerInfo( String reducerTransformationXml, String reducerInputStep, String reducerOutputStep ) {
    this.reducerTransformationXml = reducerTransformationXml;
    this.reducerInputStep = reducerInputStep;
    this.reducerOutputStep = reducerOutputStep;
  }

  @Override
  public void setMapperInfo( String mapperTransformationXml, String mapperInputStep, String mapperOutputStep ) {
    this.mapperTransformationXml = mapperTransformationXml;
    this.mapperInputStep = mapperInputStep;
    this.mapperOutputStep = mapperOutputStep;
  }

  @Override protected void configure( Configuration conf ) throws Exception {
    setMapRunnerClass( hadoopShim.getPentahoMapReduceMapRunnerClass().getCanonicalName() );

    conf.set( TRANSFORMATION_MAP_XML, mapperTransformationXml );
    conf.set( TRANSFORMATION_MAP_INPUT_STEPNAME, mapperInputStep );
    conf.set( TRANSFORMATION_MAP_OUTPUT_STEPNAME, mapperOutputStep );

    if ( combinerTransformationXml != null ) {
      conf.set( TRANSFORMATION_COMBINER_XML, combinerTransformationXml );
      conf.set( TRANSFORMATION_COMBINER_INPUT_STEPNAME, combinerInputStep );
      conf.set( TRANSFORMATION_COMBINER_OUTPUT_STEPNAME, combinerOutputStep );
      setCombinerClass( hadoopShim.getPentahoMapReduceCombinerClass().getCanonicalName() );
    }
    if ( reducerTransformationXml != null ) {
      conf.set( TRANSFORMATION_REDUCE_XML, reducerTransformationXml );
      conf.set( TRANSFORMATION_REDUCE_INPUT_STEPNAME, reducerInputStep );
      conf.set( TRANSFORMATION_REDUCE_OUTPUT_STEPNAME, reducerOutputStep );
      setReducerClass( hadoopShim.getPentahoMapReduceReducerClass().getCanonicalName() );
    }
    conf.setJarByClass( hadoopShim.getPentahoMapReduceMapRunnerClass() );
    conf.set( LOG_LEVEL, logLevel.toString() );

    super.configure( conf );
  }

  @Override protected MapReduceJobAdvanced submit( Configuration conf ) throws IOException {
    cleanOutputPath( conf );

    FileSystem fs = hadoopShim.getFileSystem( conf );

    if ( Boolean.parseBoolean( getProperty( conf, pmrProperties, PENTAHO_MAPREDUCE_PROPERTY_USE_DISTRIBUTED_CACHE,
      Boolean.toString( true ) ) ) ) {
      String installPath =
        getProperty( conf, pmrProperties, PENTAHO_MAPREDUCE_PROPERTY_KETTLE_HDFS_INSTALL_DIR, null );
      String installId =
        getProperty( conf, pmrProperties, PENTAHO_MAPREDUCE_PROPERTY_KETTLE_INSTALLATION_ID, BuildVersion
          .getInstance().getVersion() );
      try {
        if ( Const.isEmpty( installPath ) ) {
          throw new IllegalArgumentException( BaseMessages.getString( PKG,
            JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_KETTLE_HDFS_INSTALL_DIR_MISSING ) );
        }
        if ( Const.isEmpty( installId ) ) {
          String pluginVersion = new PluginPropertiesUtil().getVersion();

          installId = BuildVersion.getInstance().getVersion();
          if ( pluginVersion != null ) {
            installId = installId + "-" + pluginVersion;
          }

          installId = installId + "-" + hadoopConfiguration.getIdentifier();
        }
        if ( !installPath.endsWith( Const.FILE_SEPARATOR ) ) {
          installPath += Const.FILE_SEPARATOR;
        }
        Path kettleEnvInstallDir = fs.asPath( installPath, installId );
        FileObject pmrLibArchive =
          KettleVFS.getFileObject( pluginInterface.getPluginDirectory().getPath() + Const.FILE_SEPARATOR
            + getProperty( conf, pmrProperties, PENTAHO_MAPREDUCE_PROPERTY_PMR_LIBRARIES_ARCHIVE_FILE, null ) );
        // Make sure the version we're attempting to use is installed
        if ( hadoopShim.getDistributedCacheUtil().isKettleEnvironmentInstalledAt( fs, kettleEnvInstallDir ) ) {
          log.logDetailed( BaseMessages.getString( PKG, "JobEntryHadoopTransJobExecutor.UsingKettleInstallationFrom",
            kettleEnvInstallDir.toUri().getPath() ) );
        } else {
          // Load additional plugin folders as requested
          String additionalPluginNames =
            getProperty( conf, pmrProperties, PENTAHO_MAPREDUCE_PROPERTY_ADDITIONAL_PLUGINS, null );
          if ( pmrLibArchive == null ) {
            throw new KettleException( BaseMessages.getString( PKG, "JobEntryHadoopTransJobExecutor.UnableToLocateArchive",

              pmrLibArchive ) );
          }

          log.logBasic( BaseMessages.getString( PKG, "JobEntryHadoopTransJobExecutor.InstallingKettleAt",
            kettleEnvInstallDir ) );

          FileObject bigDataPluginFolder = KettleVFS.getFileObject( pluginInterface.getPluginDirectory().getPath() );
          hadoopShim.getDistributedCacheUtil().installKettleEnvironment( pmrLibArchive, fs, kettleEnvInstallDir, bigDataPluginFolder,
            additionalPluginNames );

          log.logBasic( BaseMessages
            .getString( PKG, "JobEntryHadoopTransJobExecutor.InstallationOfKettleSuccessful", kettleEnvInstallDir ) );
        }
        if ( !hadoopShim.getDistributedCacheUtil().isKettleEnvironmentInstalledAt( fs, kettleEnvInstallDir ) ) {
          throw new KettleException( BaseMessages.getString( PKG,
            "JobEntryHadoopTransJobExecutor.KettleInstallationMissingFrom", kettleEnvInstallDir.toUri().getPath() ) );
        }

        log.logBasic( BaseMessages.getString( PKG, "JobEntryHadoopTransJobExecutor.ConfiguringJobWithKettleAt",
          kettleEnvInstallDir.toUri().getPath() ) );

        String mapreduceClasspath = conf.get( MAPREDUCE_APPLICATION_CLASSPATH, DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH );
        conf.set( MAPREDUCE_APPLICATION_CLASSPATH, "classes/," + mapreduceClasspath );

        hadoopShim.getDistributedCacheUtil().configureWithKettleEnvironment( conf, fs, kettleEnvInstallDir );
        log.logBasic( MAPREDUCE_APPLICATION_CLASSPATH + ": " + conf.get( MAPREDUCE_APPLICATION_CLASSPATH ) );
      } catch ( Exception ex ) {
        throw new IOException(
          BaseMessages.getString( PKG, JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_INSTALLATION_OF_KETTLE_FAILED ), ex );
      }
    }

    return super.submit( conf );
  }

  /**
   * Gets a property from the configuration. If it is missing it will load it from the properties provided. If it cannot
   * be found there the default value provided will be used.
   *
   * @param conf         Configuration to check for property first.
   * @param properties   Properties to check for property second.
   * @param propertyName Name of the property to return
   * @param defaultValue Default value to use if no property by the given name could be found in {@code conf} or {@code
   *                     properties}
   * @return Value of {@code propertyName}
   */
  public String getProperty( Configuration conf, Properties properties, String propertyName, String defaultValue ) {
    String fromConf = conf.get( propertyName );
    return !Const.isEmpty( fromConf ) ? fromConf : properties.getProperty( propertyName, defaultValue );
  }

  @VisibleForTesting void cleanOutputPath( Configuration conf ) throws IOException {
    if ( cleanOutputPath ) {
      FileSystem fs = hadoopShim.getFileSystem( conf );
      Path path = getOutputPath( conf, fs );
      String outputPath = path.toUri().toString();
      if ( log.isBasic() ) {
        log.logBasic( BaseMessages.getString( PKG, JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_CLEANING_OUTPUT_PATH, outputPath ) );
      }
      try {
        if ( !fs.exists( path ) ) {
          // If the path does not exist one could think of it as "already cleaned"
          return;
        }
        if ( !fs.delete( path, true ) ) {
          if ( log.isBasic() ) {
            log.logBasic(
              BaseMessages.getString( PKG, JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_FAILED_TO_CLEAN_OUTPUT_PATH, outputPath ) );
          }
        }
      } catch ( IOException ex ) {
        throw new IOException(
          BaseMessages.getString( PKG, JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_ERROR_CLEANING_OUTPUT_PATH, outputPath ), ex );
      }
    }
  }

  @VisibleForTesting interface TransFactory {
    Trans create( TransMeta transMeta );
  }

  @VisibleForTesting static class TransFactoryImpl implements TransFactory {

    @Override public Trans create( TransMeta transMeta ) {
      return new Trans( transMeta );
    }
  }
}
