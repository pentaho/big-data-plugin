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

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.bigdata.api.mapreduce.MapReduceJobAdvanced;
import org.pentaho.bigdata.api.mapreduce.MapReduceJobBuilder;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.api.fs.FileSystem;
import org.pentaho.hadoop.shim.api.fs.Path;
import org.pentaho.hadoop.shim.spi.HadoopShim;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by bryan on 12/3/15.
 */
public class MapReduceJobBuilderImpl implements MapReduceJobBuilder {
  private final NamedCluster namedCluster;
  private final HadoopShim hadoopShim;
  private final LogChannelInterface log;
  private final VariableSpace variableSpace;
  private final Map<String, String> userDefined;
  private URL resolvedJarUrl;
  private String jarUrl;
  private String hadoopJobName;
  private String outputKeyClass;
  private String outputValueClass;
  private String mapperClass;
  private String combinerClass;
  private String reducerClass;
  private String inputFormatClass;
  private String outputFormatClass;
  private String inputPath;
  private int numMapTasks;
  private int numReduceTasks;
  private String outputPath;

  public MapReduceJobBuilderImpl( NamedCluster namedCluster, HadoopShim hadoopShim, LogChannelInterface log,
                                  VariableSpace variableSpace ) {
    this.namedCluster = namedCluster;
    this.hadoopShim = hadoopShim;
    this.log = log;
    this.variableSpace = variableSpace;
    this.userDefined = new HashMap<>();
  }

  @Override public void setResolvedJarUrl( URL resolvedJarUrl ) {
    this.resolvedJarUrl = resolvedJarUrl;
  }

  @Override
  public void setJarUrl( String jarUrl ) {
    this.jarUrl = jarUrl;
  }

  @Override public void setHadoopJobName( String hadoopJobName ) {
    this.hadoopJobName = hadoopJobName;
  }

  @Override public void setOutputKeyClass( String outputKeyClass ) {
    this.outputKeyClass = outputKeyClass;
  }

  @Override public void setOutputValueClass( String outputValueClass ) {
    this.outputValueClass = outputValueClass;
  }

  @Override public void setMapperClass( String mapperClass ) {
    this.mapperClass = mapperClass;
  }

  @Override public void setCombinerClass( String combinerClass ) {
    this.combinerClass = combinerClass;
  }

  @Override public void setReducerClass( String reducerClass ) {
    this.reducerClass = reducerClass;
  }

  @Override public void setInputFormatClass( String inputFormatClass ) {
    this.inputFormatClass = inputFormatClass;
  }

  @Override public void setOutputFormatClass( String outputFormatClass ) {
    this.outputFormatClass = outputFormatClass;
  }

  @Override public void setInputPath( String inputPath ) {
    this.inputPath = inputPath;
  }

  @Override public void setNumMapTasks( int numMapTasks ) {
    this.numMapTasks = numMapTasks;
  }

  @Override public void setNumReduceTasks( int numReduceTasks ) {
    this.numReduceTasks = numReduceTasks;
  }

  @Override public void setOutputPath( String outputPath ) {
    this.outputPath = outputPath;
  }

  @Override public void putUserDefined( String key, String value ) {
    userDefined.put( key, value );
  }

  @Override public MapReduceJobAdvanced submit() throws Exception {
    Configuration conf = hadoopShim.createConfiguration();
    FileSystem fs = hadoopShim.getFileSystem( conf );
    URL[] urls = new URL[] { resolvedJarUrl };
    URLClassLoader loader = new URLClassLoader( urls, hadoopShim.getClass().getClassLoader() );
    conf.setJobName( hadoopJobName );

    conf.setOutputKeyClass( loader.loadClass( outputKeyClass ) );
    conf.setOutputValueClass( loader.loadClass( outputValueClass ) );

    if ( mapperClass != null ) {
      Class<?> mapper = loader.loadClass( mapperClass );
      conf.setMapperClass( mapper );
    }
    if ( combinerClass != null ) {
      Class<?> combiner = loader.loadClass( combinerClass );
      conf.setCombinerClass( combiner );
    }
    if ( reducerClass != null ) {
      Class<?> reducer = loader.loadClass( reducerClass );
      conf.setReducerClass( reducer );
    }

    if ( inputFormatClass != null ) {
      Class<?> inputFormat = loader.loadClass( inputFormatClass );
      conf.setInputFormat( inputFormat );
    }
    if ( outputFormatClass != null ) {
      Class<?> outputFormat = loader.loadClass( outputFormatClass );
      conf.setOutputFormat( outputFormat );
    }

    String hdfsHostnameS = variableSpace.environmentSubstitute( namedCluster.getHdfsHost() );
    String hdfsPortS = variableSpace.environmentSubstitute( namedCluster.getHdfsPort() );
    String jobTrackerHostnameS = variableSpace.environmentSubstitute( namedCluster.getJobTrackerHost() );
    String jobTrackerPortS = variableSpace.environmentSubstitute( namedCluster.getJobTrackerPort() );

    List<String> configMessages = new ArrayList<String>();
    hadoopShim.configureConnectionInformation( hdfsHostnameS, hdfsPortS, jobTrackerHostnameS, jobTrackerPortS, conf,
      configMessages );
    for ( String m : configMessages ) {
      log.logBasic( m );
    }

    String[] inputPathParts = inputPath.split( "," );
    List<Path> paths = new ArrayList<Path>();
    for ( String path : inputPathParts ) {
      paths.add( fs.asPath( conf.getDefaultFileSystemURL(), path ) );
    }
    Path[] finalPaths = paths.toArray( new Path[ paths.size() ] );

    conf.setInputPaths( finalPaths );
    conf.setOutputPath( fs.asPath( conf.getDefaultFileSystemURL(), outputPath ) );

    // process user defined values
    for ( Map.Entry<String, String> stringStringEntry : userDefined.entrySet() ) {
      String key = stringStringEntry.getKey();
      String value = stringStringEntry.getValue();
      if ( key != null && !"".equals( key ) && value != null && !"".equals( value ) ) {
        conf.set( key, value );
      }
    }

    conf.setJar( jarUrl );

    conf.setNumMapTasks( numMapTasks );
    conf.setNumReduceTasks( numReduceTasks );

    return new RunningJobMapReduceJobAdvancedImpl( hadoopShim.submitJob( conf ) );
  }
}
