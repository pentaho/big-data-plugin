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

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.api.fs.FileSystem;
import org.pentaho.hadoop.shim.api.fs.Path;
import org.pentaho.hadoop.shim.api.mapred.RunningJob;
import org.pentaho.hadoop.shim.spi.HadoopShim;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 12/7/15.
 */
public class MapReduceJobBuilderImplTest {
  private NamedCluster namedCluster;
  private HadoopShim hadoopShim;
  private LogChannelInterface logChannelInterface;
  private VariableSpace variableSpace;
  private MapReduceJobBuilderImpl mapReduceJobBuilder;
  private Configuration configuration;
  private FileSystem fileSystem;
  private String testJobName;
  private String[] inputPaths;
  private URL resolvedJarUrl;
  private String jarUrl;
  private int numMapTasks;
  private int numReduceTasks;
  private String userPath;
  private String userPath2;
  private String fsUrl;
  private Path userPath1Path;
  private Path userPath2Path;
  private RunningJob runningJob;
  private float magicSetupNumber;
  private String logMeBasic;
  private String hdfsHost;
  private String hdfsPort;
  private String jobTrackerHost;
  private String jobTrackerPort;

  @Before
  public void setup() throws IOException {
    namedCluster = mock( NamedCluster.class );
    hadoopShim = mock( HadoopShim.class );
    logChannelInterface = mock( LogChannelInterface.class );
    variableSpace = mock( VariableSpace.class );
    configuration = mock( Configuration.class );
    fileSystem = mock( FileSystem.class );
    runningJob = mock( RunningJob.class );
    magicSetupNumber = 1337f;
    when( runningJob.setupProgress() ).thenReturn( magicSetupNumber );

    when( hadoopShim.createConfiguration() ).thenReturn( configuration );
    when( hadoopShim.getFileSystem( configuration ) ).thenReturn( fileSystem );
    when( hadoopShim.submitJob( configuration ) ).thenReturn( runningJob );
    fsUrl = "hdfs://";
    when( configuration.getDefaultFileSystemURL() ).thenReturn( fsUrl );

    hdfsHost = "hdfsHost";
    hdfsPort = "hdfsPort";
    jobTrackerHost = "jobTrackerHost";
    jobTrackerPort = "jobTrackerPort";
    when( variableSpace.environmentSubstitute( "a" ) ).thenReturn( hdfsHost );
    when( variableSpace.environmentSubstitute( "b" ) ).thenReturn( hdfsPort );
    when( variableSpace.environmentSubstitute( "c" ) ).thenReturn( jobTrackerHost );
    when( variableSpace.environmentSubstitute( "d" ) ).thenReturn( jobTrackerPort );
    when( namedCluster.getHdfsHost() ).thenReturn( "a" );
    when( namedCluster.getHdfsPort() ).thenReturn( "b" );
    when( namedCluster.getJobTrackerHost() ).thenReturn( "c" );
    when( namedCluster.getJobTrackerPort() ).thenReturn( "d" );

    mapReduceJobBuilder = new MapReduceJobBuilderImpl( namedCluster, hadoopShim, logChannelInterface, variableSpace );

    testJobName = "testJobName";
    userPath = "/user/path";
    userPath1Path = mock( Path.class );
    when( fileSystem.asPath( fsUrl, userPath ) ).thenReturn( userPath1Path );
    userPath2 = "/user/path2";
    userPath2Path = mock( Path.class );
    when( fileSystem.asPath( fsUrl, userPath2 ) ).thenReturn( userPath2Path );
    inputPaths = new String[] { userPath, userPath2 };
    jarUrl = "http://jar.com/myjar";
    resolvedJarUrl = new URL( "http://jar.com/myjar" );
    numMapTasks = 3;
    numReduceTasks = 1;
    logMeBasic = "log me basic";
  }

  @Test
  public void testMinimal() throws Exception {
    mapReduceJobBuilder.setHadoopJobName( testJobName );
    mapReduceJobBuilder.setInputPaths( inputPaths );
    mapReduceJobBuilder.setResolvedJarUrl( resolvedJarUrl );
    mapReduceJobBuilder.setNumMapTasks( numMapTasks );
    mapReduceJobBuilder.setNumReduceTasks( numReduceTasks );

    assertEquals( magicSetupNumber, mapReduceJobBuilder.submit().getSetupProgress(), 0 );

    verify( configuration ).setJobName( testJobName );
    verify( configuration ).setInputPaths( userPath1Path, userPath2Path );
    verify( configuration ).setNumMapTasks( numMapTasks );
    verify( configuration ).setNumReduceTasks( numReduceTasks );
  }

  @Test
  public void testMaxConfig() throws Exception {
    mapReduceJobBuilder.setHadoopJobName( testJobName );
    mapReduceJobBuilder.setOutputKeyClass( String.class.getCanonicalName() );
    mapReduceJobBuilder.setOutputValueClass( String.class.getCanonicalName() );
    mapReduceJobBuilder.setMapperClass( Map.class.getCanonicalName() );
    mapReduceJobBuilder.setCombinerClass( List.class.getCanonicalName() );
    mapReduceJobBuilder.setReducerClass( Set.class.getCanonicalName() );
    mapReduceJobBuilder.setInputFormatClass( Integer.class.getCanonicalName() );
    mapReduceJobBuilder.setOutputFormatClass( Float.class.getCanonicalName() );
    mapReduceJobBuilder.setInputPaths( inputPaths );
    mapReduceJobBuilder.setOutputPath( userPath2 );
    mapReduceJobBuilder.setJarUrl( jarUrl );
    mapReduceJobBuilder.setResolvedJarUrl( resolvedJarUrl );
    mapReduceJobBuilder.setNumMapTasks( numMapTasks );
    mapReduceJobBuilder.setNumReduceTasks( numReduceTasks );
    mapReduceJobBuilder.setMapOutputKeyClass( String.class.getCanonicalName() );
    mapReduceJobBuilder.setMapOutputValueClass( Integer.class.getCanonicalName() );
    mapReduceJobBuilder.setMapRunnerClass( Void.class.getCanonicalName() );
    String defA = "defA";
    String valA = "valA";
    mapReduceJobBuilder.set( defA, valA );
    mapReduceJobBuilder.set( null, "valB" );
    mapReduceJobBuilder.set( "", "valB" );
    mapReduceJobBuilder.set( "valC", null );
    mapReduceJobBuilder.set( "valB", "" );

    doAnswer( new Answer<Void>() {
      @Override public Void answer( InvocationOnMock invocation ) throws Throwable {
        List<String> msgs = (List<String>) invocation.getArguments()[ 5 ];
        msgs.add( logMeBasic );
        return null;
      }
    } ).when( hadoopShim ).configureConnectionInformation( eq( hdfsHost ), eq( hdfsPort ), eq( jobTrackerHost ),
      eq( jobTrackerPort ), eq( configuration ), eq( new ArrayList<String>() ) );
    assertEquals( magicSetupNumber, mapReduceJobBuilder.submit().getSetupProgress(), 0 );

    verify( configuration ).setJobName( testJobName );
    verify( configuration ).setOutputKeyClass( String.class );
    verify( configuration ).setOutputValueClass( String.class );
    verify( configuration ).setMapperClass( Map.class );
    verify( configuration ).setCombinerClass( List.class );
    verify( configuration ).setReducerClass( Set.class );
    verify( configuration ).setInputFormat( Integer.class );
    verify( configuration ).setOutputFormat( Float.class );
    verify( configuration ).setInputPaths( userPath1Path, userPath2Path );
    verify( configuration ).setOutputPath( userPath2Path );
    verify( configuration ).setJar( jarUrl );
    verify( configuration ).setNumMapTasks( numMapTasks );
    verify( configuration ).setNumReduceTasks( numReduceTasks );
    verify( configuration ).setMapOutputKeyClass( String.class );
    verify( configuration ).setMapOutputValueClass( Integer.class );
    verify( configuration ).setMapRunnerClass( Void.class );
    verify( logChannelInterface ).logBasic( logMeBasic );
    verify( configuration ).set( defA, valA );
    verify( configuration, never() ).set( isNull( String.class ), anyString() );
    verify( configuration, never() ).set( eq( "" ), anyString() );
    verify( configuration, never() ).set( eq( "valB" ), isNull( String.class ) );
    verify( configuration, never() ).set( eq( "valB" ), eq( "" ) );
  }
}
