/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2022 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.impl.shim.pig;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.bigdata.api.pig.PigResult;
import org.pentaho.bigdata.api.pig.PigService;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hadoop.shim.spi.PigShim;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * Created by bryan on 10/1/15.
 */
public class PigServiceImplTest {
  private NamedCluster namedCluster;
  private PigShim pigShim;
  private HadoopShim hadoopShim;
  private PigServiceImpl pigService;
  private WriterAppenderManager.Factory writerAppenderManagerFactory;
  private WriterAppenderManager writerAppenderManager;
  private LogChannelInterface logChannelInterface;
  private LogLevel logLevel;
  private String testName;
  private VariableSpace variableSpace;

  @Before
  public void setup() {
    namedCluster = mock( NamedCluster.class );
    pigShim = mock( PigShim.class );
    hadoopShim = mock( HadoopShim.class );
    writerAppenderManagerFactory = mock( WriterAppenderManager.Factory.class );
    writerAppenderManager = mock( WriterAppenderManager.class );
    logChannelInterface = mock( LogChannelInterface.class );
    logLevel = LogLevel.DETAILED;
    testName = "testName";
    variableSpace = mock( VariableSpace.class );
    when( writerAppenderManagerFactory.create( logChannelInterface, logLevel, testName, PigServiceImpl.PIG_LOGGERS ) ).thenReturn(
      writerAppenderManager );
    pigService = new PigServiceImpl( namedCluster, pigShim, hadoopShim, writerAppenderManagerFactory );
  }

  @Test
  public void testIsLocalExecutionSupported() {
    when( pigShim.isLocalExecutionSupported() ).thenReturn( true, false );
    assertTrue( pigService.isLocalExecutionSupported() );
    assertFalse( pigService.isLocalExecutionSupported() );
  }

  @Test
  public void testExecuteScriptLocal() throws Exception {
    String testScript = "testScript";
    String testScript2 = "testScript2";
    String pigScript = "pigScript";
    when( variableSpace.environmentSubstitute( testScript ) ).thenReturn( testScript2 );
    ArrayList<String> parameters = new ArrayList<>( Arrays.asList( "param" ) );
    when( pigShim.substituteParameters( eq( new File( testScript2 ).toURI().toURL() ), eq( parameters ) ) )
      .thenReturn( pigScript );
    PigResult pigResult =
      pigService.executeScript( testScript, PigService.ExecutionMode.LOCAL, parameters, testName,
        logChannelInterface, variableSpace, logLevel );
    assertNull( "Unexpected exception: " + pigResult.getException(), pigResult.getException() );
    verify( pigShim ).executeScript( eq( pigScript ), eq( PigShim.ExecutionMode.LOCAL ), eq( new Properties() ) );
  }

  @Test
  public void testExecuteScriptMapReduce() throws Exception {
    String testScript = "testScript";
    String testScript2 = "http://testScript2";
    String pigScript = "pigScript";
    final String configMessage = "configMessage";
    String hdfsHost = "hdfsHost";
    String hdfsPort = "hdfsPort";
    String jobTrackerHost = "jobTrackerHost";
    String jobTrackerPort = "jobTrackerPort";
    String hdfsHostSub = "hdfsHostSub";
    String hdfsPortSub = "hdfsPortSub";
    String jobTrackerHostSub = "jobTrackerHostSub";
    String jobTrackerPortSub = "jobTrackerPortSub";
    when( namedCluster.getHdfsHost() ).thenReturn( hdfsHost );
    when( namedCluster.getHdfsPort() ).thenReturn( hdfsPort );
    when( namedCluster.getJobTrackerHost() ).thenReturn( jobTrackerHost );
    when( namedCluster.getJobTrackerPort() ).thenReturn( jobTrackerPort );
    when( variableSpace.environmentSubstitute( hdfsHost ) ).thenReturn( hdfsHostSub );
    when( variableSpace.environmentSubstitute( hdfsPort ) ).thenReturn( hdfsPortSub );
    when( variableSpace.environmentSubstitute( jobTrackerHost ) ).thenReturn( jobTrackerHostSub );
    when( variableSpace.environmentSubstitute( jobTrackerPort ) ).thenReturn( jobTrackerPortSub );

    when( variableSpace.environmentSubstitute( testScript ) ).thenReturn( testScript2 );
    ArrayList<String> parameters = new ArrayList<>( Arrays.asList( "param" ) );
    when( pigShim.substituteParameters( eq( new URL( testScript2 ) ), eq( parameters ) ) )
      .thenReturn( pigScript );
    Configuration configuration = mock( Configuration.class );
    when( hadoopShim.createConfiguration() ).thenReturn( configuration );
    doAnswer( new Answer() {
      @Override public Object answer( InvocationOnMock invocation ) throws Throwable {
        return ( (ArrayList<String>) invocation.getArguments()[ 5 ] ).add( configMessage );
      }
    } ).when( hadoopShim )
      .configureConnectionInformation( eq( hdfsHostSub ), eq( hdfsPortSub ), eq( jobTrackerHostSub ),
        eq( jobTrackerPortSub ), eq( configuration ), eq( new ArrayList<String>() ) );
    PigResult pigResult =
      pigService.executeScript( testScript, PigService.ExecutionMode.MAPREDUCE, parameters, testName,
        logChannelInterface, variableSpace, logLevel );
    assertNull( "Unexpected exception: " + pigResult.getException(), pigResult.getException() );
    verify( pigShim ).executeScript( eq( pigScript ), eq( PigShim.ExecutionMode.MAPREDUCE ), eq( new Properties() ) );
    verify( logChannelInterface ).logBasic( configMessage );
  }

  @Test
  public void testExecuteScriptException() throws Exception {
    PigResult pigResult =
      pigService.executeScript( "ab", PigService.ExecutionMode.LOCAL, new ArrayList<String>(), testName,
        logChannelInterface, variableSpace, logLevel );
    assertNotNull( "Expected exception", pigResult.getException() );
  }
}
