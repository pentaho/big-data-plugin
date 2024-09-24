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

import org.junit.Before;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.HadoopClientServices;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.hadoop.shim.api.cluster.ClusterInitializationException;
import org.pentaho.big.data.impl.cluster.NamedClusterImpl;

import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.entry.loadSave.LoadSaveTester;
import org.pentaho.di.trans.steps.loadsave.validator.FieldLoadSaveValidator;
import org.pentaho.di.trans.steps.loadsave.validator.MapLoadSaveValidator;
import org.pentaho.di.trans.steps.loadsave.validator.StringLoadSaveValidator;
import org.pentaho.hadoop.shim.api.pig.PigResult;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 7/10/15.
 */
public class JobEntryPigScriptExecutorTest {
  private NamedClusterService namedClusterService;
  private RuntimeTestActionService runtimeTestActionService;
  private NamedClusterServiceLocator namedClusterServiceLocator;

  private HadoopClientServices hadoopClientServices;
  private JobEntryPigScriptExecutor jobEntryPigScriptExecutor;
  private NamedCluster namedCluster;
  private String jobEntryName;
  private String namedClusterName;
  private String namedClusterHdfsHost;
  private String namedClusterHdfsPort;
  private String namedClusterJobTrackerPort;
  private String namedClusterJobTrackerHost;
  private RuntimeTester runtimeTester;
  private Result result;
  private LogChannelInterface logChannelInterface;
  private PigResult pigResult;
  private Job job;

  @Before
  public void setup() throws ClusterInitializationException {
    namedClusterService = mock( NamedClusterService.class );
    when( namedClusterService.getClusterTemplate() ).thenReturn( new NamedClusterImpl() );
    namedClusterServiceLocator = mock( NamedClusterServiceLocator.class );
    pigResult = mock( PigResult.class );
    runtimeTester = mock( RuntimeTester.class );
    runtimeTestActionService = mock( RuntimeTestActionService.class );
    result = mock( Result.class );
    job = mock( Job.class );
    logChannelInterface = mock( LogChannelInterface.class );
    jobEntryPigScriptExecutor =
      new JobEntryPigScriptExecutor( namedClusterService, runtimeTestActionService, runtimeTester,
        namedClusterServiceLocator );
    jobEntryPigScriptExecutor.setScriptFilename(
      getClass().getClassLoader().getResource( "org/pentaho/big/data/kettle/plugins/pig/pig.script" ).toString() );
    jobEntryPigScriptExecutor.setParentJob( job );
    jobEntryPigScriptExecutor.setLog( logChannelInterface );

    jobEntryName = "jobEntryName";
    namedClusterName = "namedClusterName";
    namedClusterHdfsHost = "namedClusterHdfsHost";
    namedClusterHdfsPort = "namedClusterHdfsPort";
    namedClusterJobTrackerHost = "namedClusterJobTrackerHost";
    namedClusterJobTrackerPort = "namedClusterJobTrackerPort";

    hadoopClientServices = mock( HadoopClientServices.class );
    namedCluster = mock( NamedCluster.class );
    when( namedClusterServiceLocator.getService( namedCluster, HadoopClientServices.class ) ).thenReturn( hadoopClientServices );
    when( namedCluster.getName() ).thenReturn( namedClusterName );
    when( namedCluster.getHdfsHost() ).thenReturn( namedClusterHdfsHost );
    when( namedCluster.getHdfsPort() ).thenReturn( namedClusterHdfsPort );
    when( namedCluster.getJobTrackerHost() ).thenReturn( namedClusterJobTrackerHost );
    when( namedCluster.getJobTrackerPort() ).thenReturn( namedClusterJobTrackerPort );
    jobEntryPigScriptExecutor.setNamedCluster( namedCluster );
  }

  @Test
  public void testLoadSave() throws KettleException {
    List<String> commonAttributes = new ArrayList<>();
    commonAttributes.add( "namedCluster" );
    commonAttributes.add( "enableBlocking" );
    commonAttributes.add( "localExecution" );
    commonAttributes.add( "scriptFilename" );
    commonAttributes.add( "scriptParameters" );

    Map<String, FieldLoadSaveValidator<?>> fieldLoadSaveValidatorTypeMap = new HashMap<>();
    fieldLoadSaveValidatorTypeMap.put( NamedCluster.class.getCanonicalName(), new PigNamedClusterValidator() );

    Map<String, FieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap = new HashMap<>();
    fieldLoadSaveValidatorAttributeMap.put( "scriptParameters",
      new MapLoadSaveValidator<>( new StringLoadSaveValidator(), new StringLoadSaveValidator() ) );

    LoadSaveTester<JobEntryPigScriptExecutor> jobEntryPigScriptExecutorLoadSaveTester =
      new LoadSaveTester<JobEntryPigScriptExecutor>( JobEntryPigScriptExecutor.class, commonAttributes,
        new HashMap<String, String>(), new HashMap<String, String>(), fieldLoadSaveValidatorAttributeMap,
        fieldLoadSaveValidatorTypeMap ) {
        @Override public JobEntryPigScriptExecutor createMeta() {
          return new JobEntryPigScriptExecutor( namedClusterService, runtimeTestActionService, runtimeTester,
            namedClusterServiceLocator );
        }
      };

    jobEntryPigScriptExecutorLoadSaveTester.testSerialization();
  }

  @Test
  public void testGetNamedClusterService() {
    assertEquals( namedClusterService, jobEntryPigScriptExecutor.getNamedClusterService() );
  }

  @Test
  public void testGetRuntimeTestActionService() {
    assertEquals( runtimeTestActionService, jobEntryPigScriptExecutor.getRuntimeTestActionService() );
  }

  @Test
  public void testGetRuntimeTester() {
    assertEquals( runtimeTester, jobEntryPigScriptExecutor.getRuntimeTester() );
  }

  @Test( expected = KettleException.class )
  public void testExecuteNoScriptFile() throws KettleException {
    jobEntryPigScriptExecutor.setScriptFilename( "" );
    try {
      jobEntryPigScriptExecutor.execute( result, 0 );
    } catch ( KettleException e ) {
      assertEquals( BaseMessages.getString( JobEntryPigScriptExecutor.PKG,
        JobEntryPigScriptExecutor.JOB_ENTRY_PIG_SCRIPT_EXECUTOR_ERROR_NO_PIG_SCRIPT_SPECIFIED ), e.getSuperMessage() );
      throw e;
    }
  }

  @Test
  public void testNonzeroBlocking() throws KettleException {
    jobEntryPigScriptExecutor.setEnableBlocking( true );
    when( pigResult.getResult() ).thenReturn( new int[] { 0, 10 } );
    when( hadoopClientServices
      .runPig( eq( jobEntryPigScriptExecutor.getScriptFilename() ), eq( HadoopClientServices.PigExecutionMode.MAPREDUCE ),
        eq( new ArrayList<String>() ), eq( (String) null ), eq( logChannelInterface ), eq( jobEntryPigScriptExecutor ),
        eq( (LogLevel) null ) ) )
      .thenReturn( pigResult );
    assertEquals( result, jobEntryPigScriptExecutor.execute( result, 0 ) );
    verify( result ).setStopped( true );
    verify( result ).setNrErrors( 10 );
    verify( result ).setResult( false );
  }

  @Test
  public void testGettingFailedStatusIfExecutionException() {
    pigResult = mock( PigResult.class );
    result = new Result(  );
    when( pigResult.getResult() ).thenReturn( null );
    when( pigResult.getException() ).thenReturn( new Exception(  ) );
    jobEntryPigScriptExecutor.processScriptExecutionResult( pigResult, result );
    assertFalse( result.getResult() );
    assertTrue( result.isStopped() );
    assertEquals( 1L, result.getNrErrors() );
  }

  @Test
  public void testGettingFailedStatusIfNrErrors() {
    pigResult = mock( PigResult.class );
    result = new Result(  );
    when( pigResult.getResult() ).thenReturn( new int[] { 0, 1} ).thenReturn( null );
    when( pigResult.getException() ).thenReturn( null );
    jobEntryPigScriptExecutor.processScriptExecutionResult(  pigResult, result );
    assertFalse( result.getResult() );
    assertTrue( result.isStopped() );
    assertEquals( 1L, result.getNrErrors() );
  }

}
