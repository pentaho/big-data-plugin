/*
 * ! ******************************************************************************
 *  *
 *  * Pentaho Data Integration
 *  *
 *  * Copyright (C) 2002-2014 by Pentaho : http://www.pentaho.com
 *  *
 *  *******************************************************************************
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with
 *  * the License. You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  *
 *  *****************************************************************************
 */
package org.pentaho.di.job.entries.hadoopjobexecutor;

import org.apache.commons.vfs.VFS;
import org.pentaho.hadoop.shim.api.mapred.TaskCompletionEvent;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.job.Job;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.api.fs.Path;
import org.pentaho.hadoop.shim.api.mapred.RunningJob;
import org.pentaho.hadoop.shim.common.CommonHadoopShim;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;

/**
 * User: Dzmitry Stsiapanau Date: 9/11/14 Time: 1:36 PM
 */
public class JobEntryHadoopJobExecutorTest {
  @Before
  public void setUp() throws Exception {
    KettleEnvironment.init();
  }

  @Test
  public void testExecuteSubstituteJobUrl() {
    File file = null;
    try {
      file = File.createTempFile( "hadoopJobExecutorTest", ".txt" );
    } catch ( IOException e ) {
      e.printStackTrace();
      fail( e.getMessage() );
    }
    final String path = file.getAbsolutePath();
    Job job = new Job();
    job.injectVariables( System.getenv() );
    JobEntryHadoopJobExecutor jobExecutor = new JobEntryHadoopJobExecutor() {
      protected HadoopConfiguration getHadoopConfiguration() throws ConfigurationException {
        try {
          CommonHadoopShim shim = Mockito.spy( new CommonHadoopShim() );
          HadoopConfiguration hc = new HadoopConfiguration( VFS.getManager().resolveFile( "ram:///" ), "test", "test",
            shim );
          hc = Mockito.spy( hc );
          Configuration conf = Mockito.spy( shim.createConfiguration() );
          doAnswer( new Answer() {
            @Override public Object answer( InvocationOnMock invocation ) throws Throwable {
              String jobUrl = (String) invocation.getArguments()[ 0 ];
              File tmpFile = new File( jobUrl );
              if ( path.equals( tmpFile.getAbsolutePath() ) ) {
                return null;
              } else {
                throw new ConfigurationException(
                  "Unsubstituted job url was set to conf \n Path = " + path + "\n jobUrl = " + jobUrl );
              }
            }
          } ).when( conf ).setJar( (String) anyObject() );
          doNothing().when( conf ).setInputPaths( (Path[]) anyObject() );
          doNothing().when( conf ).setOutputPath( (Path) anyObject() );
          when( shim.createConfiguration() ).thenReturn( conf );
          doAnswer( new Answer<Object>() {
            @Override public Object answer( InvocationOnMock invocation ) throws Throwable {
              RunningJob rj = mock( RunningJob.class );
              when( rj.isComplete() ).thenReturn( true );
              when( rj.isSuccessful() ).thenReturn( true );
              when( rj.getTaskCompletionEvents( anyInt() ) ).thenReturn( new TaskCompletionEvent[ 0 ] );
              return rj;
            }
          } ).when( shim ).submitJob( (Configuration) anyObject() );
          return hc;
        } catch ( Exception ex ) {
          throw new ConfigurationException( "Error creating mock hadoop configuration", ex );
        }
      }
    };
    jobExecutor.setParentJob( job );
    jobExecutor.initializeVariablesFrom( jobExecutor.getParentVariableSpace() );
    jobExecutor.setHadoopJobName( "hadoop job name" );
    jobExecutor.setOutputKeyClass( getClass().getName() );
    jobExecutor.setOutputValueClass( getClass().getName() );
    jobExecutor.setHdfsHostname( "hostname" );
    jobExecutor.setHdfsPort( "8020" );
    jobExecutor.setJobTrackerHostname( "hostname" );
    jobExecutor.setJobTrackerPort( "50030" );
    jobExecutor.setInputPath( "/" );
    jobExecutor.setOutputPath( "/" );
    jobExecutor.setBlocking( true );

    file.deleteOnExit();
    jobExecutor.setJarUrl( "${java.io.tmpdir}${file.separator}" + file.getName() );
    jobExecutor.setSimple( false );
    Result result = new Result();
    try {
      jobExecutor.execute( result, 0 );
    } catch ( KettleException e ) {
      e.printStackTrace();
      fail( e.getMessage() );
    }
    assertTrue( result.getResult() );
  }
}
