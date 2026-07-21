/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/


package org.pentaho.big.data.kettle.plugins.sqoop;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.big.data.impl.cluster.NamedClusterImpl;
import org.pentaho.big.data.kettle.plugins.job.BlockableJobConfig;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.encryption.TwoWayPasswordEncoderPluginType;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.plugins.DatabasePluginType;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pentaho.hadoop.shim.api.HadoopClientServices;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.platform.api.util.LogUtil;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;

import java.io.PrintStream;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class AbstractSqoopJobEntryTest {
  @Mock NamedClusterService mockNamedClusterService;
  @Mock NamedClusterServiceLocator mockNamedClusterServiceLocator;
  @Mock RuntimeTestActionService mockRuntimeTestActionService;
  @Mock RuntimeTester mockRuntimeTester;
  @Mock Job mockJob;
  @Spy JobMeta mockJobMeta;
  @Mock DatabaseMeta mockDatabaseMeta;
  @Mock HadoopClientServices mockHadoopClientServices;
  NamedClusterImpl namedClusterTemplate = new NamedClusterImpl();
  NamedClusterImpl namedClusterActual = new NamedClusterImpl();
  SqoopConfig sqoopConfig;
  AbstractSqoopJobEntry sqoopJobEntry;

  private static final String CLUSTER_NAME_VARIABLE = "ClusterNameVariable";
  private static final String CLUSTER_NAME = "MyClusterName";

  @Before
  public void setUp() throws Exception {
    PluginRegistry.addPluginType( TwoWayPasswordEncoderPluginType.getInstance() );
    PluginRegistry.addPluginType( DatabasePluginType.getInstance() );
    PluginRegistry.init( false );
    Encr.init( "Kettle" );
    KettleLogStore.init();
    namedClusterTemplate.setName( "${" + CLUSTER_NAME_VARIABLE + "}" );
    namedClusterActual.setName( CLUSTER_NAME );

    sqoopJobEntry =
      new AbstractSqoopJobEntry( mockNamedClusterService, mockNamedClusterServiceLocator, mockRuntimeTestActionService,
        mockRuntimeTester ) {

        @Override protected BlockableJobConfig createJobConfig() {
          return null;
        }

        @Override public List<String> getValidationWarnings( BlockableJobConfig config ) {
          return null;
        }

        @Override protected String getToolName() {
          return "toolName";
        }
      };

    sqoopConfig = new SqoopConfig() {
      @Override protected NamedCluster createClusterTemplate() {
        return namedClusterTemplate;
      }
    };

    sqoopConfig.setClusterName( "${" + CLUSTER_NAME_VARIABLE + "}" );
    sqoopConfig.setJobEntryName( "jobname" );
    
    sqoopJobEntry.setParentJob( mockJob );
    VariableSpace variables = new Variables();
    variables.setVariable( CLUSTER_NAME_VARIABLE, CLUSTER_NAME );

    mockJobMeta.initializeVariablesFrom( variables );
    sqoopJobEntry.setParentJobMeta( mockJobMeta );
    when( mockJob.getJobMeta() ).thenReturn( mockJobMeta );
    when( mockJobMeta.findDatabase( any() ) ).thenReturn( mockDatabaseMeta );
    when( mockNamedClusterService.contains( eq( CLUSTER_NAME ), any() ) ).thenReturn( true );
    when( mockNamedClusterService.read( eq( CLUSTER_NAME ), any() ) ).thenReturn( namedClusterActual );

    when( mockNamedClusterServiceLocator.getService( any(), any() ) ).thenReturn( mockHadoopClientServices );
    when( mockHadoopClientServices.runSqoop( any(), any() ) ).thenReturn( 0 );
  }

  @Test
  public void executeSqoopWithVariableClusterName() throws Exception {

    sqoopJobEntry.setJobConfig( sqoopConfig );
    Result jobResult = new Result();

    sqoopJobEntry.executeSqoop( jobResult );
    verify( mockNamedClusterService, times( 1 ) ).read( any(), any() );
  }

  @Test
  public void keepsSystemErrorRedirectedUntilAllConcurrentExecutionsFinish() {
    AbstractSqoopJobEntry secondJobEntry = createJobEntry();
    PrintStream originalSystemError = System.err;

    try {
      sqoopJobEntry.attachLoggingAppenders();
      secondJobEntry.attachLoggingAppenders();

      sqoopJobEntry.removeLoggingAppenders();
      assertNotSame( originalSystemError, System.err );

      secondJobEntry.removeLoggingAppenders();
      assertSame( originalSystemError, System.err );
    } finally {
      sqoopJobEntry.removeLoggingAppenders();
      secondJobEntry.removeLoggingAppenders();
    }
  }

  @Test
  public void attachesLoggingAppendersOnlyOnce() {
    Logger sqoopLogger = LogManager.getLogger( "org.apache.sqoop" );
    int initialAppenderCount = LogUtil.getAppenders( sqoopLogger ).size();
    PrintStream originalSystemError = System.err;

    try {
      sqoopJobEntry.attachLoggingAppenders();
      sqoopJobEntry.attachLoggingAppenders();

      sqoopJobEntry.removeLoggingAppenders();

      assertSame( originalSystemError, System.err );
      assertEquals( initialAppenderCount, LogUtil.getAppenders( sqoopLogger ).size() );
    } finally {
      sqoopJobEntry.removeLoggingAppenders();
    }
  }

  private AbstractSqoopJobEntry createJobEntry() {
    return new AbstractSqoopJobEntry( mockNamedClusterService, mockNamedClusterServiceLocator,
      mockRuntimeTestActionService, mockRuntimeTester ) {
      @Override protected BlockableJobConfig createJobConfig() {
        return null;
      }

      @Override public List<String> getValidationWarnings( BlockableJobConfig config ) {
        return null;
      }

      @Override protected String getToolName() {
        return "toolName";
      }
    };
  }

}