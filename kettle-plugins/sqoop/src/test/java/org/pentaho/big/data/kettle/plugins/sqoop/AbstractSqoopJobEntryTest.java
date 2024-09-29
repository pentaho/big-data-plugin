/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package org.pentaho.big.data.kettle.plugins.sqoop;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.big.data.impl.cluster.NamedClusterImpl;
import org.pentaho.big.data.kettle.plugins.job.BlockableJobConfig;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.encryption.TwoWayPasswordEncoderPluginType;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pentaho.hadoop.shim.api.HadoopClientServices;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;

import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
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

}