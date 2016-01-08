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
import org.pentaho.bigdata.api.mapreduce.PentahoMapReduceOutputStepMetaInterface;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.api.fs.FileSystem;
import org.pentaho.hadoop.shim.api.fs.Path;
import org.pentaho.hadoop.shim.spi.HadoopShim;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 1/12/16.
 */
public class PentahoMapReduceJobBuilderImplTest {

  private NamedCluster namedCluster;
  private HadoopConfiguration hadoopConfiguration;
  private LogChannelInterface logChannelInterface;
  private VariableSpace variableSpace;
  private PentahoMapReduceJobBuilderImpl pentahoMapReduceJobBuilder;
  private HadoopShim hadoopShim;
  private PluginInterface pluginInterface;
  private Properties pmrProperties;
  private TransMeta transMeta;
  private PentahoMapReduceJobBuilderImpl.TransFactory transFactory;

  @Before
  public void setup() {
    namedCluster = mock( NamedCluster.class );
    hadoopConfiguration = mock( HadoopConfiguration.class );
    hadoopShim = mock( HadoopShim.class );
    when( hadoopConfiguration.getHadoopShim() ).thenReturn( hadoopShim );
    logChannelInterface = mock( LogChannelInterface.class );
    variableSpace = mock( VariableSpace.class );
    pluginInterface = mock( PluginInterface.class );
    pmrProperties = mock( Properties.class );
    transMeta = mock( TransMeta.class );
    transFactory = mock( PentahoMapReduceJobBuilderImpl.TransFactory.class );
    pentahoMapReduceJobBuilder =
      new PentahoMapReduceJobBuilderImpl( namedCluster, hadoopConfiguration, logChannelInterface, variableSpace,
        pluginInterface, pmrProperties, transFactory );
  }

  @Test
  public void testGetHadoopWritableCompatibleClassName() {
    ValueMetaInterface valueMetaInterface = mock( ValueMetaInterface.class );
    when( hadoopShim.getHadoopWritableCompatibleClass( valueMetaInterface ) ).thenReturn( null, (Class) String.class );
    assertNull( pentahoMapReduceJobBuilder.getHadoopWritableCompatibleClassName( valueMetaInterface ) );
    assertEquals( String.class.getCanonicalName(),
      pentahoMapReduceJobBuilder.getHadoopWritableCompatibleClassName( valueMetaInterface ) );
  }

  @Test( expected = KettleException.class )
  public void testVerifyTransMetaEmptyInputStepName() throws KettleException {
    try {
      pentahoMapReduceJobBuilder.verifyTransMeta( transMeta, null, null );
    } catch ( KettleException e ) {
      assertEquals( BaseMessages.getString( PentahoMapReduceJobBuilderImpl.PKG,
        PentahoMapReduceJobBuilderImpl.PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_INPUT_STEP_NOT_SPECIFIED ),
        e.getMessage().trim() );
      throw e;
    }
  }

  @Test( expected = KettleException.class )
  public void testVerifyTransMetaCanFindInputStep() throws KettleException {
    String inputStepName = "inputStepName";
    when( transMeta.findStep( inputStepName ) ).thenReturn( null );
    try {
      pentahoMapReduceJobBuilder.verifyTransMeta( transMeta, inputStepName, null );
    } catch ( KettleException e ) {
      assertEquals( BaseMessages.getString( PentahoMapReduceJobBuilderImpl.PKG,
        PentahoMapReduceJobBuilderImpl.PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_INPUT_STEP_NOT_FOUND, inputStepName ),
        e.getMessage().trim() );
      throw e;
    }
  }

  @Test( expected = KettleException.class )
  public void testVerifyTransMetaNoKeyOrdinal() throws KettleException {
    String inputStepName = "inputStepName";
    StepMeta inputStepMeta = mock( StepMeta.class );
    when( transMeta.findStep( inputStepName ) ).thenReturn( inputStepMeta );
    RowMetaInterface rowMetaInterface = mock( RowMetaInterface.class );
    when( rowMetaInterface.getFieldNames() ).thenReturn( new String[] {} );
    when( transMeta.getStepFields( inputStepMeta ) ).thenReturn( rowMetaInterface );
    try {
      pentahoMapReduceJobBuilder.verifyTransMeta( transMeta, inputStepName, null );
    } catch ( KettleException e ) {
      assertEquals( BaseMessages.getString( PentahoMapReduceJobBuilderImpl.PKG,
        PentahoMapReduceJobBuilderImpl.PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_NO_KEY_ORDINAL, inputStepName ),
        e.getMessage().trim() );
      throw e;
    }
  }

  @Test( expected = KettleException.class )
  public void testVerifyTransMetaNoValueOrdinal() throws KettleException {
    String inputStepName = "inputStepName";
    StepMeta inputStepMeta = mock( StepMeta.class );
    when( transMeta.findStep( inputStepName ) ).thenReturn( inputStepMeta );
    RowMetaInterface rowMetaInterface = mock( RowMetaInterface.class );
    when( rowMetaInterface.getFieldNames() ).thenReturn( new String[] { "key" } );
    when( transMeta.getStepFields( inputStepMeta ) ).thenReturn( rowMetaInterface );
    try {
      pentahoMapReduceJobBuilder.verifyTransMeta( transMeta, inputStepName, null );
    } catch ( KettleException e ) {
      assertEquals( BaseMessages.getString( PentahoMapReduceJobBuilderImpl.PKG,
        PentahoMapReduceJobBuilderImpl.PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_NO_VALUE_ORDINAL, inputStepName ),
        e.getMessage().trim() );
      throw e;
    }
  }

  @Test( expected = KettleException.class )
  public void testVerifyTransMetaInputHopDisabled() throws KettleException {
    String inputStepName = "inputStepName";
    StepMeta inputStepMeta = mock( StepMeta.class );
    when( transMeta.findStep( inputStepName ) ).thenReturn( inputStepMeta );
    RowMetaInterface rowMetaInterface = mock( RowMetaInterface.class );
    when( rowMetaInterface.getFieldNames() ).thenReturn( new String[] { "key", "value" } );
    when( transMeta.getStepFields( inputStepMeta ) ).thenReturn( rowMetaInterface );
    when( transFactory.create( transMeta ) ).thenReturn( mock( Trans.class ) );
    try {
      pentahoMapReduceJobBuilder.verifyTransMeta( transMeta, inputStepName, null );
    } catch ( KettleException e ) {
      assertEquals( BaseMessages.getString( PentahoMapReduceJobBuilderImpl.PKG,
        PentahoMapReduceJobBuilderImpl.PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_INPUT_HOP_DISABLED, inputStepName ),
        e.getMessage().trim() );
      throw e;
    }
  }

  @Test( expected = KettleException.class )
  public void testVerifyTransMetaOutputStepNotSpecified() throws KettleException {
    String inputStepName = "inputStepName";
    StepMeta inputStepMeta = mock( StepMeta.class );
    when( transMeta.findStep( inputStepName ) ).thenReturn( inputStepMeta );
    RowMetaInterface rowMetaInterface = mock( RowMetaInterface.class );
    when( rowMetaInterface.getFieldNames() ).thenReturn( new String[] { "key", "value" } );
    when( transMeta.getStepFields( inputStepMeta ) ).thenReturn( rowMetaInterface );
    Trans trans = mock( Trans.class );
    when( transFactory.create( transMeta ) ).thenReturn( trans );
    when( trans.getStepInterface( inputStepName, 0 ) ).thenReturn( mock( StepInterface.class ) );
    try {
      pentahoMapReduceJobBuilder.verifyTransMeta( transMeta, inputStepName, null );
    } catch ( KettleException e ) {
      assertEquals( BaseMessages.getString( PentahoMapReduceJobBuilderImpl.PKG,
        PentahoMapReduceJobBuilderImpl.PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_OUTPUT_STEP_NOT_SPECIFIED ),
        e.getMessage().trim() );
      throw e;
    }
  }

  @Test( expected = KettleException.class )
  public void testVerifyTransMetaOutputStepNotFound() throws KettleException {
    String inputStepName = "inputStepName";
    String outputStepName = "outputStepName";
    StepMeta inputStepMeta = mock( StepMeta.class );
    when( transMeta.findStep( inputStepName ) ).thenReturn( inputStepMeta );
    RowMetaInterface rowMetaInterface = mock( RowMetaInterface.class );
    when( rowMetaInterface.getFieldNames() ).thenReturn( new String[] { "key", "value" } );
    when( transMeta.getStepFields( inputStepMeta ) ).thenReturn( rowMetaInterface );
    Trans trans = mock( Trans.class );
    when( transFactory.create( transMeta ) ).thenReturn( trans );
    when( trans.getStepInterface( inputStepName, 0 ) ).thenReturn( mock( StepInterface.class ) );
    try {
      pentahoMapReduceJobBuilder.verifyTransMeta( transMeta, inputStepName, outputStepName );
    } catch ( KettleException e ) {
      assertEquals( BaseMessages.getString( PentahoMapReduceJobBuilderImpl.PKG,
        PentahoMapReduceJobBuilderImpl.PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_OUTPUT_STEP_NOT_FOUND, outputStepName ),
        e.getMessage().trim() );
      throw e;
    }
  }

  @Test( expected = KettleException.class )
  public void testVerifyTransMetaCheckException() throws KettleException {
    String inputStepName = "inputStepName";
    String outputStepName = "outputStepName";
    StepMeta inputStepMeta = mock( StepMeta.class );
    when( transMeta.findStep( inputStepName ) ).thenReturn( inputStepMeta );
    RowMetaInterface rowMetaInterface = mock( RowMetaInterface.class );
    when( rowMetaInterface.getFieldNames() ).thenReturn( new String[] { "key", "value" } );
    when( transMeta.getStepFields( inputStepMeta ) ).thenReturn( rowMetaInterface );
    Trans trans = mock( Trans.class );
    when( transFactory.create( transMeta ) ).thenReturn( trans );
    when( trans.getStepInterface( inputStepName, 0 ) ).thenReturn( mock( StepInterface.class ) );
    final StepMeta outputStepMeta = mock( StepMeta.class );
    PentahoMapReduceOutputStepMetaInterface pentahoMapReduceOutputStepMetaInterface =
      mock( PentahoMapReduceOutputStepMetaInterface.class );
    doAnswer( new Answer<Void>() {
      @Override public Void answer( InvocationOnMock invocation ) throws Throwable {
        List<CheckResultInterface> checkResultInterfaces = (List<CheckResultInterface>) invocation.getArguments()[ 0 ];
        checkResultInterfaces.add( new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, "test", outputStepMeta ) );
        return null;
      }
    } ).when( pentahoMapReduceOutputStepMetaInterface )
      .checkPmr( anyList(), eq( transMeta ), eq( outputStepMeta ), any( RowMetaInterface.class ) );
    when( outputStepMeta.getStepMetaInterface() ).thenReturn( pentahoMapReduceOutputStepMetaInterface );
    when( transMeta.findStep( outputStepName ) ).thenReturn( outputStepMeta );
    try {
      pentahoMapReduceJobBuilder.verifyTransMeta( transMeta, inputStepName, outputStepName );
    } catch ( KettleException e ) {
      assertTrue( e.getMessage().trim().startsWith( BaseMessages.getString( PentahoMapReduceJobBuilderImpl.PKG,
        PentahoMapReduceJobBuilderImpl.PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_VALIDATION_ERROR ).trim() ) );
      throw e;
    }
  }

  @Test
  public void testVerifyTransMetaCheckSuccess() throws KettleException {
    String inputStepName = "inputStepName";
    String outputStepName = "outputStepName";
    StepMeta inputStepMeta = mock( StepMeta.class );
    when( transMeta.findStep( inputStepName ) ).thenReturn( inputStepMeta );
    RowMetaInterface rowMetaInterface = mock( RowMetaInterface.class );
    when( rowMetaInterface.getFieldNames() ).thenReturn( new String[] { "key", "value" } );
    when( transMeta.getStepFields( inputStepMeta ) ).thenReturn( rowMetaInterface );
    Trans trans = mock( Trans.class );
    when( transFactory.create( transMeta ) ).thenReturn( trans );
    when( trans.getStepInterface( inputStepName, 0 ) ).thenReturn( mock( StepInterface.class ) );
    final StepMeta outputStepMeta = mock( StepMeta.class );
    PentahoMapReduceOutputStepMetaInterface pentahoMapReduceOutputStepMetaInterface =
      mock( PentahoMapReduceOutputStepMetaInterface.class );
    doAnswer( new Answer<Void>() {
      @Override public Void answer( InvocationOnMock invocation ) throws Throwable {
        List<CheckResultInterface> checkResultInterfaces = (List<CheckResultInterface>) invocation.getArguments()[ 0 ];
        checkResultInterfaces.add( new CheckResult( CheckResultInterface.TYPE_RESULT_OK, "test", outputStepMeta ) );
        return null;
      }
    } ).when( pentahoMapReduceOutputStepMetaInterface )
      .checkPmr( anyList(), eq( transMeta ), eq( outputStepMeta ), any( RowMetaInterface.class ) );
    when( outputStepMeta.getStepMetaInterface() ).thenReturn( pentahoMapReduceOutputStepMetaInterface );
    when( transMeta.findStep( outputStepName ) ).thenReturn( outputStepMeta );
    pentahoMapReduceJobBuilder.verifyTransMeta( transMeta, inputStepName, outputStepName );
  }

  @Test( expected = KettleException.class )
  public void testVerifyTransMetaOutKeyNotDefined() throws KettleException {
    String inputStepName = "inputStepName";
    String outputStepName = "outputStepName";
    StepMeta inputStepMeta = mock( StepMeta.class );
    when( transMeta.findStep( inputStepName ) ).thenReturn( inputStepMeta );
    RowMetaInterface rowMetaInterface = mock( RowMetaInterface.class );
    when( rowMetaInterface.getFieldNames() ).thenReturn( new String[] { "key", "value" } );
    when( transMeta.getStepFields( inputStepMeta ) ).thenReturn( rowMetaInterface );
    Trans trans = mock( Trans.class );
    when( transFactory.create( transMeta ) ).thenReturn( trans );
    when( trans.getStepInterface( inputStepName, 0 ) ).thenReturn( mock( StepInterface.class ) );
    final StepMeta outputStepMeta = mock( StepMeta.class );
    StepMetaInterface stepMetaInterface = mock( StepMetaInterface.class );
    when( outputStepMeta.getStepMetaInterface() ).thenReturn( stepMetaInterface );
    when( transMeta.findStep( outputStepName ) ).thenReturn( outputStepMeta );
    RowMetaInterface outputRowMetaInterface = mock( RowMetaInterface.class );
    when( transMeta.getStepFields( outputStepMeta ) ).thenReturn( outputRowMetaInterface );
    when( outputRowMetaInterface.getFieldNames() ).thenReturn( new String[] {} );
    try {
      pentahoMapReduceJobBuilder.verifyTransMeta( transMeta, inputStepName, outputStepName );
    } catch ( KettleException e ) {
      assertEquals( BaseMessages.getString( PentahoMapReduceJobBuilderImpl.PKG,
        PentahoMapReduceJobBuilderImpl.PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_NO_OUTPUT_KEY_ORDINAL, outputStepName ),
        e.getMessage().trim() );
      throw e;
    }
  }

  @Test( expected = KettleException.class )
  public void testVerifyTransMetaOutValueNotDefined() throws KettleException {
    String inputStepName = "inputStepName";
    String outputStepName = "outputStepName";
    StepMeta inputStepMeta = mock( StepMeta.class );
    when( transMeta.findStep( inputStepName ) ).thenReturn( inputStepMeta );
    RowMetaInterface rowMetaInterface = mock( RowMetaInterface.class );
    when( rowMetaInterface.getFieldNames() ).thenReturn( new String[] { "key", "value" } );
    when( transMeta.getStepFields( inputStepMeta ) ).thenReturn( rowMetaInterface );
    Trans trans = mock( Trans.class );
    when( transFactory.create( transMeta ) ).thenReturn( trans );
    when( trans.getStepInterface( inputStepName, 0 ) ).thenReturn( mock( StepInterface.class ) );
    final StepMeta outputStepMeta = mock( StepMeta.class );
    StepMetaInterface stepMetaInterface = mock( StepMetaInterface.class );
    when( outputStepMeta.getStepMetaInterface() ).thenReturn( stepMetaInterface );
    when( transMeta.findStep( outputStepName ) ).thenReturn( outputStepMeta );
    RowMetaInterface outputRowMetaInterface = mock( RowMetaInterface.class );
    when( transMeta.getStepFields( outputStepMeta ) ).thenReturn( outputRowMetaInterface );
    when( outputRowMetaInterface.getFieldNames() ).thenReturn( new String[] { "outKey" } );
    try {
      pentahoMapReduceJobBuilder.verifyTransMeta( transMeta, inputStepName, outputStepName );
    } catch ( KettleException e ) {
      assertEquals( BaseMessages.getString( PentahoMapReduceJobBuilderImpl.PKG,
        PentahoMapReduceJobBuilderImpl.PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_NO_OUTPUT_VALUE_ORDINAL, outputStepName ),
        e.getMessage().trim() );
      throw e;
    }
  }

  @Test
  public void testVerifyTransMetaOutSuccess() throws KettleException {
    String inputStepName = "inputStepName";
    String outputStepName = "outputStepName";
    StepMeta inputStepMeta = mock( StepMeta.class );
    when( transMeta.findStep( inputStepName ) ).thenReturn( inputStepMeta );
    RowMetaInterface rowMetaInterface = mock( RowMetaInterface.class );
    when( rowMetaInterface.getFieldNames() ).thenReturn( new String[] { "key", "value" } );
    when( transMeta.getStepFields( inputStepMeta ) ).thenReturn( rowMetaInterface );
    Trans trans = mock( Trans.class );
    when( transFactory.create( transMeta ) ).thenReturn( trans );
    when( trans.getStepInterface( inputStepName, 0 ) ).thenReturn( mock( StepInterface.class ) );
    final StepMeta outputStepMeta = mock( StepMeta.class );
    StepMetaInterface stepMetaInterface = mock( StepMetaInterface.class );
    when( outputStepMeta.getStepMetaInterface() ).thenReturn( stepMetaInterface );
    when( transMeta.findStep( outputStepName ) ).thenReturn( outputStepMeta );
    RowMetaInterface outputRowMetaInterface = mock( RowMetaInterface.class );
    when( transMeta.getStepFields( outputStepMeta ) ).thenReturn( outputRowMetaInterface );
    when( outputRowMetaInterface.getFieldNames() ).thenReturn( new String[] { "outKey", "outValue" } );
    pentahoMapReduceJobBuilder.verifyTransMeta( transMeta, inputStepName, outputStepName );
  }

  @Test
  public void testTransFactoryImpl() {
    TransMeta transMeta = mock( TransMeta.class );
    when( transMeta.listVariables() ).thenReturn( new String[ 0 ] );
    when( transMeta.listParameters() ).thenReturn( new String[ 0 ] );
    assertNotNull( new PentahoMapReduceJobBuilderImpl.TransFactoryImpl().create( transMeta ) );
  }

  @Test
  public void testCleanOutputPathFalse() throws IOException {
    Configuration configuration = mock( Configuration.class );
    pentahoMapReduceJobBuilder.cleanOutputPath( configuration );
    verifyNoMoreInteractions( configuration );
  }

  @Test
  public void testCleanOutputPathDoesntExist() throws IOException, URISyntaxException {
    pentahoMapReduceJobBuilder.setCleanOutputPath( true );
    String outputPath = "/test/path";
    pentahoMapReduceJobBuilder.setOutputPath( outputPath );
    Configuration configuration = mock( Configuration.class );
    String defaultFilesystemURL = "test:prefix";
    when( configuration.getDefaultFileSystemURL() ).thenReturn( defaultFilesystemURL );
    FileSystem fileSystem = mock( FileSystem.class );
    when( hadoopShim.getFileSystem( configuration ) ).thenReturn( fileSystem );
    Path path = mock( Path.class );
    when( path.toUri() ).thenReturn( new URI( "hdfs://test/uri" ) );
    when( fileSystem.asPath( defaultFilesystemURL, outputPath ) ).thenReturn( path );
    when( fileSystem.exists( path ) ).thenReturn( false );
    pentahoMapReduceJobBuilder.cleanOutputPath( configuration );
    verify( fileSystem, never() ).delete( any( Path.class ), anyBoolean() );
  }

  @Test
  public void testCleanOutputPathFailure() throws IOException, URISyntaxException {
    pentahoMapReduceJobBuilder.setCleanOutputPath( true );
    String outputPath = "/test/path";
    pentahoMapReduceJobBuilder.setOutputPath( outputPath );
    Configuration configuration = mock( Configuration.class );
    String defaultFilesystemURL = "test:prefix";
    when( configuration.getDefaultFileSystemURL() ).thenReturn( defaultFilesystemURL );
    FileSystem fileSystem = mock( FileSystem.class );
    when( hadoopShim.getFileSystem( configuration ) ).thenReturn( fileSystem );
    Path path = mock( Path.class );
    URI uri = new URI( "hdfs://test/uri" );
    when( path.toUri() ).thenReturn( uri );
    when( fileSystem.asPath( defaultFilesystemURL, outputPath ) ).thenReturn( path );
    when( fileSystem.exists( path ) ).thenReturn( true );
    when( fileSystem.delete( path, true ) ).thenReturn( false );
    when( logChannelInterface.isBasic() ).thenReturn( true );
    pentahoMapReduceJobBuilder.cleanOutputPath( configuration );
    verify( fileSystem ).delete( path, true );
    verify( logChannelInterface )
      .logBasic( BaseMessages
        .getString( PentahoMapReduceJobBuilderImpl.PKG,
          PentahoMapReduceJobBuilderImpl.JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_CLEANING_OUTPUT_PATH,
          uri.toString() ) );
    verify( logChannelInterface )
      .logBasic( BaseMessages
        .getString( PentahoMapReduceJobBuilderImpl.PKG,
          PentahoMapReduceJobBuilderImpl.JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_FAILED_TO_CLEAN_OUTPUT_PATH,
          uri.toString() ) );
  }

  @Test
  public void testCleanOutputPathFailureNoLog() throws IOException, URISyntaxException {
    pentahoMapReduceJobBuilder.setCleanOutputPath( true );
    String outputPath = "/test/path";
    pentahoMapReduceJobBuilder.setOutputPath( outputPath );
    Configuration configuration = mock( Configuration.class );
    String defaultFilesystemURL = "test:prefix";
    when( configuration.getDefaultFileSystemURL() ).thenReturn( defaultFilesystemURL );
    FileSystem fileSystem = mock( FileSystem.class );
    when( hadoopShim.getFileSystem( configuration ) ).thenReturn( fileSystem );
    Path path = mock( Path.class );
    URI uri = new URI( "hdfs://test/uri" );
    when( path.toUri() ).thenReturn( uri );
    when( fileSystem.asPath( defaultFilesystemURL, outputPath ) ).thenReturn( path );
    when( fileSystem.exists( path ) ).thenReturn( true );
    when( fileSystem.delete( path, true ) ).thenReturn( false );
    when( logChannelInterface.isBasic() ).thenReturn( false );
    pentahoMapReduceJobBuilder.cleanOutputPath( configuration );
    verify( fileSystem ).delete( path, true );
    verify( logChannelInterface, never() ).logBasic( anyString() );
  }

  @Test( expected = IOException.class )
  public void testCleanOutputPathException() throws IOException, URISyntaxException {
    pentahoMapReduceJobBuilder.setCleanOutputPath( true );
    String outputPath = "/test/path";
    pentahoMapReduceJobBuilder.setOutputPath( outputPath );
    Configuration configuration = mock( Configuration.class );
    String defaultFilesystemURL = "test:prefix";
    when( configuration.getDefaultFileSystemURL() ).thenReturn( defaultFilesystemURL );
    FileSystem fileSystem = mock( FileSystem.class );
    when( hadoopShim.getFileSystem( configuration ) ).thenReturn( fileSystem );
    Path path = mock( Path.class );
    URI uri = new URI( "hdfs://test/uri" );
    when( path.toUri() ).thenReturn( uri );
    when( fileSystem.asPath( defaultFilesystemURL, outputPath ) ).thenReturn( path );
    when( fileSystem.exists( path ) ).thenReturn( true );
    when( fileSystem.delete( path, true ) ).thenThrow( new IOException() );
    when( logChannelInterface.isBasic() ).thenReturn( false );
    pentahoMapReduceJobBuilder.cleanOutputPath( configuration );
  }

  @Test
  public void testCleanOutputPathSuccess() throws IOException, URISyntaxException {
    pentahoMapReduceJobBuilder.setCleanOutputPath( true );
    String outputPath = "/test/path";
    pentahoMapReduceJobBuilder.setOutputPath( outputPath );
    Configuration configuration = mock( Configuration.class );
    String defaultFilesystemURL = "test:prefix";
    when( configuration.getDefaultFileSystemURL() ).thenReturn( defaultFilesystemURL );
    FileSystem fileSystem = mock( FileSystem.class );
    when( hadoopShim.getFileSystem( configuration ) ).thenReturn( fileSystem );
    Path path = mock( Path.class );
    URI uri = new URI( "hdfs://test/uri" );
    when( path.toUri() ).thenReturn( uri );
    when( fileSystem.asPath( defaultFilesystemURL, outputPath ) ).thenReturn( path );
    when( fileSystem.exists( path ) ).thenReturn( true );
    when( fileSystem.delete( path, true ) ).thenReturn( true );
    when( logChannelInterface.isBasic() ).thenReturn( true );
    pentahoMapReduceJobBuilder.cleanOutputPath( configuration );
    verify( fileSystem ).delete( path, true );
    verify( logChannelInterface )
      .logBasic( BaseMessages
        .getString( PentahoMapReduceJobBuilderImpl.PKG,
          PentahoMapReduceJobBuilderImpl.JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_CLEANING_OUTPUT_PATH,
          uri.toString() ) );
    verify( logChannelInterface, times( 1 ) ).logBasic( anyString() );
  }

  @Test
  public void testGetPropertyFromConf() {
    Configuration configuration = mock( Configuration.class );
    Properties properties = mock( Properties.class );
    String property = "property";
    String value = "value";
    when( configuration.get( property ) ).thenReturn( value );
    assertEquals( value, pentahoMapReduceJobBuilder.getProperty( configuration, properties, property, null ) );
  }

  @Test
  public void testGetPropertyFromProperties() {
    Configuration configuration = mock( Configuration.class );
    Properties properties = mock( Properties.class );
    String property = "property";
    String value = "value";
    when( properties.getProperty( property, value ) ).thenReturn( value );
    assertEquals( value, pentahoMapReduceJobBuilder.getProperty( configuration, properties, property, value ) );
  }

  @Test
  public void testConfigureMinimal() throws Exception {
    when( hadoopShim.getPentahoMapReduceMapRunnerClass() ).thenReturn( (Class) String.class );
    pentahoMapReduceJobBuilder.setLogLevel( LogLevel.BASIC );
    pentahoMapReduceJobBuilder.setInputPaths( new String[ 0 ] );
    pentahoMapReduceJobBuilder.setOutputPath( "test" );
    Configuration configuration = mock( Configuration.class );
    when( hadoopShim.getFileSystem( configuration ) ).thenReturn( mock( FileSystem.class ) );
    String xml = "testMr.xml";
    String testMrInput = "testMrInput";
    String testMrOutput = "testMrOutput";
    pentahoMapReduceJobBuilder.setMapperInfo( xml, testMrInput, testMrOutput );
    pentahoMapReduceJobBuilder.configure( configuration );

    verify( configuration ).setMapRunnerClass( String.class );
    verify( configuration ).set( PentahoMapReduceJobBuilderImpl.TRANSFORMATION_MAP_XML, xml );
    verify( configuration ).set( PentahoMapReduceJobBuilderImpl.TRANSFORMATION_MAP_INPUT_STEPNAME, testMrInput );
    verify( configuration ).set( PentahoMapReduceJobBuilderImpl.TRANSFORMATION_MAP_OUTPUT_STEPNAME, testMrOutput );
    verify( configuration ).setJarByClass( String.class );
    verify( configuration ).set( PentahoMapReduceJobBuilderImpl.LOG_LEVEL, LogLevel.BASIC.toString() );

    verify( configuration, never() ).setCombinerClass( any( Class.class ) );
    verify( configuration, never() ).setReducerClass( any( Class.class ) );
  }

  @Test
  public void testConfigureFull() throws Exception {
    when( hadoopShim.getPentahoMapReduceMapRunnerClass() ).thenReturn( (Class) String.class );
    when( hadoopShim.getPentahoMapReduceCombinerClass() ).thenReturn( (Class) Void.class );
    when( hadoopShim.getPentahoMapReduceReducerClass() ).thenReturn( (Class) Integer.class );
    pentahoMapReduceJobBuilder.setLogLevel( LogLevel.BASIC );
    pentahoMapReduceJobBuilder.setInputPaths( new String[ 0 ] );
    pentahoMapReduceJobBuilder.setOutputPath( "test" );
    Configuration configuration = mock( Configuration.class );
    when( hadoopShim.getFileSystem( configuration ) ).thenReturn( mock( FileSystem.class ) );
    String mrXml = "testMr.xml";
    String testMrInput = "testMrInput";
    String testMrOutput = "testMrOutput";
    pentahoMapReduceJobBuilder.setMapperInfo( mrXml, testMrInput, testMrOutput );
    String combinerTransformationXml = "testC.xml";
    String combinerInputStep = "testCInput";
    String combinerOutputStep = "testCOutput";
    pentahoMapReduceJobBuilder.setCombinerInfo( combinerTransformationXml, combinerInputStep, combinerOutputStep );
    String reducerTransformationXml = "testR.xml";
    String testRInput = "testRInput";
    String testROutput = "testROutput";
    pentahoMapReduceJobBuilder.setReducerInfo( reducerTransformationXml, testRInput, testROutput );
    pentahoMapReduceJobBuilder.configure( configuration );

    verify( configuration ).setMapRunnerClass( String.class );
    verify( configuration ).set( PentahoMapReduceJobBuilderImpl.TRANSFORMATION_MAP_XML, mrXml );
    verify( configuration ).set( PentahoMapReduceJobBuilderImpl.TRANSFORMATION_MAP_INPUT_STEPNAME, testMrInput );
    verify( configuration ).set( PentahoMapReduceJobBuilderImpl.TRANSFORMATION_MAP_OUTPUT_STEPNAME, testMrOutput );

    verify( configuration )
      .set( PentahoMapReduceJobBuilderImpl.TRANSFORMATION_COMBINER_XML, combinerTransformationXml );
    verify( configuration )
      .set( PentahoMapReduceJobBuilderImpl.TRANSFORMATION_COMBINER_INPUT_STEPNAME, combinerInputStep );
    verify( configuration )
      .set( PentahoMapReduceJobBuilderImpl.TRANSFORMATION_COMBINER_OUTPUT_STEPNAME, combinerOutputStep );
    verify( configuration ).setCombinerClass( Void.class );

    verify( configuration ).set( PentahoMapReduceJobBuilderImpl.TRANSFORMATION_REDUCE_XML, reducerTransformationXml );
    verify( configuration ).set( PentahoMapReduceJobBuilderImpl.TRANSFORMATION_REDUCE_INPUT_STEPNAME, testRInput );
    verify( configuration ).set( PentahoMapReduceJobBuilderImpl.TRANSFORMATION_REDUCE_OUTPUT_STEPNAME, testROutput );
    verify( configuration ).setReducerClass( Integer.class );

    verify( configuration ).setJarByClass( String.class );
    verify( configuration ).set( PentahoMapReduceJobBuilderImpl.LOG_LEVEL, LogLevel.BASIC.toString() );
  }

  @Test
  public void testSubmitNoDistributedCache() throws IOException {
    Configuration conf = mock( Configuration.class );
    pentahoMapReduceJobBuilder.submit( conf );
    verify( hadoopShim ).submitJob( conf );
  }

  @Test( expected = IOException.class )
  public void testSubmitNoInstallPath() throws IOException {
    Configuration conf = mock( Configuration.class );
    when( conf.get( PentahoMapReduceJobBuilderImpl.PENTAHO_MAPREDUCE_PROPERTY_USE_DISTRIBUTED_CACHE ) )
      .thenReturn( "true" );
    try {
      pentahoMapReduceJobBuilder.submit( conf );
    } catch ( IOException e ) {
      assertEquals( BaseMessages.getString( PentahoMapReduceJobBuilderImpl.PKG,
        PentahoMapReduceJobBuilderImpl.JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_INSTALLATION_OF_KETTLE_FAILED ),
        e.getMessage() );
      assertEquals( BaseMessages.getString( PentahoMapReduceJobBuilderImpl.PKG,
        PentahoMapReduceJobBuilderImpl.JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_KETTLE_HDFS_INSTALL_DIR_MISSING ),
        e.getCause().getMessage() );
      throw e;
    }
  }

  @Test
  public void testSubmitEmptyInstallId() {
    Configuration conf = mock( Configuration.class );
    when( conf.get( PentahoMapReduceJobBuilderImpl.PENTAHO_MAPREDUCE_PROPERTY_USE_DISTRIBUTED_CACHE ) )
      .thenReturn( "true" );
    try {
      pentahoMapReduceJobBuilder.submit( conf );
    } catch ( IOException e ) {
      // Ignore
    }
  }
}
