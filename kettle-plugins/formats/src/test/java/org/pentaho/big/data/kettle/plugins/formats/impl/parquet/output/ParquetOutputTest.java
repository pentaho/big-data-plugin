/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package org.pentaho.big.data.kettle.plugins.formats.impl.parquet.output;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.big.data.kettle.plugins.formats.impl.NamedClusterResolver;
import org.pentaho.big.data.kettle.plugins.formats.parquet.output.ParquetOutputField;
import org.pentaho.di.core.bowl.DefaultBowl;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.service.PluginServiceLoader;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.RowHandler;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.named.cluster.NamedClusterEmbedManager;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.hadoop.shim.api.format.FormatService;
import org.pentaho.hadoop.shim.api.format.IPentahoParquetOutputFormat;
import org.pentaho.hadoop.shim.api.format.ParquetSpec;
import org.pentaho.metastore.locator.api.MetastoreLocator;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class ParquetOutputTest {

  private static final String OUTPUT_STEP_NAME = "Output Step Name";
  private static final String OUTPUT_TRANS_NAME = "Output Trans Name";
  private static final String OUTPUT_FILE_NAME = "outputFileName";

  @Rule
  public ExpectedException expectedException;

  @Mock
  private StepMeta mockStepMeta;
  @Mock
  private StepDataInterface mockStepDataInterface;
  @Mock
  private TransMeta mockTransMeta;
  @Mock
  private Trans mockTrans;
  @Mock
  private NamedClusterServiceLocator mockNamedClusterServiceLocator;
  @Mock
  private NamedClusterService mockNamedClusterService;
  @Mock
  private MetastoreLocator mockMetaStoreLocator;
  @Mock
  private FormatService mockFormatService;
  @Mock
  private ParquetOutputData parquetOutputData;
  @Mock
  private RowHandler mockRowHandler;
  @Mock
  private IPentahoParquetOutputFormat mockPentahoParquetOutputFormat;
  @Mock
  private LogChannelInterface mockLogChannelInterface;
  @Mock
  private IPentahoParquetOutputFormat.IPentahoRecordWriter mockPentahoParquetRecordWriter;

  private ParquetOutput parquetOutput;
  private List<ParquetOutputField> parquetOutputFields;
  private ParquetOutputMeta parquetOutputMeta;
  private RowMeta dataInputRowMeta;
  private RowMetaAndData[] dataInputRows;
  private int currentParquetRow;

  @Before
  public void setUp() throws Exception {
    KettleLogStore.init();
    expectedException = ExpectedException.none();
    currentParquetRow = 0;
    setDataInputRows();
    setParquetOutputRows();
    Collection<MetastoreLocator> metastoreLocatorCollection = new ArrayList<>();
    metastoreLocatorCollection.add( mockMetaStoreLocator );
    NamedClusterResolver namedClusterResolver;
    try ( MockedStatic<PluginServiceLoader> pluginServiceLoaderMockedStatic = Mockito.mockStatic( PluginServiceLoader.class ) ) {
      pluginServiceLoaderMockedStatic.when( () -> PluginServiceLoader.loadServices( MetastoreLocator.class ) )
        .thenReturn( metastoreLocatorCollection );
      namedClusterResolver = new NamedClusterResolver( mockNamedClusterServiceLocator, mockNamedClusterService );

      parquetOutputMeta = new ParquetOutputMeta( namedClusterResolver );
      parquetOutputMeta.setFilename( OUTPUT_FILE_NAME );
      parquetOutputMeta.setOverrideOutput( true );
      parquetOutputMeta.setOutputFields( parquetOutputFields );

      parquetOutputMeta.setParentStepMeta( mockStepMeta );
      when( mockStepMeta.getName() ).thenReturn( OUTPUT_STEP_NAME );
      when( mockTransMeta.findStep( OUTPUT_STEP_NAME ) ).thenReturn( mockStepMeta );
      when( mockTransMeta.findStep( OUTPUT_STEP_NAME ) ).thenReturn( mockStepMeta );
      when( mockTransMeta.getBowl() ).thenReturn( DefaultBowl.getInstance() );

      try {
        when( mockRowHandler.getRow() ).thenAnswer( answer -> returnNextParquetRow() );
      } catch ( KettleException ke ) {
        ke.printStackTrace();
      }

      when( mockFormatService.createOutputFormat( IPentahoParquetOutputFormat.class,
        parquetOutputMeta.getNamedClusterResolver().resolveNamedCluster( parquetOutputMeta.getFilename() ) ) )
        .thenReturn( mockPentahoParquetOutputFormat );
      when( mockNamedClusterServiceLocator.getService( nullable( NamedCluster.class ), any( Class.class ) ) )
        .thenReturn( mockFormatService );
      when( mockPentahoParquetOutputFormat.createRecordWriter() ).thenReturn( mockPentahoParquetRecordWriter );

      parquetOutput = spy( new ParquetOutput( mockStepMeta, mockStepDataInterface, 0, mockTransMeta, mockTrans ) );
      parquetOutput.init( parquetOutputMeta, parquetOutputData );
      parquetOutput.setInputRowMeta( dataInputRowMeta );
      parquetOutput.setRowHandler( mockRowHandler );
      parquetOutput.setLogLevel( LogLevel.ERROR );
      parquetOutput.setTransMeta( mockTransMeta );
    }
  }

  @Test
  public void testProcessRow() throws Exception {
    boolean result;
    int rowsProcessed = 0;
    ArgumentCaptor<RowMeta> rowMetaCaptor = ArgumentCaptor.forClass( RowMeta.class );
    ArgumentCaptor<Object[]> dataCaptor = ArgumentCaptor.forClass( Object[].class );

    do {
      result = parquetOutput.processRow( parquetOutputMeta, parquetOutputData );
      if ( result ) {
        rowsProcessed++;
      }
    } while ( result );

    // 3 rows to be outputted to an parquet file
    assertEquals( 3, rowsProcessed );
    verify( mockRowHandler, times( 3 ) ).putRow( rowMetaCaptor.capture(), dataCaptor.capture() );
    verify( parquetOutput, times( 3 ) ).incrementLinesOutput();
    List<RowMeta> rowMetaCaptured = rowMetaCaptor.getAllValues();
    List<Object[]> dataCaptured = dataCaptor.getAllValues();
    for ( int rowNum = 0; rowNum < 3; rowNum++ ) {
      assertEquals( 0, rowMetaCaptured.get( rowNum ).indexOfValue( "StringName" ) );
      assertEquals( "string" + ( rowNum % 3 + 1 ), dataCaptured.get( rowNum )[ 0 ] );
    }
  }

  @Test
  public void initShouldPassEmbeddedMetastoreKey() {
    ParquetOutputMeta stepMetaInterface = mock( ParquetOutputMeta.class );
    ParquetOutputData stepDataInterface = mock( ParquetOutputData.class );
    NamedClusterEmbedManager namedClusterEmbedManager = mock( NamedClusterEmbedManager.class );
    when( mockTransMeta.getNamedClusterEmbedManager() ).thenReturn( namedClusterEmbedManager );
    when( mockTransMeta.getEmbeddedMetastoreProviderKey() ).thenReturn( "metastoreProviderKey" );
    parquetOutput.init( stepMetaInterface, stepDataInterface );

    verify( namedClusterEmbedManager ).passEmbeddedMetastoreKey( mockTransMeta, "metastoreProviderKey" );
  }

  @Test
  public void testProcessRowIllegalState() throws Exception {
    doThrow( new IllegalStateException( "IllegalStateExceptionMessage" ) ).when( mockPentahoParquetOutputFormat )
      .setOutputFile( anyString(), anyBoolean() );
    when( parquetOutput.getLogChannel() ).thenReturn( mockLogChannelInterface );
    assertFalse( parquetOutput.processRow( parquetOutputMeta, parquetOutputData ) );

    verify( mockLogChannelInterface,
      times( 1 ) )
      .logError( "IllegalStateExceptionMessage" );
  }

  @Test
  public void testProcessRowKettleFailure() {
    String expectedMessage = "KettleExceptionMessage";
    try {
      doNothing().when( parquetOutput ).closeWriter();
      doThrow( new KettleException( expectedMessage ) ).when( parquetOutput ).init( any() );
      parquetOutput.processRow( parquetOutputMeta, parquetOutputData );
      fail( "No Kettle Exception thrown" );
    } catch ( KettleException kex ) {
      assertTrue( kex.getMessage().contains( expectedMessage ) );
    } catch ( Exception ex ) {
      fail( "No other type of exception should be thrown" );
    }
  }

  @Test
  public void testProcessRowGeneralFailure() {
    String expectedMessage = "GeneralExceptionMessage";
    try {
      doNothing().when( parquetOutput ).closeWriter();
      doThrow( new Exception( expectedMessage ) ).when( parquetOutput ).init( any() );
      parquetOutput.processRow( parquetOutputMeta, parquetOutputData );
      fail( "No Kettle Exception thrown" );
    } catch ( KettleException kex ) {
      assertTrue( kex.getMessage().contains( expectedMessage ) );
    } catch ( Exception ex ) {
      fail( "No other type of exception should be thrown" );
    }
  }

  private Object[] returnNextParquetRow() {
    Object[] result = null;
    if ( currentParquetRow < dataInputRows.length ) {
      result = dataInputRows[ currentParquetRow ].getData().clone();
      currentParquetRow++;
    }
    return result;
  }

  private void setParquetOutputRows() {
    ParquetOutputField parquetOutputField = mock( ParquetOutputField.class );
    parquetOutputFields =  new ArrayList<>();
    parquetOutputFields.add( parquetOutputField );
  }

  private void setDataInputRowMeta() {
    dataInputRowMeta = new RowMeta();
    ValueMetaInterface valueMetaString = new ValueMetaString( "StringName" );
    dataInputRowMeta.addValueMeta( valueMetaString );
  }

  private void setDataInputRows() {
    setDataInputRowMeta();
    dataInputRows = new RowMetaAndData[] {
      new RowMetaAndData( dataInputRowMeta, "string1" ),
      new RowMetaAndData( dataInputRowMeta, "string2" ),
      new RowMetaAndData( dataInputRowMeta, "string3" )
    };
  }

  @Test
  public void testAliasFile() throws Exception {
    String aliasPath = Files.createTempDirectory( "testAliasFile" ) + File.separator + "dummyFile";
    new File( aliasPath ).createNewFile();  //create the alias file so it and it's parent can be successfully deleted
    when( mockPentahoParquetOutputFormat.generateAlias( anyString() ) ).thenReturn( aliasPath );
    boolean result;
    int rowsProcessed = 0;
    ArgumentCaptor<RowMeta> rowMetaCaptor = ArgumentCaptor.forClass( RowMeta.class );
    ArgumentCaptor<Object[]> dataCaptor = ArgumentCaptor.forClass( Object[].class );

    do {
      result = parquetOutput.processRow( parquetOutputMeta, parquetOutputData );
      if ( result ) {
        rowsProcessed++;
      }
    } while ( result );

    // 3 rows to be outputted to an parquet file
    assertEquals( 3, rowsProcessed );
    verify( mockRowHandler, times( 3 ) ).putRow( rowMetaCaptor.capture(), dataCaptor.capture() );
    List<RowMeta> rowMetaCaptured = rowMetaCaptor.getAllValues();
    List<Object[]> dataCaptured = dataCaptor.getAllValues();
    for ( int rowNum = 0; rowNum < 3; rowNum++ ) {
      assertEquals( 0, rowMetaCaptured.get( rowNum ).indexOfValue( "StringName" ) );
      assertEquals( "string" + ( rowNum % 3 + 1 ), dataCaptured.get( rowNum )[ 0 ] );
    }
    assertFalse( new File( aliasPath ).exists() );
    File outputFile = new File( OUTPUT_FILE_NAME );
    assertTrue( outputFile.exists() );
    outputFile.delete();
  }
}
