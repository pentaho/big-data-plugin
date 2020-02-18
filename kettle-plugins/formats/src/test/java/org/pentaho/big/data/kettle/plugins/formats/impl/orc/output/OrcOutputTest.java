/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2020 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.big.data.kettle.plugins.formats.impl.orc.output;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.big.data.kettle.plugins.formats.orc.output.OrcOutputField;
import org.pentaho.big.data.kettle.plugins.formats.impl.NamedClusterResolver;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.osgi.api.MetastoreLocatorOsgi;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.trans.step.RowHandler;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.hadoop.shim.api.format.FormatService;
import org.pentaho.hadoop.shim.api.format.IPentahoOrcOutputFormat;
import org.pentaho.hadoop.shim.api.format.OrcSpec;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class OrcOutputTest {

  private static final String OUTPUT_STEP_NAME = "Output Step Name";
  private static final String OUTPUT_TRANS_NAME = "Output Trans Name";
  private static final String OUTPUT_FILE_NAME = "outputFileName";

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
  private MetastoreLocatorOsgi mockMetaStoreLocator;
  @Mock
  private FormatService mockFormatService;
  @Mock
  private OrcOutputData orcOutputData;
  @Mock
  private RowHandler mockRowHandler;
  @Mock
  private IPentahoOrcOutputFormat mockPentahoOrcOutputFormat;
  @Mock
  private LogChannelInterface mockLogChannelInterface;
  @Mock
  private IPentahoOrcOutputFormat.IPentahoRecordWriter mockPentahoOrcRecordWriter;

  private OrcOutput orcOutput;
  private List<OrcOutputField> orcOutputFields;
  private OrcOutputMeta orcOutputMeta;
  private RowMeta dataInputRowMeta;
  private RowMetaAndData[] dataInputRows;
  private int currentOrcRow;

  @Before
  public void setUp() throws Exception {
    currentOrcRow = 0;
    setDataInputRows();
    setOrcOutputRows();
    NamedClusterResolver namedClusterResolver =
      new NamedClusterResolver( mockNamedClusterServiceLocator, mockNamedClusterService, mockMetaStoreLocator );
    orcOutputMeta = new OrcOutputMeta( namedClusterResolver );
    orcOutputMeta.setFilename( OUTPUT_FILE_NAME );
    orcOutputMeta.setOutputFields( orcOutputFields );

    orcOutputMeta.setParentStepMeta( mockStepMeta );
    when( mockStepMeta.getParentTransMeta() ).thenReturn( mockTransMeta );
    when( mockStepMeta.getName() ).thenReturn( OUTPUT_STEP_NAME );
    when( mockTransMeta.getName() ).thenReturn( OUTPUT_TRANS_NAME );
    when( mockTransMeta.findStep( OUTPUT_STEP_NAME ) ).thenReturn( mockStepMeta );
    when( mockTransMeta.findStep( OUTPUT_STEP_NAME ) ).thenReturn( mockStepMeta );
    when( mockTrans.isRunning() ).thenReturn( true );

    try {
      when( mockRowHandler.getRow() ).thenAnswer( answer -> returnNextParquetRow() );
    } catch ( KettleException ke ) {
      ke.printStackTrace();
    }

    when( mockFormatService.createOutputFormat( IPentahoOrcOutputFormat.class,
      orcOutputMeta.getNamedClusterResolver().resolveNamedCluster( orcOutputMeta.getFilename() ) ) )
      .thenReturn( mockPentahoOrcOutputFormat );
    when( mockNamedClusterServiceLocator.getService( any( NamedCluster.class ), any( Class.class ) ) )
      .thenReturn( mockFormatService );
    when( mockPentahoOrcOutputFormat.createRecordWriter() ).thenReturn( mockPentahoOrcRecordWriter );

    orcOutput = spy( new OrcOutput( mockStepMeta, mockStepDataInterface, 0, mockTransMeta, mockTrans ) );
    orcOutput.init( orcOutputMeta, orcOutputData );
    orcOutput.setInputRowMeta( dataInputRowMeta );
    orcOutput.setRowHandler( mockRowHandler );
    orcOutput.setLogLevel( LogLevel.ERROR );
  }

  @Test
  public void testProcessRow() throws Exception {
    boolean result;
    int rowsProcessed = 0;
    ArgumentCaptor<RowMeta> rowMetaCaptor = ArgumentCaptor.forClass( RowMeta.class );
    ArgumentCaptor<Object[]> dataCaptor = ArgumentCaptor.forClass( Object[].class );

    do {
      result = orcOutput.processRow( orcOutputMeta, orcOutputData );
      if ( result ) {
        rowsProcessed++;
      }
    } while ( result );

    // 3 rows to be outputted to an Orc file
    assertEquals( 3, rowsProcessed );
    verify( mockRowHandler, times( 3 ) ).putRow( rowMetaCaptor.capture(), dataCaptor.capture() );
    List<RowMeta> rowMetaCaptured = rowMetaCaptor.getAllValues();
    List<Object[]> dataCaptured = dataCaptor.getAllValues();
    for ( int rowNum = 0; rowNum < 3; rowNum++ ) {
      assertEquals( 0, rowMetaCaptured.get( rowNum ).indexOfValue( "StringName" ) );
      assertEquals( "string" + ( rowNum % 3 + 1 ), dataCaptured.get( rowNum )[ 0 ] );
    }
  }

  @Test
  public void testProcessRowIllegalState() throws Exception {
    doThrow( new IllegalStateException( "IllegalStateExceptionMessage" ) ).when( orcOutput ).init();
    when( orcOutput.getLogChannel() ).thenReturn( mockLogChannelInterface );
    assertFalse( orcOutput.processRow( orcOutputMeta, orcOutputData ) );

    verify( mockLogChannelInterface, times( 1 ) ).logError( "IllegalStateExceptionMessage" );
  }

  @Test
  public void testProcessRowKettleFailure() {
    String expectedMessage = "KettleExceptionMessage";
    try {
      doThrow( new KettleException( expectedMessage ) ).when( orcOutput ).init();
      orcOutput.processRow( orcOutputMeta, orcOutputData );
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
      doThrow( new Exception( expectedMessage ) ).when( orcOutput ).init();
      orcOutput.processRow( orcOutputMeta, orcOutputData );
      fail( "No Kettle Exception thrown" );
    } catch ( KettleException kex ) {
      assertTrue( kex.getMessage().contains( expectedMessage ) );
    } catch ( Exception ex ) {
      fail( "No other type of exception should be thrown" );
    }
  }

  private Object[] returnNextParquetRow() {
    Object[] result = null;
    if ( currentOrcRow < dataInputRows.length ) {
      result = dataInputRows[ currentOrcRow ].getData().clone();
      currentOrcRow++;
    }
    return result;
  }

  private void setOrcOutputRows() {
    OrcOutputField orcOutputField = mock( OrcOutputField.class );
    when( orcOutputField.getOrcType() ).thenReturn( OrcSpec.DataType.STRING );
    when( orcOutputField.getPentahoFieldName() ).thenReturn( "StringName" );
    orcOutputFields =  new ArrayList<>();
    orcOutputFields.add( orcOutputField );
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
}
