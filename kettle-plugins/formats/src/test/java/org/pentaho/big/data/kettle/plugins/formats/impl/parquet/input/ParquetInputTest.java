/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2020-2024 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.formats.impl.parquet.input;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.big.data.kettle.plugins.formats.impl.NamedClusterResolver;
import org.pentaho.di.core.bowl.DefaultBowl;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBoolean;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.service.PluginServiceLoader;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.RowHandler;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.hadoop.shim.api.format.FormatService;
import org.pentaho.hadoop.shim.api.format.IPentahoInputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoParquetInputFormat;
import org.pentaho.metastore.locator.api.MetastoreLocator;

import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class ParquetInputTest {
  private static final String INPUT_STEP_NAME = "Input Step Name";
  private static final String INPUT_STREAM_FIELD_NAME = "inputStreamFieldName";
  private static final String PASS_FIELD_NAME = "passFieldName";

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
  private ParquetInputData parquetInputData;

  @Mock
  private RowHandler mockRowHandler;
  @Mock
  private IPentahoParquetInputFormat mockPentahoParquetInputFormat;
  @Mock
  private IPentahoParquetInputFormat.IPentahoRecordReader mockPentahoParquetRecordReader;
  @Mock
  private IPentahoParquetInputFormat.IPentahoInputSplit mockPentahoInputSplit;

  private ParquetInputMeta parquetInputMeta;
  private ParquetInput parquetInput;
  private RowMeta parquetRowMeta;
  private RowMetaAndData[] parquetRows;
  private RowMeta inputRowMeta;
  private RowMetaAndData[] inputRows;
  private int currentParquetInputRow;

  @Before
  public void setUp() throws Exception {
    KettleLogStore.init();
    currentParquetInputRow = 0;
    setInputRows();
    setParquetRows();
    Collection<MetastoreLocator> metastoreLocatorCollection = new ArrayList<>();
    metastoreLocatorCollection.add( mockMetaStoreLocator );
    NamedClusterResolver namedClusterResolver;
    try ( MockedStatic<PluginServiceLoader> pluginServiceLoaderMockedStatic = Mockito.mockStatic( PluginServiceLoader.class ) ) {
      pluginServiceLoaderMockedStatic.when( () -> PluginServiceLoader.loadServices( MetastoreLocator.class ) )
        .thenReturn( metastoreLocatorCollection );
      namedClusterResolver = new NamedClusterResolver( mockNamedClusterServiceLocator, mockNamedClusterService );

      parquetInputMeta = new ParquetInputMeta( namedClusterResolver );
      parquetInputMeta.inputFiles.fileName = new String[ 1 ];
      parquetInputMeta.setFilename( INPUT_STREAM_FIELD_NAME );

      parquetInputMeta.setParentStepMeta( mockStepMeta );
      when( mockStepMeta.getName() ).thenReturn( INPUT_STEP_NAME );
      when( mockTransMeta.findStep( INPUT_STEP_NAME ) ).thenReturn( mockStepMeta );
      when( mockTransMeta.getBowl() ).thenReturn( DefaultBowl.getInstance() );

      parquetInputData.input = mockPentahoParquetInputFormat;
      when( mockFormatService.createInputFormat( IPentahoParquetInputFormat.class,
        parquetInputMeta.getNamedClusterResolver().resolveNamedCluster( parquetInputMeta.getFilename() ) ) )
        .thenReturn( mockPentahoParquetInputFormat );
      when( mockNamedClusterServiceLocator.getService( nullable( NamedCluster.class ), any( Class.class ) ) )
        .thenReturn( mockFormatService );
      when( mockPentahoParquetInputFormat.createRecordReader( mockPentahoInputSplit ) ).thenReturn(
        mockPentahoParquetRecordReader );
      when( mockPentahoParquetRecordReader.iterator() ).thenReturn( new ParquetInputTest.ParquetRecordIterator() );
      List<IPentahoInputFormat.IPentahoInputSplit> splits = new ArrayList<>();
      splits.add( mockPentahoInputSplit );
      when( parquetInputData.input.getSplits() ).thenReturn( splits );

      parquetInput = spy( new ParquetInput( mockStepMeta, mockStepDataInterface, 0, mockTransMeta,
        mockTrans ) );
      parquetInput.setRowHandler( mockRowHandler );
      parquetInput.setInputRowMeta( inputRowMeta );
      parquetInput.setLogLevel( LogLevel.ERROR );
      parquetInput.setTransMeta( mockTransMeta );
    }
  }

  private Object[] returnNextInputRow() {
    Object[] result = null;
    if ( currentParquetInputRow < inputRows.length ) {
      result = inputRows[ currentParquetInputRow ].getData().clone();
      currentParquetInputRow++;
    }
    return result;
  }

  @Test
  public void testProcessRow() throws Exception {
    boolean result;
    int rowsProcessed = 0;
    ArgumentCaptor<RowMeta> rowMetaCaptor = ArgumentCaptor.forClass( RowMeta.class );
    ArgumentCaptor<Object[]> dataCaptor = ArgumentCaptor.forClass( Object[].class );

    do {
      result = parquetInput.processRow( parquetInputMeta, parquetInputData );
      if ( result ) {
        rowsProcessed++;
      }
    } while ( result );

    // 1 file, 2 rows. The third run is to increase the split count, which will return false on the next processRow call
    assertEquals( 3, rowsProcessed );
    verify( mockRowHandler, times( 2 ) ).putRow( rowMetaCaptor.capture(), dataCaptor.capture() );
    List<RowMeta> rowMeta = rowMetaCaptor.getAllValues();
    List<Object[]> dataCaptured = dataCaptor.getAllValues();
    for ( int rowNum = 0; rowNum < 2; rowNum++ ) {
      assertEquals( 0, rowMeta.get( rowNum ).indexOfValue( "str" ) );
      assertEquals( "string" + ( rowNum % 2 + 1 ), dataCaptured.get( rowNum )[ 0 ] );
    }
  }

  @Test
  public void testInit() {
    assertEquals( true, parquetInput.init() );
  }

  @Test
  public void testProcessNoSuchFile() throws Exception {
    String expectedMessage = "No input file";
    try {
      doThrow( new NoSuchFileException( "NoSuchFileExceptionMessage" ) ).when( parquetInput ).initSplits();
      parquetInput.processRow( parquetInputMeta, parquetInputData );
      fail( "No Kettle Exception thrown" );

    } catch ( KettleException kex ) {
      assertTrue( kex.getMessage().contains( expectedMessage ) );
    }
  }

  @Test
  public void testProcessRowKettleFailure() {
    String expectedMessage = "KettleExceptionMessage";
    try {
      doThrow( new KettleException( expectedMessage ) ).when( parquetInput ).initSplits();
      parquetInput.processRow( parquetInputMeta, parquetInputData );
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
      doThrow( new Exception( expectedMessage ) ).when( parquetInput ).initSplits();
      parquetInput.processRow( parquetInputMeta, parquetInputData );
      fail( "No Kettle Exception thrown" );
    } catch ( KettleException kex ) {
      assertTrue( kex.getMessage().contains( expectedMessage ) );
    } catch ( Exception ex ) {
      fail( "No other type of exception should be thrown" );
    }
  }

  private RowMeta setParquetRowMeta() {
    parquetRowMeta = new RowMeta();
    ValueMetaInterface valueMetaString = new ValueMetaString( "str" );
    parquetRowMeta.addValueMeta( valueMetaString );
    ValueMetaInterface valueMetaBoolean = new ValueMetaBoolean( "bool" );
    parquetRowMeta.addValueMeta( valueMetaBoolean );
    ValueMetaInterface valueMetaInteger = new ValueMetaInteger( "int" );
    parquetRowMeta.addValueMeta( valueMetaInteger );
    return parquetRowMeta;
  }

  private RowMeta setInputRowMeta() {
    inputRowMeta = new RowMeta();
    ValueMetaInterface valueMetaString = new ValueMetaString( INPUT_STREAM_FIELD_NAME );
    inputRowMeta.addValueMeta( valueMetaString );
    ValueMetaInterface valueMetaString2 = new ValueMetaString( PASS_FIELD_NAME );
    inputRowMeta.addValueMeta( valueMetaString2 );
    return inputRowMeta;
  }

  private void setInputRows() {
    setInputRowMeta();
    inputRows = new RowMetaAndData[] {
      new RowMetaAndData( parquetRowMeta, "parquetFile", "pass1" )
    };

  }

  private void setParquetRows() {
    setParquetRowMeta();
    parquetRows = new RowMetaAndData[] {
      new RowMetaAndData( parquetRowMeta, "string1", true, new Integer( 123 ) ),
      new RowMetaAndData( parquetRowMeta, "string2", true, new Integer( 321 ) )
    };
  }

  private class ParquetRecordIterator implements Iterator<RowMetaAndData> {
    private Iterator<RowMetaAndData> iter;
    private boolean reset;

    ParquetRecordIterator() {
      init();
    }

    private void init() {
      iter = Arrays.asList( parquetRows ).iterator();
      reset = false;
    }

    @Override public boolean hasNext() {
      if ( reset ) {
        init();
      }
      if ( !iter.hasNext() ) {
        reset = true;
      }
      return iter.hasNext();
    }

    @Override public RowMetaAndData next() {
      if ( reset ) {
        init(); // Simultate a new iterator for the new file
      }
      return iter.next().clone();
    }
  }
}
