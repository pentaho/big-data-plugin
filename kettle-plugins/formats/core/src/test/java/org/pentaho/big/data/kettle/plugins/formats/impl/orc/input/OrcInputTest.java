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

package org.pentaho.big.data.kettle.plugins.formats.impl.orc.input;

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
import org.pentaho.hadoop.shim.api.format.IPentahoOrcInputFormat;
import org.pentaho.metastore.locator.api.MetastoreLocator;

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
public class OrcInputTest {
  private static final String INPUT_STEP_NAME = "Input Step Name";
  private static final String INPUT_STREAM_FIELD_NAME = "inputStreamFieldName";
  private static final String PASS_FIELD_NAME = "passFieldName";
  private static final String FILENAME = "orcFile";

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
  private OrcInputData orcInputData;

  @Mock
  private RowHandler mockRowHandler;
  @Mock
  private IPentahoOrcInputFormat mockPentahoOrcInputFormat;
  @Mock
  private IPentahoOrcInputFormat.IPentahoRecordReader mockPentahoOrcRecordReader;

  private OrcInputMeta orcInputMeta;
  private OrcInput orcInput;
  private RowMeta orcRowMeta;
  private RowMetaAndData[] orcRows;
  private RowMeta inputRowMeta;
  private RowMetaAndData[] inputRows;
  private int currentOrcInputRow;

  @Before
  public void setUp() throws Exception {
    KettleLogStore.init();
    currentOrcInputRow = 0;
    setInputRows();
    setOrcRows();
    Collection<MetastoreLocator> metastoreLocatorCollection = new ArrayList<>();
    metastoreLocatorCollection.add( mockMetaStoreLocator );
    NamedClusterResolver namedClusterResolver;
    try ( MockedStatic<PluginServiceLoader> pluginServiceLoaderMockedStatic = Mockito.mockStatic( PluginServiceLoader.class ) ) {
      pluginServiceLoaderMockedStatic.when( () -> PluginServiceLoader.loadServices( MetastoreLocator.class ) )
        .thenReturn( metastoreLocatorCollection );
      namedClusterResolver = new NamedClusterResolver( mockNamedClusterServiceLocator, mockNamedClusterService );

      orcInputMeta = spy( new OrcInputMeta( namedClusterResolver ) );
      orcInputMeta.inputFiles.fileName = new String[ 1 ];
      orcInputMeta.setFilename( INPUT_STREAM_FIELD_NAME );

      orcInputMeta.setParentStepMeta( mockStepMeta );
      when( mockStepMeta.getParentTransMeta() ).thenReturn( mockTransMeta );
      when( mockStepMeta.getName() ).thenReturn( INPUT_STEP_NAME );
      when( mockTransMeta.findStep( INPUT_STEP_NAME ) ).thenReturn( mockStepMeta );
      when( mockTransMeta.getBowl() ).thenReturn( DefaultBowl.getInstance() );

      orcInputData.input = mockPentahoOrcInputFormat;
      when( mockFormatService.createInputFormat( IPentahoOrcInputFormat.class,
        orcInputMeta.getNamedClusterResolver().resolveNamedCluster( orcInputMeta.getFilename() ) ) )
        .thenReturn( mockPentahoOrcInputFormat );
      when( mockNamedClusterServiceLocator.getService( nullable( NamedCluster.class ), any( Class.class ) ) )
        .thenReturn( mockFormatService );
      when( mockTransMeta.environmentSubstitute( INPUT_STREAM_FIELD_NAME ) ).thenReturn( INPUT_STREAM_FIELD_NAME );
      when( mockPentahoOrcInputFormat.createRecordReader( null ) ).thenReturn( mockPentahoOrcRecordReader );
      when( mockPentahoOrcRecordReader.iterator() ).thenReturn( new OrcInputTest.OrcRecordIterator() );

      orcInput = spy( new OrcInput( mockStepMeta, mockStepDataInterface, 0, mockTransMeta,
        mockTrans ) );
      orcInput.setRowHandler( mockRowHandler );
      orcInput.setInputRowMeta( inputRowMeta );
      orcInput.setLogLevel( LogLevel.ERROR );
      orcInput.setTransMeta( mockTransMeta );
    }
  }

  private Object[] returnNextInputRow() {
    Object[] result = null;
    if ( currentOrcInputRow < inputRows.length ) {
      result = inputRows[ currentOrcInputRow ].getData().clone();
      currentOrcInputRow++;
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
      result = orcInput.processRow( orcInputMeta, orcInputData );
      if ( result ) {
        rowsProcessed++;
      }
    } while ( result );

    // 1 file, 2 rows.
    assertEquals( 2, rowsProcessed );
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
    assertEquals( true, orcInput.init() );
  }

  @Test
  public void testProcessRowKettleFailure() {
    String expectedMessage = "KettleExceptionMessage";
    try {
      doThrow( new KettleException( expectedMessage ) )
        .when( mockPentahoOrcInputFormat ).createRecordReader( null );
      orcInput.processRow( orcInputMeta, orcInputData );
      fail( "No Kettle Exception thrown" );
    } catch ( KettleException kex ) {
      assertTrue( kex.getMessage().contains( expectedMessage ) );
    } catch ( Exception ex ) {
      fail( "No other type of exception should be thrown" );
    }
  }

  @Test
  public void testProcessRowGeneralFailure() {
    String expectedMessage = "KettleExceptionMessage";
    try {
      doThrow( new Exception( expectedMessage ) )
        .when( mockPentahoOrcInputFormat ).createRecordReader( null );
      orcInput.processRow( orcInputMeta, orcInputData );
      fail( "No Kettle Exception thrown" );
    } catch ( KettleException kex ) {
      assertTrue( kex.getMessage().contains( expectedMessage ) );
    } catch ( Exception ex ) {
      fail( "No other type of exception should be thrown" );
    }
  }

  private RowMeta setOrcRowMeta() {
    orcRowMeta = new RowMeta();
    ValueMetaInterface valueMetaString = new ValueMetaString( "str" );
    orcRowMeta.addValueMeta( valueMetaString );
    ValueMetaInterface valueMetaBoolean = new ValueMetaBoolean( "bool" );
    orcRowMeta.addValueMeta( valueMetaBoolean );
    ValueMetaInterface valueMetaInteger = new ValueMetaInteger( "int" );
    orcRowMeta.addValueMeta( valueMetaInteger );
    return orcRowMeta;
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
      new RowMetaAndData( orcRowMeta, FILENAME, "pass1" )
    };

  }

  private void setOrcRows() {
    setOrcRowMeta();
    orcRows = new RowMetaAndData[] {
      new RowMetaAndData( orcRowMeta, "string1", true, new Integer( 123 ) ),
      new RowMetaAndData( orcRowMeta, "string2", true, new Integer( 321 ) )
    };
  }

  private class OrcRecordIterator implements Iterator<RowMetaAndData> {
    private Iterator<RowMetaAndData> iter;
    private boolean reset;

    OrcRecordIterator() {
      init();
    }

    private void init() {
      iter = Arrays.asList( orcRows ).iterator();
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
