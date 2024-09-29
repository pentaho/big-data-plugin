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


package org.pentaho.big.data.kettle.plugins.hbase.output;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.big.data.kettle.plugins.hbase.mapping.MappingUtils;
import org.pentaho.big.data.kettle.plugins.hbase.output.KettleRowToHBaseTuple.FieldException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.hadoop.shim.api.hbase.ByteConversionUtil;
import org.pentaho.hadoop.shim.api.hbase.mapping.Mapping;
import org.pentaho.hadoop.shim.api.hbase.mapping.Mapping.KeyType;
import org.pentaho.hadoop.shim.api.hbase.mapping.Mapping.TupleMapping;
import org.pentaho.hadoop.shim.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.hadoop.shim.api.hbase.table.HBasePut;
import org.pentaho.hadoop.shim.api.hbase.table.HBaseTableWriteOperationManager;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class KettleRowToHBaseTupleTest {

  private Mapping tupleMapping;

  @Before
  public void setup() {
    tupleMapping = Mockito.mock( Mapping.class );
    when( tupleMapping.getKeyName() ).thenReturn( Mapping.TupleMapping.KEY.toString() );
    when( tupleMapping.getKeyType() ).thenReturn( KeyType.STRING );
  }

  @Test
  public void testRowConversion() throws Exception {

    RowMetaInterface inputRowMeta = Mockito.mock( RowMetaInterface.class );

    when( inputRowMeta.indexOfValue( Mapping.TupleMapping.KEY.toString() ) ).thenReturn( 0 );
    when( inputRowMeta.indexOfValue( Mapping.TupleMapping.FAMILY.toString() ) ).thenReturn( 1 );
    when( inputRowMeta.indexOfValue( Mapping.TupleMapping.COLUMN.toString() ) ).thenReturn( 2 );
    when( inputRowMeta.indexOfValue( Mapping.TupleMapping.VALUE.toString() ) ).thenReturn( 3 );
    when( inputRowMeta.indexOfValue( MappingUtils.TUPLE_MAPPING_VISIBILITY ) ).thenReturn( 4 );

    ValueMetaString keyMeta = new ValueMetaString( Mapping.TupleMapping.KEY.toString() );
    ValueMetaString familyMeta = new ValueMetaString( Mapping.TupleMapping.FAMILY.toString() );
    ValueMetaString columnMeta = new ValueMetaString( Mapping.TupleMapping.COLUMN.toString() );
    ValueMetaString valueMeta = new ValueMetaString( Mapping.TupleMapping.VALUE.toString() );
    ValueMetaString visMeta = new ValueMetaString( MappingUtils.TUPLE_MAPPING_VISIBILITY );

    when( inputRowMeta.getValueMeta( 0 ) ).thenReturn( keyMeta );
    when( inputRowMeta.getValueMeta( 1 ) ).thenReturn( familyMeta );
    when( inputRowMeta.getValueMeta( 2 ) ).thenReturn( columnMeta );
    when( inputRowMeta.getValueMeta( 3 ) ).thenReturn( valueMeta );
    when( inputRowMeta.getValueMeta( 4 ) ).thenReturn( visMeta );

    Map<String, HBaseValueMetaInterface> columnMap = new HashMap<>();

    HBaseValueMetaInterface hvmi = Mockito.mock( HBaseValueMetaInterface.class );

    columnMap.put( valueMeta.getName(), hvmi );

    HBaseValueMetaInterface hvmiv = Mockito.mock( HBaseValueMetaInterface.class );

    columnMap.put( visMeta.getName(), hvmiv );

    KettleRowToHBaseTuple rowConverter = new KettleRowToHBaseTuple( inputRowMeta, tupleMapping, columnMap );

    ByteConversionUtil byteConversionUtil = Mockito.mock( ByteConversionUtil.class );

    String[] row = { "key", "family", "@@@binary@@@column", "value", "public" };

    HBaseTableWriteOperationManager writeManager = Mockito.mock( HBaseTableWriteOperationManager.class );

    HBasePut put = Mockito.mock( HBasePut.class );

    when( writeManager.createPut( row[ 0 ].getBytes() ) ).thenReturn( put );

    when( byteConversionUtil.encodeKeyValue( row[ 0 ], keyMeta, KeyType.STRING ) ).thenReturn( row[ 0 ].getBytes() );

    rowConverter.createTuplePut( writeManager, byteConversionUtil, row, true );

    verify( put, times( 1 ) ).addColumn( eq( row[ 1 ] ), eq( "column" ), eq( true ), any() );
    verify( put, times( 1 ) ).addColumn( eq( row[ 1 ] ), eq( MappingUtils.TUPLE_MAPPING_VISIBILITY ), eq( false ),
      any() );
    verify( put, times( 1 ) ).setWriteToWAL( true );

    try {
      rowConverter.createTuplePut( null, null, new String[] { null, null, null, null, null }, true );
    } catch ( FieldException fe ) {
      Assert.assertEquals( fe.getFieldString(), TupleMapping.KEY.toString() );
    }

    try {
      rowConverter.createTuplePut( null, null, new String[] { "key", null, null, null, null }, true );
    } catch ( FieldException fe ) {
      Assert.assertEquals( fe.getFieldString(), TupleMapping.FAMILY.toString() );
    }

    try {
      rowConverter.createTuplePut( null, null, new String[] { "key", "family", null, null, null }, true );
    } catch ( FieldException fe ) {
      Assert.assertEquals( fe.getFieldString(), TupleMapping.COLUMN.toString() );
    }

    try {
      rowConverter.createTuplePut( null, null, new String[] { "key", "family", "column", null, null }, true );
    } catch ( FieldException fe ) {
      Assert.assertEquals( fe.getFieldString(), TupleMapping.VALUE.toString() );
    }

  }

  @Test
  public void testMissingValues() {

    try {
      RowMetaInterface inputRowMeta = Mockito.mock( RowMetaInterface.class );
      when( inputRowMeta.indexOfValue( Mapping.TupleMapping.KEY.toString() ) ).thenReturn( -1 );
      new KettleRowToHBaseTuple( inputRowMeta, tupleMapping, null );
      Assert.fail();
    } catch ( KettleException e ) {
    }

    try {
      RowMetaInterface inputRowMeta = Mockito.mock( RowMetaInterface.class );
      when( inputRowMeta.indexOfValue( Mapping.TupleMapping.KEY.toString() ) ).thenReturn( 0 );
      when( inputRowMeta.indexOfValue( Mapping.TupleMapping.FAMILY.toString() ) ).thenReturn( -1 );
      new KettleRowToHBaseTuple( inputRowMeta, tupleMapping, null );
      Assert.fail();
    } catch ( KettleException e ) {
    }

    try {
      RowMetaInterface inputRowMeta = Mockito.mock( RowMetaInterface.class );
      when( inputRowMeta.indexOfValue( Mapping.TupleMapping.KEY.toString() ) ).thenReturn( 0 );
      when( inputRowMeta.indexOfValue( Mapping.TupleMapping.FAMILY.toString() ) ).thenReturn( 1 );
      when( inputRowMeta.indexOfValue( Mapping.TupleMapping.COLUMN.toString() ) ).thenReturn( -1 );
      new KettleRowToHBaseTuple( inputRowMeta, tupleMapping, null );
      Assert.fail();
    } catch ( KettleException e ) {
    }

    try {
      RowMetaInterface inputRowMeta = Mockito.mock( RowMetaInterface.class );
      when( inputRowMeta.indexOfValue( Mapping.TupleMapping.KEY.toString() ) ).thenReturn( 0 );
      when( inputRowMeta.indexOfValue( Mapping.TupleMapping.FAMILY.toString() ) ).thenReturn( 1 );
      when( inputRowMeta.indexOfValue( Mapping.TupleMapping.COLUMN.toString() ) ).thenReturn( 2 );
      when( inputRowMeta.indexOfValue( Mapping.TupleMapping.VALUE.toString() ) ).thenReturn( -1 );
      new KettleRowToHBaseTuple( inputRowMeta, tupleMapping, null );
      Assert.fail();
    } catch ( KettleException e ) {
    }

  }

  @Test
  public void testException() {

    FieldException fieldException = new FieldException( TupleMapping.KEY );
    Assert.assertEquals( fieldException.getFieldString(), TupleMapping.KEY.toString() );

  }

}
