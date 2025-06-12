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


package org.pentaho.big.data.kettle.plugins.hbase.rowdecoder;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.big.data.kettle.plugins.hbase.mapping.HBaseRowToKettleTuple;
import org.pentaho.hadoop.shim.api.hbase.ByteConversionUtil;
import org.pentaho.hadoop.shim.api.hbase.HBaseService;
import org.pentaho.hadoop.shim.api.hbase.mapping.Mapping;
import org.pentaho.hadoop.shim.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

/**
 * Step for decoding incoming HBase row objects using a supplied mapping. Can be used in a Hadoop MR job for processing
 * tables split by org.pentaho.hbase.mapred.PentahoTableInputFormat (see the javadoc for this class for properties that
 * can be set in the job to control the query)
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class HBaseRowDecoder extends BaseStep implements StepInterface {
  public static final String HBASE_ROW_DECODER_ERROR_NOT_RESULT = "HBaseRowDecoder.Error.NotResult";
  public static final String HBASE_ROW_DECODER_ERROR_NOT_IMMUTABLE_BYTES_WRITABLE =
    "HBaseRowDecoder.Error.NotImmutableBytesWritable";
  private static Class<?> hBaseRowDecoderMetaClass = HBaseRowDecoderMeta.class;

  private final NamedClusterServiceLocator namedClusterServiceLocator;

  protected HBaseRowDecoderMeta hBaseRowDecoderMeta;
  protected HBaseRowDecoderData hBaseRowDecoderData;
  private HBaseService hBaseService;

  public HBaseRowDecoder( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                          Trans trans, NamedClusterServiceLocator namedClusterServiceLocator ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
    this.namedClusterServiceLocator = namedClusterServiceLocator;
  }

  /**
   * The mapping information to use in order to decode HBase column values
   */
  protected Mapping mTableMapping;

  /**
   * Information from the mapping
   */
  protected HBaseValueMetaInterface[] mOutputColumns;

  /**
   * Index of incoming key value
   */
  protected int mKeyInIndex = -1;

  /**
   * Index of incoming HBase row (Result object)
   */
  protected int mResultInIndex = -1;

  /**
   * Used when decoding columns to <key, family, column, value, time stamp> tuples
   */
  protected HBaseRowToKettleTuple mTupleHandler;

  /**
   * Bytes util
   */
  protected ByteConversionUtil mBytesUtil;

  @Override
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    Object[] inputRow = getRow();

    if ( inputRow == null ) {
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;
      hBaseRowDecoderMeta = (HBaseRowDecoderMeta) smi;
      hBaseRowDecoderData = (HBaseRowDecoderData) sdi;

      try {
        hBaseService =
          namedClusterServiceLocator.getService( hBaseRowDecoderMeta.getNamedCluster(), HBaseService.class );
        mBytesUtil = hBaseService.getByteConversionUtil();

        // no configuration needed here because we don't need access to the
        // actual database, just a few utility routines from HBaseShim for
        // decoding row objects handed to us by the table input format
      } catch ( Exception ex ) {
        throw new KettleException( ex.getMessage(), ex );
      }

      mTableMapping = hBaseRowDecoderMeta.getMapping();

      if ( mTableMapping == null || StringUtils.isEmpty( mTableMapping.getKeyName() ) ) {
        throw new KettleException(
          BaseMessages.getString( hBaseRowDecoderMetaClass, "HBaseRowDecoder.Error.NoMappingInfo" ) );
      }

      if ( mTableMapping.isTupleMapping() ) {
        mTupleHandler = new HBaseRowToKettleTuple( mBytesUtil );
      }

      mOutputColumns = new HBaseValueMetaInterface[ mTableMapping.getMappedColumns().keySet().size() ];
      int k = 0;
      for ( String alias : mTableMapping.getMappedColumns().keySet() ) {
        mOutputColumns[ k++ ] = mTableMapping.getMappedColumns().get( alias );
      }

      hBaseRowDecoderData.setOutputRowMeta( getInputRowMeta().clone() );
      hBaseRowDecoderMeta.getFields( getTransMeta().getBowl(), hBaseRowDecoderData.getOutputRowMeta(), getStepname(),
        null, null, this );

      // check types first
      RowMetaInterface inputMeta = getInputRowMeta();
      String inKey = environmentSubstitute( hBaseRowDecoderMeta.getIncomingKeyField() );

      mKeyInIndex = inputMeta.indexOfValue( inKey );
      if ( mKeyInIndex == -1 ) {
        throw new KettleException(
          BaseMessages.getString( hBaseRowDecoderMetaClass, "HBaseRowDecoder.Error.UnableToFindHBaseKey", inKey ) );
      }

      try {
        inputRow[ mKeyInIndex ] = mBytesUtil.convertToImmutableBytesWritable( inputRow[ mKeyInIndex ] );
      } catch ( InvocationTargetException | IllegalAccessException | NoSuchMethodException e ) {
        throw new KettleException( BaseMessages.getString( hBaseRowDecoderMetaClass,
          HBASE_ROW_DECODER_ERROR_NOT_IMMUTABLE_BYTES_WRITABLE,
          hBaseRowDecoderMeta.getIncomingKeyField() ) );
      }

      if ( !mBytesUtil.isImmutableBytesWritable( inputRow[ mKeyInIndex ] ) ) {
        throw new KettleException( BaseMessages.getString( hBaseRowDecoderMetaClass,
          HBASE_ROW_DECODER_ERROR_NOT_IMMUTABLE_BYTES_WRITABLE,
          hBaseRowDecoderMeta.getIncomingKeyField() ) );
      }

      String inResult = environmentSubstitute( hBaseRowDecoderMeta.getIncomingResultField() );
      mResultInIndex = inputMeta.indexOfValue( inResult );
      if ( mResultInIndex == -1 ) {
        throw new KettleException(
          BaseMessages.getString( hBaseRowDecoderMetaClass, "HBaseRowDecoder.Error.UnableToFindHBaseRow", inResult ) );
      }
    }

    try {
      inputRow[ mKeyInIndex ] = mBytesUtil.convertToImmutableBytesWritable( inputRow[ mKeyInIndex ] );
    } catch ( InvocationTargetException | IllegalAccessException | NoSuchMethodException e ) {
      throw new KettleException( BaseMessages.getString( hBaseRowDecoderMetaClass,
        HBASE_ROW_DECODER_ERROR_NOT_IMMUTABLE_BYTES_WRITABLE,
        hBaseRowDecoderMeta.getIncomingKeyField() ) );
    }

    Object hRow = inputRow[ mResultInIndex ];
    if ( inputRow[ mKeyInIndex ] != null && hRow != null ) {
      if ( mTableMapping.isTupleMapping() ) {
        List<Object[]> hrowToKettleRow =
          mTupleHandler.hbaseRowToKettleTupleMode( hBaseService.getHBaseValueMetaInterfaceFactory(), hRow,
            mTableMapping, mTableMapping
              .getMappedColumns(), hBaseRowDecoderData.getOutputRowMeta() );

        for ( Object[] tuple : hrowToKettleRow ) {
          putRow( hBaseRowDecoderData.getOutputRowMeta(), tuple );
        }
      } else {
        Object[] outputRowData = RowDataUtil.allocateRowData( mOutputColumns.length + 1 ); // + 1 for key

        byte[] rowKey = null;
        try {
          rowKey = (byte[]) hRow.getClass().getMethod( "getRow" ).invoke( hRow );
        } catch ( Exception ex ) {
          throw new KettleException(
            BaseMessages.getString( hBaseRowDecoderMetaClass, "HBaseRowDecoder.Error.UnableToGetRowKey" ), ex );
        }
        Object decodedKey = mTableMapping.decodeKeyValue( rowKey );
        outputRowData[ 0 ] = decodedKey;

        for ( int i = 0; i < mOutputColumns.length; i++ ) {
          HBaseValueMetaInterface current = mOutputColumns[ i ];

          byte[] colFamilyName = current.getColumnFamily().getBytes();
          byte[] qualifier = current.getColumnName().getBytes();

          byte[] kv = null;
          try {
            kv = (byte[]) hRow.getClass().getMethod( "getValue", byte[].class, byte[].class )
              .invoke( hRow, colFamilyName, qualifier );
          } catch ( Exception ex ) {
            throw new KettleException(
              BaseMessages.getString( hBaseRowDecoderMetaClass, "HBaseRowDecoder.Error.UnableToGetColumnValue" ),
              ex );
          }

          Object decodedVal = current.decodeColumnValue( ( kv == null ) ? null : kv );
          outputRowData[ i + 1 ] = decodedVal;
        }

        // output the row
        putRow( hBaseRowDecoderData.getOutputRowMeta(), outputRowData );
      }
    }

    return true;
  }

  @Override
  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    if ( super.init( smi, sdi ) ) {
      HBaseRowDecoderMeta meta = (HBaseRowDecoderMeta) smi;
      try {
        meta.applyInjection();
        return true;
      } catch ( KettleException e ) {
        logError( "Error while injecting properties", e );
      }
    }
    return false;
  }

}
