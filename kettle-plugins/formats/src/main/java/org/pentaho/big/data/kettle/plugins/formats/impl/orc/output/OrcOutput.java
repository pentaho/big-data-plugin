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

package org.pentaho.big.data.kettle.plugins.formats.impl.orc.output;


import org.pentaho.big.data.kettle.plugins.formats.impl.output.PvfsFileAliaser;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.ClusterInitializationException;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.hadoop.shim.api.format.FormatService;
import org.pentaho.hadoop.shim.api.format.IPentahoOrcOutputFormat;

import java.io.IOException;

public class OrcOutput extends BaseStep implements StepInterface {

  private OrcOutputMeta meta;

  private OrcOutputData data;

  private PvfsFileAliaser pvfsFileAliaser;

  public OrcOutput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                    Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override
  public synchronized boolean processRow( StepMetaInterface smi, StepDataInterface sdi )
    throws KettleException {
    try {
      meta = (OrcOutputMeta) smi;
      data = (OrcOutputData) sdi;

      if ( data.output == null ) {
        init();
      }

      Object[] currentRow = getRow();
      if ( currentRow != null ) {
        //create new outputMeta
        RowMetaInterface outputRMI = new RowMeta();
        //create data equals with output fileds
        Object[] outputData = new Object[ meta.getOutputFields().size() ];
        for ( int i = 0; i < meta.getOutputFields().size(); i++ ) {
          int inputRowIndex = getInputRowMeta().indexOfValue( meta.getOutputFields().get( i ).getPentahoFieldName() );
          if ( inputRowIndex == -1 ) {
            throw new KettleException( "Field name [" + meta.getOutputFields().get( i ).getPentahoFieldName()
              + " ] couldn't be found in the input stream!" );
          } else {
            ValueMetaInterface vmi = ValueMetaFactory.cloneValueMeta( getInputRowMeta().getValueMeta( inputRowIndex ) );
            //add output value meta according output fields
            outputRMI.addValueMeta( i, vmi );
            //add output data according output fields
            outputData[ i ] = currentRow[ inputRowIndex ];
          }
        }
        RowMetaAndData row = new RowMetaAndData( outputRMI, outputData );
        data.writer.write( row );
        putRow( row.getRowMeta(), row.getData() );
        return true;
      } else {
        // no more input to be expected...
        closeWriter();
        pvfsFileAliaser.copyFileToFinalDestination();
        pvfsFileAliaser.deleteTempFileAndFolder();
        setOutputDone();
        return false;
      }
    } catch ( IllegalStateException e ) {
      getLogChannel().logError( e.getMessage() );
      setErrors( 1 );
      pvfsFileAliaser.deleteTempFileAndFolder();
      setOutputDone();
      return false;
    } catch ( KettleException ex ) {
      throw ex;
    } catch ( Exception ex ) {
      throw new KettleException( ex );
    }
  }

  public void init() throws Exception {
    FormatService formatService;
    try {
      formatService = meta.getNamedClusterResolver().getNamedClusterServiceLocator()
        .getService( getNamedCluster(), FormatService.class );
    } catch ( ClusterInitializationException e ) {
      throw new KettleException( "can't get service format shim ", e );
    }

    if ( meta.getFilename() == null ) {
      throw new KettleException( "No output files defined" );
    }

    data.output = formatService.createOutputFormat( IPentahoOrcOutputFormat.class, getNamedCluster() );

    String outputFileName = environmentSubstitute( meta.constructOutputFilename() );
    pvfsFileAliaser = new PvfsFileAliaser( outputFileName, getTransMeta(), data.output, meta.isOverrideOutput(),
      getLogChannel() );

    data.output.setOutputFile( pvfsFileAliaser.generateAlias(), meta.isOverrideOutput() );
    data.output.setFields( meta.getOutputFields() );

    IPentahoOrcOutputFormat.COMPRESSION compression;
    try {
      compression = IPentahoOrcOutputFormat.COMPRESSION.valueOf( meta.getCompressionType().toUpperCase() );
    } catch ( Exception ex ) {
      compression = IPentahoOrcOutputFormat.COMPRESSION.NONE;
    }
    data.output.setCompression( compression );
    if ( compression != IPentahoOrcOutputFormat.COMPRESSION.NONE ) {
      data.output.setCompressSize( meta.getCompressSize() );
    }
    data.output.setRowIndexStride( meta.getRowsBetweenEntries() );
    data.output.setStripeSize( meta.getStripeSize() );
    data.writer = data.output.createRecordWriter();
  }

  private NamedCluster getNamedCluster() {
    return meta.getNamedClusterResolver().resolveNamedCluster( environmentSubstitute( meta.getFilename() ) );
  }

  public void closeWriter() throws KettleException {
    try {
      data.writer.close();
    } catch ( IOException e ) {
      throw new KettleException( e );
    }
    data.output = null;
  }

  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (OrcOutputMeta) smi;
    data = (OrcOutputData) sdi;
    return super.init( smi, sdi );
  }
}
