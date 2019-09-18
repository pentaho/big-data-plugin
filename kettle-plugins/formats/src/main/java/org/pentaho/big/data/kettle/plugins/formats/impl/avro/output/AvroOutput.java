/*******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.formats.impl.avro.output;

import java.io.IOException;

import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
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
import org.pentaho.hadoop.shim.api.format.IPentahoAvroOutputFormat;

public class AvroOutput extends BaseStep implements StepInterface {

  private final NamedClusterServiceLocator namedClusterServiceLocator;

  private AvroOutputMeta meta;

  private AvroOutputData data;

  public AvroOutput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                     Trans trans, NamedClusterServiceLocator namedClusterServiceLocator ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
    this.namedClusterServiceLocator = namedClusterServiceLocator;
  }

  @Override
  public synchronized boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    try {
      meta = (AvroOutputMeta) smi;
      data = (AvroOutputData) sdi;

      if ( data.output == null ) {
        try {
          init();
        } catch ( Throwable e ) {
          String error = e.getMessage().replaceAll( "TRANS_NAME", getTrans().getName() );
          error = error.replaceAll( "STEP_NAME", getStepname() );
          getLogChannel().logError( error );
          setErrors( 1 );
          setOutputDone();
          return false;
        }
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
        setOutputDone();
        return false;
      }
    } catch ( KettleException ex ) {
      throw ex;
    } catch ( Exception ex ) {
      throw new KettleException( ex );
    }
  }

  public void init() throws Exception {
    FormatService formatService;
    try {
      formatService = namedClusterServiceLocator.getService( meta.getNamedCluster(), FormatService.class );
    } catch ( ClusterInitializationException e ) {
      throw new KettleException( "can't get service format shim ", e );
    }
    TransMeta parentTransMeta = meta.getParentStepMeta().getParentTransMeta();
    data.output = formatService.createOutputFormat( IPentahoAvroOutputFormat.class, meta.getNamedCluster() );
    data.output
      .setOutputFile( parentTransMeta.environmentSubstitute( meta.constructOutputFilename( meta.getFilename() ) ),
        meta.isOverrideOutput() );
    data.output.setFields( meta.getOutputFields() );
    IPentahoAvroOutputFormat.COMPRESSION compression;
    try {
      compression = IPentahoAvroOutputFormat.COMPRESSION
        .valueOf( parentTransMeta.environmentSubstitute( meta.getCompressionType() ).toUpperCase() );
    } catch ( Exception ex ) {
      compression = IPentahoAvroOutputFormat.COMPRESSION.UNCOMPRESSED;
    }
    data.output.setCompression( compression );
    data.output.setNameSpace( parentTransMeta.environmentSubstitute( meta.getNamespace() ) );
    data.output.setRecordName( parentTransMeta.environmentSubstitute( meta.getRecordName() ) );
    data.output.setDocValue( parentTransMeta.environmentSubstitute( meta.getDocValue() ) );
    if ( meta.getSchemaFilename() != null && meta.getSchemaFilename().length() != 0 ) {
      data.output.setSchemaFilename(
        parentTransMeta.environmentSubstitute( meta.constructOutputFilename( meta.getSchemaFilename() ) ) );
    }
    data.writer = data.output.createRecordWriter();
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
    meta = (AvroOutputMeta) smi;
    data = (AvroOutputData) sdi;
    if ( super.init( smi, sdi ) ) {
      return true;
    }
    return false;
  }
}
