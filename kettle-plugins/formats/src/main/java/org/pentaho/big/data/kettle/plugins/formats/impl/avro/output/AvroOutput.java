/*******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2017 by Pentaho : http://www.pentaho.com
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

import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceLocator;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.big.data.kettle.plugins.formats.FormatInputOutputField;
import org.pentaho.bigdata.api.format.FormatService;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.hadoop.shim.api.format.IPentahoAvroOutputFormat;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

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
        init();
      }

      Object[] currentRow = getRow();
      if ( currentRow != null ) {
        //create new outputMeta
        RowMetaInterface outputRMI = new RowMeta();
        //create data equals with output fileds
        Object[] outputData = new Object[meta.getOutputFields().size()];
        for ( int i = 0; i < meta.getOutputFields().size(); i++ ) {
          int inputRowIndex = getInputRowMeta().indexOfValue( meta.getOutputFields().get( i ).getName() );
          if ( inputRowIndex == -1 ) {
            throw new KettleException( "Field name [" + meta.getOutputFields().get( i ).getName() + " ] couldn't be found in the input stream!" );
          } else {
            ValueMetaInterface vmi = ValueMetaFactory.cloneValueMeta( getInputRowMeta().getValueMeta( inputRowIndex ) );
            //add output value meta according output fields
            outputRMI.addValueMeta( i,  vmi );
            //add output data according output fields
            outputData[i] = currentRow[inputRowIndex];
          }
        }
        data.writer.write( new RowMetaAndData( outputRMI, outputData ) );
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
    if ( meta.getFilename() == null ) {
      throw new KettleException( "No output files defined" );
    }
    SchemaDescription schemaDescription = new SchemaDescription();
    for ( FormatInputOutputField f : meta.getOutputFields() ) {
      SchemaDescription.Field field = schemaDescription.new Field( f.getPath(), f.getName(), f.getType(), ValueMetaBase.convertStringToBoolean( f.getNullString() ) );
      field.defaultValue = f.getIfNullValue();
      schemaDescription.addField( field );
    }

    data.output = formatService.createOutputFormat( IPentahoAvroOutputFormat.class );
    data.output.setOutputFile( meta.getFilename() );
    data.output.setSchemaDescription( schemaDescription );
    IPentahoAvroOutputFormat.COMPRESSION compression;
    try {
      compression = IPentahoAvroOutputFormat.COMPRESSION.valueOf( meta.getCompressionType().toUpperCase() );
    } catch ( Exception ex ) {
      compression = IPentahoAvroOutputFormat.COMPRESSION.UNCOMPRESSED;
    }
    data.output.setCompression( compression );
    data.output.setNameSpace( meta.getNamespace() );
    data.output.setRecordName( meta.getRecordName() );
    data.output.setDocValue( meta.getDocValue() );
    data.output.setSchemaFilename( meta.getSchemaFilename() );
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
