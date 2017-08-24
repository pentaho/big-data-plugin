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

package org.pentaho.big.data.kettle.plugins.formats.avro.output;

import java.io.IOException;

import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceLocator;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.big.data.kettle.plugins.formats.FormatInputField;
import org.pentaho.bigdata.api.format.FormatService;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
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
      if ( data.output == null ) {
        init();
      }

      Object[] currentRow = getRow();
      if ( currentRow != null ) {
        RowMetaAndData row = new RowMetaAndData( getInputRowMeta(), currentRow );
        data.writer.write( row );
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
    SchemaDescription schema = new SchemaDescription();
    for ( FormatInputField f : meta.getOutputFields() ) {
      SchemaDescription.Field field =
          schema.new Field( f.getPath(), f.getName(), f.getType(), Boolean.parseBoolean( f.getNullString() ) );
      field.defaultValue = f.getIfNullValue();
      schema.addField( field );
    }

    data.output = formatService.createOutputFormat( IPentahoAvroOutputFormat.class );
    data.output.setOutputFile( meta.getFilename() );
    data.output.setSchema( schema );
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
