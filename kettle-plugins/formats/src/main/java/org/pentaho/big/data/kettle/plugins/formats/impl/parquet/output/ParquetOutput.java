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

package org.pentaho.big.data.kettle.plugins.formats.impl.parquet.output;

import java.io.IOException;

import org.apache.commons.vfs2.FileObject;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceLocator;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.big.data.kettle.plugins.formats.FormatInputOutputField;
import org.pentaho.big.data.kettle.plugins.formats.parquet.output.ParquetOutputMetaBase;
import org.pentaho.bigdata.api.format.FormatService;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.vfs.AliasedFileObject;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.hadoop.shim.api.format.IPentahoParquetOutputFormat;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

public class ParquetOutput extends BaseStep implements StepInterface {

  private final NamedClusterServiceLocator namedClusterServiceLocator;

  private ParquetOutputMeta meta;

  private ParquetOutputData data;

  public ParquetOutput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans, NamedClusterServiceLocator namedClusterServiceLocator ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
    this.namedClusterServiceLocator = namedClusterServiceLocator;
  }

  @Override
  public synchronized boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    try {
      Object[] currentRow = getRow();
      if ( data.output == null ) {
        if ( currentRow == null ) {
          setOutputDone();
          return true;
        }
        init( getInputRowMeta() );
      }

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

  public void init( RowMetaInterface rowMeta ) throws Exception {
    FormatService formatService;
    try {
      formatService = namedClusterServiceLocator.getService( meta.getNamedCluster(), FormatService.class );
    } catch ( ClusterInitializationException e ) {
      throw new KettleException( "can't get service format shim ", e );
    }
    if ( meta.getFilename() == null ) {
      throw new KettleException( "No output files defined" );
    }

    data.output = formatService.createOutputFormat( IPentahoParquetOutputFormat.class );

    String outputFileName = meta.constructOutputFilename();
    FileObject outputFileObject = KettleVFS.getFileObject( outputFileName );
    if ( AliasedFileObject.isAliasedFile( outputFileObject ) ) {
      outputFileName = ( (AliasedFileObject) outputFileObject ).getOriginalURIString();
    }

    data.output.setOutputFile( outputFileName, meta.overrideOutput );
    data.output.setSchema( createSchema( rowMeta ) );

    IPentahoParquetOutputFormat.COMPRESSION compression;
    try {
      compression = IPentahoParquetOutputFormat.COMPRESSION.valueOf( meta.getCompressionType().toUpperCase() );
    } catch ( Exception ex ) {
      compression = IPentahoParquetOutputFormat.COMPRESSION.UNCOMPRESSED;
    }
    data.output.setCompression( compression );
    data.output
        .setVersion( IPentahoParquetOutputFormat.VERSION.VERSION_1_0.toString().equals( meta.getParquetVersion() )
            ? IPentahoParquetOutputFormat.VERSION.VERSION_1_0 : IPentahoParquetOutputFormat.VERSION.VERSION_2_0 );
    if ( meta.getRowGroupSize( variables ) > 0 ) {
      data.output.setRowGroupSize( meta.getRowGroupSize( variables ) * 1024 * 1024 );
    }
    if ( meta.getDataPageSize( variables ) > 0 ) {
      data.output.setDataPageSize( meta.getDataPageSize( variables ) * 1024 );
    }
    data.output.enableDictionary( meta.enableDictionary );
    if ( meta.getDictPageSize( variables ) > 0 ) {
      data.output.setDictionaryPageSize( meta.getDictPageSize( variables ) * 1024 );
    }

    data.writer = data.output.createRecordWriter();
  }

  private SchemaDescription createSchema( RowMetaInterface rowMeta ) {
    SchemaDescription schema = createSchemaFromMeta( meta );
    if ( schema.isEmpty() ) {
      for ( ValueMetaInterface v : rowMeta.getValueMetaList() ) {
        SchemaDescription.Field field = schema.new Field( v.getName(), v.getName(), v.getType(), true );
        switch ( v.getType() ) {
          case ValueMetaInterface.TYPE_STRING:
            field.defaultValue = "";
            break;
          case ValueMetaInterface.TYPE_NUMBER:
          case ValueMetaInterface.TYPE_BOOLEAN:
          case ValueMetaInterface.TYPE_INTEGER:
          case ValueMetaInterface.TYPE_BIGNUMBER:
            field.defaultValue = "0";
            break;
        }
        schema.addField( field );
      }
    }
    return schema;
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
    meta = (ParquetOutputMeta) smi;
    data = (ParquetOutputData) sdi;
    if ( super.init( smi, sdi ) ) {
      return true;
    }
    return false;
  }

  public static SchemaDescription createSchemaFromMeta( ParquetOutputMetaBase meta ) {
    SchemaDescription schema = new SchemaDescription();
    for ( FormatInputOutputField f : meta.outputFields ) {
      SchemaDescription.Field field =
          schema.new Field( f.getPath(), f.getName(), f.getType(), Boolean.parseBoolean( f.getNullString() ) );
      field.defaultValue = f.getIfNullValue();
      schema.addField( field );
    }
    return schema;
  }
}
