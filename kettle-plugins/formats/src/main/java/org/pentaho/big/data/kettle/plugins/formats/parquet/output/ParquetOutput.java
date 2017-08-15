/*! ******************************************************************************
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

package org.pentaho.big.data.kettle.plugins.formats.parquet.output;

import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceLocator;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.bigdata.api.format.FormatService;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class ParquetOutput extends BaseStep implements StepInterface {

  private final NamedClusterServiceLocator namedClusterServiceLocator;
  //private final FormatService formatService;
  private ParquetOutputMetaBase meta;

  private ParquetOutputData data;

  public ParquetOutput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                        Trans trans, NamedClusterServiceLocator namedClusterServiceLocator ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
    this.namedClusterServiceLocator = namedClusterServiceLocator;
  }

  public static SchemaDescription makeScheme() {
    //this part should come from step data
    SchemaDescription s = new SchemaDescription();
    s.addField( s.new Field( "b", "Name", ValueMetaInterface.TYPE_STRING ) );
    s.addField( s.new Field( "c", "Age", ValueMetaInterface.TYPE_STRING ) );
    return s;
  }

  @Override
  public synchronized boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    ParquetOutputMeta parquetOutputMeta = (ParquetOutputMeta) smi;
    FormatService formatService = null;
    try {
      formatService = namedClusterServiceLocator.getService( parquetOutputMeta.getNamedCluster(), FormatService.class );

      Configuration configuration = formatService.createConfiguration();
      Path tempFile = null;
      try {
        tempFile = Files.createTempDirectory( "parquet" );
      } catch ( IOException e ) {
        throw new RuntimeException( "error writing to temp file parquet ", e );
      }
      //here should be builder to set property
      configuration.set( "mapreduce.output.fileoutputformat.outputdir", tempFile.toString() );
      formatService.getOutputFormat( configuration, makeScheme() );
    } catch ( ClusterInitializationException e ) {
      throw new RuntimeException( "can't get service forma shim ", e );
    }
    return true;
    /*  meta = (ParquetOutputMetaBase) smi;
    data = (ParquetOutputData) sdi;

    boolean result = true;
    Object[] currentRow = getRow(); // This also waits for a row to be finished.
    if ( currentRow != null && first ) {
      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getStepname(), null, null, this, repository, metaStore );
      Schema avroSchema = createAvroSchema( meta.getOutputFields() );
      data.avroSchema  = avroSchema;

      FileObject file = KettleVFS.getFileObject( meta.getFilename() );
      // Path path = shim.getFileSystem( conf ).asPath( file.getName().getURI() );
      
//      HadoopConfiguration hadoopConfiguration =
//          HadoopConfigurationBootstrap.getHadoopConfigurationProvider().createConfiguration();
//        HadoopShim shim = hadoopConfiguration.getHadoopShim();
//        org.pentaho.hadoop.shim.api.Configuration conf = shim.createConfiguration();
//        org.pentaho.hadoop.shim.api.fs.FileSystem fs = shim.getFileSystem( conf );
        //Path path = fs.asPath( file.getName().getURI() );

      Configuration configuration = new Configuration();
      configuration.set("fs.file.impl",
          org.apache.hadoop.fs.LocalFileSystem.class.getName()
      );
      configuration.setClassLoader(getClass().getClassLoader());
      
      try {
        FileSystem fs = FileSystem.getLocal( configuration ).getRawFileSystem();
        configuration.set( "fs.file.impl", fs.getClass().getName() );
      } catch ( IOException e1 ) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }
      
      Path path = new Path( file.getName().getURI() );
      
      try {
        data.parquetWriter =
            AvroParquetWriter.<GenericRecord> builder( path ).withConf( configuration ).withWriteMode( Mode.OVERWRITE )
                .withSchema( avroSchema ).build();
      } catch ( IOException e ) {
        throw new KettleException( "Cannot create file ", e );
      }
    }

    if ( currentRow == null ) {
      // no more input to be expected...
      closeFile();
      setOutputDone();
      // data.avroSchema = null;
      return false;
    }

    String filename = meta.getFilename();

    if ( Utils.isEmpty( filename ) ) {
      throw new KettleException( "Filename is empty!" );
    }

    writeRow( currentRow );
    putRow( data.outputRowMeta, currentRow ); // in case we want it to go further...

    if ( checkFeedback( getLinesOutput() ) ) {
      logBasic( "linenr " + getLinesOutput() );
    }

    return result;*/
  }

  public void writeRow( Object[] row ) throws KettleException {
    //throw new KettleException( "Requires Shim API changes" );
    System.out.println( "write row" );
   /* GenericRecord record = new GenericData.Record( data.avroSchema );
    List<ParquetOutputField> outputFields = meta.getOutputFields();
    for ( ParquetOutputField field : outputFields ) {
      int fieldIndex = data.outputRowMeta.indexOfValue( field.getName() );
      Object value = getValue( row, meta.getOutputFields().get( fieldIndex ), fieldIndex );
      if ( value != null ) {
        record.put( field.getName(), value );
      }
    }
    try {
      data.parquetWriter.write( record );
    } catch ( IOException e ) {
      throw new KettleException( "Error writing row", e );
    }*/
  }

  /*public Schema createAvroSchema( List<ParquetOutputField> outputFields ) {
    String recordName = "parquet";
    String doc = "Generated by Parquet Output Step";
    String namespace = "pentaho";

    Schema avroSchema = Schema.createRecord( recordName, doc, namespace, false );
    final List<Schema.Field> avroSchemaFields = new ArrayList<Schema.Field>();
    outputFields.forEach( field -> {
      int fieldIndex = data.outputRowMeta.indexOfValue( field.getName() );
      Schema.Field outField = createAvroField( field.getName(), data.outputRowMeta.getValueMeta( fieldIndex ) );
      avroSchemaFields.add( outField );
    } );
    avroSchema.setFields( avroSchemaFields );
    return avroSchema;
  }

  public static Schema.Field createAvroField( String name, ValueMetaInterface valueMeta ) {
    Schema.Type fieldType = ParquetOutputField.getDefaultAvroType( valueMeta.getType() );
    Schema fieldSchema = Schema.create( fieldType );
    return new Schema.Field( name, fieldSchema, null, null );
  }

  public Object getValue( Object[] r, ParquetOutputField outputField, int inputFieldIndex ) throws KettleException {
    Object value;

    switch ( data.outputRowMeta.getValueMeta( inputFieldIndex ).getType() ) {
      case ValueMetaInterface.TYPE_INTEGER:
        value = data.outputRowMeta.getInteger( r, inputFieldIndex );
        break;
      case ValueMetaInterface.TYPE_BIGNUMBER:
      case ValueMetaInterface.TYPE_NUMBER:
        value = data.outputRowMeta.getNumber( r, inputFieldIndex );
        break;
      case ValueMetaInterface.TYPE_BOOLEAN:
        value = data.outputRowMeta.getBoolean( r, inputFieldIndex );
        break;
      default:
        value = data.outputRowMeta.getString( r, inputFieldIndex );
        break;
    }

    return value;
  }

  private boolean closeFile() {
    boolean retval = false;
    if ( data.parquetWriter != null ) {
      try {
        data.parquetWriter.close();
      } catch ( IOException e ) {
        // TODO log this
      }
    }
    return retval;
  }*/

  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (ParquetOutputMetaBase) smi;
    data = (ParquetOutputData) sdi;
    if ( super.init( smi, sdi ) ) {
      return true;
    }
    return false;
  }

}
