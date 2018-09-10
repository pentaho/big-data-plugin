/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.formats.impl.avro.input;

import org.apache.commons.vfs2.FileObject;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceLocator;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.bigdata.api.format.FormatService;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.file.BaseFileInputStep;
import org.pentaho.di.trans.steps.file.IBaseFileInputReader;
import org.pentaho.hadoop.shim.api.format.IAvroInputField;
import org.pentaho.hadoop.shim.api.format.IPentahoAvroInputFormat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class AvroInput extends BaseFileInputStep<AvroInputMeta, AvroInputData> {
  public static long SPLIT_SIZE = 128 * 1024 * 1024;
  private Object[] inputToStepRow;

  private final NamedClusterServiceLocator namedClusterServiceLocator;

  public AvroInput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans, NamedClusterServiceLocator namedClusterServiceLocator ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
    this.namedClusterServiceLocator = namedClusterServiceLocator;
  }

  @Override
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    meta = (AvroInputMeta) smi;
    data = (AvroInputData) sdi;

    if ( !meta.isComplex() ) {
      return processRowSimple( smi, sdi );
    } else {
      return processRowComplex( smi, sdi );
    }
  }

  private boolean processRowComplex( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    do {
      try {
        if ( data.input == null || data.reader == null || data.rowIterator == null ) {
          FormatService formatService;
          try {
            formatService = namedClusterServiceLocator.getService( meta.getNamedCluster(), FormatService.class );
            inputToStepRow = getRow();
            if ( inputToStepRow == null && meta.isUseFieldAsInputStream() ) {
              fileFinishedHousekeeping();
              break; //We have processed all rows streaming in
            }
          } catch ( ClusterInitializationException e ) {
            throw new KettleException( "can't get service format shim ", e );
          }
          if ( meta.getFilename() == null && !meta.isUseFieldAsInputStream() ) {
            throw new KettleException( "No input files defined" );
          }

          // setup the output row meta
          RowMetaInterface outRowMeta = null;
          outRowMeta = getInputRowMeta();
          if ( outRowMeta != null ) {
            outRowMeta = outRowMeta.clone();
          } else {
            outRowMeta = new RowMeta();
          }

          data.input = formatService.createInputFormat( IPentahoAvroInputFormat.class );

          data.input.setVariableSpace( this );
          data.input.setIncomingFields( new Object[]{} ); //********* fix this
          data.input.setOutputRowMeta( null ); //***** fix this

          data.input
            .setInputFile( meta.getParentStepMeta().getParentTransMeta().environmentSubstitute( meta.getFilename() ) );
          data.input.setInputSchemaFile(
            meta.getParentStepMeta().getParentTransMeta().environmentSubstitute( meta.getSchemaFilename() ) );
          data.input.setInputFields( Arrays.asList( meta.getInputFields() ) );
          if ( meta.isUseFieldAsInputStream() ) {
            data.input.setInputStreamFieldName( meta.getInputStreamFieldName() );
            int fieldIndex = getInputRowMeta().indexOfValue( data.input.getInputStreamFieldName() );
            if ( fieldIndex == -1 ) {
              throw new KettleException(
                "Field '" + data.input.getInputStreamFieldName() + "' was not found in step's input fields" );
            }

            data.input
              .setInputStream( new ByteArrayInputStream( getInputRowMeta().getBinary( inputToStepRow, fieldIndex ) ) );
          }
          data.reader = data.input.createRecordReader( null );
          data.rowIterator = data.reader.iterator();
        }
        if ( data.rowIterator.hasNext() ) {
          RowMetaAndData row = data.rowIterator.next();

          //Merge the incoming avro data row with the fields that entered the AvroInputStep, if any
          if ( getInputRowMeta() != null && inputToStepRow != null ) {
            row.mergeRowMetaAndData( new RowMetaAndData( getInputRowMeta(), inputToStepRow ), null );
          }

          putRow( row.getRowMeta(), row.getData() );
          return true;
        }
        //Finished with Avro file
        fileFinishedHousekeeping();

      } catch ( KettleException ex ) {
        throw ex;
      } catch ( Exception ex ) {
        throw new KettleException( ex );
      }

    } while ( meta.isUseFieldAsInputStream() );

    setOutputDone();
    return false;
  }

  private boolean processRowSimple( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    do {
      try {
        if ( data.input == null || data.reader == null || data.rowIterator == null ) {
          FormatService formatService;
          try {
            formatService = namedClusterServiceLocator.getService( meta.getNamedCluster(), FormatService.class );
            inputToStepRow = getRow();
            if ( inputToStepRow == null && meta.isUseFieldAsInputStream() ) {
              fileFinishedHousekeeping();
              break; //We have processed all rows streaming in
            }
          } catch ( ClusterInitializationException e ) {
            throw new KettleException( "can't get service format shim ", e );
          }
          if ( meta.getFilename() == null && !meta.isUseFieldAsInputStream() ) {
            throw new KettleException( "No input files defined" );
          }

          data.input = formatService.createInputFormat( IPentahoAvroInputFormat.class );
          data.input
            .setInputFile( meta.getParentStepMeta().getParentTransMeta().environmentSubstitute( meta.getFilename() ) );
          data.input.setInputSchemaFile(
            meta.getParentStepMeta().getParentTransMeta().environmentSubstitute( meta.getSchemaFilename() ) );
          data.input.setInputFields( Arrays.asList( meta.getInputFields() ) );
          if ( meta.isUseFieldAsInputStream() ) {
            data.input.setInputStreamFieldName( meta.getInputStreamFieldName() );
            int fieldIndex = getInputRowMeta().indexOfValue( data.input.getInputStreamFieldName() );
            if ( fieldIndex == -1 ) {
              throw new KettleException(
                "Field '" + data.input.getInputStreamFieldName() + "' was not found in step's input fields" );
            }

            data.input
              .setInputStream( new ByteArrayInputStream( getInputRowMeta().getBinary( inputToStepRow, fieldIndex ) ) );
          }
          data.reader = data.input.createRecordReader( null );
          data.rowIterator = data.reader.iterator();
        }
        if ( data.rowIterator.hasNext() ) {
          RowMetaAndData row = data.rowIterator.next();

          //Merge the incoming avro data row with the fields that entered the AvroInputStep, if any
          if ( getInputRowMeta() != null && inputToStepRow != null ) {
            row.mergeRowMetaAndData( new RowMetaAndData( getInputRowMeta(), inputToStepRow ), null );
          }

          putRow( row.getRowMeta(), row.getData() );
          return true;
        }
        //Finished with Avro file
        fileFinishedHousekeeping();

      } catch ( KettleException ex ) {
        throw ex;
      } catch ( Exception ex ) {
        throw new KettleException( ex );
      }

    } while ( meta.isUseFieldAsInputStream() );

    setOutputDone();
    return false;
  }

  private void fileFinishedHousekeeping() {
    try {
      if ( data.reader != null ) {
        data.reader.close();
      }
    } catch ( IOException e ) {
      //Don't care if we can't close
    }
    data.reader = null;
    data.input = null;
  }

  @Override
  protected boolean init() {
    return true;
  }

  @Override
  protected IBaseFileInputReader createReader( AvroInputMeta meta, AvroInputData data, FileObject file )
    throws Exception {
    return null;
  }

  public static List<? extends IAvroInputField> getDefaultFields( NamedClusterServiceLocator namedClusterServiceLocator,
                                                                   NamedCluster namedCluster, String schemaPath, String dataPath ) throws Exception {
    FormatService formatService = namedClusterServiceLocator.getService( namedCluster, FormatService.class );
    IPentahoAvroInputFormat in = formatService.createInputFormat( IPentahoAvroInputFormat.class );
    in.setInputSchemaFile( schemaPath );
    in.setInputFile( dataPath );
    return in.getFields();
  }

}
