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

package org.pentaho.big.data.kettle.plugins.formats.parquet.input;

import org.apache.commons.vfs2.FileObject;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.file.BaseFileInputStep;
import org.pentaho.di.trans.steps.file.IBaseFileInputReader;

public class ParquetInput extends BaseFileInputStep<ParquetInputMetaBase, ParquetInputData> {

  public ParquetInput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );

    // dirty hack for inialize file: filesystem
 /*   try {
      Field f = FileSystem.class.getDeclaredField( "SERVICE_FILE_SYSTEMS" );
      f.setAccessible( true );
      Map<String, Object> m = (Map) f.get( FileSystem.class );
      m.put( "file", LocalFileSystem.class );
      System.out.println( "-------------------------- local filesystem initialized" );
    } catch ( Exception ex ) {
      ex.printStackTrace();
    }*/
  }

  @Override
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    throw new KettleException( "Requires Shim API changes" );
    /* ParquetInputData data = (ParquetInputData) sdi;

    try {
      if ( data.splits == null ) {
        initFiles( data );
      }

      if ( data.currentSplit >= data.splits.size() ) {
        setOutputDone();
        return false;
      }

      if ( data.reader == null ) {
        openReader( data );
      }
      if ( data.reader.nextKeyValue() ) {
        Group obj = (Group) data.reader.getCurrentValue();

        Object[] row = new Object[data.outputRowMeta.getFieldNames().length];
        for ( int i = 0; i < data.outputRowMeta.getFieldNames().length; i++ ) {
          String fn = data.outputRowMeta.getFieldNames()[i];
          row[i] = obj.getValueToString( obj.getType().getFieldIndex( fn ), 0 );
        }

        putRow( data.outputRowMeta, row );
        return true;
      }
      data.reader.close();
      data.reader = null;
      data.currentSplit++;
      return true;
    } catch ( Exception ex ) {
      throw new KettleException( ex );
    }*/
  }

  void initFiles( ParquetInputData data ) throws Exception {
    throw new KettleException( "Requires Shim API changes" );
    /* data.input = new ParquetInputFormat<>( PentahoParquetReadSupport.class );
    Job job = new Job();
    job.getConfiguration().set( FileInputFormat.INPUT_DIR, meta.dir );
    job.getConfiguration().set( ParquetInputFormat.SPLIT_MAXSIZE, "10000000" );
    job.getConfiguration().set( ParquetInputFormat.TASK_SIDE_METADATA, "false" );

    data.splits = data.input.getSplits( job );
    data.currentSplit = 0;

    data.outputRowMeta = new RowMeta();
    for ( Type t : PentahoParquetReadSupport.schema.getFields() ) {
      ValueMetaInterface v = ValueMetaFactory.createValueMeta( t.getName(), ValueMetaInterface.TYPE_STRING );
      data.outputRowMeta.addValueMeta( v );
    }*/
  }

  void openReader( ParquetInputData data ) throws Exception {
    throw new KettleException( "Requires Shim API changes" );
    /*Configuration c = new Configuration();
    TaskAttemptID id = new TaskAttemptID();
    TaskAttemptContextImpl task = new TaskAttemptContextImpl( c, id );

    InputSplit sp = data.splits.get( data.currentSplit );
    data.reader = data.input.createRecordReader( sp, task );
    data.reader.initialize( sp, task );*/
  }

  @Override
  protected IBaseFileInputReader createReader( ParquetInputMetaBase meta, ParquetInputData data, FileObject file )
    throws Exception {
    return null;
  }

  @Override
  protected boolean init() {
    return true;
  }
}
