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
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceLocator;
import org.pentaho.big.data.kettle.plugins.formats.parquet.output.ParquetOutput;
import org.pentaho.bigdata.api.format.FormatService;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.file.BaseFileInputStep;
import org.pentaho.di.trans.steps.file.IBaseFileInputReader;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.api.format.PentahoInputSplit;

public class ParquetInput extends BaseFileInputStep<ParquetInputMeta, ParquetInputData> {

  private final NamedClusterServiceLocator namedClusterServiceLocator;

  public ParquetInput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans, NamedClusterServiceLocator namedClusterServiceLocator ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
    this.namedClusterServiceLocator = namedClusterServiceLocator;
  }

  @Override
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    meta = (ParquetInputMeta) smi;
    data = (ParquetInputData) sdi;

    try {
      if ( data.splits == null ) {
        initSplits();
      }

      if ( data.currentSplit >= data.splits.size() ) {
        setOutputDone();
        return false;
      }

      if ( data.reader == null ) {
        openReader( data );
      }

      if ( data.rowIterator.hasNext() ) {
        RowMetaAndData row = data.rowIterator.next();
        putRow( row.getRowMeta(), row.getData() );
        return true;
      } else {
        data.reader.close();
        data.reader = null;
        data.currentSplit++;
        return true;
      }
    } catch ( Exception ex ) {
      throw new KettleException( ex );
    }
  }

  void initSplits() throws Exception {
    FormatService formatService = namedClusterServiceLocator.getService( meta.getNamedCluster(), FormatService.class );
    Configuration configuration = formatService.createConfiguration();
    // configuration.set( FileInputFormat.INPUT_DIR, meta.dir );
    // configuration.set( ParquetInputFormat.SPLIT_MAXSIZE, "10000000" );
    // configuration.set( ParquetInputFormat.TASK_SIDE_METADATA, "false" );
    data.input = formatService.getInputFormat( configuration, ParquetOutput.makeScheme() );

    data.splits = data.input.getSplits();
    data.currentSplit = 0;

    // HadoopConfiguration hc=null;
    // hc.getFormatShim();
    // throw new KettleException( "Requires Shim API changes" );

    // data.outputRowMeta = new RowMeta();
    // for ( Type t : PentahoParquetReadSupport.schema.getFields() ) {
    // ValueMetaInterface v = ValueMetaFactory.createValueMeta( t.getName(), ValueMetaInterface.TYPE_STRING );
    // data.outputRowMeta.addValueMeta( v );
    // }
  }

  void openReader( ParquetInputData data ) throws Exception {
    PentahoInputSplit sp = data.splits.get( data.currentSplit );
    data.reader = data.input.getRecordReader( sp );
    data.rowIterator = data.reader.iterator();
  }

  @Override
  protected boolean init() {
    return true;
  }

  @Override
  protected IBaseFileInputReader createReader( ParquetInputMeta meta, ParquetInputData data, FileObject file )
    throws Exception {
    return null;
  }
}
