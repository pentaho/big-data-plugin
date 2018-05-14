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

package org.pentaho.big.data.kettle.plugins.formats.impl.parquet.output;

import org.apache.commons.vfs2.FileObject;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.hadoop.shim.api.cluster.ClusterInitializationException;
import org.pentaho.big.data.kettle.plugins.formats.parquet.output.ParquetOutputMetaBase;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.vfs.AliasedFileObject;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.hadoop.shim.api.format.FormatService;
import org.pentaho.hadoop.shim.api.format.IPentahoParquetOutputFormat;

import java.io.IOException;

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
      if ( data.output == null ) {
        init( getInputRowMeta() );
      }

      Object[] currentRow = getRow();
      if ( currentRow != null ) {
        RowMetaAndData row = new RowMetaAndData( getInputRowMeta(), currentRow );
        data.writer.write( row );
        putRow( row.getRowMeta(), row.getData() ); // in case we want it to go further or DET...
        return true;
      } else {
        // no more input to be expected...
        closeWriter();
        setOutputDone();
        return false;
      }
    } catch ( KettleException ex ) {
      try {
        closeWriter();
      } catch ( Exception ex2 ) {
        // Do nothing
      }
      throw ex;
    } catch ( Exception ex ) {
      try {
        closeWriter();
      } catch ( Exception ex2 ) {
        // Do nothing
      }
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

    data.output = formatService.createOutputFormat( IPentahoParquetOutputFormat.class, meta.getNamedCluster() );

    String outputFileName = environmentSubstitute( meta.constructOutputFilename() );
    FileObject outputFileObject = KettleVFS.getFileObject( outputFileName, getTransMeta() );
    if ( AliasedFileObject.isAliasedFile( outputFileObject ) ) {
      outputFileName = ( (AliasedFileObject) outputFileObject ).getOriginalURIString();
    }

    data.output.setOutputFile( outputFileName, meta.overrideOutput );
    data.output.setFields( meta.getOutputFields() );

    IPentahoParquetOutputFormat.COMPRESSION compression;
    try {
      compression =
        IPentahoParquetOutputFormat.COMPRESSION.valueOf( meta.getCompressionType( variables ).name().toUpperCase() );
    } catch ( Exception ex ) {
      compression = IPentahoParquetOutputFormat.COMPRESSION.UNCOMPRESSED;
    }
    data.output.setCompression( compression );
    data.output
      .setVersion(
        ParquetOutputMetaBase.ParquetVersion.PARQUET_1.equals( meta.getParquetVersion( variables ) )
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
    if ( !super.init( smi, sdi ) ) {
      return false;
    }

    //Set Embedded NamedCluter MetatStore Provider Key so that it can be passed to VFS
    if ( getTransMeta().getNamedClusterEmbedManager() != null ) {
      getTransMeta().getNamedClusterEmbedManager()
        .passEmbeddedMetastoreKey( getTransMeta(), getTransMeta().getEmbeddedMetastoreProviderKey() );
    }
    return true;
  }
}
