/*! ******************************************************************************
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

package org.pentaho.big.data.kettle.plugins.formats.impl.parquet.input;

import org.apache.commons.vfs2.FileObject;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.big.data.kettle.plugins.formats.parquet.input.ParquetInputField;
import org.pentaho.big.data.kettle.plugins.formats.parquet.input.ParquetInputMetaBase;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.vfs.AliasedFileObject;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.file.BaseFileInputStep;
import org.pentaho.di.trans.steps.file.IBaseFileInputReader;
import org.pentaho.hadoop.shim.api.format.FormatService;
import org.pentaho.hadoop.shim.api.format.IParquetInputField;
import org.pentaho.hadoop.shim.api.format.IPentahoInputFormat.IPentahoInputSplit;
import org.pentaho.hadoop.shim.api.format.IPentahoParquetInputFormat;

import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ParquetInput extends BaseFileInputStep<ParquetInputMeta, ParquetInputData> {
  public static long SPLIT_SIZE = 128 * 1024 * 1024;

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
        logDebug( "Close split {0}", data.currentSplit );
        data.currentSplit++;
        return true;
      }
    } catch ( NoSuchFileException ex ) {
      throw new KettleException( "No input file" );
    } catch ( KettleException ex ) {
      throw ex;
    } catch ( Exception ex ) {
      throw new KettleException( ex );
    }
  }

  void initSplits() throws Exception {
    FormatService formatService = namedClusterServiceLocator.getService( meta.getNamedCluster(), FormatService.class );
    if ( meta.inputFiles == null || meta.inputFiles.fileName == null || meta.inputFiles.fileName.length == 0 ) {
      throw new KettleException( "No input files defined" );
    }

    String inputFileName = environmentSubstitute( meta.inputFiles.fileName[ 0 ] );
    FileObject inputFileObject = KettleVFS.getFileObject( inputFileName, getTransMeta() );
    if ( AliasedFileObject.isAliasedFile( inputFileObject ) ) {
      inputFileName = ( (AliasedFileObject) inputFileObject ).getOriginalURIString();
    }

    data.input = formatService.createInputFormat( IPentahoParquetInputFormat.class, meta.getNamedCluster() );

    // Pentaho 8.0 transformations will have the formatType set to 0. Get the fields from the schema and set the
    // formatType to the formatType retrieved from the schema.
    List<? extends IParquetInputField> actualFileFields =
      ParquetInput.retrieveSchema( meta.namedClusterServiceLocator, meta.getNamedCluster(), inputFileName );

    if ( meta.isIgnoreEmptyFolder() && ( actualFileFields.size() == 0 ) ) {
      data.splits = new ArrayList<>(  );
      logBasic( "No Parquet input files found." );
    } else {
      Map<String, IParquetInputField> fieldNamesToTypes = actualFileFields.stream().collect(
        Collectors.toMap( IParquetInputField::getFormatFieldName, Function.identity() ) );
      for ( ParquetInputField f : meta.getInputFields() ) {
        if ( fieldNamesToTypes.containsKey( f.getFormatFieldName() ) ) {
          if ( f.getFormatType() == 0 ) {
            f.setFormatType( fieldNamesToTypes.get( f.getFormatFieldName() ).getFormatType() );
          }
          f.setPrecision( fieldNamesToTypes.get( f.getFormatFieldName() ).getPrecision() );
          f.setScale( fieldNamesToTypes.get( f.getFormatFieldName() ).getScale() );
        }
      }

      data.input.setSchema( createSchemaFromMeta( meta ) );
      data.input.setInputFile( inputFileName );
      data.input.setSplitSize( SPLIT_SIZE );

      data.splits = data.input.getSplits();
      logDebug( "Input split count: {0}", data.splits.size() );
    }
    data.currentSplit = 0;
  }


  void openReader( ParquetInputData data ) throws Exception {
    logDebug( "Open split {0}", data.currentSplit );
    IPentahoInputSplit sp = data.splits.get( data.currentSplit );
    data.reader = data.input.createRecordReader( sp );
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

  public static List<? extends IParquetInputField> retrieveSchema( NamedClusterServiceLocator namedClusterServiceLocator,
                                                                   NamedCluster namedCluster, String path ) throws Exception {
    FormatService formatService = namedClusterServiceLocator.getService( namedCluster, FormatService.class );
    IPentahoParquetInputFormat in = formatService.createInputFormat( IPentahoParquetInputFormat.class, namedCluster );
    FileObject inputFileObject = KettleVFS.getFileObject( path );
    if ( AliasedFileObject.isAliasedFile( inputFileObject ) ) {
      path = ( (AliasedFileObject) inputFileObject ).getOriginalURIString();
    }
    return in.readSchema( path );
  }

  public static List<IParquetInputField> createSchemaFromMeta( ParquetInputMetaBase meta ) {
    List<IParquetInputField> fields = new ArrayList<>(  );
    for ( ParquetInputField f : meta.getInputFields() ) {
      fields.add( f );
    }
    return fields;
  }
}
