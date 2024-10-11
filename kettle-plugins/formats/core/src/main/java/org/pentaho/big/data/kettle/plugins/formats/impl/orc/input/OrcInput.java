/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2019-2020 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.formats.impl.orc.input;

import org.apache.commons.vfs2.FileObject;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.hadoop.shim.api.cluster.ClusterInitializationException;
import org.pentaho.big.data.kettle.plugins.formats.orc.input.OrcInputMetaBase;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
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
import org.pentaho.hadoop.shim.api.format.IOrcInputField;
import org.pentaho.hadoop.shim.api.format.IPentahoOrcInputFormat;

import java.util.Arrays;
import java.util.List;

public class OrcInput extends BaseFileInputStep<OrcInputMeta, OrcInputData> {
  public static final long SPLIT_SIZE = 128L * 1024L * 1024L;

  public OrcInput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                   Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    meta = (OrcInputMeta) smi;
    data = (OrcInputData) sdi;
    try {
      if ( data.input == null || data.reader == null || data.rowIterator == null ) {
        FormatService formatService = getFormatService();
        if ( meta.inputFiles == null || meta.getFilename() == null || meta.getFilename().length() == 0 ) {
          throw new KettleException( "No input files defined" );
        }
        data.input = formatService.createInputFormat( IPentahoOrcInputFormat.class, getNamedCluster() );

        String inputFileName = getKettleVFSFileName(
          meta.getParentStepMeta().getParentTransMeta().environmentSubstitute( meta.getFilename() ) );

        data.input.setInputFile( inputFileName );
        data.input.setSchema( createSchemaFromMeta( meta ) );
        data.reader = data.input.createRecordReader( null );
        data.rowIterator = data.reader.iterator();
      }
      if ( data.rowIterator.hasNext() ) {
        RowMetaAndData row = data.rowIterator.next();
        putRow( row.getRowMeta(), row.getData() );
        return true;
      } else {
        data.reader.close();
        data.reader = null;
        data.input = null;
        setOutputDone();
        return false;
      }
    } catch ( KettleException ex ) {
      throw ex;
    } catch ( Exception ex ) {
      throw new KettleException( ex );
    }
  }

  private NamedCluster getNamedCluster() {
    return meta.getNamedClusterResolver().resolveNamedCluster( environmentSubstitute( meta.getFilename() ) );
  }

  private FormatService getFormatService() throws KettleException {
    FormatService formatService;
    try {
      formatService = meta.getNamedClusterResolver().getNamedClusterServiceLocator()
        .getService( getNamedCluster(), FormatService.class );
    } catch ( ClusterInitializationException e ) {
      throw new KettleException( "can't get service format shim ", e );
    }
    return formatService;
  }


  @Override
  protected boolean init() {
    return true;
  }

  @Override
  protected IBaseFileInputReader createReader( OrcInputMeta meta, OrcInputData data, FileObject file )
    throws Exception {
    return null;
  }

  public static List<IOrcInputField> retrieveSchema( NamedClusterServiceLocator namedClusterServiceLocator,
                                                     NamedCluster namedCluster, String dataPath ) throws Exception {
    FormatService formatService = namedClusterServiceLocator.getService( namedCluster, FormatService.class );
    IPentahoOrcInputFormat in = formatService.createInputFormat( IPentahoOrcInputFormat.class, namedCluster );

    in.setInputFile( getKettleVFSFileName( dataPath ) );
    return in.readSchema();
  }

  public static List<IOrcInputField> createSchemaFromMeta( OrcInputMetaBase meta ) {
    return Arrays.asList( meta.getInputFields() );
  }

  public static String getKettleVFSFileName( String path ) throws KettleFileException {
    String inputFileName = path;
    FileObject inputFileObject = KettleVFS.getFileObject( path );
    if ( AliasedFileObject.isAliasedFile( inputFileObject ) ) {
      inputFileName = ( (AliasedFileObject) inputFileObject ).getOriginalURIString();
    }

    return inputFileName;
  }
}
