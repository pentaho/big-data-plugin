/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018-2020 by Hitachi Vantara : http://www.pentaho.com
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

import org.pentaho.big.data.kettle.plugins.formats.parquet.ParquetTypeConverter;
import org.apache.commons.vfs2.FileObject;
import org.pentaho.big.data.kettle.plugins.formats.FormatInputFile;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.vfs.AliasedFileObject;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.file.BaseFileInputAdditionalField;
import org.pentaho.di.trans.steps.file.BaseFileInputMeta;
import org.pentaho.di.workarounds.ResolvableResource;
import org.pentaho.hadoop.shim.api.format.IParquetInputField;
import org.pentaho.hadoop.shim.api.format.ParquetSpec;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.List;

/**
 * Parquet input meta step without Hadoop-dependent classes. Required for read meta in the spark native code.
 *
 * @author <alexander_buloichik@epam.com>
 */
@SuppressWarnings( "deprecation" )
public abstract class ParquetInputMetaBase extends
  BaseFileInputMeta<BaseFileInputAdditionalField, FormatInputFile, ParquetInputField> implements ResolvableResource {

  /** If receiving input rows, should we pass through existing fields? */
  @Injection( name = "IGNORE_EMPTY_FOLDER" )
  boolean ignoreEmptyFolder = false;

  public ParquetInputMetaBase() {
    additionalOutputFields = new BaseFileInputAdditionalField();
    inputFiles = new FormatInputFile();
    inputFields = new ParquetInputField[ 0 ];
  }

  public boolean isIgnoreEmptyFolder() {
    return ignoreEmptyFolder;
  }

  public void setIgnoreEmptyFolder( boolean ignoreEmptyFolder ) {
    this.ignoreEmptyFolder = ignoreEmptyFolder;
  }

  public String getFilename() {
    if ( inputFiles != null && inputFiles.fileName != null && inputFiles.fileName.length > 0 ) {
      return inputFiles.fileName[0];
    } else {
      return null;
    }
  }

  public String[] getFileNames() {
    if ( inputFiles != null && inputFiles.fileName != null && inputFiles.fileName.length > 0 ) {
      return inputFiles.fileName;
    } else {
      return null;
    }
  }

  public void setFilename( String filename ) {
    inputFiles.fileName[0] = filename;
  }

  public void setFilenames( String[] filenames ) {
    inputFiles.fileName = filenames;
  }

  public ParquetInputField[] getInputFields() {
    return inputFields;
  }

  public void setInputFields( ParquetInputField[] inputFields ) {
    this.inputFields = inputFields;
  }

  public void setInputFields( List<ParquetInputField> inputFields ) {
    this.inputFields = new ParquetInputField[ inputFields.size() ];
    this.inputFields = inputFields.toArray( this.inputFields );
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder( 1500 );

    retval.append( "    " ).append( XMLHandler.addTagValue( "passing_through_fields", inputFiles.passingThruFields ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "ignore_empty_folder", ignoreEmptyFolder ) );
    retval.append( "    <file>" ).append( Const.CR );
    //we need the equals by size arrays for inputFiles.fileName[i], inputFiles.fileMask[i], inputFiles.fileRequired[i], inputFiles.includeSubFolders[i]
    //to prevent the ArrayIndexOutOfBoundsException
    inputFiles.normalizeAllocation( inputFiles.fileName.length );
    for ( int i = 0; i < inputFiles.fileName.length; i++ ) {
      retval.append( "      " ).append( XMLHandler.addTagValue( "environment", inputFiles.environment[ i ] ) );

      if ( parentStepMeta != null && parentStepMeta.getParentTransMeta() != null ) {
        parentStepMeta.getParentTransMeta().getNamedClusterEmbedManager().registerUrl( inputFiles.fileName[ i ] );
      }

      retval.append( "      " ).append( XMLHandler.addTagValue( "name", inputFiles.fileName[ i ] ) );
      retval.append( "      " ).append( XMLHandler.addTagValue( "filemask", inputFiles.fileMask[ i ] ) );
      retval.append( "      " ).append( XMLHandler.addTagValue( "exclude_filemask", inputFiles.excludeFileMask[ i ] ) );
      retval.append( "      " ).append( XMLHandler.addTagValue( "file_required", inputFiles.fileRequired[ i ] ) );
      retval.append( "      " ).append( XMLHandler.addTagValue( "include_subfolders",
        inputFiles.includeSubFolders[ i ] ) );
    }
    retval.append( "    </file>" ).append( Const.CR );

    retval.append( "    <fields>" ).append( Const.CR );
    for ( int i = 0; i < inputFields.length; i++ ) {
      ParquetInputField field = inputFields[ i ];
      retval.append( "      <field>" ).append( Const.CR );
      retval.append( "        " ).append( XMLHandler.addTagValue( "path", field.getFormatFieldName() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "name", field.getPentahoFieldName() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "type", field.getTypeDesc() ) );
      ParquetSpec.DataType parquetType = field.getParquetType();
      if ( parquetType != null  && !parquetType.equals( ParquetSpec.DataType.NULL ) ) {
        retval.append( "        " )
          .append( XMLHandler.addTagValue( "parquet_type", parquetType.getName() ) );
      } else {
        retval.append( "        " )
            .append( XMLHandler.addTagValue( "parquet_type", ParquetTypeConverter.convertToParquetType( field.getTypeDesc() ) ) );
      }
      if ( field.getStringFormat() != null ) {
        retval.append( "        " ).append( XMLHandler.addTagValue( "format", field.getStringFormat() ) );
      }
      retval.append( "      </field>" ).append( Const.CR );
    }
    retval.append( "    </fields>" ).append( Const.CR );

    return retval.toString();
  }

  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
    throws KettleException {
    try {
      rep.saveStepAttribute( id_transformation, id_step, "ignore_empty_folder", ignoreEmptyFolder );
      rep.saveStepAttribute( id_transformation, id_step, "passing_through_fields", inputFiles.passingThruFields );
      if ( !( inputFiles.fileName.length == 1 && inputFiles.fileName[0].equalsIgnoreCase( "" ) ) ) {
        for ( int i = 0; i < inputFiles.fileName.length; i++ ) {
          rep.saveStepAttribute( id_transformation, id_step, i, "environment", inputFiles.environment[i] );
          rep.saveStepAttribute( id_transformation, id_step, i, "file_name", inputFiles.fileName[i] );
          rep.saveStepAttribute( id_transformation, id_step, i, "file_mask", inputFiles.fileMask[i] );
          rep.saveStepAttribute( id_transformation, id_step, i, "exclude_file_mask", inputFiles.excludeFileMask[i] );
          rep.saveStepAttribute( id_transformation, id_step, i, "file_required", inputFiles.fileRequired[i] );
          rep.saveStepAttribute( id_transformation, id_step, i, "include_subfolders", inputFiles.includeSubFolders[i] );
        }
      }

      for ( int i = 0; i < inputFields.length; i++ ) {
        ParquetInputField field = inputFields[ i ];

        rep.saveStepAttribute( id_transformation, id_step, i, "path", field.getFormatFieldName() );
        rep.saveStepAttribute( id_transformation, id_step, i, "field_name", field.getPentahoFieldName() );
        rep.saveStepAttribute( id_transformation, id_step, i, "field_type", field.getTypeDesc() );
        ParquetSpec.DataType parquetType = field.getParquetType();
        if ( parquetType != null  && !parquetType.equals( ParquetSpec.DataType.NULL ) ) {
          rep.saveStepAttribute( id_transformation, id_step, i, "parquet_type", parquetType.getName() );
        } else {
          rep.saveStepAttribute( id_transformation, id_step, i, "parquet_type", ParquetTypeConverter.convertToParquetType( field.getTypeDesc() ) );
        }
        if ( field.getStringFormat() != null ) {
          rep.saveStepAttribute( id_transformation, id_step, i, "format", field.getStringFormat() );
        }
      }
    } catch ( Exception e ) {
      throw new KettleException( "Unable to save step information to the repository for id_step=" + id_step, e );
    }
  }

  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    Node filenode = XMLHandler.getSubNode( stepnode, "file" );
    Node fields = XMLHandler.getSubNode( stepnode, "fields" );
    int nrfiles = XMLHandler.countNodes( filenode, "name" );
    int nrfields = XMLHandler.countNodes( fields, "field" );

    String passThroughFields = XMLHandler.getTagValue( stepnode, "passing_through_fields" ) == null ? "false"
      : XMLHandler.getTagValue( stepnode, "passing_through_fields" );
    String skipIfNoFile = XMLHandler.getTagValue( stepnode, "ignore_empty_folder" ) == null ? "false"
      : XMLHandler.getTagValue( stepnode, "ignore_empty_folder" );
    allocateFiles( nrfiles );
    inputFiles.passingThruFields = ValueMetaBase.convertStringToBoolean( passThroughFields );
    ignoreEmptyFolder = ValueMetaBase.convertStringToBoolean( skipIfNoFile );
    for ( int i = 0; i < nrfiles; i++ ) {
      Node envnode = XMLHandler.getSubNodeByNr( filenode, "environment", i );
      Node filenamenode = XMLHandler.getSubNodeByNr( filenode, "name", i );
      Node filemasknode = XMLHandler.getSubNodeByNr( filenode, "filemask", i );
      Node excludefilemasknode = XMLHandler.getSubNodeByNr( filenode, "exclude_filemask", i );
      Node fileRequirednode = XMLHandler.getSubNodeByNr( filenode, "file_required", i );
      Node includeSubFoldersnode = XMLHandler.getSubNodeByNr( filenode, "include_subfolders", i );
      inputFiles.environment[ i ] = XMLHandler.getNodeValue( envnode );
      inputFiles.fileName[ i ] = XMLHandler.getNodeValue( filenamenode );
      inputFiles.fileMask[ i ] = XMLHandler.getNodeValue( filemasknode );
      inputFiles.excludeFileMask[ i ] = XMLHandler.getNodeValue( excludefilemasknode );
      inputFiles.fileRequired[ i ] = XMLHandler.getNodeValue( fileRequirednode );
      inputFiles.includeSubFolders[ i ] = XMLHandler.getNodeValue( includeSubFoldersnode );
    }

    this.inputFields = new ParquetInputField[ nrfields ];
    for ( int i = 0; i < nrfields; i++ ) {
      Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );

      ParquetInputField field = new ParquetInputField();
      field.setFormatFieldName( XMLHandler.getTagValue( fnode, "path" ) );
      field.setPentahoFieldName( XMLHandler.getTagValue( fnode, "name" ) );
      field.setPentahoType( ValueMetaFactory.getIdForValueMeta( XMLHandler.getTagValue( fnode, "type" ) ) );
      String parquetType = XMLHandler.getTagValue( fnode, "parquet_type" );
      if ( parquetType != null && !parquetType.equalsIgnoreCase( "null" ) ) {
        field.setParquetType( parquetType );
      } else {
        field.setParquetType( ParquetTypeConverter.convertToParquetType( field.getPentahoType() ) );
      }

      String stringFormat = XMLHandler.getTagValue( fnode, "format" );
      field.setStringFormat( stringFormat == null ? "" : stringFormat );
      this.inputFields[ i ] = field;
    }
  }

  @Override
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws KettleException {
    try {
      int nrfiles = rep.countNrStepAttributes( id_step, "file_name" );

      allocateFiles( nrfiles );

      inputFiles.passingThruFields = rep.getStepAttributeBoolean( id_step, "passing_through_fields" );
      ignoreEmptyFolder = rep.getStepAttributeBoolean( id_step, "ignore_empty_folder" );
      for ( int i = 0; i < nrfiles; i++ ) {
        inputFiles.environment[ i ] = rep.getStepAttributeString( id_step, i, "environment" );
        inputFiles.fileName[ i ] = rep.getStepAttributeString( id_step, i, "file_name" );
        inputFiles.fileMask[ i ] = rep.getStepAttributeString( id_step, i, "file_mask" );
        inputFiles.excludeFileMask[ i ] = rep.getStepAttributeString( id_step, i, "exclude_file_mask" );
        inputFiles.fileRequired[ i ] = rep.getStepAttributeString( id_step, i, "file_required" );
        if ( !YES.equalsIgnoreCase( inputFiles.fileRequired[ i ] ) ) {
          inputFiles.fileRequired[ i ] = NO;
        }
        inputFiles.includeSubFolders[ i ] = rep.getStepAttributeString( id_step, i, "include_subfolders" );
        if ( !YES.equalsIgnoreCase( inputFiles.includeSubFolders[ i ] ) ) {
          inputFiles.includeSubFolders[ i ] = NO;
        }
      }

      int nrfields = rep.countNrStepAttributes( id_step, "field_name" );
      this.inputFields = new ParquetInputField[ nrfields ];
      for ( int i = 0; i < nrfields; i++ ) {
        ParquetInputField field = new ParquetInputField();
        field.setFormatFieldName( rep.getStepAttributeString( id_step, i, "path" ) );
        field.setPentahoFieldName( rep.getStepAttributeString( id_step, i, "field_name" ) );
        field.setPentahoType( rep.getStepAttributeString( id_step, i, "field_type" ) );
        String parquetType = rep.getStepAttributeString( id_step, i, "parquet_type" );
        if ( parquetType != null && !parquetType.equalsIgnoreCase( "null" ) ) {
          field.setParquetType( parquetType );
        } else {
          field.setParquetType( ParquetTypeConverter.convertToParquetType( field.getPentahoType() ) );
        }
        String stringFormat = rep.getStepAttributeString( id_step, i, "format" );
        field.setStringFormat( stringFormat == null ? "" : stringFormat );
        this.inputFields[ i ] = field;
      }

    } catch ( Exception e ) {
      throw new KettleException( "Unexpected error reading step information from the repository", e );
    }
  }

  public void allocateFiles( int nrFiles ) {
    inputFiles.environment = new String[ nrFiles ];
    inputFiles.fileName = new String[ nrFiles ];
    inputFiles.fileMask = new String[ nrFiles ];
    inputFiles.excludeFileMask = new String[ nrFiles ];
    inputFiles.fileRequired = new String[ nrFiles ];
    inputFiles.includeSubFolders = new String[ nrFiles ];
  }
  /**
   * TODO: remove from base
   */
  @Override
  public String getEncoding() {
    return null;
  }

  @Override
  public void setDefault() {
    allocateFiles( 0 );
    inputFields = new ParquetInputField[ 0 ];
  }

  @Override
  public void resolve() {
    if ( inputFiles != null && inputFiles.fileName != null ) {
      for ( int i = 0; i < inputFiles.fileName.length; i++ ) {
        try {
          String realFileName =
            getParentStepMeta().getParentTransMeta().environmentSubstitute( inputFiles.fileName[ i ] );
          FileObject fileObject = KettleVFS.getFileObject( realFileName );
          if ( AliasedFileObject.isAliasedFile( fileObject ) ) {
            inputFiles.fileName[ i ] = ( (AliasedFileObject) fileObject ).getAELSafeURIString();
          }
        } catch ( KettleFileException e ) {
          throw new RuntimeException( e );
        }
      }
    }
  }

  @Override
  public void getFields( RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, Repository repository, IMetaStore metaStore ) throws
    KettleStepException {
    try {
      for ( int i = 0; i < inputFields.length; i++ ) {
        IParquetInputField field = inputFields[ i ];
        String value = space.environmentSubstitute( field.getPentahoFieldName() );
        ValueMetaInterface v = ValueMetaFactory.createValueMeta( value,
          field.getPentahoType() );
        v.setOrigin( origin );
        rowMeta.addValueMeta( v );
      }
    } catch ( KettlePluginException e ) {
      throw new KettleStepException( "Unable to create value type", e );
    }
  }
}
