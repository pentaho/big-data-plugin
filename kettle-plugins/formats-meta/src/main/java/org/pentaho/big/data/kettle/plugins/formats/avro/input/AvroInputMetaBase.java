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

package org.pentaho.big.data.kettle.plugins.formats.avro.input;

import org.apache.commons.vfs2.FileObject;
import org.pentaho.big.data.kettle.plugins.formats.FormatInputFile;
import org.pentaho.big.data.kettle.plugins.formats.avro.AvroTypeConverter;
import org.pentaho.big.data.kettle.plugins.formats.avro.output.AvroOutputMetaBase;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.vfs.AliasedFileObject;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.steps.file.BaseFileInputAdditionalField;
import org.pentaho.di.trans.steps.file.BaseFileInputMeta;
import org.pentaho.di.workarounds.ResolvableResource;
import org.pentaho.hadoop.shim.api.format.AvroSpec;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.List;

/**
 * Avro input meta step without Hadoop-dependent classes. Required for read meta in the spark native code.
 *
 * @author Alexander Buloichik
 */
@SuppressWarnings( "deprecation" )
public abstract class AvroInputMetaBase
  extends BaseFileInputMeta<BaseFileInputAdditionalField, FormatInputFile, AvroInputField>
  implements ResolvableResource {

  public static final Class<?> PKG = AvroOutputMetaBase.class;

  public static enum LocationDescriptor {
    FILE_NAME, FIELD_NAME, FIELD_CONTAINING_FILE_NAME;
  }

  private String dataLocation;
  private int dataLocationType = LocationDescriptor.FILE_NAME.ordinal();
  private boolean isDataBinaryEncoded = true;
  private String schemaLocation;
  private int schemaLocationType = LocationDescriptor.FILE_NAME.ordinal();
  private boolean isCacheSchemas;
  private boolean allowNullForMissingFields;
  private int format;

  public String getDataLocation() {
    return dataLocation;
  }

  public void setDataLocation( String dataLocation, LocationDescriptor locationDescriptor ) {
    this.dataLocation = dataLocation;
    this.dataLocationType = locationDescriptor.ordinal();
  }

  public LocationDescriptor getDataLocationType() {
    return LocationDescriptor.values()[ dataLocationType ];
  }

  public boolean isDataBinaryEncoded() {
    return isDataBinaryEncoded;
  }

  public void setDataBinaryEncoded( boolean idDataBinaryEncoded ) {
    this.isDataBinaryEncoded = idDataBinaryEncoded;
  }

  public void setFormat( int formatIndex ) {
    this.format = formatIndex;
  }

  public int getFormat() {
    return this.format;
  }

  public String getSchemaLocation() {
    return schemaLocation;
  }

  public void setSchemaLocation( String schemaLocation, LocationDescriptor locationDescriptor ) {
    this.schemaLocation = schemaLocation;
    this.schemaLocationType = locationDescriptor.ordinal();
  }

  public LocationDescriptor getSchemaLocationType() {
    return LocationDescriptor.values()[ schemaLocationType ];
  }

  public boolean isCacheSchemas() {
    return isCacheSchemas;
  }

  public void setCacheSchemas( boolean cacheSchemas ) {
    isCacheSchemas = cacheSchemas;
  }

  public boolean isAllowNullForMissingFields() {
    return allowNullForMissingFields;
  }

  public void setAllowNullForMissingFields( boolean allowNullForMissingFields ) {
    this.allowNullForMissingFields = allowNullForMissingFields;
  }

  public AvroInputMetaBase() {
    additionalOutputFields = new BaseFileInputAdditionalField();
    inputFiles = new FormatInputFile();
    inputFields = new AvroInputField[ 0 ];
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
  }

  public AvroInputField[] getInputFields() {
    return this.inputFields;
  }

  public void setInputFields( AvroInputField[] inputFields ) {
    this.inputFields = inputFields;
  }

  public void setInputFields( List<AvroInputField> inputFields ) {
    this.inputFields = new AvroInputField[ inputFields.size() ];
    this.inputFields = inputFields.toArray( this.inputFields );
  }

  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    readData( stepnode, metaStore );
  }

  private void readData( Node stepnode, IMetaStore metastore ) throws KettleXMLException {
    try {
      String passFileds = XMLHandler.getTagValue( stepnode, "passing_through_fields" ) == null ? "false"
        : XMLHandler.getTagValue( stepnode, "passing_through_fields" );
      inputFiles.passingThruFields = ValueMetaBase.convertStringToBoolean( passFileds );
      dataLocation = XMLHandler.getTagValue( stepnode, "dataLocation" );
      format =
          XMLHandler.getTagValue( stepnode, "format" ) == null ? LocationDescriptor.FILE_NAME.ordinal()
            : Integer.parseInt( XMLHandler.getTagValue( stepnode, "format" ) );
      dataLocationType =
        XMLHandler.getTagValue( stepnode, "dataLocationType" ) == null ? LocationDescriptor.FILE_NAME.ordinal()
          : Integer.parseInt( XMLHandler.getTagValue( stepnode, "dataLocationType" ) );
      isDataBinaryEncoded = ValueMetaBase.convertStringToBoolean(
        XMLHandler.getTagValue( stepnode, "isDataBinaryEncoded" ) == null ? "false"
          : XMLHandler.getTagValue( stepnode, "isDataBinaryEncoded" ) );
      schemaLocation = XMLHandler.getTagValue( stepnode, "schemaLocation" );
      schemaLocationType =
        XMLHandler.getTagValue( stepnode, "schemaLocationType" ) == null ? LocationDescriptor.FILE_NAME.ordinal()
          : Integer.parseInt( XMLHandler.getTagValue( stepnode, "schemaLocationType" ) );
      isCacheSchemas = ValueMetaBase.convertStringToBoolean(
        XMLHandler.getTagValue( stepnode, "isCacheSchemas" ) == null ? "false"
          : XMLHandler.getTagValue( stepnode, "isCacheSchemas" ) );
      allowNullForMissingFields = ValueMetaBase.convertStringToBoolean(
        XMLHandler.getTagValue( stepnode, "allowNullForMissingFields" ) == null ? "false"
          : XMLHandler.getTagValue( stepnode, "allowNullForMissingFields" ) );

      Node fields = XMLHandler.getSubNode( stepnode, "fields" );
      int nrfields = XMLHandler.countNodes( fields, "field" );
      this.inputFields = new AvroInputField[ nrfields ];
      for ( int i = 0; i < nrfields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );
        AvroInputField inputField = new AvroInputField();
        inputField.setFormatFieldName( XMLHandler.getTagValue( fnode, "path" ) );
        inputField.setPentahoFieldName( XMLHandler.getTagValue( fnode, "name" ) );
        inputField.setPentahoType( XMLHandler.getTagValue( fnode, "type" ) );
        String avroType = XMLHandler.getTagValue( fnode, "avro_type" );
        if ( avroType != null && !avroType.equalsIgnoreCase( "null" ) ) {
          inputField.setAvroType( avroType );
        } else {
          inputField.setAvroType( AvroTypeConverter.convertToAvroType( inputField.getPentahoType() ) );
        }
        String stringFormat = XMLHandler.getTagValue( fnode, "format" );
        inputField.setStringFormat( stringFormat == null ? "" : stringFormat );
        this.inputFields[ i ] = inputField;
      }
    } catch ( Exception e ) {
      throw new KettleXMLException( "Unable to load step info from XML", e );
    }
  }

  @Override
  public String getXML() {
    StringBuffer retval = new StringBuffer( 800 );
    final String INDENT = "    ";

    //we need the equals by size arrays for inputFiles.fileName[i], inputFiles.fileMask[i], inputFiles
    // .fileRequired[i], inputFiles.includeSubFolders[i]
    //to prevent the ArrayIndexOutOfBoundsException
    //This line was introduced to prevent future bug if we will suppport the several input files for avro like we do
    // for orc and parquet
    inputFiles.normalizeAllocation( inputFiles.fileName.length );

    retval.append( INDENT ).append( XMLHandler.addTagValue( "passing_through_fields", inputFiles.passingThruFields ) );
    retval.append( INDENT ).append( XMLHandler.addTagValue( "dataLocation", getDataLocation() ) );
    retval.append( INDENT ).append( XMLHandler.addTagValue( "format", getFormat() ) );
    retval.append( INDENT ).append( XMLHandler.addTagValue( "dataLocationType", dataLocationType ) );
    retval.append( INDENT ).append( XMLHandler.addTagValue( "isDataBinaryEncoded", isDataBinaryEncoded() ) );
    retval.append( INDENT ).append( XMLHandler.addTagValue( "schemaLocation", getSchemaLocation() ) );
    retval.append( INDENT ).append( XMLHandler.addTagValue( "schemaLocationType", schemaLocationType ) );
    retval.append( INDENT ).append( XMLHandler.addTagValue( "isCacheSchemas", isCacheSchemas() ) );
    retval.append( INDENT )
      .append( XMLHandler.addTagValue( "allowNullForMissingFields", isAllowNullForMissingFields() ) );

    retval.append( "    <fields>" ).append( Const.CR );
    for ( int i = 0; i < inputFields.length; i++ ) {
      AvroInputField field = inputFields[ i ];

      if ( field.getPentahoFieldName() != null && field.getPentahoFieldName().length() != 0 ) {
        retval.append( "      <field>" ).append( Const.CR );
        retval.append( "        " ).append( XMLHandler.addTagValue( "path", field.getAvroFieldName() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "name", field.getPentahoFieldName() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "type", field.getTypeDesc() ) );
        AvroSpec.DataType avroDataType = field.getAvroType();
        if ( avroDataType != null && !avroDataType.equals( AvroSpec.DataType.NULL ) ) {
          retval.append( "        " ).append( XMLHandler.addTagValue( "avro_type", avroDataType.getName() ) );
        } else {
          retval.append( "        " ).append(
            XMLHandler.addTagValue( "avro_type", AvroTypeConverter.convertToAvroType( field.getTypeDesc() ) ) );
        }
        if ( field.getStringFormat() != null ) {
          retval.append( "        " ).append( XMLHandler.addTagValue( "format", field.getStringFormat() ) );
        }
        retval.append( "      </field>" ).append( Const.CR );
      }
    }
    retval.append( "    </fields>" ).append( Const.CR );

    return retval.toString();
  }


  @Override
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws KettleException {
    try {
      inputFiles.passingThruFields = rep.getStepAttributeBoolean( id_step, "passing_through_fields" );
      dataLocation = rep.getStepAttributeString( id_step, "dataLocation" );
      format = (int) rep.getStepAttributeInteger( id_step, "format" );
      dataLocationType = (int) rep.getStepAttributeInteger( id_step, "dataLocationType" );
      isDataBinaryEncoded = rep.getStepAttributeBoolean( id_step, "isDataBinaryEncoded" );
      schemaLocation = rep.getStepAttributeString( id_step, "schemaLocation" );
      schemaLocationType = (int) rep.getStepAttributeInteger( id_step, "schemaLocationType" );
      isCacheSchemas = rep.getStepAttributeBoolean( id_step, "isCacheSchemas" );
      allowNullForMissingFields = rep.getStepAttributeBoolean( id_step, "allowNullForMissingFields" );


      // using the "type" column to get the number of field rows because "type" is guaranteed not to be null.
      int nrfields = rep.countNrStepAttributes( id_step, "type" );
      this.inputFields = new AvroInputField[ nrfields ];
      for ( int i = 0; i < nrfields; i++ ) {
        AvroInputField inputField = new AvroInputField();
        inputField.setFormatFieldName( rep.getStepAttributeString( id_step, i, "path" ) );
        inputField.setPentahoFieldName( rep.getStepAttributeString( id_step, i, "name" ) );
        inputField.setPentahoType( rep.getStepAttributeString( id_step, i, "type" ) );
        String avroType = rep.getStepAttributeString( id_step, i, "avro_type" );
        if ( avroType != null && !avroType.equalsIgnoreCase( "null" ) ) {
          inputField.setAvroType( avroType );
        } else {
          inputField.setAvroType( AvroTypeConverter.convertToAvroType( inputField.getPentahoType() ) );
        }
        String stringFormat = rep.getStepAttributeString( id_step, i, "format" );
        inputField.setStringFormat( stringFormat == null ? "" : stringFormat );
        this.inputFields[ i ] = inputField;
      }
    } catch ( Exception e ) {
      throw new KettleException( "Unexpected error reading step information from the repository", e );
    }
  }

  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
    throws KettleException {
    try {
      rep.saveStepAttribute( id_transformation, id_step, "passing_through_fields", inputFiles.passingThruFields );
      rep.saveStepAttribute( id_transformation, id_step, "dataLocation", getDataLocation() );
      rep.saveStepAttribute( id_transformation, id_step, "format", getFormat() );
      rep.saveStepAttribute( id_transformation, id_step, "dataLocationType", dataLocationType );
      rep.saveStepAttribute( id_transformation, id_step, "isDataBinaryEncoded", isDataBinaryEncoded() );
      rep.saveStepAttribute( id_transformation, id_step, "schemaLocation", getSchemaLocation() );
      rep.saveStepAttribute( id_transformation, id_step, "schemaLocationType", schemaLocationType );
      rep.saveStepAttribute( id_transformation, id_step, "isCacheSchemas", isCacheSchemas() );
      rep.saveStepAttribute( id_transformation, id_step, "allowNullForMissingFields", isAllowNullForMissingFields() );

      for ( int i = 0; i < inputFields.length; i++ ) {
        AvroInputField field = inputFields[ i ];

        rep.saveStepAttribute( id_transformation, id_step, i, "path", field.getAvroFieldName() );
        rep.saveStepAttribute( id_transformation, id_step, i, "name", field.getPentahoFieldName() );
        rep.saveStepAttribute( id_transformation, id_step, i, "type", field.getTypeDesc() );
        AvroSpec.DataType avroDataType = field.getAvroType();
        if ( avroDataType != null && !avroDataType.equals( AvroSpec.DataType.NULL ) ) {
          rep.saveStepAttribute( id_transformation, id_step, i, "avro_type", avroDataType.getName() );
        } else {
          rep.saveStepAttribute( id_transformation, id_step, i, "avro_type",
            AvroTypeConverter.convertToAvroType( field.getTypeDesc() ) );
        }
        if ( field.getStringFormat() != null ) {
          rep.saveStepAttribute( id_transformation, id_step, i, "format", field.getStringFormat() );
        }
      }
      super.saveRep( rep, metaStore, id_transformation, id_step );
    } catch ( Exception e ) {
      throw new KettleException( "Unable to save step information to the repository for id_step=" + id_step, e );
    }
  }

  @Override
  public void resolve() {
    if ( dataLocation != null && !dataLocation.isEmpty() ) {
      try {
        String realFileName = getParentStepMeta().getParentTransMeta().environmentSubstitute( dataLocation );
        FileObject fileObject = KettleVFS.getFileObject( realFileName );
        if ( AliasedFileObject.isAliasedFile( fileObject ) ) {
          dataLocation = ( (AliasedFileObject) fileObject ).getOriginalURIString();
        }
      } catch ( KettleFileException e ) {
        throw new RuntimeException( e );
      }
    }

    if ( schemaLocation != null && !schemaLocation.isEmpty() ) {
      try {
        String realSchemaFilename = getParentStepMeta().getParentTransMeta().environmentSubstitute( schemaLocation );
        FileObject fileObject = KettleVFS.getFileObject( realSchemaFilename );
        if ( AliasedFileObject.isAliasedFile( fileObject ) ) {
          schemaLocation = ( (AliasedFileObject) fileObject ).getOriginalURIString();
        }
      } catch ( KettleFileException e ) {
        throw new RuntimeException( e );
      }
    }
  }
}
