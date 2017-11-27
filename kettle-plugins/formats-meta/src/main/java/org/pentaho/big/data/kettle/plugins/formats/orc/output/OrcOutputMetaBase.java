/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.formats.orc.output;

import org.apache.commons.vfs2.FileObject;
import org.pentaho.big.data.kettle.plugins.formats.orc.OrcFormatInputOutputField;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionDeep;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.vfs.AliasedFileObject;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.workarounds.ResolvableResource;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Orc output meta step without Hadoop-dependent classes. Required for read meta in the spark native code.
 *
 * @author Alexander Buloichik@epam.com>
 */
public abstract class OrcOutputMetaBase extends BaseStepMeta implements StepMetaInterface, ResolvableResource {

  private static final Class<?> PKG = OrcOutputMetaBase.class;

  @Injection( name = "FILENAME" )
  private String filename;

  @InjectionDeep
  private List<OrcFormatInputOutputField> outputFields = new ArrayList<OrcFormatInputOutputField>();

  @Injection( name = "OPTIONS_COMPRESSION" )
  protected String compressionType = "";

  @Injection( name = "OPTIONS_STRIPE_SIZE" )
  protected String stripeSize = "67";

  @Injection( name = "OPTIONS_COMPRESS_SIZE" )
  protected String compressSize = "256";

  @Injection( name = "OPTIONS_INLINE_INDEXES" )
  protected boolean inlineIndexes = false;

  @Injection( name = "OPTIONS_ROWS_BETWEEN_ENTRIES" )
  protected String rowsBetweenEntries = "10000";

  @Injection( name = "OPTIONS_DATE_IN_FILE_NAME" )
  protected boolean dateInFileName = false;

  @Injection( name = "OPTIONS_TIME_IN_FILE_NAME" )
  protected boolean timeInFileName = false;

  @Injection( name = "OPTIONS_DATE_FORMAT" )
  protected String dateTimeFormat = "";

  @Override
  public void setDefault() {
    // TODO Auto-generated method stub
  }

  public String getFilename() {

    return filename;
  }

  public void setFilename( String filename ) {

    this.filename = filename;
  }

  public List<OrcFormatInputOutputField> getOutputFields() {

    return outputFields;
  }

  public void setOutputFields( List<OrcFormatInputOutputField> outputFields ) {

    this.outputFields = outputFields;
  }

  public String getStripeSize() {
    return stripeSize;
  }

  public void setStripeSize( String stripeSize ) {
    this.stripeSize = stripeSize;
  }

  public String getCompressSize() {
    return compressSize;
  }

  public void setCompressSize( String compressSize ) {
    this.compressSize = compressSize;
  }

  public boolean isInlineIndexes() {
    return inlineIndexes;
  }

  public void setInlineIndexes( boolean inlineIndexes ) {
    this.inlineIndexes = inlineIndexes;
  }

  public String getRowsBetweenEntries() {
    return rowsBetweenEntries;
  }

  public void setRowsBetweenEntries( String rowsBetweenEntries ) {
    this.rowsBetweenEntries = rowsBetweenEntries;
  }

  public boolean isDateInFileName() {
    return dateInFileName;
  }

  public void setDateInFileName( boolean dateInFileName ) {
    this.dateInFileName = dateInFileName;
  }

  public boolean isTimeInFileName() {
    return timeInFileName;
  }

  public void setTimeInFileName( boolean timeInFileName ) {
    this.timeInFileName = timeInFileName;
  }

  public String getDateTimeFormat() {
    return dateTimeFormat;
  }

  public void setDateTimeFormat( String dateTimeFormat ) {
    this.dateTimeFormat = dateTimeFormat;
  }

  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    readData( stepnode, metaStore );
  }

  private void readData( Node stepnode, IMetaStore metastore ) throws KettleXMLException {
    try {
      Node fields = XMLHandler.getSubNode( stepnode, "fields" );
      int nrfields = XMLHandler.countNodes( fields, "field" );
      List<OrcFormatInputOutputField> orcOutputFields = new ArrayList<>();
      for ( int i = 0; i < nrfields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );
        OrcFormatInputOutputField outputField = new OrcFormatInputOutputField();
        outputField.setPath( XMLHandler.getTagValue( fnode, "path" ) );
        outputField.setName( XMLHandler.getTagValue( fnode, "name" ) );
        outputField.setType( XMLHandler.getTagValue( fnode, "type" ) );
        outputField.setNullString( XMLHandler.getTagValue( fnode, "nullable" ) );
        outputField.setIfNullValue( XMLHandler.getTagValue( fnode, "default" ) );
        orcOutputFields.add( outputField );
      }
      this.outputFields = orcOutputFields;

      filename = XMLHandler.getTagValue( stepnode, FieldNames.FILE_NAME );
      compressionType = XMLHandler.getTagValue( stepnode, FieldNames.COMPRESSION );
      stripeSize = XMLHandler.getTagValue( stepnode, FieldNames.STRIPE_SIZE );
      compressSize = XMLHandler.getTagValue( stepnode, FieldNames.COMPRESS_SIZE );
      rowsBetweenEntries = XMLHandler.getTagValue( stepnode, FieldNames.ROWS_BETWEEN_ENTRIES );
      dateTimeFormat = XMLHandler.getTagValue( stepnode, FieldNames.DATE_FORMAT );
      inlineIndexes = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, FieldNames.INLINE_INDEXES ) );
      dateInFileName = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, FieldNames.DATE_IN_FILE_NAME ) );
      timeInFileName = "Y".equalsIgnoreCase( ( XMLHandler.getTagValue( stepnode, FieldNames.TIME_IN_FILE_NAME ) ) );

    } catch ( Exception e ) {
      throw new KettleXMLException( "Unable to load step info from XML", e );
    }
  }


  @Override
  public String getXML() {
    StringBuffer retval = new StringBuffer( 800 );
    final String INDENT = "    ";

    retval.append( INDENT ).append( XMLHandler.addTagValue( FieldNames.FILE_NAME, filename ) );
    retval.append( INDENT ).append( XMLHandler.addTagValue( FieldNames.COMPRESSION, compressionType ) );
    retval.append( INDENT ).append( XMLHandler.addTagValue( FieldNames.STRIPE_SIZE, stripeSize ) );
    retval.append( INDENT ).append( XMLHandler.addTagValue( FieldNames.COMPRESS_SIZE, compressSize ) );
    retval.append( INDENT ).append( XMLHandler.addTagValue( FieldNames.ROWS_BETWEEN_ENTRIES, rowsBetweenEntries ) );
    retval.append( INDENT ).append( XMLHandler.addTagValue( FieldNames.DATE_FORMAT, dateTimeFormat ) );
    retval.append( INDENT ).append( XMLHandler.addTagValue( FieldNames.INLINE_INDEXES, inlineIndexes ) );
    retval.append( INDENT ).append( XMLHandler.addTagValue( FieldNames.DATE_IN_FILE_NAME, dateInFileName ) );
    retval.append( INDENT ).append( XMLHandler.addTagValue( FieldNames.TIME_IN_FILE_NAME, timeInFileName ) );

    retval.append( "    <fields>" ).append( Const.CR );
    for ( int i = 0; i < outputFields.size(); i++ ) {
      OrcFormatInputOutputField field = outputFields.get( i );

      if ( field.getName() != null && field.getName().length() != 0 ) {
        retval.append( "      <field>" ).append( Const.CR );
        retval.append( "        " ).append( XMLHandler.addTagValue( "path", field.getPath() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "name", field.getName() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "type", field.getTypeDesc() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "nullable", field.getNullString() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "default", field.getIfNullValue() ) );
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
      filename = rep.getStepAttributeString( id_step, FieldNames.FILE_NAME );
      compressionType = rep.getStepAttributeString( id_step, FieldNames.COMPRESSION );
      stripeSize = rep.getStepAttributeString( id_step, FieldNames.STRIPE_SIZE );
      compressSize = rep.getStepAttributeString( id_step, FieldNames.COMPRESS_SIZE );
      rowsBetweenEntries = rep.getStepAttributeString( id_step, FieldNames.ROWS_BETWEEN_ENTRIES );
      dateTimeFormat = rep.getStepAttributeString( id_step, FieldNames.DATE_FORMAT );
      inlineIndexes = rep.getStepAttributeBoolean( id_step, FieldNames.INLINE_INDEXES );
      dateInFileName = rep.getStepAttributeBoolean( id_step, FieldNames.DATE_IN_FILE_NAME );
      timeInFileName = rep.getStepAttributeBoolean( id_step, FieldNames.TIME_IN_FILE_NAME );

      // using the "type" column to get the number of field rows because "type" is guaranteed not to be null.
      int nrfields = rep.countNrStepAttributes( id_step, "type" );

      List<OrcFormatInputOutputField> orcOutputFields = new ArrayList<>();
      for ( int i = 0; i < nrfields; i++ ) {
        OrcFormatInputOutputField outputField = new OrcFormatInputOutputField();

        outputField.setPath( rep.getStepAttributeString( id_step, i, "path" ) );
        outputField.setName( rep.getStepAttributeString( id_step, i, "name" ) );
        outputField.setType( rep.getStepAttributeString( id_step, i, "type" ) );
        outputField.setNullString( rep.getStepAttributeString( id_step, i, "nullable" ) );
        outputField.setIfNullValue( rep.getStepAttributeString( id_step, i, "default" ) );

        orcOutputFields.add( outputField );
      }
      this.outputFields = orcOutputFields;
    } catch ( Exception e ) {
      throw new KettleException( "Unexpected error reading step information from the repository", e );
    }
  }

  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
    throws KettleException {
    try {
      super.saveRep( rep, metaStore, id_transformation, id_step );
      rep.saveStepAttribute( id_transformation, id_step, FieldNames.FILE_NAME, filename );
      rep.saveStepAttribute( id_transformation, id_step, FieldNames.COMPRESSION, compressionType );
      rep.saveStepAttribute( id_transformation, id_step, FieldNames.STRIPE_SIZE, stripeSize );
      rep.saveStepAttribute( id_transformation, id_step, FieldNames.COMPRESS_SIZE, compressSize );
      rep.saveStepAttribute( id_transformation, id_step, FieldNames.ROWS_BETWEEN_ENTRIES, rowsBetweenEntries );
      rep.saveStepAttribute( id_transformation, id_step, FieldNames.DATE_FORMAT, dateTimeFormat );
      rep.saveStepAttribute( id_transformation, id_step, FieldNames.INLINE_INDEXES, inlineIndexes );
      rep.saveStepAttribute( id_transformation, id_step, FieldNames.DATE_IN_FILE_NAME, dateInFileName );
      rep.saveStepAttribute( id_transformation, id_step, FieldNames.TIME_IN_FILE_NAME, timeInFileName );

      for ( int i = 0; i < outputFields.size(); i++ ) {
        OrcFormatInputOutputField field = outputFields.get( i );

        rep.saveStepAttribute( id_transformation, id_step, i, "path", field.getPath() );
        rep.saveStepAttribute( id_transformation, id_step, i, "name", field.getName() );
        rep.saveStepAttribute( id_transformation, id_step, i, "type", field.getTypeDesc() );
        rep.saveStepAttribute( id_transformation, id_step, i, "nullable", field.getNullString() );
        rep.saveStepAttribute( id_transformation, id_step, i, "default", field.getIfNullValue() );
      }

    } catch ( Exception e ) {
      throw new KettleException( "Unable to save step information to the repository for id_step=" + id_step, e );
    }
  }

  @Override
  public void resolve() {
    if ( filename != null && !filename.isEmpty() ) {
      try {
        String realFileName = getParentStepMeta().getParentTransMeta().environmentSubstitute( filename );
        FileObject fileObject = KettleVFS.getFileObject( realFileName );
        if ( AliasedFileObject.isAliasedFile( fileObject ) ) {
          filename = ( (AliasedFileObject) fileObject ).getOriginalURIString();
        }
      } catch ( KettleFileException e ) {
        throw new RuntimeException( e );
      }
    }
  }

  public String getCompressionType() {
    return StringUtil.isVariable( compressionType ) ? compressionType : getCompressionType( null ).toString();
  }

  public void setCompressionType( String value ) {
    compressionType = StringUtil.isVariable( value ) ? value : parseFromToString( value, CompressionType.values(), null ).name();
  }

  public CompressionType getCompressionType( VariableSpace vspace ) {
    return parseReplace( compressionType, vspace, str -> CompressionType.valueOf( str ), CompressionType.NONE );
  }

  public String[] getCompressionTypes() {
    return getStrings( CompressionType.values() );
  }

  public static enum CompressionType {
    NONE( getMsg( "OrcOutput.CompressionType.NONE" ) ),
    ZLIB( getMsg( "OrcOutput.CompressionType.ZLIB" ) ),
    LZO( getMsg( "OrcOutput.CompressionType.LZO" ) ),
    SNAPPY( getMsg( "OrcOutput.CompressionType.SNAPPY" ) );

    private final String name;

    private CompressionType( String name ) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  protected static <T> String[] getStrings( T[] objects ) {
    String[] names = new String[objects.length];
    int i = 0;
    for ( T obj : objects ) {
      names[i++] = obj.toString();
    }
    return names;
  }

  protected static <T> T parseFromToString( String str, T[] values, T defaultValue ) {
    if ( !Utils.isEmpty( str ) ) {
      for ( T type : values ) {
        if ( str.equalsIgnoreCase( type.toString() ) ) {
          return type;
        }
      }
    }
    return defaultValue;
  }

  private <T> T parseReplace( String value, VariableSpace vspace, Function<String, T> parser, T defaultValue ) {
    String replaced = vspace != null ? vspace.environmentSubstitute( value ) : value;
    if ( !Utils.isEmpty( replaced ) ) {
      try {
        return parser.apply( replaced );
      } catch ( Exception e ) {
        // ignored
      }
    }
    return defaultValue;
  }

  private static String getMsg( String key ) {
    return BaseMessages.getString( PKG, key );
  }

  protected static class FieldNames {
    public static final String FILE_NAME = "filename";
    public static final String COMPRESSION = "compression";
    public static final String COMPRESS_SIZE = "compressSize";
    public static final String INLINE_INDEXES = "inlineIndexes";
    public static final String ROWS_BETWEEN_ENTRIES = "rowsBetweenEntries";
    public static final String DATE_IN_FILE_NAME = "dateInFileName";
    public static final String TIME_IN_FILE_NAME = "timeInFileName";
    public static final String DATE_FORMAT = "dateTimeFormat";
    public static final String STRIPE_SIZE = "stripeSize";
  }
}
