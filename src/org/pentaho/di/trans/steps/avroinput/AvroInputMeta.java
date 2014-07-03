/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.trans.steps.avroinput;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.util.Utf8;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.w3c.dom.Node;

/**
 * Class providing an input step for reading data from an Avro serialized file or an incoming field. Handles both
 * container files (where the schema is serialized into the file) and schemaless files. In the case of the later (and
 * incoming field), the user must supply a schema in order to read objects from the file/field. In the case of the
 * former, a schema can be optionally supplied.
 * 
 * Currently supports Avro records, arrays, maps, unions and primitive types. Union types are limited to two base types,
 * where one of the base types must be "null". Paths use the "dot" notation and "$" indicates the root of the object.
 * Arrays and maps are accessed via "[]" and differ only in that array elements are accessed via zero-based integer
 * indexes and map values are accessed by string keys.
 * 
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision$
 */
@Step( id = "AvroInput", image = "Avro.png", name = "AvroInput.Name", description = "AvroInput.Description",
    categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.BigData",
    i18nPackageName = "org.pentaho.di.trans.steps.avroinput" )
public class AvroInputMeta extends BaseStepMeta implements StepMetaInterface {

  protected static Class<?> PKG = AvroInputMeta.class;

  /**
   * Inner class encapsulating a field to provide lookup values. Field values (non-avro) from the incoming row stream
   * can be substituted into the avro paths used to extract avro fields from an incoming binary/json avro field.
   * 
   * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
   * 
   */
  public static class LookupField {

    /** The name of the field in the incoming rows to use for a lookup */
    public String m_fieldName = "";

    /** The name of the variable to hold this field's values */
    public String m_variableName = "";

    /** A default value to use if the incoming field is null */
    public String m_defaultValue = "";

    protected String m_cleansedVariableName;
    protected String m_resolvedFieldName;
    protected String m_resolvedDefaultValue;

    /** False if this field does not exist in the incoming row stream */
    protected boolean m_isValid = true;

    /** Index of this field in the incoming row stream */
    protected int m_inputIndex = -1;

    protected ValueMetaInterface m_fieldVM;

    public boolean init( RowMetaInterface inRowMeta, VariableSpace space ) {

      if ( inRowMeta == null ) {
        m_isValid = false;
        return false;
      }

      m_resolvedFieldName = ( space != null ) ? space.environmentSubstitute( m_fieldName ) : m_fieldName;

      m_inputIndex = inRowMeta.indexOfValue( m_resolvedFieldName );
      if ( m_inputIndex < 0 ) {
        m_isValid = false;

        return m_isValid;
      }

      m_fieldVM = inRowMeta.getValueMeta( m_inputIndex );

      if ( !Const.isEmpty( m_variableName ) ) {
        m_cleansedVariableName = m_variableName.replaceAll( "\\.", "_" );
      } else {
        m_isValid = false;
        return m_isValid;
      }

      m_resolvedDefaultValue = space.environmentSubstitute( m_defaultValue );

      return m_isValid;
    }

    public void setVariable( VariableSpace space, Object[] inRow ) {
      if ( !m_isValid ) {
        return;
      }

      String valueToSet = "";
      try {
        if ( m_fieldVM.isNull( inRow[m_inputIndex] ) ) {
          if ( !Const.isEmpty( m_resolvedDefaultValue ) ) {
            valueToSet = m_resolvedDefaultValue;
          } else {
            valueToSet = "null";
          }
        } else {
          valueToSet = m_fieldVM.getString( inRow[m_inputIndex] );
        }
      } catch ( KettleValueException e ) {
        valueToSet = "null";
      }

      space.setVariable( m_cleansedVariableName, valueToSet );
    }
  }

  /**
   * Inner class for encapsulating name, path and type information for a field to be extracted from an Avro file.
   * 
   * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
   * @version $Revision$
   */
  public static class AvroField {

    /** the name that the field will take in the outputted kettle stream */
    public String m_fieldName = "";

    /** the path to the field in the avro file */
    public String m_fieldPath = "";

    /** the kettle type for this field */
    public String m_kettleType = "";

    /** any indexed values (i.e. enum types in avro) */
    public List<String> m_indexedVals;

    protected int m_outputIndex; // the index that this field is in the output
                                 // row structure
    private ValueMeta m_tempValueMeta;
    private List<String> m_pathParts;
    private List<String> m_tempParts;

    /**
     * Initialize this field by parsing the path etc.
     * 
     * @param outputIndex
     *          the index in the output row structure for this field
     * @throws KettleException
     *           if a problem occurs
     */
    public void init( int outputIndex ) throws KettleException {
      if ( Const.isEmpty( m_fieldPath ) ) {
        throw new KettleException( BaseMessages.getString( PKG, "AvroInput.Error.NoPathSet" ) );
      }
      if ( m_pathParts != null ) {
        return;
      }

      String fieldPath = AvroInputData.cleansePath( m_fieldPath );

      String[] temp = fieldPath.split( "\\." );
      m_pathParts = new ArrayList<String>();
      for ( String part : temp ) {
        m_pathParts.add( part );
      }

      if ( m_pathParts.get( 0 ).equals( "$" ) ) {
        m_pathParts.remove( 0 ); // root record indicator
      } else if ( m_pathParts.get( 0 ).startsWith( "$[" ) ) {

        // strip leading $ off of array
        String r = m_pathParts.get( 0 ).substring( 1, m_pathParts.get( 0 ).length() );
        m_pathParts.set( 0, r );
      }

      m_tempParts = new ArrayList<String>();

      m_tempValueMeta = new ValueMeta();
      m_tempValueMeta.setType( ValueMeta.getType( m_kettleType ) );
      m_outputIndex = outputIndex;
    }

    /**
     * Reset this field. Should be called prior to processing a new field value from the avro file
     * 
     * @param space
     *          environment variables (values that environment variables resolve to cannot contain "."s)
     */
    public void reset( VariableSpace space ) {
      // first clear because there may be stuff left over from processing
      // the previous avro object (especially if a path exited early due to
      // non-existent map key or array index out of bounds)
      m_tempParts.clear();

      for ( String part : m_pathParts ) {
        m_tempParts.add( space.environmentSubstitute( part ) );
      }
    }

    /**
     * Perform Kettle type conversions for the Avro leaf field value.
     * 
     * @param fieldValue
     *          the leaf value from the Avro structure
     * @return an Object of the appropriate Kettle type
     * @throws KettleException
     *           if a problem occurs
     */
    protected Object getKettleValue( Object fieldValue ) throws KettleException {

      switch ( m_tempValueMeta.getType() ) {
        case ValueMetaInterface.TYPE_BIGNUMBER:
          return m_tempValueMeta.getBigNumber( fieldValue );
        case ValueMetaInterface.TYPE_BINARY:
          return m_tempValueMeta.getBinary( fieldValue );
        case ValueMetaInterface.TYPE_BOOLEAN:
          return m_tempValueMeta.getBoolean( fieldValue );
        case ValueMetaInterface.TYPE_DATE:
          return m_tempValueMeta.getDate( fieldValue );
        case ValueMetaInterface.TYPE_INTEGER:
          return m_tempValueMeta.getInteger( fieldValue );
        case ValueMetaInterface.TYPE_NUMBER:
          return m_tempValueMeta.getNumber( fieldValue );
        case ValueMetaInterface.TYPE_STRING:
          return m_tempValueMeta.getString( fieldValue );
        default:
          return null;
      }
    }

    /**
     * Get the value of the Avro leaf primitive with respect to the Kettle type for this path.
     * 
     * @param fieldValue
     *          the Avro leaf value
     * @param s
     *          the schema for the leaf value
     * @return the appropriate Kettle typed value
     * @throws KettleException
     *           if a problem occurs
     */
    protected Object getPrimitive( Object fieldValue, Schema s ) throws KettleException {

      if ( fieldValue == null ) {
        return null;
      }

      switch ( s.getType() ) {
        case BOOLEAN:
        case LONG:
        case DOUBLE:
        case BYTES:
        case ENUM:
        case STRING:
          return getKettleValue( fieldValue );
        case INT:
          return getKettleValue( new Long( (Integer) fieldValue ) );
        case FLOAT:
          return getKettleValue( new Double( (Float) fieldValue ) );
        case FIXED:
          return ( (GenericFixed) fieldValue ).bytes();
        default:
          return null;
      }
    }

    /**
     * Processes a map at this point in the path.
     * 
     * @param map
     *          the map to process
     * @param s
     *          the current schema at this point in the path
     * @param ignoreMissing
     *          true if null is to be returned for user fields that don't appear in the schema
     * @return the field value or null for out-of-bounds array indexes, non-existent map keys or unsupported avro types.
     * @throws KettleException
     *           if a problem occurs
     */
    public Object convertToKettleValue(
        Map<Utf8, Object> map, Schema s, boolean ignoreMissing ) throws KettleException {

      if ( map == null ) {
        return null;
      }

      if ( m_tempParts.size() == 0 ) {
        throw new KettleException( BaseMessages.getString( PKG, "AvroInput.Error.MalformedPathMap" ) );
      }

      String part = m_tempParts.remove( 0 );
      if ( !( part.charAt( 0 ) == '[' ) ) {
        throw new KettleException( BaseMessages.getString( PKG, "AvroInput.Error.MalformedPathMap2", part ) );
      }

      String key = part.substring( 1, part.indexOf( ']' ) );

      if ( part.indexOf( ']' ) < part.length() - 1 ) {
        // more dimensions to the array/map
        part = part.substring( part.indexOf( ']' ) + 1, part.length() );
        m_tempParts.add( 0, part );
      }

      Object value = map.get( new Utf8( key ) );
      if ( value == null ) {
        return null;
      }

      Schema valueType = s.getValueType();

      if ( valueType.getType() == Schema.Type.UNION ) {
        if ( value instanceof GenericContainer ) {
          // we can ask these things for their schema (covers
          // records, arrays, enums and fixed)
          valueType = ( (GenericContainer) value ).getSchema();
        } else {
          // either have a map or primitive here
          if ( value instanceof Map ) {
            // now have to look for the schema of the map
            Schema mapSchema = null;
            for ( Schema ts : valueType.getTypes() ) {
              if ( ts.getType() == Schema.Type.MAP ) {
                mapSchema = ts;
                break;
              }
            }
            if ( mapSchema == null ) {
              throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG,
                  "AvroInput.Error.UnableToFindSchemaForUnionMap" ) );
            }
            valueType = mapSchema;
          } else {
            if ( m_tempValueMeta.getType() != ValueMetaInterface.TYPE_STRING ) {
              // we have a two element union, where one element is the type
              // "null". So in this case we actually have just one type and can
              // output specific values of it (instead of using String as a
              // catch all for varying primitive types in the union)
              valueType = AvroInputData.checkUnion( valueType );
            } else {
              // use the string representation of the value
              valueType = Schema.create( Schema.Type.STRING );
            }
          }
        }
      }

      // what have we got?
      if ( valueType.getType() == Schema.Type.RECORD ) {
        return convertToKettleValue( (GenericData.Record) value, valueType, ignoreMissing );
      } else if ( valueType.getType() == Schema.Type.ARRAY ) {
        return convertToKettleValue( (GenericData.Array) value, valueType, ignoreMissing );
      } else if ( valueType.getType() == Schema.Type.MAP ) {
        return convertToKettleValue( (Map<Utf8, Object>) value, valueType, ignoreMissing );
      } else {
        // assume a primitive
        return getPrimitive( value, valueType );
      }
    }

    /**
     * Processes an array at this point in the path.
     * 
     * @param array
     *          the array to process
     * @param s
     *          the current schema at this point in the path
     * @param ignoreMissing
     *          true if null is to be returned for user fields that don't appear in the schema
     * @return the field value or null for out-of-bounds array indexes, non-existent map keys or unsupported avro types.
     * @throws KettleException
     *           if a problem occurs
     */
    public Object convertToKettleValue( GenericData.Array array, Schema s, boolean ignoreMissing )
      throws KettleException {

      if ( array == null ) {
        return null;
      }

      if ( m_tempParts.size() == 0 ) {
        throw new KettleException( BaseMessages.getString( PKG, "AvroInput.Error.MalformedPathArray" ) );
      }

      String part = m_tempParts.remove( 0 );
      if ( !( part.charAt( 0 ) == '[' ) ) {
        throw new KettleException( BaseMessages.getString( PKG, "AvroInput.Error.MalformedPathArray2", part ) );
      }

      String index = part.substring( 1, part.indexOf( ']' ) );
      int arrayI = 0;
      try {
        arrayI = Integer.parseInt( index.trim() );
      } catch ( NumberFormatException e ) {
        throw new KettleException( BaseMessages.getString( PKG, "AvroInput.Error.UnableToParseArrayIndex", index ) );
      }

      if ( part.indexOf( ']' ) < part.length() - 1 ) {
        // more dimensions to the array
        part = part.substring( part.indexOf( ']' ) + 1, part.length() );
        m_tempParts.add( 0, part );
      }

      if ( arrayI >= array.size() || arrayI < 0 ) {
        return null;
      }

      Object element = array.get( arrayI );
      Schema elementType = s.getElementType();

      if ( element == null ) {
        return null;
      }

      if ( elementType.getType() == Schema.Type.UNION ) {
        if ( element instanceof GenericContainer ) {
          // we can ask these things for their schema (covers
          // records, arrays, enums and fixed)
          elementType = ( (GenericContainer) element ).getSchema();
        } else {
          // either have a map or primitive here
          if ( element instanceof Map ) {
            // now have to look for the schema of the map
            Schema mapSchema = null;
            for ( Schema ts : elementType.getTypes() ) {
              if ( ts.getType() == Schema.Type.MAP ) {
                mapSchema = ts;
                break;
              }
            }
            if ( mapSchema == null ) {
              throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG,
                  "AvroInput.Error.UnableToFindSchemaForUnionMap" ) );
            }
            elementType = mapSchema;
          } else {
            if ( m_tempValueMeta.getType() != ValueMetaInterface.TYPE_STRING ) {
              // we have a two element union, where one element is the type
              // "null". So in this case we actually have just one type and can
              // output specific values of it (instead of using String as a
              // catch all for varying primitive types in the union)
              elementType = AvroInputData.checkUnion( elementType );
            } else {
              // use the string representation of the value
              elementType = Schema.create( Schema.Type.STRING );
            }
          }
        }
      }

      // what have we got?
      if ( elementType.getType() == Schema.Type.RECORD ) {
        return convertToKettleValue( (GenericData.Record) element, elementType, ignoreMissing );
      } else if ( elementType.getType() == Schema.Type.ARRAY ) {
        return convertToKettleValue( (GenericData.Array) element, elementType, ignoreMissing );
      } else if ( elementType.getType() == Schema.Type.MAP ) {
        return convertToKettleValue( (Map<Utf8, Object>) element, elementType, ignoreMissing );
      } else {
        // assume a primitive (covers bytes encapsulated in FIXED type)
        return getPrimitive( element, elementType );
      }
    }

    /**
     * Processes a record at this point in the path.
     * 
     * @param record
     *          the record to process
     * @param s
     *          the current schema at this point in the path
     * @param ignoreMissing
     *          true if null is to be returned for user fields that don't appear in the schema
     * @return the field value or null for out-of-bounds array indexes, non-existent map keys or unsupported avro types.
     * @throws KettleException
     *           if a problem occurs
     */
    public Object convertToKettleValue( GenericData.Record record, Schema s, boolean ignoreMissing )
      throws KettleException {

      if ( record == null ) {
        return null;
      }

      if ( m_tempParts.size() == 0 ) {
        throw new KettleException( BaseMessages.getString( PKG, "AvroInput.Error.MalformedPathRecord" ) );
      }

      String part = m_tempParts.remove( 0 );
      if ( part.charAt( 0 ) == '[' ) {
        throw new KettleException( BaseMessages.getString( PKG, "AvroInput.Error.InvalidPath" ) + m_tempParts );
      }

      if ( part.indexOf( '[' ) > 0 ) {
        String arrayPart = part.substring( part.indexOf( '[' ) );
        part = part.substring( 0, part.indexOf( '[' ) );

        // put the array section back into location zero
        m_tempParts.add( 0, arrayPart );
      }

      // part is a named field of the record
      Schema.Field fieldS = s.getField( part );
      if ( fieldS == null && !ignoreMissing ) {
        throw new KettleException( BaseMessages.getString( PKG, "AvroInput.Error.NonExistentField", part ) );
      }
      Object field = record.get( part );

      if ( field == null ) {
        return null;
      }

      Schema.Type fieldT = fieldS.schema().getType();
      Schema fieldSchema = fieldS.schema();

      if ( fieldT == Schema.Type.UNION ) {
        if ( field instanceof GenericContainer ) {
          // we can ask these things for their schema (covers
          // records, arrays, enums and fixed)
          fieldSchema = ( (GenericContainer) field ).getSchema();
          fieldT = fieldSchema.getType();
        } else {
          // either have a map or primitive here
          if ( field instanceof Map ) {
            // now have to look for the schema of the map
            Schema mapSchema = null;
            for ( Schema ts : fieldSchema.getTypes() ) {
              if ( ts.getType() == Schema.Type.MAP ) {
                mapSchema = ts;
                break;
              }
            }
            if ( mapSchema == null ) {
              throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG,
                  "AvroInput.Error.UnableToFindSchemaForUnionMap" ) );

            }
            fieldSchema = mapSchema;
            fieldT = Schema.Type.MAP;
          } else {
            if ( m_tempValueMeta.getType() != ValueMetaInterface.TYPE_STRING ) {
              // we have a two element union, where one element is the type
              // "null". So in this case we actually have just one type and can
              // output specific values of it (instead of using String as a
              // catch all for varying primitive types in the union)
              fieldSchema = AvroInputData.checkUnion( fieldSchema );
              fieldT = fieldSchema.getType();
            } else {

              // use the string representation of the value
              fieldSchema = Schema.create( Schema.Type.STRING );
              fieldT = fieldSchema.getType();
            }
          }
        }
      }

      // what have we got?
      if ( fieldT == Schema.Type.RECORD ) {
        return convertToKettleValue( (GenericData.Record) field, fieldSchema, ignoreMissing );
      } else if ( fieldT == Schema.Type.ARRAY ) {
        return convertToKettleValue( (GenericData.Array) field, fieldSchema, ignoreMissing );
      } else if ( fieldT == Schema.Type.MAP ) {
        return convertToKettleValue( (Map<Utf8, Object>) field, fieldSchema, ignoreMissing );
      } else {
        // assume primitive (covers bytes encapsulated in FIXED type)
        return getPrimitive( field, fieldSchema );
      }
    }
  }

  /** The avro file to read */
  protected String m_filename = "";

  /** The schema to use if not reading from a container file */
  protected String m_schemaFilename = "";

  /** True if the user's avro file is json encoded rather than binary */
  protected boolean m_isJsonEncoded = false;

  /** True if the avro to be decoded is contained in an incoming field */
  protected boolean m_avroInField = false;

  /** Holds the source field name (if decoding from an incoming field) */
  protected String m_avroFieldName = "";

  /**
   * True if the schema to be used to decode an incoming Avro object is contained in an incoming field (only applies
   * when m_avroInField == true)
   */
  protected boolean m_schemaInField;

  /**
   * The name of the source field holding the avro schema (either the JSON schema itself or a path to the schema on
   * disk.
   */
  protected String m_schemaFieldName;

  /**
   * True if the value in the incoming schema field is actual a path to the schema on disk (rather than the actual
   * schema itself)
   */
  protected boolean m_schemaInFieldIsPath;

  /**
   * True if schemas read from incoming fields are to be cached in memory for speed
   */
  protected boolean m_cacheSchemasInMemory;

  /**
   * True if null should be output if a specified field is not present in the Avro schema (otherwise an exception is
   * raised)
   */
  protected boolean m_dontComplainAboutMissingFields;

  /** The fields to emit */
  protected List<AvroField> m_fields;

  /** Incoming field values to use for lookup/substitution in avro paths */
  protected List<LookupField> m_lookups;

  /**
   * Set whether the avro to be decoded is contained in an incoming field
   * 
   * @param a
   *          true if the avro to be decoded is contained in an incoming field
   */
  public void setAvroInField( boolean a ) {
    m_avroInField = a;
  }

  /**
   * Get whether the avro to be decoded is contained in an incoming field
   * 
   * @return true if the avro to be decoded is contained in an incoming field
   */
  public boolean getAvroInField() {
    return m_avroInField;
  }

  /**
   * Set the name of the incoming field to decode avro from (if decoding from a field rather than a file)
   * 
   * @param f
   *          the name of the incoming field to decode from
   */
  public void setAvroFieldName( String f ) {
    m_avroFieldName = f;
  }

  /**
   * Get the name of the incoming field to decode avro from (if decoding from a field rather than a file)
   * 
   * @return the name of the incoming field to decode from
   */
  public String getAvroFieldName() {
    return m_avroFieldName;
  }

  /**
   * Set whether the schema to use for decoding incoming Avro objects is contained in an incoming field
   * 
   * @param s
   *          true if the schema to use for decoding incoming objects is itself in an incoming field
   */
  public void setSchemaInField( boolean s ) {
    m_schemaInField = s;
  }

  /**
   * Get whether the schema to use for decoding incoming Avro objects is contained in an incoming field
   * 
   * @return true if the schema to use for decoding incoming objects is itself in an incoming field
   */
  public boolean getSchemaInField() {
    return m_schemaInField;
  }

  /**
   * Set the name of the incoming field that contains the schema to use.
   * 
   * @param fn
   *          the name of the incoming field that holds the schema to use
   */
  public void setSchemaFieldName( String fn ) {
    m_schemaFieldName = fn;
  }

  /**
   * Get the name of the incoming field that contains the schema to use.
   * 
   * @return the name of the incoming field that holds the schema to use
   */
  public String getSchemaFieldName() {
    return m_schemaFieldName;
  }

  /**
   * Set whether the incoming schema field value contains a path to a schema on disk.
   * 
   * @param p
   *          true if the incoming schema field value is actually a path to an on disk schema file.
   */
  public void setSchemaInFieldIsPath( boolean p ) {
    m_schemaInFieldIsPath = p;
  }

  /**
   * Get whether the incoming schema field value contains a path to a schema on disk.
   * 
   * @return true if the incoming schema field value is actually a path to an on disk schema file.
   */
  public boolean getSchemaInFieldIsPath() {
    return m_schemaInFieldIsPath;
  }

  /**
   * Set whether to cache schemas in memory when they are being supplied via an incoming field.
   * 
   * @param c
   *          true if schemas are to be cached in memory.
   */
  public void setCacheSchemasInMemory( boolean c ) {
    m_cacheSchemasInMemory = c;
  }

  /**
   * Get whether to cache schemas in memory when they are being supplied via an incoming field.
   * 
   * @return true if schemas are to be cached in memory.
   */
  public boolean getCacheSchemasInMemory() {
    return m_cacheSchemasInMemory;
  }

  /**
   * Set the avro filename
   * 
   * @param filename
   *          the avro filename
   */
  public void setFilename( String filename ) {
    m_filename = filename;
  }

  /**
   * Get the avro filename
   * 
   * @return the avro filename
   */
  public String getFilename() {
    return m_filename;
  }

  /**
   * Set the schema filename to use
   * 
   * @param schemaFile
   *          the name of the schema file to use
   */
  public void setSchemaFilename( String schemaFile ) {
    m_schemaFilename = schemaFile;
  }

  /**
   * Get the schema filename to use
   * 
   * @return the name of the schema file to use
   */
  public String getSchemaFilename() {
    return m_schemaFilename;
  }

  /**
   * Get whether the avro file to read is json encoded rather than binary
   * 
   * @return true if the file to read is json encoded
   */
  public boolean getAvroIsJsonEncoded() {
    return m_isJsonEncoded;
  }

  /**
   * Set whether the avro file to read is json encoded rather than binary
   * 
   * @param j
   *          true if the file to read is json encoded
   */
  public void setAvroIsJsonEncoded( boolean j ) {
    m_isJsonEncoded = j;
  }

  /**
   * Set the Avro fields that will be extracted
   * 
   * @param fields
   *          the Avro fields that will be extracted
   */
  public void setAvroFields( List<AvroField> fields ) {
    m_fields = fields;
  }

  /**
   * Get the Avro fields that will be extracted
   * 
   * @return the Avro fields that will be extracted
   */
  public List<AvroField> getAvroFields() {
    return m_fields;
  }

  /**
   * Get the incoming field values that will be used for lookup/substitution in the avro paths
   * 
   * @return the lookup fields
   */
  public List<LookupField> getLookupFields() {
    return m_lookups;
  }

  /**
   * Set the incoming field values that will be used for lookup/substitution in the avro paths
   * 
   * @param lookups
   *          the lookup fields
   */
  public void setLookupFields( List<LookupField> lookups ) {
    m_lookups = lookups;
  }

  /**
   * Set whether null is to be output if a user-supplied field path does not exist in the Avro schema being used.
   * Otherwise, an exception is raised
   * 
   * @param c
   *          true to ignore missing fields
   */
  public void setDontComplainAboutMissingFields( boolean c ) {
    m_dontComplainAboutMissingFields = c;
  }

  /**
   * Get whether null is to be output if a user-supplied field path does not exist in the Avro schema being used.
   * Otherwise, an exception is raised
   * 
   * @return true to ignore missing fields
   */
  public boolean getDontComplainAboutMissingFields() {
    return m_dontComplainAboutMissingFields;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.di.trans.step.BaseStepMeta#getFields(org.pentaho.di.core.row .RowMetaInterface, java.lang.String,
   * org.pentaho.di.core.row.RowMetaInterface[], org.pentaho.di.trans.step.StepMeta,
   * org.pentaho.di.core.variables.VariableSpace)
   */
  @Override
  public void getFields( RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
      VariableSpace space ) throws KettleStepException {

    List<AvroField> fieldsToOutput = null;

    if ( m_fields != null && m_fields.size() > 0 ) {
      // we have some stored field info - use this
      fieldsToOutput = m_fields;
    } else {
      // outputting all fields from either supplied schema or schema embedded
      // in a container file

      if ( !Const.isEmpty( getSchemaFilename() ) ) {
        String fn = space.environmentSubstitute( m_schemaFilename );

        try {
          Schema s = AvroInputData.loadSchema( fn );
          fieldsToOutput = AvroInputData.getLeafFields( s );
        } catch ( KettleException e ) {
          throw new KettleStepException( BaseMessages.getString( PKG, "AvroInput.Error.UnableToLoadSchema", fn ), e );
        }
      } else {

        if ( m_avroInField ) {
          throw new KettleStepException( BaseMessages.getString( PKG, "AvroInput.Error.NoSchemaSupplied" ) );
        }
        // assume a container file and grab from there...
        String avroFilename = m_filename;
        avroFilename = space.environmentSubstitute( avroFilename );
        try {
          Schema s = AvroInputData.loadSchemaFromContainer( avroFilename );
          fieldsToOutput = AvroInputData.getLeafFields( s );
        } catch ( KettleException e ) {
          throw new KettleStepException( BaseMessages.getString( PKG,
              "AvroInput.Error.UnableToLoadSchemaFromContainerFile", avroFilename ) );
        }
      }
    }

    for ( AvroField f : fieldsToOutput ) {
      ValueMetaInterface vm = new ValueMeta();
      vm.setName( f.m_fieldName );
      vm.setOrigin( origin );
      vm.setType( ValueMeta.getType( f.m_kettleType ) );
      if ( f.m_indexedVals != null ) {
        vm.setIndex( f.m_indexedVals.toArray() ); // indexed values
      }
      rowMeta.addValueMeta( vm );
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.di.trans.step.StepMetaInterface#check(java.util.List, org.pentaho.di.trans.TransMeta,
   * org.pentaho.di.trans.step.StepMeta, org.pentaho.di.core.row.RowMetaInterface, java.lang.String[],
   * java.lang.String[], org.pentaho.di.core.row.RowMetaInterface)
   */
  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
      String[] input, String[] output, RowMetaInterface info ) {
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.di.trans.step.StepMetaInterface#getStep(org.pentaho.di.trans .step.StepMeta,
   * org.pentaho.di.trans.step.StepDataInterface, int, org.pentaho.di.trans.TransMeta, org.pentaho.di.trans.Trans)
   */
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
      TransMeta transMeta, Trans trans ) {

    return new AvroInput( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.di.trans.step.StepMetaInterface#getStepData()
   */
  public StepDataInterface getStepData() {
    return new AvroInputData();
  }

  /**
   * Helper function that takes a list of indexed values and returns them as a String in comma-separated form.
   * 
   * @param indexedVals
   *          a list of indexed values
   * @return the list a String in comma-separated form
   */
  protected static String indexedValsList( List<String> indexedVals ) {
    StringBuffer temp = new StringBuffer();

    for ( int i = 0; i < indexedVals.size(); i++ ) {
      temp.append( indexedVals.get( i ) );
      if ( i < indexedVals.size() - 1 ) {
        temp.append( "," );
      }
    }

    return temp.toString();
  }

  /**
   * Helper function that takes a comma-separated list in a String and returns a list.
   * 
   * @param indexedVals
   *          the String containing the lsit
   * @return a List containing the values
   */
  protected static List<String> indexedValsList( String indexedVals ) {

    String[] parts = indexedVals.split( "," );
    List<String> list = new ArrayList<String>();
    for ( String s : parts ) {
      list.add( s.trim() );
    }

    return list;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.di.trans.step.BaseStepMeta#getXML()
   */
  @Override
  public String getXML() {
    StringBuffer retval = new StringBuffer();

    if ( !Const.isEmpty( m_filename ) ) {
      retval.append( "\n    " ).append( XMLHandler.addTagValue( "avro_filename", m_filename ) );
    }

    if ( !Const.isEmpty( m_schemaFilename ) ) {
      retval.append( "\n    " ).append( XMLHandler.addTagValue( "schema_filename", m_schemaFilename ) );
    }

    retval.append( "\n    " ).append( XMLHandler.addTagValue( "json_encoded", m_isJsonEncoded ) );

    retval.append( "\n    " ).append( XMLHandler.addTagValue( "avro_in_field", m_avroInField ) );

    if ( !Const.isEmpty( m_avroFieldName ) ) {
      retval.append( "\n    " ).append( XMLHandler.addTagValue( "avro_field_name", m_avroFieldName ) );
    }

    retval.append( "\n    " ).append( XMLHandler.addTagValue( "schema_in_field", m_schemaInField ) );

    if ( !Const.isEmpty( m_schemaFieldName ) ) {
      retval.append( "\n    " ).append( XMLHandler.addTagValue( "schema_field_name", m_schemaFieldName ) );
    }

    retval.append( "\n    " ).append( XMLHandler.addTagValue( "schema_in_field_is_path", m_schemaInFieldIsPath ) );

    retval.append( "\n    " ).append( XMLHandler.addTagValue( "cache_schemas", m_cacheSchemasInMemory ) );

    retval.append( "\n    " ).append(
        XMLHandler.addTagValue( "ignore_missing_fields", m_dontComplainAboutMissingFields ) );

    if ( m_fields != null && m_fields.size() > 0 ) {
      retval.append( "\n    " ).append( XMLHandler.openTag( "avro_fields" ) );

      for ( AvroField f : m_fields ) {
        retval.append( "\n      " ).append( XMLHandler.openTag( "avro_field" ) );

        retval.append( "\n        " ).append( XMLHandler.addTagValue( "field_name", f.m_fieldName ) );
        retval.append( "\n        " ).append( XMLHandler.addTagValue( "field_path", f.m_fieldPath ) );
        retval.append( "\n        " ).append( XMLHandler.addTagValue( "field_type", f.m_kettleType ) );
        if ( f.m_indexedVals != null && f.m_indexedVals.size() > 0 ) {
          retval.append( "\n        " ).append(
              XMLHandler.addTagValue( "indexed_vals", indexedValsList( f.m_indexedVals ) ) );
        }
        retval.append( "\n      " ).append( XMLHandler.closeTag( "avro_field" ) );
      }

      retval.append( "\n    " ).append( XMLHandler.closeTag( "avro_fields" ) );
    }

    if ( m_lookups != null && m_lookups.size() > 0 ) {
      retval.append( "\n    " ).append( XMLHandler.openTag( "lookup_fields" ) );

      for ( LookupField f : m_lookups ) {
        retval.append( "\n      " ).append( XMLHandler.openTag( "lookup_field" ) );

        retval.append( "\n        " ).append( XMLHandler.addTagValue( "lookup_field_name", f.m_fieldName ) );
        retval.append( "\n        " ).append( XMLHandler.addTagValue( "variable_name", f.m_variableName ) );
        retval.append( "\n        " ).append( XMLHandler.addTagValue( "default_value", f.m_defaultValue ) );

        retval.append( "\n      " ).append( XMLHandler.closeTag( "lookup_field" ) );
      }

      retval.append( "\n    " ).append( XMLHandler.closeTag( "lookup_fields" ) );
    }

    return retval.toString();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.di.trans.step.StepMetaInterface#loadXML(org.w3c.dom.Node, java.util.List, java.util.Map)
   */
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters )
    throws KettleXMLException {
    m_filename = XMLHandler.getTagValue( stepnode, "avro_filename" );
    m_schemaFilename = XMLHandler.getTagValue( stepnode, "schema_filename" );

    String jsonEnc = XMLHandler.getTagValue( stepnode, "json_encoded" );
    if ( !Const.isEmpty( jsonEnc ) ) {
      m_isJsonEncoded = jsonEnc.equalsIgnoreCase( "Y" );
    }

    String avroInField = XMLHandler.getTagValue( stepnode, "avro_in_field" );
    if ( !Const.isEmpty( avroInField ) ) {
      m_avroInField = avroInField.equalsIgnoreCase( "Y" );
    }
    m_avroFieldName = XMLHandler.getTagValue( stepnode, "avro_field_name" );

    String schemaInField = XMLHandler.getTagValue( stepnode, "schema_in_field" );
    if ( !Const.isEmpty( schemaInField ) ) {
      m_schemaInField = schemaInField.equalsIgnoreCase( "Y" );
    }
    m_schemaFieldName = XMLHandler.getTagValue( stepnode, "schema_field_name" );

    String schemaInFieldIsPath = XMLHandler.getTagValue( stepnode, "schema_in_field_is_path" );
    if ( !Const.isEmpty( schemaInFieldIsPath ) ) {
      m_schemaInFieldIsPath = schemaInFieldIsPath.equalsIgnoreCase( "Y" );
    }

    String cacheSchemas = XMLHandler.getTagValue( stepnode, "cache_schemas" );
    if ( !Const.isEmpty( cacheSchemas ) ) {
      m_cacheSchemasInMemory = cacheSchemas.equalsIgnoreCase( "Y" );
    }

    String ignoreMissing = XMLHandler.getTagValue( stepnode, "ignore_missing_fields" );
    if ( !Const.isEmpty( ignoreMissing ) ) {
      m_dontComplainAboutMissingFields = ignoreMissing.equalsIgnoreCase( "Y" );
    }

    Node fields = XMLHandler.getSubNode( stepnode, "avro_fields" );
    if ( fields != null && XMLHandler.countNodes( fields, "avro_field" ) > 0 ) {
      int nrfields = XMLHandler.countNodes( fields, "avro_field" );

      m_fields = new ArrayList<AvroField>();
      for ( int i = 0; i < nrfields; i++ ) {
        Node fieldNode = XMLHandler.getSubNodeByNr( fields, "avro_field", i );

        AvroField newField = new AvroField();
        newField.m_fieldName = XMLHandler.getTagValue( fieldNode, "field_name" );
        newField.m_fieldPath = XMLHandler.getTagValue( fieldNode, "field_path" );
        newField.m_kettleType = XMLHandler.getTagValue( fieldNode, "field_type" );
        String indexedVals = XMLHandler.getTagValue( fieldNode, "indexed_vals" );
        if ( indexedVals != null && indexedVals.length() > 0 ) {
          newField.m_indexedVals = indexedValsList( indexedVals );
        }

        m_fields.add( newField );
      }
    }

    Node lFields = XMLHandler.getSubNode( stepnode, "lookup_fields" );
    if ( lFields != null && XMLHandler.countNodes( lFields, "lookup_field" ) > 0 ) {
      int nrfields = XMLHandler.countNodes( lFields, "lookup_field" );

      m_lookups = new ArrayList<LookupField>();

      for ( int i = 0; i < nrfields; i++ ) {
        Node fieldNode = XMLHandler.getSubNodeByNr( lFields, "lookup_field", i );

        LookupField newField = new LookupField();
        newField.m_fieldName = XMLHandler.getTagValue( fieldNode, "lookup_field_name" );
        newField.m_variableName = XMLHandler.getTagValue( fieldNode, "variable_name" );
        newField.m_defaultValue = XMLHandler.getTagValue( fieldNode, "default_value" );

        m_lookups.add( newField );
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.di.trans.step.StepMetaInterface#readRep(org.pentaho.di.repository .Repository,
   * org.pentaho.di.repository.ObjectId, java.util.List, java.util.Map)
   */
  public void readRep( Repository rep, ObjectId id_step, List<DatabaseMeta> databases, Map<String, Counter> counters )
    throws KettleException {

    m_filename = rep.getStepAttributeString( id_step, 0, "avro_filename" );
    m_schemaFilename = rep.getStepAttributeString( id_step, 0, "schema_filename" );

    m_isJsonEncoded = rep.getStepAttributeBoolean( id_step, 0, "json_encoded" );

    m_avroInField = rep.getStepAttributeBoolean( id_step, 0, "avro_in_field" );
    m_avroFieldName = rep.getStepAttributeString( id_step, 0, "avro_field_name" );

    m_schemaInField = rep.getStepAttributeBoolean( id_step, 0, "schema_in_field" );
    m_schemaFieldName = rep.getStepAttributeString( id_step, 0, "schema_field_name" );
    m_schemaInFieldIsPath = rep.getStepAttributeBoolean( id_step, 0, "schema_in_field_is_path" );
    m_cacheSchemasInMemory = rep.getStepAttributeBoolean( id_step, 0, "cache_schemas" );
    m_dontComplainAboutMissingFields = rep.getStepAttributeBoolean( id_step, 0, "ignore_missing_fields" );

    int nrfields = rep.countNrStepAttributes( id_step, "field_name" );
    if ( nrfields > 0 ) {
      m_fields = new ArrayList<AvroField>();

      for ( int i = 0; i < nrfields; i++ ) {
        AvroField newField = new AvroField();

        newField.m_fieldName = rep.getStepAttributeString( id_step, i, "field_name" );
        newField.m_fieldPath = rep.getStepAttributeString( id_step, i, "field_path" );
        newField.m_kettleType = rep.getStepAttributeString( id_step, i, "field_type" );
        String indexedVals = rep.getStepAttributeString( id_step, i, "indexed_vals" );
        if ( indexedVals != null && indexedVals.length() > 0 ) {
          newField.m_indexedVals = indexedValsList( indexedVals );
        }

        m_fields.add( newField );
      }
    }

    nrfields = rep.countNrStepAttributes( id_step, "lookup_field_name" );
    if ( nrfields > 0 ) {
      m_lookups = new ArrayList<LookupField>();

      for ( int i = 0; i < nrfields; i++ ) {
        LookupField newField = new LookupField();

        newField.m_fieldName = rep.getStepAttributeString( id_step, i, "lookup_field_name" );
        newField.m_variableName = rep.getStepAttributeString( id_step, i, "variable_name" );
        newField.m_defaultValue = rep.getStepAttributeString( id_step, i, "default_value" );

        m_lookups.add( newField );
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.di.trans.step.StepMetaInterface#saveRep(org.pentaho.di.repository .Repository,
   * org.pentaho.di.repository.ObjectId, org.pentaho.di.repository.ObjectId)
   */
  public void saveRep( Repository rep, ObjectId id_transformation, ObjectId id_step ) throws KettleException {

    if ( !Const.isEmpty( m_filename ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "avro_filename", m_filename );
    }
    if ( !Const.isEmpty( m_schemaFilename ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "schema_filename", m_schemaFilename );
    }

    rep.saveStepAttribute( id_transformation, id_step, 0, "json_encoded", m_isJsonEncoded );

    rep.saveStepAttribute( id_transformation, id_step, 0, "avro_in_field", m_avroInField );
    if ( !Const.isEmpty( m_avroFieldName ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "avro_field_name", m_avroFieldName );
    }

    rep.saveStepAttribute( id_transformation, id_step, 0, "schema_in_field", m_schemaInField );
    if ( !Const.isEmpty( m_schemaFieldName ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "schema_field_name", m_schemaFieldName );
    }
    rep.saveStepAttribute( id_transformation, id_step, 0, "schema_in_field_is_path", m_schemaInFieldIsPath );
    rep.saveStepAttribute( id_transformation, id_step, 0, "cache_schemas", m_cacheSchemasInMemory );
    rep.saveStepAttribute( id_transformation, id_step, 0, "ignore_missing_fields", m_dontComplainAboutMissingFields );

    if ( m_fields != null && m_fields.size() > 0 ) {
      for ( int i = 0; i < m_fields.size(); i++ ) {
        AvroField f = m_fields.get( i );

        rep.saveStepAttribute( id_transformation, id_step, i, "field_name", f.m_fieldName );
        rep.saveStepAttribute( id_transformation, id_step, i, "field_path", f.m_fieldPath );
        rep.saveStepAttribute( id_transformation, id_step, i, "field_type", f.m_kettleType );
        if ( f.m_indexedVals != null && f.m_indexedVals.size() > 0 ) {
          String indexedVals = indexedValsList( f.m_indexedVals );

          rep.saveStepAttribute( id_transformation, id_step, i, "indexed_vals", indexedVals );
        }
      }
    }

    if ( m_lookups != null && m_lookups.size() > 0 ) {
      for ( int i = 0; i < m_lookups.size(); i++ ) {
        LookupField f = m_lookups.get( i );

        rep.saveStepAttribute( id_transformation, id_step, i, "lookup_field_name", f.m_fieldName );
        rep.saveStepAttribute( id_transformation, id_step, i, "variable_name", f.m_variableName );
        rep.saveStepAttribute( id_transformation, id_step, i, "default_value", f.m_defaultValue );
      }
    }
  }

  public void setDefault() {
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.di.trans.step.BaseStepMeta#getDialogClassName()
   */
  @Override
  public String getDialogClassName() {
    return "org.pentaho.di.trans.steps.avroinput.AvroInputDialog";
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.di.trans.step.BaseStepMeta#supportsErrorHandling()
   */
  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
