/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.di.trans.steps.avroinput;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

/**
 * Data class for the AvroInput step. Contains methods to determine the type of Avro file (i.e. container or just
 * serialized objects), extract all the leaf fields from the object structure described in the schema and convert Avro
 * leaf fields to kettle values.
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class AvroInputData extends BaseStepData implements StepDataInterface {

  /** For logging */
  protected LogChannelInterface m_log;

  /** The output data format */
  protected RowMetaInterface m_outputRowMeta;

  /** For reading container files - will be null if file is not a container file */
  protected DataFileStream m_containerReader;

  /** For reading from files of just serialized objects */
  protected GenericDatumReader m_datumReader;
  protected Decoder m_decoder;
  protected InputStream m_inStream;

  /**
   * The schema used to write the file - will be null if the file is not a container file
   */
  protected Schema m_writerSchema;

  /** The schema to use for extracting values */
  protected Schema m_schemaToUse;

  /**
   * The default schema to use (in the case where the schema is in an incoming field and a particular row has a null (or
   * unparsable/unavailable) schema
   */
  protected Schema m_defaultSchema;

  /** The default datum reader (constructed with the default schema) */
  protected GenericDatumReader m_defaultDatumReader;
  protected Object m_defaultTopLevelObject;

  /**
   * Schema cache. Map of strings (actual schema or path to schema) to two element array. Element 0 = GenericDatumReader
   * configured with schema; 2 = top level structure object to use.
   */
  protected Map<String, Object[]> m_schemaCache = new HashMap<String, Object[]>();

  /** True if the data to be decoded is json rather than binary */
  protected boolean m_jsonEncoded;

  /** If the top level is a record */
  protected Record m_topLevelRecord;

  /** If the top level is an array */
  protected GenericData.Array m_topLevelArray;

  /** If the top level is a map */
  protected Map<Utf8, Object> m_topLevelMap;

  protected List<AvroInputMeta.AvroField> m_normalFields;
  protected AvroArrayExpansion m_expansionHandler;

  /** The index that the decoded fields start at in the output row */
  protected int m_newFieldOffset;

  /** True if decoding from an incoming field */
  protected boolean m_decodingFromField;

  /** If decoding from an incoming field, this holds its index */
  protected int m_fieldToDecodeIndex = -1;

  /** True if schema is in an incoming field */
  protected boolean m_schemaInField;

  /**
   * If decoding from an incoming field and schema is in an incoming field, then this holds the schema field's index
   */
  protected int m_schemaFieldIndex = -1;

  /**
   * True if the schema field contains a path to a schema rather than the schema itself
   */
  protected boolean m_schemaFieldIsPath;

  /** True if schemas read from incoming fields are to be cached in memory */
  protected boolean m_cacheSchemas;

  /**
   * True if null should be output for a field if it is not present in the schema being used (otherwise an exeption is
   * raised)
   */
  protected boolean m_dontComplainAboutMissingFields;

  /** Factory for obtaining a decoder */
  protected DecoderFactory m_factory;

  /**
   * Cleanses a string path by ensuring that any variables names present in the path do not contain "."s (replaces any
   * dots with underscores).
   *
   * @param path
   *          the path to cleanse
   * @return the cleansed path
   */
  public static String cleansePath( String path ) {
    // look for variables and convert any "." to "_"

    int index = path.indexOf( "${" );

    int endIndex = 0;
    String tempStr = path;
    while ( index >= 0 ) {
      index += 2;
      endIndex += tempStr.indexOf( "}" );
      if ( endIndex > 0 && endIndex > index + 1 ) {
        String key = path.substring( index, endIndex );

        String cleanKey = key.replace( '.', '_' );
        path = path.replace( key, cleanKey );
      } else {
        break;
      }

      if ( endIndex + 1 < path.length() ) {
        tempStr = path.substring( endIndex + 1, path.length() );
      } else {
        break;
      }

      index = tempStr.indexOf( "${" );

      if ( index > 0 ) {
        index += endIndex;
      }
    }

    return path;
  }

  /**
   * Inner class that handles a single array/map expansion process. Expands an array or map to multiple Kettle rows.
   * Delegates to AvroInptuMeta.AvroField objects to handle the extraction of leaf primitives.
   *
   * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
   * @version $Revision$
   */
  protected static class AvroArrayExpansion {

    /** The prefix of the full path that defines the expansion */
    public String m_expansionPath;

    /**
     * Subfield objects that handle the processing of the path after the expansion prefix
     */
    protected List<AvroInputMeta.AvroField> m_subFields;

    private List<String> m_pathParts;
    private List<String> m_tempParts;

    protected RowMetaInterface m_outputRowMeta;

    public AvroArrayExpansion( List<AvroInputMeta.AvroField> subFields ) {
      m_subFields = subFields;
    }

    /**
     * Initialize this field by parsing the path etc.
     *
     * @throws KettleException
     *           if a problem occurs
     */
    public void init() throws KettleException {
      if ( Const.isEmpty( m_expansionPath ) ) {
        throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG, "AvroInput.Error.NoPathSet" ) );
      }
      if ( m_pathParts != null ) {
        return;
      }

      String expansionPath = cleansePath( m_expansionPath );

      String[] temp = expansionPath.split( "\\." );
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

      // initialize the sub fields
      if ( m_subFields != null ) {
        for ( AvroInputMeta.AvroField f : m_subFields ) {
          int outputIndex = m_outputRowMeta.indexOfValue( f.m_fieldName );
          f.init( outputIndex );
        }
      }
    }

    /**
     * Reset this field. Should be called prior to processing a new field value from the avro file
     *
     * @param space
     *          environment variables (values that environment variables resolve to cannot contain "."s)
     */
    public void reset( VariableSpace space ) {
      m_tempParts.clear();

      for ( String part : m_pathParts ) {
        m_tempParts.add( space.environmentSubstitute( part ) );
      }

      // reset sub fields
      for ( AvroInputMeta.AvroField f : m_subFields ) {
        f.reset( space );
      }
    }

    /**
     * Processes a map at this point in the path.
     *
     * @param map
     *          the map to process
     * @param s
     *          the current schema at this point in the path
     * @param space
     *          environment variables
     * @param ignoreMissing
     *          true if null is to be returned for user fields that don't appear in the schema
     * @return an array of Kettle rows corresponding to the expanded map/array and containing all leaf values as defined
     *         in the paths
     * @throws KettleException
     *           if a problem occurs
     */
    public Object[][] convertToKettleValues(
        Map<Utf8, Object> map, Schema s, Schema defaultSchema, VariableSpace space,  boolean ignoreMissing ) throws KettleException {

      if ( map == null ) {
        return null;
      }

      if ( m_tempParts.size() == 0 ) {
        throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG, "AvroInput.Error.MalformedPathMap" ) );
      }

      String part = m_tempParts.remove( 0 );
      if ( !( part.charAt( 0 ) == '[' ) ) {
        throw new KettleException( BaseMessages
            .getString( AvroInputMeta.PKG, "AvroInput.Error.MalformedPathMap2", part ) );
      }

      String key = part.substring( 1, part.indexOf( ']' ) );

      if ( part.indexOf( ']' ) < part.length() - 1 ) {
        // more dimensions to the array/map
        part = part.substring( part.indexOf( ']' ) + 1, part.length() );
        m_tempParts.add( 0, part );
      }

      if ( key.equals( "*" ) ) {
        // start the expansion - we delegate conversion to our subfields
        Schema valueType = s.getValueType();
        Object[][] result = new Object[map.keySet().size()][m_outputRowMeta.size() + RowDataUtil.OVER_ALLOCATE_SIZE];

        int i = 0;
        for ( Utf8 mk : map.keySet() ) {
          Object value = map.get( mk );

          for ( int j = 0; j < m_subFields.size(); j++ ) {
            AvroInputMeta.AvroField sf = m_subFields.get( j );
            sf.reset( space );

            // what have we got
            if ( valueType.getType() == Schema.Type.RECORD ) {
              result[i][sf.m_outputIndex] =
                  sf.convertToKettleValue( (Record) value, valueType, defaultSchema, ignoreMissing );
            } else if ( valueType.getType() == Schema.Type.ARRAY ) {
              result[i][sf.m_outputIndex] =
                  sf.convertToKettleValue( (GenericData.Array) value, valueType, defaultSchema, ignoreMissing );
            } else if ( valueType.getType() == Schema.Type.MAP ) {
              result[i][sf.m_outputIndex] =
                  sf.convertToKettleValue( (Map<Utf8, Object>) value, valueType, defaultSchema, ignoreMissing );
            } else {
              // assume a primitive
              result[i][sf.m_outputIndex] = sf.getPrimitive( value, valueType );
            }
          }
          i++; // next row
        }

        return result;
      } else {
        Object value = map.get( new Utf8( key ) );

        if ( value == null ) {
          // key doesn't exist in map
          Object[][] result = new Object[1][m_outputRowMeta.size() + RowDataUtil.OVER_ALLOCATE_SIZE];

          for ( int i = 0; i < m_subFields.size(); i++ ) {
            AvroInputMeta.AvroField sf = m_subFields.get( i );
            result[0][sf.m_outputIndex] = null;
          }

          return result;
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
              // We shouldn't have a primitive here
              if ( !ignoreMissing ) {
                throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG,
                    "AvroInput.Error.EncounteredAPrimitivePriorToMapExpansion" ) );
              }
              Object[][] result = new Object[1][m_outputRowMeta.size() + RowDataUtil.OVER_ALLOCATE_SIZE];
              return result;
            }
          }
        }

        // what have we got?
        if ( valueType.getType() == Schema.Type.RECORD ) {
          return convertToKettleValues( (Record) value, valueType, defaultSchema, space, ignoreMissing );
        } else if ( valueType.getType() == Schema.Type.ARRAY ) {
          return convertToKettleValues( (GenericData.Array) value, valueType, defaultSchema, space, ignoreMissing );
        } else if ( valueType.getType() == Schema.Type.MAP ) {
          return convertToKettleValues( (Map<Utf8, Object>) value, valueType, defaultSchema, space, ignoreMissing );
        } else {
          // we shouldn't have a primitive at this point. If we are
          // extracting a particular key from the map then we're not to the
          // expansion phase,
          // so normally there must be a non-primitive sub-structure. Only if
          // the user is switching schema versions on a per-row basis or the
          // schema is a union at the top level could we end up here
          if ( !ignoreMissing ) {
            throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG,
                "AvroInput.Error.UnexpectedMapValueTypeAtNonExpansionPoint" ) );
          }
          Object[][] result = new Object[1][m_outputRowMeta.size() + RowDataUtil.OVER_ALLOCATE_SIZE];
          return result;
        }
      }
    }

    /**
     * Processes an array at this point in the path.
     *
     * @param array
     *          the array to process
     * @param s
     *          the current schema at this point in the path
     * @param space
     *          environment variables
     * @param ignoreMissing
     *          true if null is to be returned for user fields that don't appear in the schema
     * @return an array of Kettle rows corresponding to the expanded map/array and containing all leaf values as defined
     *         in the paths
     * @throws KettleException
     *           if a problem occurs
     */
    public Object[][] convertToKettleValues( GenericData.Array array, Schema s, Schema defaultSchema, VariableSpace space,
        boolean ignoreMissing ) throws KettleException {

      if ( array == null ) {
        return null;
      }

      if ( m_tempParts.size() == 0 ) {
        throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG, "AvroInput.Error.MalformedPathArray" ) );
      }

      String part = m_tempParts.remove( 0 );
      if ( !( part.charAt( 0 ) == '[' ) ) {
        throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG, "AvroInput.Error.MalformedPathArray2",
            part ) );
      }

      String index = part.substring( 1, part.indexOf( ']' ) );

      if ( part.indexOf( ']' ) < part.length() - 1 ) {
        // more dimensions to the array
        part = part.substring( part.indexOf( ']' ) + 1, part.length() );
        m_tempParts.add( 0, part );
      }

      if ( index.equals( "*" ) ) {
        // start the expansion - we delegate conversion to our subfields

        Schema elementType = s.getElementType();
        Object[][] result = new Object[array.size()][m_outputRowMeta.size() + RowDataUtil.OVER_ALLOCATE_SIZE];

        for ( int i = 0; i < array.size(); i++ ) {
          Object value = array.get( i );

          for ( int j = 0; j < m_subFields.size(); j++ ) {
            AvroInputMeta.AvroField sf = m_subFields.get( j );
            sf.reset( space );
            // what have we got
            if ( elementType.getType() == Schema.Type.RECORD ) {
              result[i][sf.m_outputIndex] =
                  sf.convertToKettleValue( (Record) value, elementType, defaultSchema, ignoreMissing );
            } else if ( elementType.getType() == Schema.Type.ARRAY ) {
              result[i][sf.m_outputIndex] =
                  sf.convertToKettleValue( (GenericData.Array) value, elementType, defaultSchema, ignoreMissing );
            } else if ( elementType.getType() == Schema.Type.MAP ) {
              result[i][sf.m_outputIndex] =
                  sf.convertToKettleValue( (Map<Utf8, Object>) value, elementType, defaultSchema, ignoreMissing );
            } else {
              // assume a primitive
              result[i][sf.m_outputIndex] = sf.getPrimitive( value, elementType );
            }
          }

        }
        return result;
      } else {
        int arrayI = 0;
        try {
          arrayI = Integer.parseInt( index.trim() );
        } catch ( NumberFormatException e ) {
          throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG,
              "AvroInput.Error.UnableToParseArrayIndex", index ) );
        }

        if ( arrayI >= array.size() || arrayI < 0 ) {

          // index is out of bounds
          Object[][] result = new Object[1][m_outputRowMeta.size() + RowDataUtil.OVER_ALLOCATE_SIZE];
          for ( int i = 0; i < m_subFields.size(); i++ ) {
            AvroInputMeta.AvroField sf = m_subFields.get( i );
            result[0][sf.m_outputIndex] = null;
          }

          return result;
        }

        Object value = array.get( arrayI );
        Schema elementType = s.getElementType();

        if ( elementType.getType() == Schema.Type.UNION ) {
          if ( value instanceof GenericContainer ) {
            // we can ask these things for their schema (covers
            // records, arrays, enums and fixed)
            elementType = ( (GenericContainer) value ).getSchema();
          } else {
            // either have a map or primitive here
            if ( value instanceof Map ) {
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
              // We shouldn't have a primitive here
              if ( !ignoreMissing ) {
                throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG,
                    "AvroInput.Error.EncounteredAPrimitivePriorToMapExpansion" ) );
              }
              Object[][] result = new Object[1][m_outputRowMeta.size() + RowDataUtil.OVER_ALLOCATE_SIZE];
              return result;
            }
          }
        }

        // what have we got?
        if ( elementType.getType() == Schema.Type.RECORD ) {
          return convertToKettleValues( (Record) value, elementType, defaultSchema, space, ignoreMissing );
        } else if ( elementType.getType() == Schema.Type.ARRAY ) {
          return convertToKettleValues( (GenericData.Array) value, elementType, defaultSchema, space, ignoreMissing );
        } else if ( elementType.getType() == Schema.Type.MAP ) {
          return convertToKettleValues( (Map<Utf8, Object>) value, elementType, defaultSchema, space, ignoreMissing );
        } else {
          // we shouldn't have a primitive at this point. If we are
          // extracting a particular index from the array then we're not to the
          // expansion phase,
          // so normally there must be a non-primitive sub-structure. Only if
          // the user is switching schema versions on a per-row basis or the
          // schema is a union at the top level could we end up here
          if ( !ignoreMissing ) {
            throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG,
                "AvroInput.Error.UnexpectedArrayElementTypeAtNonExpansionPoint" ) );
          } else {
            Object[][] result = new Object[1][m_outputRowMeta.size() + RowDataUtil.OVER_ALLOCATE_SIZE];
            return result;
          }
        }
      }
    }

    /**
     * Processes a record at this point in the path.
     *
     * @param record
     *          the record to process
     * @param s
     *          the current schema at this point in the path
     * @param space
     *          environment variables
     * @param ignoreMissing
     *          true if null is to be returned for user fields that don't appear in the schema
     * @return an array of Kettle rows corresponding to the expanded map/array and containing all leaf values as defined
     *         in the paths
     * @throws KettleException
     *           if a problem occurs
     */
    public Object[][] convertToKettleValues( Record record, Schema s, Schema defaultSchema, VariableSpace space,
        boolean ignoreMissing ) throws KettleException {

      if ( record == null ) {
        return null;
      }

      if ( m_tempParts.size() == 0 ) {
        throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG, "AvroInput.Error.MalformedPathRecord" ) );
      }

      String part = m_tempParts.remove( 0 );
      if ( part.charAt( 0 ) == '[' ) {
        throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG, "AvroInput.Error.InvalidPath" )
            + m_tempParts );
      }

      if ( part.indexOf( '[' ) > 0 ) {
        String arrayPart = part.substring( part.indexOf( '[' ) );
        part = part.substring( 0, part.indexOf( '[' ) );

        // put the array section back into location zero
        m_tempParts.add( 0, arrayPart );
      }

      // part is a named field of the record
      Schema.Field fieldS = s.getField( part );

      if ( fieldS == null ) {
        if ( !ignoreMissing ) {
          throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG, "AvroInput.Error.NonExistentField",
              part ) );
        }
      }

      Object field = record.get( part );

      if ( field == null ) {
        // field is null and we haven't hit the expansion yet. There will be
        // nothing
        // to return for all the sub-fields grouped in the expansion
        Object[][] result = new Object[1][m_outputRowMeta.size() + RowDataUtil.OVER_ALLOCATE_SIZE];
        return result;
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
            // We shouldn't have a primitive here
            if ( !ignoreMissing ) {
              throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG,
                  "AvroInput.Error.EncounteredAPrimitivePriorToMapExpansion" ) );
            }
            Object[][] result = new Object[1][m_outputRowMeta.size() + RowDataUtil.OVER_ALLOCATE_SIZE];
            return result;
          }
        }
      }

      // what have we got?
      if ( fieldT == Schema.Type.RECORD ) {
        return convertToKettleValues( (Record) field, fieldSchema, defaultSchema, space, ignoreMissing );
      } else if ( fieldT == Schema.Type.ARRAY ) {
        return convertToKettleValues( (GenericData.Array) field, fieldSchema, defaultSchema, space, ignoreMissing );
      } else if ( fieldT == Schema.Type.MAP ) {

        return convertToKettleValues( (Map<Utf8, Object>) field, fieldSchema, defaultSchema, space, ignoreMissing );
      } else {
        // primitives will always be handled by the subField delegates, so we
        // should'nt
        // get here
        throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG,
            "AvroInput.Error.UnexpectedRecordFieldTypeAtNonExpansionPoint" ) );
      }
    }
  }

  /**
   * Get the output row format
   *
   * @return the output row format
   */
  public RowMetaInterface getOutputRowMeta() {
    return m_outputRowMeta;
  }

  /**
   * Helper function that creates a field object once we've reached a leaf in the schema.
   *
   * @param path
   *          the path so far
   * @param s
   *          the schema for the primitive
   * @return an avro field object.
   */
  protected static AvroInputMeta.AvroField createAvroField( String path, Schema s, String namePrefix ) {
    AvroInputMeta.AvroField newField = new AvroInputMeta.AvroField();
    // newField.m_fieldName = s.getName(); // this will set the name to the
    // primitive type if the schema is for a primitive
    String fieldName = path;
    if ( !Const.isEmpty( namePrefix ) ) {
      fieldName = namePrefix;
    }
    newField.m_fieldName = fieldName; // set the name to the path, so that for
    // primitives within arrays we can at least
    // distinguish among them
    newField.m_fieldPath = path;
    switch ( s.getType() ) {
      case BOOLEAN:
        newField.m_kettleType = ValueMeta.getTypeDesc( ValueMetaInterface.TYPE_BOOLEAN );
        break;
      case ENUM:
      case STRING:
        newField.m_kettleType = ValueMeta.getTypeDesc( ValueMetaInterface.TYPE_STRING );
        if ( s.getType() == Schema.Type.ENUM ) {
          newField.m_indexedVals = s.getEnumSymbols();
        }

        break;
      case FLOAT:
      case DOUBLE:
        newField.m_kettleType = ValueMeta.getTypeDesc( ValueMetaInterface.TYPE_NUMBER );
        break;
      case INT:
      case LONG:
        newField.m_kettleType = ValueMeta.getTypeDesc( ValueMetaInterface.TYPE_INTEGER );
        break;
      case BYTES:
      case FIXED:
        newField.m_kettleType = ValueMeta.getTypeDesc( ValueMetaInterface.TYPE_BINARY );
        break;
      default:
        // unhandled type
        newField = null;
    }

    return newField;
  }

  /**
   * Helper function that checks the validity of a union. We can only handle unions that contain two elements: a type
   * and null.
   *
   * @param s
   *          the union schema to check
   * @return the type of the element that is not null.
   * @throws KettleException
   *           if a problem occurs.
   */
  protected static Schema checkUnion( Schema s ) throws KettleException {
    boolean ok = false;
    List<Schema> types = s.getTypes();

    // the type other than null
    Schema otherSchema = null;

    if ( types.size() != 2 ) {
      throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG, "AvroInput.Error.UnionError1" ) );
    }

    for ( Schema p : types ) {
      if ( p.getType() == Schema.Type.NULL ) {
        ok = true;
      } else {
        otherSchema = p;
      }
    }

    if ( !ok ) {
      throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG, "AvroInput.Error.UnionError2" ) );
    }

    return otherSchema;
  }

  /**
   * Check the supplied union for primitive/leaf types
   *
   * @param s
   *          the union schema to check
   * @return a list of primitive/leaf types in this union
   */
  protected static List<Schema> checkUnionForLeafTypes( Schema s ) {

    List<Schema> types = s.getTypes();
    List<Schema> primitives = new ArrayList<Schema>();

    for ( Schema toCheck : types ) {
      switch ( toCheck.getType() ) {
        case BOOLEAN:
        case LONG:
        case DOUBLE:
        case BYTES:
        case ENUM:
        case STRING:
        case INT:
        case FLOAT:
        case FIXED:
          primitives.add( toCheck );
          break;
      }
    }

    return primitives;
  }

  /**
   * Builds a list of field objects holding paths corresponding to the leaf primitives in an Avro schema.
   *
   * @param s
   *          the schema to process
   * @return a List of field objects
   * @throws KettleException
   *           if a problem occurs
   */
  protected static List<AvroInputMeta.AvroField> getLeafFields( Schema s ) throws KettleException {
    List<AvroInputMeta.AvroField> fields = new ArrayList<AvroInputMeta.AvroField>();

    String root = "$";

    if ( s.getType() == Schema.Type.ARRAY || s.getType() == Schema.Type.MAP ) {
      while ( s.getType() == Schema.Type.ARRAY || s.getType() == Schema.Type.MAP ) {
        if ( s.getType() == Schema.Type.ARRAY ) {
          root += "[0]";
          s = s.getElementType();
        } else {
          root += "[*key*]";
          s = s.getValueType();
        }
      }
    }

    if ( s.getType() == Schema.Type.RECORD ) {
      processRecord( root, s, fields, root );
    } else if ( s.getType() == Schema.Type.UNION ) {
      processUnion( root, s, fields, root );
    } else {

      // our top-level array/map structure bottoms out with primitive types
      // we'll create one zero-indexed path through to a primitive - the
      // user can copy and paste the path if they want to extract other
      // indexes out to separate Kettle fields
      AvroInputMeta.AvroField newField = createAvroField( root, s, null );
      if ( newField != null ) {
        fields.add( newField );
      }
    }

    return fields;
  }

  /**
   * Helper function used to build paths automatically when extracting leaf fields from a schema
   *
   * @param path
   *          the path so far
   * @param s
   *          the schema
   * @param fields
   *          a list of field objects that will correspond to leaf primitives
   * @throws KettleException
   *           if a problem occurs
   */
  protected static void processUnion( String path, Schema s, List<AvroInputMeta.AvroField> fields, String namePrefix )
    throws KettleException {

    boolean topLevelUnion = path.equals( "$" );

    // first check for the presence of primitive/leaf types in this union
    List<Schema> primitives = checkUnionForLeafTypes( s );
    if ( primitives.size() > 0 ) {
      // if there is exactly one primitive then we can set the kettle type
      // for this primitive's type. If there is more than one primitive
      // then we'll have to use String to cover them all
      if ( primitives.size() == 1 ) {
        Schema single = primitives.get( 0 );
        namePrefix = topLevelUnion ? single.getName() : namePrefix;
        AvroInputMeta.AvroField newField = createAvroField( path, single, namePrefix );
        if ( newField != null ) {
          fields.add( newField );
        }
      } else {
        Schema stringS = Schema.create( Schema.Type.STRING );
        AvroInputMeta.AvroField newField =
            createAvroField( path, stringS, topLevelUnion ? path + "union:primitive/fixed" : namePrefix );
        if ( newField != null ) {
          fields.add( newField );
        }
      }
    }

    // now scan for arrays, maps and records. Unions may not immediately contain
    // other unions (according to the spec)
    for ( Schema toCheck : s.getTypes() ) {
      if ( toCheck.getType() == Schema.Type.RECORD ) {
        String recordName = "[u:" + toCheck.getName() + "]";

        processRecord( path, toCheck, fields, namePrefix + recordName );
      } else if ( toCheck.getType() == Schema.Type.MAP ) {
        processMap( path + "[*key*]", toCheck, fields, namePrefix + "[*key*]" );
      } else if ( toCheck.getType() == Schema.Type.ARRAY ) {
        processArray( path + "[0]", toCheck, fields, namePrefix + "[0]" );
      }
    }
  }

  /**
   * Helper function used to build paths automatically when extracting leaf fields from a schema
   *
   * @param path
   *          the path so far
   * @param s
   *          the schema
   * @param fields
   *          a list of field objects that will correspond to leaf primitives
   * @throws KettleException
   *           if a problem occurs
   */
  protected static void processRecord( String path, Schema s, List<AvroInputMeta.AvroField> fields, String namePrefix )
    throws KettleException {

    List<Schema.Field> recordFields = s.getFields();
    for ( Schema.Field rField : recordFields ) {
      Schema rSchema = rField.schema();
      /*
       * if (rSchema.getType() == Schema.Type.UNION) { rSchema = checkUnion(rSchema); }
       */

      if ( rSchema.getType() == Schema.Type.UNION ) {
        processUnion( path + "." + rField.name(), rSchema, fields, namePrefix + "." + rField.name() );
      } else if ( rSchema.getType() == Schema.Type.RECORD ) {
        processRecord( path + "." + rField.name(), rSchema, fields, namePrefix + "." + rField.name() );
      } else if ( rSchema.getType() == Schema.Type.ARRAY ) {
        processArray( path + "." + rField.name() + "[0]", rSchema, fields, namePrefix + "." + rField.name() + "[0]" );
      } else if ( rSchema.getType() == Schema.Type.MAP ) {
        processMap( path + "." + rField.name() + "[*key*]", rSchema, fields, namePrefix + "." + rField.name()
            + "[*key*]" );
      } else {
        // primitive
        AvroInputMeta.AvroField newField =
            createAvroField( path + "." + rField.name(), rSchema, namePrefix + "." + rField.name() );
        if ( newField != null ) {
          fields.add( newField );
        }
      }
    }
  }

  /**
   * Helper function used to build paths automatically when extracting leaf fields from a schema
   *
   * @param path
   *          the path so far
   * @param s
   *          the schema
   * @param fields
   *          a list of field objects that will correspond to leaf primitives
   * @throws KettleException
   *           if a problem occurs
   */
  protected static void processMap( String path, Schema s, List<AvroInputMeta.AvroField> fields, String namePrefix )
    throws KettleException {

    s = s.getValueType(); // type of the values of the map

    if ( s.getType() == Schema.Type.UNION ) {
      processUnion( path, s, fields, namePrefix );
    } else if ( s.getType() == Schema.Type.ARRAY ) {
      processArray( path + "[0]", s, fields, namePrefix + "[0]" );
    } else if ( s.getType() == Schema.Type.RECORD ) {
      processRecord( path, s, fields, namePrefix );
    } else if ( s.getType() == Schema.Type.MAP ) {
      processMap( path + "[*key*]", s, fields, namePrefix + "[*key*]" );
    } else {
      AvroInputMeta.AvroField newField = createAvroField( path, s, namePrefix );
      if ( newField != null ) {
        fields.add( newField );
      }
    }
  }

  /**
   * Helper function used to build paths automatically when extracting leaf fields from a schema
   *
   * @param path
   *          the path so far
   * @param s
   *          the schema
   * @param fields
   *          a list of field objects that will correspond to leaf primitives
   * @throws KettleException
   *           if a problem occurs
   */
  protected static void processArray( String path, Schema s, List<AvroInputMeta.AvroField> fields, String namePrefix )
    throws KettleException {

    s = s.getElementType(); // type of the array elements

    if ( s.getType() == Schema.Type.UNION ) {
      processUnion( path, s, fields, namePrefix );
    } else if ( s.getType() == Schema.Type.ARRAY ) {
      processArray( path + "[0]", s, fields, namePrefix );
    } else if ( s.getType() == Schema.Type.RECORD ) {
      processRecord( path, s, fields, namePrefix );
    } else if ( s.getType() == Schema.Type.MAP ) {
      processMap( path + "[*key*]", s, fields, namePrefix + "[*key*]" );
    } else {
      AvroInputMeta.AvroField newField = createAvroField( path, s, namePrefix );
      if ( newField != null ) {
        fields.add( newField );
      }
    }
  }

  /**
   * Load a schema from a file
   *
   * @param schemaFile
   *          the file to load from
   * @return the schema
   * @throws KettleException
   *           if a problem occurs
   */
  protected static Schema loadSchema( String schemaFile ) throws KettleException {

    Schema s = null;
    Schema.Parser p = new Schema.Parser();

    FileObject fileO = KettleVFS.getFileObject( schemaFile );
    try {
      InputStream in = KettleVFS.getInputStream( fileO );
      s = p.parse( in );

      in.close();
    } catch ( FileSystemException e ) {
      throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG, "AvroInput.Error.SchemaError" ), e );
    } catch ( IOException e ) {
      throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG, "AvroInput.Error.SchemaError" ), e );
    }

    return s;
  }

  /**
   * Load a schema from a Avro container file
   *
   * @param containerFilename
   *          the name of the Avro container file
   * @return the schema
   * @throws KettleException
   *           if a problem occurs
   */
  protected static Schema loadSchemaFromContainer( String containerFilename ) throws KettleException {
    Schema s = null;

    FileObject fileO = KettleVFS.getFileObject( containerFilename );
    InputStream in = null;

    try {
      in = KettleVFS.getInputStream( fileO );
      GenericDatumReader dr = new GenericDatumReader();
      DataFileStream reader = new DataFileStream( in, dr );
      s = reader.getSchema();

      reader.close();
    } catch ( FileSystemException e ) {
      throw new KettleException( BaseMessages
          .getString( AvroInputMeta.PKG, "AvroInputDialog.Error.KettleFileException" ), e );
    } catch ( IOException e ) {
      throw new KettleException( BaseMessages
          .getString( AvroInputMeta.PKG, "AvroInputDialog.Error.KettleFileException" ), e );
    }

    return s;
  }

  /**
   * Set the output row format
   *
   * @param rmi
   *          the output row format
   */
  public void setOutputRowMeta( RowMetaInterface rmi ) {
    m_outputRowMeta = rmi;
  }

  /**
   * Performs initialization based on decoding from an incoming field.
   *
   * @param fieldNameToDecode
   *          name of the field to decode from
   * @param readerSchemaFile
   *          the reader schema file (must be supplied)
   * @param fields
   *          the user-supplied paths to extract
   * @param jsonEncoded
   *          true if the data is JSON encoded
   * @param newFieldOffset
   *          offset in the outgoing row format for extracted fields from any incoming kettle fields
   * @param schemaInField
   *          true if the schema to use on a row-by-row basis is contained in an incoming field value
   * @param schemaFieldName
   *          the name of the incoming field containing the schema
   * @param schemaFieldIsPath
   *          true if the incoming schema field values are actually paths to schemas rather than the schema itself
   * @param cacheSchemas
   *          true if schemas read from field values are to be cached in memory
   * @param ingoreMissing
   *          true if null is to be output for fields not found in the schema
   * @param log
   *          for logging
   * @throws KettleException
   */
  public void initializeFromFieldDecoding( String fieldNameToDecode, String readerSchemaFile,
      List<AvroInputMeta.AvroField> fields, boolean jsonEncoded, int newFieldOffset, boolean schemaInField,
      String schemaFieldName, boolean schemaFieldIsPath, boolean cacheSchemas, boolean ignoreMissing,
      LogChannelInterface log ) throws KettleException {

    m_log = log;
    m_decodingFromField = true;
    m_jsonEncoded = jsonEncoded;
    m_newFieldOffset = newFieldOffset;
    m_inStream = null;
    m_normalFields = new ArrayList<AvroInputMeta.AvroField>();
    m_cacheSchemas = cacheSchemas;
    m_schemaInField = schemaInField;
    m_dontComplainAboutMissingFields = ignoreMissing;

    for ( AvroInputMeta.AvroField f : fields ) {
      m_normalFields.add( f );
    }
    m_fieldToDecodeIndex = m_outputRowMeta.indexOfValue( fieldNameToDecode );

    if ( schemaInField ) {
      m_schemaFieldIndex = m_outputRowMeta.indexOfValue( schemaFieldName );
      if ( m_schemaFieldIndex < 0 ) {
        throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG,
            "AvroInput.Error.UnableToFindIncommingSchemaField" ) );
      }
      m_schemaFieldIsPath = schemaFieldIsPath;
    }

    if ( Const.isEmpty( readerSchemaFile ) ) {
      if ( !schemaInField ) {
        throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG, "AvroInput.Error.NoSchemaSupplied" ) );
      } else {
        if ( m_log.isBasic() ) {
          m_log.logBasic( BaseMessages.getString( AvroInputMeta.PKG, "AvroInput.Message.NoDefaultSchemaWarning" ) );
        }
      }
    }

    if ( !Const.isEmpty( readerSchemaFile ) ) {
      m_schemaToUse = loadSchema( readerSchemaFile );
      m_defaultSchema = m_schemaToUse;
      m_datumReader = new GenericDatumReader( m_schemaToUse );
      m_defaultDatumReader = m_datumReader;
    }

    m_factory = new DecoderFactory();

    init();
  }

  /**
   * Performs initialization based on the Avro file and schema provided.
   * <p>
   *
   * There are four possibilities:
   * <p>
   * <ol>
   * <li>No schema file provided and no fields defined - can only process a container file, under the assumption that
   * all leaf primitives are to be output</li>
   * <li>No schema file provided but fields/paths defined - can only process a container file, and assume that supplied
   * paths match schema</li>
   * <li>Schema file provided, no fields defined - output all leaf primitives from schema and have to determine if input
   * is a container file or just serialized data</li>
   * <li>Schema file provided and fields defined - output leaf primitives associated with paths. Have to determine if
   * file is container or not. If container, assume supplied schema overrides encapsulated schema</li>
   * </ol>
   *
   * @param avroFile
   *          the Avro file
   * @param readerSchemaFile
   *          the reader schema
   * @param fields
   *          the user-supplied paths to extract
   * @param jsonEncoded
   *          true if the data is JSON encoded
   * @param newFieldOffset
   *          offset in the outgoing row format for extracted fields from any incoming kettle fields
   * @param ignoreMissing
   *          if true output null for fields that don't appear in the schema
   * @param log
   *          the logger to use
   * @throws KettleException
   *           if a problem occurs
   */
  public void establishFileType( FileObject avroFile, String readerSchemaFile, List<AvroInputMeta.AvroField> fields,
      boolean jsonEncoded, int newFieldOffset, boolean ignoreMissing, LogChannelInterface log ) throws KettleException {

    m_log = log;
    m_newFieldOffset = newFieldOffset;
    m_normalFields = new ArrayList<AvroInputMeta.AvroField>();
    for ( AvroInputMeta.AvroField f : fields ) {
      m_normalFields.add( f );
    }
    m_inStream = null;
    m_jsonEncoded = jsonEncoded;
    m_dontComplainAboutMissingFields = ignoreMissing;

    try {
      m_inStream = KettleVFS.getInputStream( avroFile );
    } catch ( FileSystemException e1 ) {
      throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG, "AvroInput.Error.UnableToOpenAvroFile" ),
          e1 );
    }

    // load and handle reader schema....
    if ( !Const.isEmpty( readerSchemaFile ) ) {
      m_schemaToUse = loadSchema( readerSchemaFile );
      m_defaultSchema = m_schemaToUse;
    } else if ( jsonEncoded ) {
      throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG, "AvroInput.Error.NoSchemaProvided" ) );
    }

    m_datumReader = new GenericDatumReader();
    boolean nonContainer = false;

    if ( !jsonEncoded ) {
      try {
        m_containerReader = new DataFileStream( m_inStream, m_datumReader );
        m_writerSchema = m_containerReader.getSchema();

        // resolve reader/writer schemas
        if ( !Const.isEmpty( readerSchemaFile ) ) {
          // map any aliases for schema migration
          m_schemaToUse = Schema.applyAliases( m_writerSchema, m_schemaToUse );
        } else {
          m_schemaToUse = m_writerSchema;
        }
      } catch ( IOException e ) {
        // doesn't look like a container file....
        nonContainer = true;
        try {
          try {
            m_inStream.close();
          } catch ( IOException e1 ) {
            if ( log.isDebug() ) {
              log.logError( Const.getStackTracker( e1 ) );
            }
          }
          m_inStream = KettleVFS.getInputStream( avroFile );
        } catch ( FileSystemException e1 ) {
          throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG,
              "AvroInputDialog.Error.KettleFileException" ), e1 );
        }

        m_containerReader = null;
      }
    }

    if ( nonContainer || jsonEncoded ) {
      if ( Const.isEmpty( readerSchemaFile ) ) {
        throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG, "AvroInput.Error.NoSchema" ) );
      }

      m_factory = new DecoderFactory();
      if ( jsonEncoded ) {
        try {
          m_decoder = m_factory.jsonDecoder( m_schemaToUse, m_inStream );
        } catch ( IOException e ) {
          throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG, "AvroInput.Error.JsonDecoderError" ) );
        }
      } else {
        m_decoder = m_factory.binaryDecoder( m_inStream, null );
      }
      m_datumReader = new GenericDatumReader( m_schemaToUse );
      m_defaultDatumReader = m_datumReader;
    }

    init();
  }

  protected void initTopLevelStructure( Schema schema, boolean setDefault ) throws KettleException {
    // what top-level structure are we using?
    if ( schema.getType() == Schema.Type.RECORD ) {
      m_topLevelRecord = new Record( schema );
      if ( setDefault ) {
        m_defaultTopLevelObject = m_topLevelRecord;
      }
    } else if ( schema.getType() == Schema.Type.UNION ) {
      // ASSUMPTION: if the top level structure is a union then each
      // object we will read will be a record. We'll assume that any
      // non-record types in the top-level union are named types that
      // are referenced in the record types. We'll scan the union for the
      // first record type to construct our
      // our initial top-level object. When reading, the read method will give
      // us a new object (with appropriate schema) if this top level object's
      // schema does not match the schema of the record being currently read
      Schema firstUnion = null;
      for ( Schema uS : schema.getTypes() ) {
        if ( uS.getType() == Schema.Type.RECORD ) {
          firstUnion = uS;
          break;
        }
      }

      m_topLevelRecord = new Record( firstUnion );
      if ( setDefault ) {
        m_defaultTopLevelObject = m_topLevelRecord;
      }
    } else if ( schema.getType() == Schema.Type.ARRAY ) {
      m_topLevelArray = new GenericData.Array( 1, schema ); // capacity,
                                                            // schema
      if ( setDefault ) {
        m_defaultTopLevelObject = m_topLevelArray;
      }
    } else if ( schema.getType() == Schema.Type.MAP ) {
      m_topLevelMap = new HashMap<Utf8, Object>();
      if ( setDefault ) {
        m_defaultTopLevelObject = m_topLevelMap;
      }
    } else {
      throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG,
          "AvroInput.Error.UnsupportedTopLevelStructure" ) );
    }
  }

  protected void setTopLevelStructure( Object topLevel ) {
    if ( topLevel instanceof Record ) {
      m_topLevelRecord = (Record) topLevel;
      m_topLevelArray = null;
      m_topLevelMap = null;
    } else if ( topLevel instanceof GenericData.Array ) {
      m_topLevelArray = (GenericData.Array<?>) topLevel;
      m_topLevelRecord = null;
      m_topLevelMap = null;
    } else {
      m_topLevelMap = (HashMap<Utf8, Object>) topLevel;
      m_topLevelRecord = null;
      m_topLevelArray = null;
    }
  }

  protected void setSchemaToUse( String schemaKey, boolean useCache, VariableSpace space ) throws KettleException {

    if ( Const.isEmpty( schemaKey ) ) {
      // switch to default
      if ( m_defaultDatumReader == null ) {
        // no key, no default schema - can't continue with this row
        throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG,
            "AvroInput.Error.IncommingSchemaIsMissingAndNoDefault" ) );
      }
      if ( m_log.isDetailed() ) {
        m_log.logDetailed( BaseMessages.getString( AvroInputMeta.PKG, "AvroInput.Message.IncommingSchemaIsMissing" ) );
      }
      m_datumReader = m_defaultDatumReader;
      m_schemaToUse = m_datumReader.getSchema();
      setTopLevelStructure( m_defaultTopLevelObject );
      return;
    } else {
      schemaKey = schemaKey.trim();
      schemaKey = space.environmentSubstitute( schemaKey );
    }

    Object[] cached = null;
    if ( useCache ) {
      cached = m_schemaCache.get( schemaKey );
      if ( m_log.isDetailed() && cached != null ) {
        m_log.logDetailed(
            BaseMessages.getString( AvroInputMeta.PKG, "AvroInput.Message.UsingCachedSchema", schemaKey ) );
      }
    }

    if ( !useCache || cached == null ) {
      Schema toUse = null;
      if ( m_schemaFieldIsPath ) {
        // load the schema from disk
        if ( m_log.isDetailed() ) {
          m_log.logDetailed(
              BaseMessages.getString( AvroInputMeta.PKG, "AvroInput.Message.LoadingSchema", schemaKey ) );
        }
        try {
          toUse = loadSchema( schemaKey );
        } catch ( KettleException ex ) {
          // fall back to default (if possible)
          if ( m_defaultDatumReader != null ) {
            if ( m_log.isBasic() ) {
              m_log.logBasic( BaseMessages.getString( AvroInputMeta.PKG,
                  "AvroInput.Message.FailedToLoadSchmeaUsingDefault", schemaKey ) );
            }
            m_datumReader = m_defaultDatumReader;
            m_schemaToUse = m_datumReader.getSchema();
            setTopLevelStructure( m_defaultTopLevelObject );
            return;
          } else {
            throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG,
                "AvroInput.Error.CantLoadIncommingSchemaAndNoDefault", schemaKey ) );
          }
        }
      } else {
        // use the supplied schema
        if ( m_log.isDetailed() ) {
          m_log.logDetailed(
              BaseMessages.getString( AvroInputMeta.PKG, "AvroInput.Message.ParsingSchema", schemaKey ) );
        }
        Schema.Parser p = new Schema.Parser();
        toUse = p.parse( schemaKey );
      }
      m_schemaToUse = toUse;
      m_datumReader = new GenericDatumReader( toUse );
      initTopLevelStructure( toUse, false );
      if ( useCache ) {
        Object[] schemaInfo = new Object[2];
        schemaInfo[0] = m_datumReader;
        schemaInfo[1] =
            ( m_topLevelArray != null ) ? m_topLevelArray : ( ( m_topLevelRecord != null ) ? m_topLevelRecord
                : m_topLevelMap );
        if ( m_log.isDetailed() ) {
          m_log.logDetailed( BaseMessages.getString( AvroInputMeta.PKG, "AvroInput.Message.StoringSchemaInCache" ) );
        }
        m_schemaCache.put( schemaKey, schemaInfo );
      }
    } else if ( useCache ) {
      // got one from the cache
      m_datumReader = (GenericDatumReader) cached[0];
      m_schemaToUse = m_datumReader.getSchema();
      setTopLevelStructure( cached[1] );
    }
  }

  protected void init() throws KettleException {
    if ( m_schemaToUse != null ) {
      initTopLevelStructure( m_schemaToUse, true );
      // any fields specified by the user, or do we need to read all leaves
      // from the schema?
      if ( m_normalFields == null || m_normalFields.size() == 0 ) {
        m_normalFields = getLeafFields( m_schemaToUse );
      }
    }

    if ( m_normalFields == null || m_normalFields.size() == 0 ) {
      throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG, "AvroInput.Error.NoFieldPathsDefined" ) );
    }

    m_expansionHandler = checkFieldPaths( m_normalFields, m_outputRowMeta );

    for ( AvroInputMeta.AvroField f : m_normalFields ) {
      int outputIndex = m_outputRowMeta.indexOfValue( f.m_fieldName );
      f.init( outputIndex );
    }

    if ( m_expansionHandler != null ) {
      m_expansionHandler.init();
    }
  }

  /**
   * Examines the user-specified paths for the presence of a map/array expansion. If such an expansion is detected it
   * checks that it is valid and, if so, creates an expansion handler for processing it.
   *
   * @param normalFields
   *          the original user-specified paths. This is modified to contain only non-expansion paths.
   * @param outputRowMeta
   *          the output row format
   * @return an AvroArrayExpansion object to handle expansions or null if no expansions are present in the user-supplied
   *         path definitions.
   * @throws KettleException
   *           if a problem occurs
   */
  protected static AvroArrayExpansion checkFieldPaths( List<AvroInputMeta.AvroField> normalFields,
      RowMetaInterface outputRowMeta ) throws KettleException {
    // here we check whether there are any full map/array expansions
    // specified in the paths (via [*]). If so, we want to make sure
    // that only one is present across all paths. E.g. we can handle
    // multiple fields like $.person[*].first, $.person[*].last etc.
    // but not $.person[*].first, $.person[*].address[*].street.

    String expansion = null;
    List<AvroInputMeta.AvroField> normalList = new ArrayList<AvroInputMeta.AvroField>();
    List<AvroInputMeta.AvroField> expansionList = new ArrayList<AvroInputMeta.AvroField>();
    for ( AvroInputMeta.AvroField f : normalFields ) {
      String path = f.m_fieldPath;

      if ( path != null && path.lastIndexOf( "[*]" ) >= 0 ) {

        if ( path.indexOf( "[*]" ) != path.lastIndexOf( "[*]" ) ) {
          throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG,
              "AvroInput.Error.PathContainsMultipleExpansions", path ) );
        }
        String pathPart = path.substring( 0, path.lastIndexOf( "[*]" ) + 3 );

        if ( expansion == null ) {
          expansion = pathPart;
        } else {
          if ( !expansion.equals( pathPart ) ) {
            throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG,
                "AvroInput.Error.MutipleDifferentExpansions" ) );
          }
        }

        expansionList.add( f );
      } else {
        normalList.add( f );
      }
    }

    normalFields.clear();
    for ( AvroInputMeta.AvroField f : normalList ) {
      normalFields.add( f );
    }

    if ( expansionList.size() > 0 ) {

      List<AvroInputMeta.AvroField> subFields = new ArrayList<AvroInputMeta.AvroField>();

      for ( AvroInputMeta.AvroField ef : expansionList ) {
        AvroInputMeta.AvroField subField = new AvroInputMeta.AvroField();
        subField.m_fieldName = ef.m_fieldName;
        String path = ef.m_fieldPath;
        if ( path.charAt( path.length() - 2 ) == '*' ) {
          path = "dummy"; // pulling a primitive out of the map/array (path
                          // doesn't matter)
        } else {
          path = path.substring( path.lastIndexOf( "[*]" ) + 3, path.length() );
          path = "$" + path;
        }

        subField.m_fieldPath = path;
        subField.m_indexedVals = ef.m_indexedVals;
        subField.m_kettleType = ef.m_kettleType;

        subFields.add( subField );
      }

      AvroArrayExpansion exp = new AvroArrayExpansion( subFields );
      exp.m_expansionPath = expansion;
      exp.m_outputRowMeta = outputRowMeta;

      return exp;
    }

    return null;
  }

  private Object[][] setKettleFields( Object[] outputRowData, VariableSpace space ) throws KettleException {
    Object[][] result = null;

    // expand map/array in path structure to multiple rows (if necessary)
    if ( m_expansionHandler != null ) {
      m_expansionHandler.reset( space );

      if ( m_schemaToUse.getType() == Schema.Type.RECORD || m_schemaToUse.getType() == Schema.Type.UNION ) {
        // call getSchema() on the top level record here in case it has been
        // read as one of the elements from a top-level union
        result =
            m_expansionHandler.convertToKettleValues( m_topLevelRecord, m_topLevelRecord.getSchema(), m_defaultSchema, space,
                m_dontComplainAboutMissingFields );
      } else if ( m_schemaToUse.getType() == Schema.Type.ARRAY ) {
        result =
            m_expansionHandler.convertToKettleValues( m_topLevelArray, m_schemaToUse, m_defaultSchema, space,
                m_dontComplainAboutMissingFields );
      } else {
        result =
            m_expansionHandler.convertToKettleValues( m_topLevelMap, m_schemaToUse, m_defaultSchema, space,
                m_dontComplainAboutMissingFields );
      }
    } else {
      result = new Object[1][];
    }

    // if there are no incoming rows (i.e. we're decoding from a file rather
    // than a field
    if ( outputRowData == null ) {
      outputRowData = RowDataUtil.allocateRowData( m_outputRowMeta.size() );
    } else {
      // make sure we allocate enough space for the new fields
      outputRowData = RowDataUtil.resizeArray( outputRowData, m_outputRowMeta.size() );
    }

    // get the normal (non expansion-related fields)
    Object value = null;
    for ( AvroInputMeta.AvroField f : m_normalFields ) {
      f.reset( space );

      if ( m_schemaToUse.getType() == Schema.Type.RECORD || m_schemaToUse.getType() == Schema.Type.UNION ) {
        // call getSchema() on the top level record here in case it has been
        // read as one of the elements from a top-level union
        value =
            f.convertToKettleValue( m_topLevelRecord, m_topLevelRecord.getSchema(), m_defaultSchema, m_dontComplainAboutMissingFields );
      } else if ( m_schemaToUse.getType() == Schema.Type.ARRAY ) {
        value = f.convertToKettleValue( m_topLevelArray, m_schemaToUse, m_defaultSchema, m_dontComplainAboutMissingFields );
      } else {
        value = f.convertToKettleValue( m_topLevelMap, m_schemaToUse, m_defaultSchema, m_dontComplainAboutMissingFields );
      }

      outputRowData[f.m_outputIndex] = value;
    }

    // copy normal fields and existing incoming over to each expansion row (if
    // necessary)
    if ( m_expansionHandler == null ) {
      result[0] = outputRowData;
    } else if ( m_normalFields.size() > 0 || m_newFieldOffset > 0 ) {
      for ( int i = 0; i < result.length; i++ ) {
        Object[] row = result[i];

        // existing incoming fields
        for ( int j = 0; j < m_newFieldOffset; j++ ) {
          row[j] = outputRowData[j];
        }

        for ( AvroInputMeta.AvroField f : m_normalFields ) {
          row[f.m_outputIndex] = outputRowData[f.m_outputIndex];
        }
      }
    }

    return result;
  }

  /**
   * Converts an incoming row to outgoing format. Extracts fields from either an Avro object in the incoming row or from
   * the next structure in the container or non-container Avro file. May return more than one row if a map/array is
   * being expanded.
   *
   * @param incoming
   *          incoming kettle row - may be null if decoding from a file rather than a field
   * @param space
   *          the variables to use
   * @return one or more rows in the outgoing format
   * @throws KettleException
   *           if a problem occurs
   */
  public Object[][] avroObjectToKettle( Object[] incoming, VariableSpace space ) throws KettleException {

    if ( m_containerReader != null ) {
      // container file
      try {
        if ( m_containerReader.hasNext() ) {
          if ( m_topLevelRecord != null ) {
            // special case for top-level record. In case we actually
            // have a top level union, reassign the record so that
            // we have the correctly populated object in the case
            // where our last record instance can't be reused (i.e.
            // the next record read is a different one from the union
            // than the last one).
            m_topLevelRecord = (Record) m_containerReader.next( m_topLevelRecord );
          } else if ( m_topLevelArray != null ) {
            m_containerReader.next( m_topLevelArray );
          } else {
            m_containerReader.next( m_topLevelMap );
          }

          return setKettleFields( incoming, space );
        } else {
          return null; // no more input
        }
      } catch ( IOException e ) {
        throw new KettleException( BaseMessages.getString( AvroInputMeta.PKG, "AvroInput.Error.ObjectReadError" ) );
      }
    } else {
      // non-container file
      try {
        /*
         * if (m_decoder.isEnd()) { return null; }
         */

        // reading from an incoming field
        if ( m_decodingFromField ) {
          if ( incoming == null || incoming.length == 0 ) {
            // must be done - just return null
            return null;
          }
          ValueMetaInterface fieldMeta = m_outputRowMeta.getValueMeta( m_fieldToDecodeIndex );

          // incoming avro field null? - all decoded fields are null
          if ( fieldMeta.isNull( incoming[m_fieldToDecodeIndex] ) ) {
            Object[][] result = new Object[1][];
            // just resize the existing incoming array (if necessary) and return
            // the incoming values
            result[0] = RowDataUtil.resizeArray( incoming, m_outputRowMeta.size() );
            return result;
          }

          // if necessary, set the current datum reader and top level structure
          // for the incoming schema
          if ( m_schemaInField ) {
            ValueMetaInterface schemaMeta = m_outputRowMeta.getValueMeta( m_schemaFieldIndex );
            String schemaToUse = schemaMeta.getString( incoming[m_schemaFieldIndex] );
            setSchemaToUse( schemaToUse, m_cacheSchemas, space );
          }

          if ( m_jsonEncoded ) {
            try {
              String fieldValue = fieldMeta.getString( incoming[m_fieldToDecodeIndex] );
              m_decoder = m_factory.jsonDecoder( m_schemaToUse, fieldValue );
            } catch ( IOException e ) {
              throw new KettleException(
                  BaseMessages.getString( AvroInputMeta.PKG, "AvroInput.Error.JsonDecoderError" ) );
            }
          } else {
            byte[] fieldValue = fieldMeta.getBinary( incoming[m_fieldToDecodeIndex] );
            m_decoder = m_factory.binaryDecoder( fieldValue, null );
          }
        }

        if ( m_topLevelRecord != null ) {
          // special case for top-level record. In case we actually
          // have a top level union, reassign the record so that
          // we have the correctly populated object in the case
          // where our last record instance can't be reused (i.e.
          // the next record read is a different one from the union
          // than the last one).
          m_topLevelRecord = (Record) m_datumReader.read( m_topLevelRecord, m_decoder );
        } else if ( m_topLevelArray != null ) {
          m_datumReader.read( m_topLevelArray, m_decoder );
        } else {
          m_datumReader.read( m_topLevelMap, m_decoder );
        }

        return setKettleFields( incoming, space );
      } catch ( IOException ex ) {
        // some IO problem or no more input
        return null;
      }
    }
  }

  public void close() throws IOException {
    if ( m_containerReader != null ) {
      m_containerReader.close();
    }
    if ( m_inStream != null ) {
      m_inStream.close();
    }
  }
}
