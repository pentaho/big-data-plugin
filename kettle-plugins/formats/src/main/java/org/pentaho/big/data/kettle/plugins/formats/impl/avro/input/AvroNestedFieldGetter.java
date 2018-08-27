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
package org.pentaho.big.data.kettle.plugins.formats.impl.avro.input;

import org.apache.avro.Schema;
import org.pentaho.big.data.kettle.plugins.formats.avro.input.AvroInputField;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.shim.api.format.AvroSpec;
import org.pentaho.hadoop.shim.api.format.IAvroInputField;

import java.util.ArrayList;
import java.util.List;

public class AvroNestedFieldGetter {

  /**
   * Builds a list of field objects holding paths corresponding to the leaf primitives in an Avro schema.
   *
   * @param s the schema to process
   * @return a List of field objects
   * @throws KettleException if a problem occurs
   */
  public static List<? extends IAvroInputField> getLeafFields( Schema s ) throws KettleException {
    List<AvroInputField> fields = new ArrayList<AvroInputField>();

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
      AvroInputField newField = createAvroField( root, s, null );
      if ( newField != null ) {
        fields.add( newField );
      }
    }

    return fields;
  }

  /**
   * Helper function used to build paths automatically when extracting leaf fields from a schema
   *
   * @param path   the path so far
   * @param s      the schema
   * @param fields a list of field objects that will correspond to leaf primitives
   * @throws KettleException if a problem occurs
   */
  protected static void processRecord( String path, Schema s, List<AvroInputField> fields, String namePrefix )
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
        AvroInputField newField =
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
   * @param path   the path so far
   * @param s      the schema
   * @param fields a list of field objects that will correspond to leaf primitives
   * @throws KettleException if a problem occurs
   */
  protected static void processUnion( String path, Schema s, List<AvroInputField> fields, String namePrefix )
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
        AvroInputField newField = createAvroField( path, single, namePrefix );
        if ( newField != null ) {
          fields.add( newField );
        }
      } else {
        Schema stringS = Schema.create( Schema.Type.STRING );
        AvroInputField newField =
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
   * @param path   the path so far
   * @param s      the schema
   * @param fields a list of field objects that will correspond to leaf primitives
   * @throws KettleException if a problem occurs
   */
  protected static void processMap( String path, Schema s, List<AvroInputField> fields, String namePrefix )
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
      AvroInputField newField = createAvroField( path, s, namePrefix );
      if ( newField != null ) {
        fields.add( newField );
      }
    }
  }

  /**
   * Helper function used to build paths automatically when extracting leaf fields from a schema
   *
   * @param path   the path so far
   * @param s      the schema
   * @param fields a list of field objects that will correspond to leaf primitives
   * @throws KettleException if a problem occurs
   */
  protected static void processArray( String path, Schema s, List<AvroInputField> fields, String namePrefix )
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
      AvroInputField newField = createAvroField( path, s, namePrefix );
      if ( newField != null ) {
        fields.add( newField );
      }
    }
  }

  /**
   * Helper function that creates a field object once we've reached a leaf in the schema.
   *
   * @param path the path so far
   * @param s    the schema for the primitive
   * @return an avro field object.
   */
  protected static AvroInputField createAvroField( String path, Schema s, String namePrefix ) {
    AvroInputField newField = new AvroInputField();
    // newField.m_fieldName = s.getName(); // this will set the name to the
    // primitive type if the schema is for a primitive
    String fieldName = path;
    if ( !Const.isEmpty( namePrefix ) ) {
      fieldName = namePrefix;
    }
    newField.setAvroFieldName( fieldName ); // set the name to the path, so that for
    newField.setPentahoFieldName( fieldName );
    // primitives within arrays we can at least
    // distinguish among them
    newField.setAvroFieldName( path );
    switch ( s.getType() ) {
      case BOOLEAN:
        newField.setAvroType( AvroSpec.DataType.BOOLEAN );
        newField.setPentahoType(  ValueMetaInterface.TYPE_BOOLEAN );
        break;
      case ENUM:
      case STRING:
        newField.setPentahoType( ValueMetaInterface.TYPE_STRING );
        newField.setAvroType( AvroSpec.DataType.STRING );
        if ( s.getType() == Schema.Type.ENUM ) {
          //TODO: Fix this - newField.m_indexedVals = s.getEnumSymbols();
        }
        break;
      case FLOAT:
        newField.setAvroType( AvroSpec.DataType.FLOAT );
        newField.setPentahoType( ValueMetaInterface.TYPE_NUMBER );
        break;
      case DOUBLE:
        newField.setAvroType( AvroSpec.DataType.DOUBLE );
        newField.setPentahoType( ValueMetaInterface.TYPE_NUMBER );
        break;
      case INT:
        newField.setAvroType( AvroSpec.DataType.INTEGER );
        newField.setPentahoType( ValueMetaInterface.TYPE_INTEGER );
        break;
      case LONG:
        newField.setAvroType( AvroSpec.DataType.LONG );
        newField.setPentahoType( ValueMetaInterface.TYPE_INTEGER );
        break;
      case BYTES:
        newField.setAvroType( AvroSpec.DataType.BYTES );
        newField.setPentahoType( ValueMetaInterface.TYPE_BINARY  );
        break;
      case FIXED:
        newField.setAvroType( AvroSpec.DataType.FIXED );
        newField.setPentahoType( ValueMetaInterface.TYPE_BINARY  );
        break;
      default:
        // unhandled type
        newField = null;
    }

    return newField;
  }

  /**
   * Check the supplied union for primitive/leaf types
   *
   * @param s the union schema to check
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
}
