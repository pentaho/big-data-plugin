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

package org.pentaho.big.data.kettle.plugins.formats.orc;

import org.junit.Test;
import org.pentaho.hadoop.shim.api.format.OrcSpec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class OrcInputFieldTest {

  @Test
  public void testSetOrcTypeByEnumToString() {
    // Test setting ORC type using enum.toString() format
    OrcInputField field = new OrcInputField();
    field.setOrcType( "BIGINT" );
    assertEquals( "BIGINT type should be set correctly", OrcSpec.DataType.BIGINT, field.getOrcType() );
    assertEquals( "Format type ID should match BIGINT", OrcSpec.DataType.BIGINT.getId(), field.getFormatType() );
  }

  @Test
  public void testSetOrcTypeByName() {
    // Test setting ORC type using getName() format
    OrcInputField field = new OrcInputField();
    field.setOrcType( "BigInt" );
    assertEquals( "BigInt name should be set correctly", OrcSpec.DataType.BIGINT, field.getOrcType() );
    assertEquals( "Format type ID should match BIGINT", OrcSpec.DataType.BIGINT.getId(), field.getFormatType() );
  }

  @Test
  public void testSetOrcTypeCaseInsensitive() {
    // Test that the method is case-insensitive
    OrcInputField field1 = new OrcInputField();
    field1.setOrcType( "string" );
    assertEquals( "Lowercase 'string' should work", OrcSpec.DataType.STRING, field1.getOrcType() );

    OrcInputField field2 = new OrcInputField();
    field2.setOrcType( "STRING" );
    assertEquals( "Uppercase 'STRING' should work", OrcSpec.DataType.STRING, field2.getOrcType() );

    OrcInputField field3 = new OrcInputField();
    field3.setOrcType( "StRiNg" );
    assertEquals( "Mixed case 'StRiNg' should work", OrcSpec.DataType.STRING, field3.getOrcType() );
  }

  @Test
  public void testSetOrcTypeAllDataTypes() {
    // Test all ORC data types can be set correctly
    for ( OrcSpec.DataType dataType : OrcSpec.DataType.values() ) {
      // Test by enum toString()
      OrcInputField field1 = new OrcInputField();
      field1.setOrcType( dataType.toString() );
      assertEquals( "Setting by toString: " + dataType, dataType, field1.getOrcType() );

      // Test by name
      OrcInputField field2 = new OrcInputField();
      field2.setOrcType( dataType.getName() );
      assertEquals( "Setting by name: " + dataType.getName(), dataType, field2.getOrcType() );
    }
  }

  @Test
  public void testSetOrcTypeInvalidValue() {
    // Test that invalid type doesn't change the field
    OrcInputField field = new OrcInputField();
    int initialFormatType = field.getFormatType();

    field.setOrcType( "INVALID_TYPE" );
    assertEquals( "Invalid type should not change formatType", initialFormatType, field.getFormatType() );
  }

  @Test
  public void testSetOrcTypeEmptyString() {
    // Test that empty string doesn't change the field
    OrcInputField field = new OrcInputField();
    int initialFormatType = field.getFormatType();

    field.setOrcType( "" );
    assertEquals( "Empty string should not change formatType", initialFormatType, field.getFormatType() );
  }

  @Test
  public void testSetOrcTypeNull() {
    // Test that null doesn't throw exception and doesn't change the field
    OrcInputField field = new OrcInputField();
    int initialFormatType = field.getFormatType();

    try {
      field.setOrcType( (String) null );
      assertEquals( "Null should not change formatType", initialFormatType, field.getFormatType() );
    } catch ( NullPointerException e ) {
      fail( "Null should not throw NullPointerException" );
    }
  }

  @Test
  public void testSetOrcTypeWithWhitespace() {
    // Test that types with leading/trailing whitespace don't match
    OrcInputField field = new OrcInputField();
    int initialFormatType = field.getFormatType();

    field.setOrcType( " STRING " );
    assertEquals( "Type with whitespace should not match", initialFormatType, field.getFormatType() );
  }

  @Test
  public void testSetOrcTypeTimestamp() {
    // Test specific type - TIMESTAMP
    OrcInputField field = new OrcInputField();
    field.setOrcType( "Timestamp" );
    assertEquals( "Timestamp type should be set correctly", OrcSpec.DataType.TIMESTAMP, field.getOrcType() );
  }

  @Test
  public void testSetOrcTypeDate() {
    // Test specific type - DATE
    OrcInputField field = new OrcInputField();
    field.setOrcType( "DATE" );
    assertEquals( "Date type should be set correctly", OrcSpec.DataType.DATE, field.getOrcType() );
  }

  @Test
  public void testSetOrcTypeDecimal() {
    // Test specific type - DECIMAL
    OrcInputField field = new OrcInputField();
    field.setOrcType( "decimal" );
    assertEquals( "Decimal type should be set correctly", OrcSpec.DataType.DECIMAL, field.getOrcType() );
  }

  @Test
  public void testSetOrcTypeBoolean() {
    // Test specific type - BOOLEAN
    OrcInputField field = new OrcInputField();
    field.setOrcType( "Boolean" );
    assertEquals( "Boolean type should be set correctly", OrcSpec.DataType.BOOLEAN, field.getOrcType() );
  }

  @Test
  public void testSetOrcTypeOverwrite() {
    // Test that setting type multiple times overwrites previous value
    OrcInputField field = new OrcInputField();

    field.setOrcType( "STRING" );
    assertEquals( "First type should be STRING", OrcSpec.DataType.STRING, field.getOrcType() );

    field.setOrcType( "INTEGER" );
    assertEquals( "Second type should overwrite to INTEGER", OrcSpec.DataType.INTEGER, field.getOrcType() );

    field.setOrcType( "DOUBLE" );
    assertEquals( "Third type should overwrite to DOUBLE", OrcSpec.DataType.DOUBLE, field.getOrcType() );
  }

  @Test
  public void testSetOrcTypeIntegerBothFormats() {
    // Test that INTEGER type can be set using both "Int" (display name) and "INTEGER" (enum name)

    // Test with "Int" (display name from getName())
    OrcInputField field1 = new OrcInputField();
    field1.setOrcType( "Int" );
    assertEquals( "Setting with 'Int' should result in INTEGER type", OrcSpec.DataType.INTEGER, field1.getOrcType() );
    assertEquals( "Format type ID should match INTEGER", OrcSpec.DataType.INTEGER.getId(), field1.getFormatType() );

    // Test with "INTEGER" (enum toString())
    OrcInputField field2 = new OrcInputField();
    field2.setOrcType( "INTEGER" );
    assertEquals( "Setting with 'INTEGER' should result in INTEGER type", OrcSpec.DataType.INTEGER, field2.getOrcType() );
    assertEquals( "Format type ID should match INTEGER", OrcSpec.DataType.INTEGER.getId(), field2.getFormatType() );

    // Test with lowercase "int"
    OrcInputField field3 = new OrcInputField();
    field3.setOrcType( "int" );
    assertEquals( "Setting with lowercase 'int' should result in INTEGER type", OrcSpec.DataType.INTEGER, field3.getOrcType() );
    assertEquals( "Format type ID should match INTEGER", OrcSpec.DataType.INTEGER.getId(), field3.getFormatType() );

    // Test with lowercase "integer"
    OrcInputField field4 = new OrcInputField();
    field4.setOrcType( "integer" );
    assertEquals( "Setting with lowercase 'integer' should result in INTEGER type", OrcSpec.DataType.INTEGER, field4.getOrcType() );
    assertEquals( "Format type ID should match INTEGER", OrcSpec.DataType.INTEGER.getId(), field4.getFormatType() );

    // Test with mixed case "InTeGeR"
    OrcInputField field5 = new OrcInputField();
    field5.setOrcType( "InTeGeR" );
    assertEquals( "Setting with mixed case 'InTeGeR' should result in INTEGER type", OrcSpec.DataType.INTEGER, field5.getOrcType() );
    assertEquals( "Format type ID should match INTEGER", OrcSpec.DataType.INTEGER.getId(), field5.getFormatType() );

    // Verify both result in the same type
    assertEquals( "Both 'Int' and 'INTEGER' should result in the same type", field1.getOrcType(), field2.getOrcType() );
    assertEquals( "Both should have the same format type ID", field1.getFormatType(), field2.getFormatType() );
  }
}

