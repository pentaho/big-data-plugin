/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2024 by Hitachi Vantara : http://www.pentaho.com
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.pentaho.di.trans.steps.avroinput.AvroInputData.checkFieldPaths;
import static org.pentaho.di.trans.steps.avroinput.AvroInputData.getLeafFields;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaPluginType;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.trans.steps.avroinput.AvroInputData.AvroArrayExpansion;

/**
 * Unit tests for AvroInput. Tests basic path handling logic and map expansion mechanism.
 * 
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision: $
 */
public class AvroInputTest {

  protected static String s_schemaTopLevelRecordManyFields = "{" + "\"type\": \"record\"," + "\"name\": \"Test\","
      + "\"fields\": [" + "{\"name\": \"field1\", \"type\": \"string\"},"
      + "{\"name\": \"field2\", \"type\": \"string\"}," + "{\"name\": \"field3\", \"type\": \"string\"},"
      + "{\"name\": \"field4\", \"type\": \"string\"}," + "{\"name\": \"field5\", \"type\": \"string\"},"
      + "{\"name\": \"field6\", \"type\": \"string\"}," + "{\"name\": \"field7\", \"type\": \"string\"},"
      + "{\"name\": \"field8\", \"type\": \"string\"}," + "{\"name\": \"field9\", \"type\": \"string\"},"
      + "{\"name\": \"field10\", \"type\": \"string\"}," + "{\"name\": \"field11\", \"type\": \"string\"},"
      + "{\"name\": \"field12\", \"type\": \"string\"}" + "]" + "}";

  protected static String[] s_jsonDataTopLevelRecordManyFields =
      new String[] { "{\"field1\":\"value1\",\"field2\":\"value2\",\"field3\":\"value3\",\"field4\":\"value4\","
            + "\"field5\":\"value5\",\"field6\":\"value6\",\"field7\":\"value7\",\"field8\":\"value8\","
            + "\"field9\":\"value9\",\"field10\":\"value10\",\"field11\":\"value11\",\"field12\":\"value12\"}" };

  protected static String s_schemaTopLevelRecord = "{" + "\"type\": \"record\"," + "\"name\": \"Person\","
      + "\"fields\": [" + "{\"name\": \"name\", \"type\": \"string\"}," + "{\"name\": \"age\", \"type\": \"int\"},"
      + "{\"name\": \"emails\", \"type\": {\"type\": \"array\", \"items\": \"string\"}}" + "]" + "}";

  protected static String s_schemaTopLevelRecord2 = "{" + "\"type\": \"record\"," + "\"name\": \"Person\","
      + "\"fields\": [" + "{\"name\": \"name\", \"type\": \"string\"}," + "{\"name\": \"age\", \"type\": \"int\"},"
      + "{\"name\": \"nickname\", \"type\": \"string\"},"
      + "{\"name\": \"emails\", \"type\": {\"type\": \"array\", \"items\": \"string\"}}" + "]" + "}";

  protected static String[] s_jsonDataTopLevelRecord = new String[] {
      "{\"name\":\"bob\",\"age\":20,\"emails\":[\"here is an email\",\"and another one\"]}",
      "{\"name\":\"fred\",\"age\":25,\"emails\":[\"hi there bob\",\"good to see you!\",\"Yarghhh!\"]}",
      "{\"name\":\"zaphod\",\"age\":254,\"emails\":[\"I'm from beetlejuice\",\"yeah yeah yeah\"]}" };

  protected static String[] s_jsonDataTopLevelRecord2 =
      new String[] {
          "{\"name\":\"bob\",\"age\":20,\"nickname\":\"goofy\",\"emails\":[\"here is an email\",\"and another one\"]}",
          "{\"name\":\"fred\",\"age\":25,\"nickname\":\"mickey\",\"emails\":[\"hi there bob\",\"good to see you!\",\"Yarghhh!\"]}",
          "{\"name\":\"zaphod\",\"age\":254,\"nickname\":\"donald\",\"emails\":[\"I'm from beetlejuice\",\"yeah yeah yeah\"]}" };

  protected static String s_schemaTopLevelRecordWithUnion = "{" + "\"type\": \"record\"," + "\"name\": \"Person\","
      + "\"fields\": [" + "{\"name\": \"name\", \"type\": [\"string\", \"null\"]},"
      + "{\"name\": \"age\", \"type\": \"int\"},"
      + "{\"name\": \"emails\", \"type\": {\"type\": \"array\", \"items\": \"string\"}}" + "]" + "}";

  protected static String[] s_jsonDataTopLevelRecordWithUnion = new String[] {
      "{\"name\":{\"string\":\"bob\"},\"age\":20,\"emails\":[\"here is an email\",\"and another one\"]}",
      "{\"name\":{\"string\":\"fred\"},\"age\":25,\"emails\":[\"hi there bob\",\"good to see you!\",\"Yarghhh!\"]}",
      "{\"name\":null,\"age\":254,\"emails\":[\"I'm from beetlejuice\",\"yeah yeah yeah\"]}" };

  protected static String s_schemaTopLevelRecordWithMultiTypeUnion = "{" + "\"type\": \"record\","
      + "\"name\": \"Person\"," + "\"fields\": [" + "{\"name\": \"name\", \"type\": [\"string\", \"int\", \"null\"]},"
      + "{\"name\": \"age\", \"type\": \"int\"},"
      + "{\"name\": \"emails\", \"type\": {\"type\": \"array\", \"items\": \"string\"}}" + "]" + "}";

  protected static String[] s_jsonDataTopLevelRecordWithMultiTypeUnion = new String[] {
      "{\"name\":{\"string\":\"bob\"},\"age\":20,\"emails\":[\"here is an email\",\"and another one\"]}",
      "{\"name\":{\"int\":42},\"age\":25,\"emails\":[\"hi there bob\",\"good to see you!\",\"Yarghhh!\"]}",
      "{\"name\":null,\"age\":254,\"emails\":[\"I'm from beetlejuice\",\"yeah yeah yeah\"]}" };

  protected static String s_schemaTopLevelMap = "{" + "\"type\": \"map\"," + "\"values\":{" + "\"type\": \"record\","
      + "\"name\":\"person\"," + "\"fields\": [" + "{\"name\": \"name\", \"type\": \"string\"},"
      + "{\"name\": \"age\", \"type\": \"int\"},"
      + "{\"name\": \"emails\", \"type\": {\"type\": \"array\", \"items\": \"string\"}}" + "]" + "}" + "}";

  protected static String s_jsonDataTopLevelMap =
      "{\"bob\":{\"name\":\"bob\",\"age\":20,\"emails\":[\"here is an email\",\"and another one\"]},"
          + "\"fred\":{\"name\":\"fred\",\"age\":25,\"emails\":[\"hi there bob\",\"good to see you!\",\"Yarghhh!\"]},"
          + "\"zaphod\":{\"name\":\"zaphod\",\"age\":254,\"emails\":[\"I'm from beetlejuice\",\"yeah yeah yeah\"]}}";

  protected static String s_schemaTopLevelRecordWithFixedType = "{" + "\"type\": \"record\"," + "\"name\": \"Person\","
      + "\"fields\": [" + "{\"name\": \"name\", \"type\": {\"type\": \"fixed\", \"size\": 2, \"name\": \"myfixed\"}},"
      + "{\"name\": \"age\", \"type\": \"int\"},"
      + "{\"name\": \"emails\", \"type\": {\"type\": \"array\", \"items\": \"string\"}}" + "]" + "}";

  protected static String[] s_jsonDataTopLevelRecordWithFixedType =
      new String[] { "{\"name\":\"\\uFFFF\\uFFFF\",\"age\":20,\"emails\":[\"here is an email\",\"and another one\"]}" };

  protected static String s_schemaTopLevelEnumWithNamedType = "["
      + "{\"type\": \"fixed\", \"size\": 2, \"name\": \"myfixed\"}," + "{\"type\": \"record\","
      + "\"name\": \"Person\"," + "\"fields\": [" + "{\"name\": \"name\", \"type\": \"myfixed\"},"
      + "{\"name\": \"age\", \"type\": \"int\"},"
      + "{\"name\": \"emails\", \"type\": {\"type\": \"array\", \"items\": \"string\"}}" + "]" + "}]";

  protected static String[] s_jsonDataTopLevelUnion =
      new String[] { "{\"Person\": {\"name\":\"\\uFFFF\\uFFFF\",\"age\":20,\"emails\":[\"here is an email\",\"and another one\"]}}" };

  static {
    try {
      ValueMetaPluginType.getInstance().searchPlugins();
    } catch ( KettlePluginException ex ) {
      ex.printStackTrace();
    }
  }

  @Test
  public void testGetLeafFieldsFromSchema() throws KettleException {

    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse( s_schemaTopLevelRecord );
    List<AvroInputMeta.AvroField> leafFields = getLeafFields( schema );

    assertTrue( leafFields.size() == 3 );
  }

  @Test
  public void testGetSimpleTopLevelRecordFieldsInteger() throws KettleException, IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse( s_schemaTopLevelRecord );

    Decoder decoder;
    DecoderFactory factory = new DecoderFactory();

    GenericData.Record topLevel = new GenericData.Record( schema );
    GenericDatumReader reader = new GenericDatumReader( schema );

    AvroInputMeta.AvroField field = new AvroInputMeta.AvroField();

    field.m_fieldName = "test";
    field.m_fieldPath = "$.age";
    field.m_kettleType = ValueMeta.getTypeDesc( ValueMetaInterface.TYPE_INTEGER );

    Long[] actualVals = new Long[] { 20L, 25L, 254L };
    int i = 0;
    for ( String row : s_jsonDataTopLevelRecord ) {
      decoder = factory.jsonDecoder( schema, row );
      reader.read( topLevel, decoder );

      field.init( 0 ); // output index isn't needed for the test
      field.reset( new Variables() );

      Object result = field.convertToKettleValue( topLevel, schema, mock( Schema.class ), false );

      assertTrue( result != null );
      assertTrue( result instanceof Long );
      assertEquals( result, actualVals[i++] );
    }
  }

  @Test
  public void testGetSimpleTopLevelRecordFieldsString() throws KettleException, IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse( s_schemaTopLevelRecord );

    Decoder decoder;
    DecoderFactory factory = new DecoderFactory();

    GenericData.Record topLevel = new GenericData.Record( schema );
    GenericDatumReader reader = new GenericDatumReader( schema );

    AvroInputMeta.AvroField field = new AvroInputMeta.AvroField();
    field.m_fieldName = "test";
    field.m_fieldPath = "$.name";
    field.m_kettleType = ValueMeta.getTypeDesc( ValueMetaInterface.TYPE_STRING );

    String[] actualVals = new String[] { "bob", "fred", "zaphod" };
    int i = 0;
    for ( String row : s_jsonDataTopLevelRecord ) {
      decoder = factory.jsonDecoder( schema, row );
      reader.read( topLevel, decoder );

      field.init( 0 ); // output index isn't needed for the test
      field.reset( new Variables() );

      Object result = field.convertToKettleValue( topLevel, schema, mock( Schema.class ), false );

      assertTrue( result != null );
      assertTrue( result instanceof String );
      assertEquals( result.toString(), actualVals[i++] );
    }
  }

  @Test
  public void testTopLevelRecordWithFixedType() throws KettleException, IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse( s_schemaTopLevelRecordWithFixedType );

    Decoder decoder;
    DecoderFactory factory = new DecoderFactory();

    GenericData.Record topLevel = new GenericData.Record( schema );
    GenericDatumReader reader = new GenericDatumReader( schema );

    AvroInputMeta.AvroField field = new AvroInputMeta.AvroField();
    field.m_fieldName = "test";
    field.m_fieldPath = "$.name";
    field.m_kettleType = ValueMeta.getTypeDesc( ValueMetaInterface.TYPE_BINARY );

    // String[] actualVals = new String[] { "bob", "fred", "zaphod" };
    int i = 0;
    for ( String row : s_jsonDataTopLevelRecordWithFixedType ) {
      decoder = factory.jsonDecoder( schema, row );
      reader.read( topLevel, decoder );

      field.init( 0 ); // output index isn't needed for the test
      field.reset( new Variables() );

      Object result = field.convertToKettleValue( topLevel, schema, mock( Schema.class ), false );

      assertTrue( result != null );
      assertTrue( result instanceof byte[] );
      assertEquals( ( (byte[]) result ).length, 2 );
      assertEquals( new String( (byte[]) result ), "??" );
    }
  }

  @Test
  public void testSchemaWithTopLevelUnionAndNamedType() throws KettleException, IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse( s_schemaTopLevelEnumWithNamedType );

    Decoder decoder;
    DecoderFactory factory = new DecoderFactory();

    GenericDatumReader reader = new GenericDatumReader( schema );
    Schema firstRec = schema.getTypes().get( 1 );
    GenericData.Record topLevel = new GenericData.Record( firstRec );

    AvroInputMeta.AvroField field = new AvroInputMeta.AvroField();
    field.m_fieldName = "test";
    field.m_fieldPath = "$.name";
    field.m_kettleType = ValueMeta.getTypeDesc( ValueMetaInterface.TYPE_BINARY );

    int i = 0;
    for ( String row : s_jsonDataTopLevelUnion ) {
      decoder = factory.jsonDecoder( schema, row );
      reader.read( topLevel, decoder );

      field.init( 0 ); // output index isn't needed for the test
      field.reset( new Variables() );

      // invoke the conversion using the schema of the record just read
      Object result = field.convertToKettleValue( topLevel, topLevel.getSchema(), mock( Schema.class ), false );

      assertTrue( result != null );
      assertTrue( result instanceof byte[] );
      assertEquals( ( (byte[]) result ).length, 2 );
      assertEquals( new String( (byte[]) result ), "??" );
    }
  }

  @Test
  public void testDecodeUsingSchemaInIncomingField() throws KettleException {
    Schema.Parser parser = new Schema.Parser();
    Schema defaultSchema = parser.parse( s_schemaTopLevelRecord );

    // Decoder decoder;
    DecoderFactory factory = new DecoderFactory();

    GenericData.Record topLevel = new GenericData.Record( defaultSchema );
    GenericDatumReader reader = new GenericDatumReader( defaultSchema );

    List<AvroInputMeta.AvroField> paths = new ArrayList<AvroInputMeta.AvroField>();
    AvroInputMeta.AvroField field = new AvroInputMeta.AvroField();
    field.m_fieldName = "test";
    field.m_fieldPath = "$.name";
    field.m_kettleType = ValueMeta.getTypeDesc( ValueMetaInterface.TYPE_STRING );
    paths.add( field );

    Object[] incomingKettleRow = new Object[2];
    VariableSpace space = new Variables();
    RowMetaInterface outputMeta = new RowMeta();
    ValueMetaInterface vm = new ValueMeta();
    vm.setName( "IncomingAvro" );
    vm.setOrigin( "Dummy" );
    vm.setType( ValueMetaInterface.TYPE_STRING );
    outputMeta.addValueMeta( vm );
    vm = new ValueMeta();
    vm.setName( "SchemaToUse" );
    vm.setOrigin( "Dummy" );
    vm.setType( ValueMetaInterface.TYPE_STRING );
    outputMeta.addValueMeta( vm );

    vm = new ValueMeta();
    vm.setName( field.m_fieldName );
    vm.setOrigin( "Dummy" );
    vm.setType( ValueMetaInterface.TYPE_STRING );
    outputMeta.addValueMeta( vm );

    AvroInputData data = new AvroInputData();
    data.m_normalFields = paths;
    data.m_decodingFromField = true;
    data.m_jsonEncoded = true;
    data.m_newFieldOffset = 2;
    data.m_inStream = null;
    data.m_fieldToDecodeIndex = 0;
    data.m_schemaToUse = defaultSchema;
    data.m_defaultSchema = defaultSchema;
    data.m_topLevelRecord = topLevel;
    data.m_factory = factory;
    data.m_datumReader = reader;
    data.m_defaultDatumReader = reader;
    data.m_outputRowMeta = outputMeta;
    data.m_schemaInField = true;
    data.m_schemaFieldIndex = 1;
    data.m_log = new LogChannel( this );
    data.init();

    String[] expectedNames = { "bob", "fred", "zaphod" };
    int count = 0;
    for ( String row : s_jsonDataTopLevelRecord ) {
      incomingKettleRow[0] = row;
      incomingKettleRow[1] = s_schemaTopLevelRecord;

      Object[][] result = data.avroObjectToKettle( incomingKettleRow, space );
      assertTrue( result != null );
      assertTrue( result.length == 1 ); // one output row
      assertEquals( result[0][2].toString(), expectedNames[count] );
      count++;
    }
  }

  @Test
  public void testDecodeUsingSchemaInIncomingFieldIncompatibleSchema() throws KettleException {
    Schema.Parser parser = new Schema.Parser();
    Schema defaultSchema = parser.parse( s_schemaTopLevelRecord );

    // Decoder decoder;
    DecoderFactory factory = new DecoderFactory();

    GenericData.Record topLevel = new GenericData.Record( defaultSchema );
    GenericDatumReader reader = new GenericDatumReader( defaultSchema );

    List<AvroInputMeta.AvroField> paths = new ArrayList<AvroInputMeta.AvroField>();
    AvroInputMeta.AvroField field = new AvroInputMeta.AvroField();
    field.m_fieldName = "test";
    field.m_fieldPath = "$.name";
    field.m_kettleType = ValueMeta.getTypeDesc( ValueMetaInterface.TYPE_STRING );
    paths.add( field );

    Object[] incomingKettleRow = new Object[2];
    VariableSpace space = new Variables();
    RowMetaInterface outputMeta = new RowMeta();
    ValueMetaInterface vm = new ValueMeta();
    vm.setName( "IncomingAvro" );
    vm.setOrigin( "Dummy" );
    vm.setType( ValueMetaInterface.TYPE_STRING );
    outputMeta.addValueMeta( vm );
    vm = new ValueMeta();
    vm.setName( "SchemaToUse" );
    vm.setOrigin( "Dummy" );
    vm.setType( ValueMetaInterface.TYPE_STRING );
    outputMeta.addValueMeta( vm );

    vm = new ValueMeta();
    vm.setName( field.m_fieldName );
    vm.setOrigin( "Dummy" );
    vm.setType( ValueMetaInterface.TYPE_STRING );
    outputMeta.addValueMeta( vm );

    AvroInputData data = new AvroInputData();
    data.m_normalFields = paths;
    data.m_decodingFromField = true;
    data.m_jsonEncoded = true;
    data.m_newFieldOffset = 2;
    data.m_inStream = null;
    data.m_fieldToDecodeIndex = 0;
    data.m_schemaToUse = defaultSchema;
    data.m_defaultSchema = defaultSchema;
    data.m_topLevelRecord = topLevel;
    data.m_factory = factory;
    data.m_datumReader = reader;
    data.m_defaultDatumReader = reader;
    data.m_outputRowMeta = outputMeta;
    data.m_schemaInField = true;
    data.m_schemaFieldIndex = 1;
    data.m_log = new LogChannel( this );
    data.init();

    String[] expectedNames = { "bob", "fred", "zaphod" };
    int count = 0;
    String row = s_jsonDataTopLevelRecord[0];
    incomingKettleRow[0] = row;
    incomingKettleRow[1] = s_schemaTopLevelRecord2;

    try {
      Object[][] result = data.avroObjectToKettle( incomingKettleRow, space );
      fail( "Was expecting an exception as the schema supplied is incompatible with the data" );
    } catch ( Exception ex ) {
      //expected
    }
  }

  @Test
  public void testDecodeUsingSchemaInIncomingFieldCacheSchemas() throws KettleException {
    Schema.Parser parser = new Schema.Parser();
    Schema defaultSchema = parser.parse( s_schemaTopLevelRecord );

    // Decoder decoder;
    DecoderFactory factory = new DecoderFactory();

    GenericData.Record topLevel = new GenericData.Record( defaultSchema );
    GenericDatumReader reader = new GenericDatumReader( defaultSchema );

    List<AvroInputMeta.AvroField> paths = new ArrayList<AvroInputMeta.AvroField>();
    AvroInputMeta.AvroField field = new AvroInputMeta.AvroField();
    field.m_fieldName = "test";
    field.m_fieldPath = "$.name";
    field.m_kettleType = ValueMeta.getTypeDesc( ValueMetaInterface.TYPE_STRING );
    paths.add( field );

    Object[] incomingKettleRow = new Object[2];
    VariableSpace space = new Variables();
    RowMetaInterface outputMeta = new RowMeta();
    ValueMetaInterface vm = new ValueMeta();
    vm.setName( "IncomingAvro" );
    vm.setOrigin( "Dummy" );
    vm.setType( ValueMetaInterface.TYPE_STRING );
    outputMeta.addValueMeta( vm );
    vm = new ValueMeta();
    vm.setName( "SchemaToUse" );
    vm.setOrigin( "Dummy" );
    vm.setType( ValueMetaInterface.TYPE_STRING );
    outputMeta.addValueMeta( vm );

    vm = new ValueMeta();
    vm.setName( field.m_fieldName );
    vm.setOrigin( "Dummy" );
    vm.setType( ValueMetaInterface.TYPE_STRING );
    outputMeta.addValueMeta( vm );

    AvroInputData data = new AvroInputData();
    data.m_normalFields = paths;
    data.m_decodingFromField = true;
    data.m_jsonEncoded = true;
    data.m_newFieldOffset = 2;
    data.m_inStream = null;
    data.m_fieldToDecodeIndex = 0;
    data.m_schemaToUse = defaultSchema;
    data.m_defaultSchema = defaultSchema;
    data.m_topLevelRecord = topLevel;
    data.m_factory = factory;
    data.m_datumReader = reader;
    data.m_defaultDatumReader = reader;
    data.m_outputRowMeta = outputMeta;
    data.m_cacheSchemas = true;
    data.m_schemaInField = true;
    data.m_schemaFieldIndex = 1;
    data.m_log = new LogChannel( this );
    data.init();

    String[] expectedNames = { "bob", "fred", "zaphod" };
    int count = 0;
    for ( String row : s_jsonDataTopLevelRecord ) {
      incomingKettleRow[0] = row;
      incomingKettleRow[1] = s_schemaTopLevelRecord;

      Object[][] result = data.avroObjectToKettle( incomingKettleRow, space );
      assertTrue( result != null );
      assertTrue( result.length == 1 ); // one output row
      assertEquals( result[0][2].toString(), expectedNames[count] );
      count++;
    }

    // should be just one entry in the cache since all rows have the same schema
    assertTrue( data.m_schemaCache != null );
    assertTrue( data.m_schemaCache.size() == 1 );
  }

  @Test
  public void testDecodeUsingSchemaInIncomingFieldTwoDifferentSchemas() throws KettleException {
    Schema.Parser parser = new Schema.Parser();
    Schema defaultSchema = parser.parse( s_schemaTopLevelRecord );

    // Decoder decoder;
    DecoderFactory factory = new DecoderFactory();

    GenericData.Record topLevel = new GenericData.Record( defaultSchema );
    GenericDatumReader reader = new GenericDatumReader( defaultSchema );

    List<AvroInputMeta.AvroField> paths = new ArrayList<AvroInputMeta.AvroField>();
    AvroInputMeta.AvroField field = new AvroInputMeta.AvroField();
    field.m_fieldName = "test";
    field.m_fieldPath = "$.name";
    field.m_kettleType = ValueMeta.getTypeDesc( ValueMetaInterface.TYPE_STRING );
    paths.add( field );

    Object[] incomingKettleRow = new Object[2];
    VariableSpace space = new Variables();
    RowMetaInterface outputMeta = new RowMeta();
    ValueMetaInterface vm = new ValueMeta();
    vm.setName( "IncomingAvro" );
    vm.setOrigin( "Dummy" );
    vm.setType( ValueMetaInterface.TYPE_STRING );
    outputMeta.addValueMeta( vm );
    vm = new ValueMeta();
    vm.setName( "SchemaToUse" );
    vm.setOrigin( "Dummy" );
    vm.setType( ValueMetaInterface.TYPE_STRING );
    outputMeta.addValueMeta( vm );

    vm = new ValueMeta();
    vm.setName( field.m_fieldName );
    vm.setOrigin( "Dummy" );
    vm.setType( ValueMetaInterface.TYPE_STRING );
    outputMeta.addValueMeta( vm );

    AvroInputData data = new AvroInputData();
    data.m_normalFields = paths;
    data.m_decodingFromField = true;
    data.m_jsonEncoded = true;
    data.m_newFieldOffset = 2;
    data.m_inStream = null;
    data.m_fieldToDecodeIndex = 0;
    data.m_schemaToUse = defaultSchema;
    data.m_defaultSchema = defaultSchema;
    data.m_topLevelRecord = topLevel;
    data.m_factory = factory;
    data.m_datumReader = reader;
    data.m_defaultDatumReader = reader;
    data.m_outputRowMeta = outputMeta;
    data.m_schemaInField = true;
    data.m_schemaFieldIndex = 1;
    data.m_log = new LogChannel( this );
    data.init();

    String[] expectedNames = { "bob", "fred", "zaphod" };
    for ( int i = 0; i < 3; i++ ) {
      String row = null;
      String schema = null;

      // first schema for first and last row
      if ( i == 0 || i == 2 ) {
        row = s_jsonDataTopLevelRecord[i];
        schema = s_schemaTopLevelRecord;
      } else {
        row = s_jsonDataTopLevelRecord2[i];
        schema = s_schemaTopLevelRecord2;
      }

      incomingKettleRow[0] = row;
      incomingKettleRow[1] = schema;

      Object[][] result = data.avroObjectToKettle( incomingKettleRow, space );
      assertTrue( result != null );
      assertTrue( result.length == 1 ); // one output row
      assertEquals( result[0][2].toString(), expectedNames[i] );
    }
  }

  @Test
  public void testDecodeUsingSchemaInIncomingFieldTwoDifferentSchemasDontComplainAboutMissingField()
    throws KettleException {
    Schema.Parser parser = new Schema.Parser();
    Schema defaultSchema = parser.parse( s_schemaTopLevelRecord );

    // Decoder decoder;
    DecoderFactory factory = new DecoderFactory();

    GenericData.Record topLevel = new GenericData.Record( defaultSchema );
    GenericDatumReader reader = new GenericDatumReader( defaultSchema );

    List<AvroInputMeta.AvroField> paths = new ArrayList<AvroInputMeta.AvroField>();
    AvroInputMeta.AvroField field = new AvroInputMeta.AvroField();
    field.m_fieldName = "test";
    field.m_fieldPath = "$.name";
    field.m_kettleType = ValueMeta.getTypeDesc( ValueMetaInterface.TYPE_STRING );
    paths.add( field );


    Object[] incomingKettleRow = new Object[2];
    VariableSpace space = new Variables();
    RowMetaInterface outputMeta = new RowMeta();
    ValueMetaInterface vm = new ValueMeta();
    vm.setName( "IncomingAvro" );
    vm.setOrigin( "Dummy" );
    vm.setType( ValueMetaInterface.TYPE_STRING );
    outputMeta.addValueMeta( vm );
    vm = new ValueMeta();
    vm.setName( "SchemaToUse" );
    vm.setOrigin( "Dummy" );
    vm.setType( ValueMetaInterface.TYPE_STRING );
    outputMeta.addValueMeta( vm );

    vm = new ValueMeta();
    vm.setName( field.m_fieldName );
    vm.setOrigin( "Dummy" );
    vm.setType( ValueMetaInterface.TYPE_STRING );
    outputMeta.addValueMeta( vm );

    AvroInputData data = new AvroInputData();
    data.m_normalFields = paths;
    data.m_decodingFromField = true;
    data.m_jsonEncoded = true;
    data.m_newFieldOffset = 2;
    data.m_inStream = null;
    data.m_fieldToDecodeIndex = 0;
    data.m_schemaToUse = defaultSchema;
    data.m_defaultSchema = defaultSchema;
    data.m_topLevelRecord = topLevel;
    data.m_factory = factory;
    data.m_datumReader = reader;
    data.m_defaultDatumReader = reader;
    data.m_outputRowMeta = outputMeta;
    data.m_schemaInField = true;
    data.m_schemaFieldIndex = 1;
    data.m_dontComplainAboutMissingFields = true;
    data.m_log = new LogChannel( this );
    data.init();

    String[] expectedNames = { "bob", "fred", "zaphod" };
    for ( int i = 0; i < 3; i++ ) {
      String row = null;
      String schema = null;

      // first schema for first and last row
      if ( i == 0 || i == 2 ) {
        row = s_jsonDataTopLevelRecord[i];
        schema = s_schemaTopLevelRecord;
      } else {
        row = s_jsonDataTopLevelRecord2[i];
        schema = s_schemaTopLevelRecord2;
      }

      incomingKettleRow[0] = row;
      incomingKettleRow[1] = schema;

      Object[][] result = data.avroObjectToKettle( incomingKettleRow, space );
      assertTrue( result != null );
      assertTrue( result.length == 1 ); // one output row
      assertEquals( result[0][2].toString(), expectedNames[i] );
    }
  }

  @Test
  public void testDecodeUsingSchemaInIncomingFieldFallbackToDefaultSchema() throws KettleException {
    Schema.Parser parser = new Schema.Parser();
    Schema defaultSchema = parser.parse( s_schemaTopLevelRecord );

    // Decoder decoder;
    DecoderFactory factory = new DecoderFactory();

    GenericData.Record topLevel = new GenericData.Record( defaultSchema );
    GenericDatumReader reader = new GenericDatumReader( defaultSchema );

    List<AvroInputMeta.AvroField> paths = new ArrayList<AvroInputMeta.AvroField>();
    AvroInputMeta.AvroField field = new AvroInputMeta.AvroField();
    field.m_fieldName = "test";
    field.m_fieldPath = "$.name";
    field.m_kettleType = ValueMeta.getTypeDesc( ValueMetaInterface.TYPE_STRING );
    paths.add( field );

    Object[] incomingKettleRow = new Object[2];
    VariableSpace space = new Variables();
    RowMetaInterface outputMeta = new RowMeta();
    ValueMetaInterface vm = new ValueMeta();
    vm.setName( "IncomingAvro" );
    vm.setOrigin( "Dummy" );
    vm.setType( ValueMetaInterface.TYPE_STRING );
    outputMeta.addValueMeta( vm );
    vm = new ValueMeta();
    vm.setName( "SchemaToUse" );
    vm.setOrigin( "Dummy" );
    vm.setType( ValueMetaInterface.TYPE_STRING );
    outputMeta.addValueMeta( vm );

    vm = new ValueMeta();
    vm.setName( field.m_fieldName );
    vm.setOrigin( "Dummy" );
    vm.setType( ValueMetaInterface.TYPE_STRING );
    outputMeta.addValueMeta( vm );

    AvroInputData data = new AvroInputData();
    data.m_normalFields = paths;
    data.m_decodingFromField = true;
    data.m_jsonEncoded = true;
    data.m_newFieldOffset = 2;
    data.m_inStream = null;
    data.m_fieldToDecodeIndex = 0;
    data.m_schemaToUse = defaultSchema;
    data.m_defaultSchema = defaultSchema;
    data.m_topLevelRecord = topLevel;
    data.m_factory = factory;
    data.m_datumReader = reader;
    data.m_defaultDatumReader = reader;
    data.m_outputRowMeta = outputMeta;
    data.m_schemaInField = true;
    data.m_schemaFieldIndex = 1;
    data.m_log = new LogChannel( this );
    data.init();

    int count = 0;
    String[] expectedNames = { "bob", "fred", "zaphod" };
    for ( String row : s_jsonDataTopLevelRecord ) {
      incomingKettleRow[0] = row;

      // no incoming schema for row 2 - should successfully fall back to the
      // default schema
      if ( count == 1 ) {
        incomingKettleRow[1] = null;
      } else {
        incomingKettleRow[1] = s_schemaTopLevelRecord;
      }

      Object[][] result = data.avroObjectToKettle( incomingKettleRow, space );
      assertTrue( result != null );
      assertTrue( result.length == 1 ); // one output row
      assertEquals( result[0][2].toString(), expectedNames[count] );
      count++;
    }
  }

  @Test
  public void testConvertToKettleRowManyFields() throws KettleException, IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse( s_schemaTopLevelRecordManyFields );

    // Decoder decoder;
    DecoderFactory factory = new DecoderFactory();

    GenericData.Record topLevel = new GenericData.Record( schema );
    GenericDatumReader reader = new GenericDatumReader( schema );

    List<AvroInputMeta.AvroField> paths = new ArrayList<AvroInputMeta.AvroField>();
    for ( int i = 1; i <= 12; i++ ) {
      AvroInputMeta.AvroField field = new AvroInputMeta.AvroField();
      field.m_fieldName = "test" + i;
      field.m_fieldPath = "$.field" + i;
      field.m_kettleType = ValueMeta.getTypeDesc( ValueMetaInterface.TYPE_STRING );
      paths.add( field );
    }

    String incomingAvro = s_jsonDataTopLevelRecordManyFields[0];
    Object[] incomingKettleRow = new Object[1];
    incomingKettleRow[0] = incomingAvro;
    // decoder = factory.jsonDecoder(schema, row);

    AvroInputData data = new AvroInputData();

    VariableSpace space = new Variables();
    RowMetaInterface outputMeta = new RowMeta();
    ValueMetaInterface vm = new ValueMeta();
    vm.setName( "IncomingAvro" );
    vm.setOrigin( "Dummy" );
    vm.setType( ValueMetaInterface.TYPE_STRING );
    outputMeta.addValueMeta( vm );

    for ( int i = 0; i < 12; i++ ) {
      AvroInputMeta.AvroField f = paths.get( i );
      vm = new ValueMeta();
      vm.setName( f.m_fieldName );
      vm.setOrigin( "Dummy" );
      vm.setType( ValueMetaInterface.TYPE_STRING );
      outputMeta.addValueMeta( vm );
    }

    data.m_normalFields = paths;
    data.m_decodingFromField = true;
    data.m_jsonEncoded = true;
    data.m_newFieldOffset = 1;
    data.m_inStream = null;
    data.m_fieldToDecodeIndex = 0;
    data.m_schemaToUse = schema;
    data.m_topLevelRecord = topLevel;
    data.m_factory = factory;
    data.m_datumReader = reader;
    data.m_outputRowMeta = outputMeta;
    data.init();

    Object[][] result = data.avroObjectToKettle( incomingKettleRow, space );

    assertTrue( result != null );
    assertTrue( result.length == 1 ); // one output row

    int count = 0;
    for ( int i = 0; i < result[0].length; i++ ) {
      if ( result[0][i] != null ) {
        count++;
      }
    }

    // should be 13 output fields (incoming + 12 decoded) in over allocated
    // kettle output row
    assertTrue( count == 13 );
  }

  @Test
  public void testGetNonExistentFieldFromTopLevelRecord() throws KettleException, IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse( s_schemaTopLevelRecord );

    Decoder decoder;
    DecoderFactory factory = new DecoderFactory();

    GenericData.Record topLevel = new GenericData.Record( schema );
    GenericDatumReader reader = new GenericDatumReader( schema );

    AvroInputMeta.AvroField field = new AvroInputMeta.AvroField();
    field.m_fieldName = "test";
    field.m_fieldPath = "$.nonExistent.notThere";
    field.m_kettleType = ValueMeta.getTypeDesc( ValueMetaInterface.TYPE_STRING );

    decoder = factory.jsonDecoder( schema, s_jsonDataTopLevelRecord[0] );
    reader.read( topLevel, decoder );

    field.init( 0 );
    field.reset( new Variables() );

    try {
      Object result = field.convertToKettleValue( topLevel, schema, mock( Schema.class ), false );
      fail( "Was expecting an exception as $.nonExistent.notThere does not exist in the schma" );
    } catch ( Exception ex ) {
      assertTrue( ex.getMessage().contains( "Field nonExistent does not seem to exist in the schema!" ) );
    }
  }

  @Test
  public void testGetTopLevelRecordArrayElement() throws KettleException, IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse( s_schemaTopLevelRecord );

    Decoder decoder;
    DecoderFactory factory = new DecoderFactory();

    GenericData.Record topLevel = new GenericData.Record( schema );
    GenericDatumReader reader = new GenericDatumReader( schema );

    AvroInputMeta.AvroField field = new AvroInputMeta.AvroField();

    field.m_fieldName = "test";
    field.m_fieldPath = "$.emails[1]";
    field.m_kettleType = ValueMeta.getTypeDesc( ValueMetaInterface.TYPE_STRING );

    String[] actualVals = new String[] { "and another one", "good to see you!", "yeah yeah yeah" };
    int i = 0;
    for ( String row : s_jsonDataTopLevelRecord ) {
      decoder = factory.jsonDecoder( schema, row );
      reader.read( topLevel, decoder );

      field.init( 0 ); // output index isn't needed for the test
      field.reset( new Variables() );

      Object result = field.convertToKettleValue( topLevel, schema, mock( Schema.class ), false );

      assertTrue( result != null );
      assertTrue( result instanceof String );
      assertEquals( result.toString(), actualVals[i++] );
    }
  }

  @Test
  public void testGetTopLevelRecordPositiveIndexOutOfBoundsArrayElement() throws KettleException, IOException {

    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse( s_schemaTopLevelRecord );

    Decoder decoder;
    DecoderFactory factory = new DecoderFactory();

    GenericData.Record topLevel = new GenericData.Record( schema );
    GenericDatumReader reader = new GenericDatumReader( schema );

    AvroInputMeta.AvroField field = new AvroInputMeta.AvroField();

    field.m_fieldName = "test";
    field.m_fieldPath = "$.emails[4]"; // non existent in all records
    field.m_kettleType = ValueMeta.getTypeDesc( ValueMetaInterface.TYPE_STRING );

    // no exception is thrown in this case - the step just outputs null for the
    // corresponding field
    for ( String row : s_jsonDataTopLevelRecord ) {
      decoder = factory.jsonDecoder( schema, row );
      reader.read( topLevel, decoder );

      field.init( 0 ); // output index isn't needed for the test
      field.reset( new Variables() );

      Object result = field.convertToKettleValue( topLevel, schema, mock( Schema.class ), false );
      assertTrue( result == null );
    }
  }

  @Test
  public void testGetTopLevelMapSimpleRecordField() throws KettleException, IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse( s_schemaTopLevelMap );

    Decoder decoder;
    DecoderFactory factory = new DecoderFactory();

    Map<Utf8, Object> topLevel = new HashMap<Utf8, Object>();
    GenericDatumReader reader = new GenericDatumReader( schema );

    AvroInputMeta.AvroField field = new AvroInputMeta.AvroField();
    field.m_fieldName = "test";
    field.m_fieldPath = "$[bob].age";
    field.m_kettleType = ValueMeta.getTypeDesc( ValueMetaInterface.TYPE_INTEGER );

    decoder = factory.jsonDecoder( schema, s_jsonDataTopLevelMap );
    reader.read( topLevel, decoder );

    field.init( 0 ); // output index isn't needed for the test
    field.reset( new Variables() );

    Object result = field.convertToKettleValue( topLevel, schema, mock( Schema.class ), false );

    assertTrue( result != null );
    assertTrue( result instanceof Long );
    assertEquals( result, new Long( 20 ) );
  }

  // test getting an array element from an array in a record that is itself
  // stored in a
  // top-level map
  @Test
  public void testGetTopLevelMapArrayElementFromRecord() throws KettleException, IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse( s_schemaTopLevelMap );

    Decoder decoder;
    DecoderFactory factory = new DecoderFactory();

    Map<Utf8, Object> topLevel = new HashMap<Utf8, Object>();
    GenericDatumReader reader = new GenericDatumReader( schema );

    AvroInputMeta.AvroField field = new AvroInputMeta.AvroField();
    field.m_fieldName = "test";
    field.m_fieldPath = "$[bob].emails[0]";
    field.m_kettleType = ValueMeta.getTypeDesc( ValueMetaInterface.TYPE_STRING );

    decoder = factory.jsonDecoder( schema, s_jsonDataTopLevelMap );
    reader.read( topLevel, decoder );

    field.init( 0 ); // output index isn't needed for the test
    field.reset( new Variables() );

    Object result = field.convertToKettleValue( topLevel, schema, mock( Schema.class ), false );

    assertTrue( result != null );
    assertTrue( result instanceof String );
    assertEquals( result, "here is an email" );
  }

  @Test
  public void testGetNonExistentTopLevelMapEntry() throws KettleException, IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse( s_schemaTopLevelMap );

    Decoder decoder;
    DecoderFactory factory = new DecoderFactory();

    Map<Utf8, Object> topLevel = new HashMap<Utf8, Object>();
    GenericDatumReader reader = new GenericDatumReader( schema );

    AvroInputMeta.AvroField field = new AvroInputMeta.AvroField();
    field.m_fieldName = "test";
    field.m_fieldPath = "$[noddy].emails[0]";
    field.m_kettleType = ValueMeta.getTypeDesc( ValueMetaInterface.TYPE_STRING );

    decoder = factory.jsonDecoder( schema, s_jsonDataTopLevelMap );
    reader.read( topLevel, decoder );

    field.init( 0 ); // output index isn't needed for the test
    field.reset( new Variables() );

    Object result = field.convertToKettleValue( topLevel, schema, mock( Schema.class ), false );

    assertTrue( result == null );
  }

  @Test
  public void testUnionHandling() throws KettleException, IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse( s_schemaTopLevelRecordWithUnion );

    Decoder decoder;
    DecoderFactory factory = new DecoderFactory();

    GenericData.Record topLevel = new GenericData.Record( schema );
    GenericDatumReader reader = new GenericDatumReader( schema );

    AvroInputMeta.AvroField field = new AvroInputMeta.AvroField();
    field.m_fieldName = "test";
    field.m_fieldPath = "$.name";
    field.m_kettleType = ValueMeta.getTypeDesc( ValueMetaInterface.TYPE_STRING );

    String[] actualVals = new String[] { "bob", "fred", null };
    int i = 0;
    for ( String row : s_jsonDataTopLevelRecordWithUnion ) {
      decoder = factory.jsonDecoder( schema, row );
      reader.read( topLevel, decoder );

      field.init( 0 ); // output index isn't needed for the test
      field.reset( new Variables() );

      Object result = field.convertToKettleValue( topLevel, schema, mock( Schema.class ), false );

      if ( i != 2 ) {
        assertTrue( result != null );
        assertTrue( result instanceof String );
        assertEquals( result.toString(), actualVals[i++] );
      } else {
        assertTrue( result == null );
      }
    }
  }

  @Test
  public void testMultiTypeUnionHandling() throws KettleException, IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse( s_schemaTopLevelRecordWithMultiTypeUnion );

    Decoder decoder;
    DecoderFactory factory = new DecoderFactory();

    GenericData.Record topLevel = new GenericData.Record( schema );
    GenericDatumReader reader = new GenericDatumReader( schema );

    AvroInputMeta.AvroField field = new AvroInputMeta.AvroField();
    field.m_fieldName = "test";
    field.m_fieldPath = "$.name";
    field.m_kettleType = ValueMeta.getTypeDesc( ValueMetaInterface.TYPE_STRING );

    String[] actualVals = new String[] { "bob", "42", null };
    int i = 0;
    for ( String row : s_jsonDataTopLevelRecordWithMultiTypeUnion ) {
      decoder = factory.jsonDecoder( schema, row );
      reader.read( topLevel, decoder );

      field.init( 0 ); // output index isn't needed for the test
      field.reset( new Variables() );

      Object result = field.convertToKettleValue( topLevel, schema, mock( Schema.class ), false );

      if ( i != 2 ) {
        assertTrue( result != null );
        assertTrue( result instanceof String );
        assertEquals( result.toString(), actualVals[i++] );
      } else {
        assertTrue( result == null );
      }
    }
  }

  @Test
  public void testMapExpansion() throws KettleException, IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse( s_schemaTopLevelMap );

    Decoder decoder;
    DecoderFactory factory = new DecoderFactory();

    Map<Utf8, Object> topLevel = new HashMap<Utf8, Object>();
    GenericDatumReader reader = new GenericDatumReader( schema );

    AvroInputMeta.AvroField field = new AvroInputMeta.AvroField();
    field.m_fieldName = "test";
    field.m_fieldPath = "$[*].name";
    field.m_kettleType = ValueMeta.getTypeDesc( ValueMetaInterface.TYPE_STRING );
    List<AvroInputMeta.AvroField> normalFields = new ArrayList<AvroInputMeta.AvroField>();
    normalFields.add( field );
    RowMetaInterface rowMeta = new RowMeta();
    ValueMetaInterface vm = new ValueMeta( "test", ValueMetaInterface.TYPE_STRING );
    rowMeta.addValueMeta( vm );

    AvroArrayExpansion expansion = checkFieldPaths( normalFields, rowMeta );
    expansion.init();
    expansion.reset( new Variables() );

    decoder = factory.jsonDecoder( schema, s_jsonDataTopLevelMap );
    reader.read( topLevel, decoder );

    Object[][] result = expansion.convertToKettleValues( topLevel, schema, mock( Schema.class ), new Variables(), false );

    assertTrue( result != null );
    assertTrue( result.length == 3 );

    List<String> expectedNames = new ArrayList<String>();
    expectedNames.add( "zaphod" );
    expectedNames.add( "bob" );
    expectedNames.add( "fred" );

    for ( int i = 0; i < result.length; i++ ) {
      assertTrue( result[i][0] != null );
      assertTrue( expectedNames.contains( result[i][0] ) );
    }
  }

  @Test
  public void testLookupFieldInitializationNoRowMetaAvailable() {
    AvroInputMeta.LookupField lf = new AvroInputMeta.LookupField();

    lf.m_fieldName = "TestField";
    lf.m_variableName = "TestVar";
    VariableSpace space = new Variables();

    assert ( lf.init( null, space ) == false );
  }

  public static void main( String[] args ) {
    try {
      AvroInputTest test = new AvroInputTest();
      test.testSchemaWithTopLevelUnionAndNamedType();
    } catch ( Exception ex ) {
      ex.printStackTrace();
    }
  }
}
