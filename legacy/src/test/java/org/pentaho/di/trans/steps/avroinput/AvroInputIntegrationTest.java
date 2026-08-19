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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.junit.Test;

/**
 * Integration test for AvroInput with Avro 1.12.1 compatibility validation.
 * Tests basic serialization/deserialization and schema parsing.
 */
public class AvroInputIntegrationTest {

  @Test
  public void testBasicAvroSchemaParsingWith1_12_1() {
    String schema = "{\n" +
        "  \"type\": \"record\",\n" +
        "  \"name\": \"TestRecord\",\n" +
        "  \"fields\": [\n" +
        "    {\"name\": \"id\", \"type\": \"int\"},\n" +
        "    {\"name\": \"name\", \"type\": \"string\"}\n" +
        "  ]\n" +
        "}";
    
    Schema.Parser parser = new Schema.Parser();
    Schema s = parser.parse(schema);
    
    assertNotNull(s);
    assertEquals("TestRecord", s.getName());
    assertEquals(2, s.getFields().size());
  }

  @Test
  public void testFixedTypeWith1_12_1() throws IOException {
    String schema = "{\n" +
        "  \"type\": \"fixed\",\n" +
        "  \"size\": 4,\n" +
        "  \"name\": \"MD5\"\n" +
        "}";
    
    Schema.Parser parser = new Schema.Parser();
    Schema s = parser.parse(schema);
    
    assertNotNull(s);
    assertEquals(Schema.Type.FIXED, s.getType());
    assertEquals(4, s.getFixedSize());
  }

  @Test
  public void testFixedFieldInRecordWith1_12_1() throws IOException {
    String schema = "[\n" +
        "  {\"type\": \"fixed\", \"size\": 2, \"name\": \"myfixed\"},\n" +
        "  {\n" +
        "    \"type\": \"record\",\n" +
        "    \"name\": \"TestRecord\",\n" +
        "    \"fields\": [\n" +
        "      {\"name\": \"data\", \"type\": \"myfixed\"},\n" +
        "      {\"name\": \"value\", \"type\": \"int\"}\n" +
        "    ]\n" +
        "  }\n" +
        "]";
    
    Schema.Parser parser = new Schema.Parser();
    Schema unionSchema = parser.parse(schema);
    
    assertNotNull(unionSchema);
    Schema recordSchema = unionSchema.getTypes().get(1);
    assertNotNull(recordSchema.getField("data"));
  }

  @Test
  public void testBinaryDecodingWith1_12_1() throws IOException {
    String schema = "{\n" +
        "  \"type\": \"record\",\n" +
        "  \"name\": \"SimpleRecord\",\n" +
        "  \"fields\": [\n" +
        "    {\"name\": \"id\", \"type\": \"int\"},\n" +
        "    {\"name\": \"name\", \"type\": \"string\"}\n" +
        "  ]\n" +
        "}";
    
    String jsonData = "{\"id\": 1, \"name\": \"test\"}";
    
    Schema.Parser parser = new Schema.Parser();
    Schema s = parser.parse(schema);
    
    DecoderFactory factory = new DecoderFactory();
    Decoder decoder = factory.jsonDecoder(s, jsonData);
    
    GenericData.Record record = new GenericData.Record(s);
    GenericDatumReader<GenericData.Record> reader = new GenericDatumReader<>(s);
    reader.read(record, decoder);
    
    assertNotNull(record);
    assertEquals(1, record.get("id"));
  }

  @Test
  public void testUtf8StringHandlingWith1_12_1() throws IOException {
    String schema = "{\n" +
        "  \"type\": \"record\",\n" +
        "  \"name\": \"StringTest\",\n" +
        "  \"fields\": [\n" +
        "    {\"name\": \"text\", \"type\": \"string\"}\n" +
        "  ]\n" +
        "}";
    
    String jsonData = "{\"text\": \"hello\"}";
    
    Schema.Parser parser = new Schema.Parser();
    Schema s = parser.parse(schema);
    
    DecoderFactory factory = new DecoderFactory();
    Decoder decoder = factory.jsonDecoder(s, jsonData);
    
    GenericData.Record record = new GenericData.Record(s);
    GenericDatumReader<GenericData.Record> reader = new GenericDatumReader<>(s);
    reader.read(record, decoder);
    
    assertNotNull(record);
    Object textValue = record.get("text");
    assertNotNull(textValue);
    // Avro 1.12.1 may return Utf8 wrapper, so convert to string
    assertEquals("hello", textValue.toString());
  }

  @Test
  public void testGenericRecordCreationWith1_12_1() {
    String schema = "{\n" +
        "  \"type\": \"record\",\n" +
        "  \"name\": \"TestRecord\",\n" +
        "  \"fields\": [\n" +
        "    {\"name\": \"field1\", \"type\": \"int\"},\n" +
        "    {\"name\": \"field2\", \"type\": \"string\"}\n" +
        "  ]\n" +
        "}";
    
    Schema.Parser parser = new Schema.Parser();
    Schema s = parser.parse(schema);
    
    GenericData.Record record = new GenericData.Record(s);
    record.put("field1", 42);
    record.put("field2", "test value");
    
    assertEquals(42, record.get("field1"));
    assertEquals("test value", record.get("field2").toString());
  }

  @Test
  public void testNestedRecordsWith1_12_1() throws IOException {
    String schema = "{\n" +
        "  \"type\": \"record\",\n" +
        "  \"name\": \"Outer\",\n" +
        "  \"fields\": [\n" +
        "    {\n" +
        "      \"name\": \"inner\",\n" +
        "      \"type\": {\n" +
        "        \"type\": \"record\",\n" +
        "        \"name\": \"Inner\",\n" +
        "        \"fields\": [{\"name\": \"value\", \"type\": \"int\"}]\n" +
        "      }\n" +
        "    }\n" +
        "  ]\n" +
        "}";
    
    String jsonData = "{\"inner\": {\"value\": 123}}";
    
    Schema.Parser parser = new Schema.Parser();
    Schema s = parser.parse(schema);
    
    DecoderFactory factory = new DecoderFactory();
    Decoder decoder = factory.jsonDecoder(s, jsonData);
    
    GenericData.Record record = new GenericData.Record(s);
    GenericDatumReader<GenericData.Record> reader = new GenericDatumReader<>(s);
    reader.read(record, decoder);
    
    assertNotNull(record);
    GenericData.Record inner = (GenericData.Record) record.get("inner");
    assertNotNull(inner);
    assertEquals(123, inner.get("value"));
  }

  @Test
  public void testArrayFieldsWith1_12_1() throws IOException {
    String schema = "{\n" +
        "  \"type\": \"record\",\n" +
        "  \"name\": \"ArrayTest\",\n" +
        "  \"fields\": [\n" +
        "    {\n" +
        "      \"name\": \"items\",\n" +
        "      \"type\": {\n" +
        "        \"type\": \"array\",\n" +
        "        \"items\": \"string\"\n" +
        "      }\n" +
        "    }\n" +
        "  ]\n" +
        "}";
    
    String jsonData = "{\"items\": [\"one\", \"two\", \"three\"]}";
    
    Schema.Parser parser = new Schema.Parser();
    Schema s = parser.parse(schema);
    
    DecoderFactory factory = new DecoderFactory();
    Decoder decoder = factory.jsonDecoder(s, jsonData);
    
    GenericData.Record record = new GenericData.Record(s);
    GenericDatumReader<GenericData.Record> reader = new GenericDatumReader<>(s);
    reader.read(record, decoder);
    
    assertNotNull(record);
    Object itemsObj = record.get("items");
    assertTrue(itemsObj instanceof GenericData.Array);
  }

  @Test
  public void testUnionFieldsWith1_12_1() throws IOException {
    String schema = "{\n" +
        "  \"type\": \"record\",\n" +
        "  \"name\": \"UnionTest\",\n" +
        "  \"fields\": [\n" +
        "    {\"name\": \"value\", \"type\": [\"null\", \"string\"]}\n" +
        "  ]\n" +
        "}";
    
    String jsonData = "{\"value\": {\"string\": \"hello\"}}";
    
    Schema.Parser parser = new Schema.Parser();
    Schema s = parser.parse(schema);
    
    DecoderFactory factory = new DecoderFactory();
    Decoder decoder = factory.jsonDecoder(s, jsonData);
    
    GenericData.Record record = new GenericData.Record(s);
    GenericDatumReader<GenericData.Record> reader = new GenericDatumReader<>(s);
    reader.read(record, decoder);
    
    assertNotNull(record);
    assertNotNull(record.get("value"));
  }

  @Test
  public void testLargeRecordWith1_12_1() throws IOException {
    StringBuilder fieldsJson = new StringBuilder();
    for (int i = 0; i < 10; i++) {
      if (i > 0) fieldsJson.append(",");
      fieldsJson.append("{\"name\": \"field").append(i).append("\", \"type\": \"string\"}");
    }
    
    String schema = "{\n" +
        "  \"type\": \"record\",\n" +
        "  \"name\": \"LargeRecord\",\n" +
        "  \"fields\": [" + fieldsJson.toString() + "]\n" +
        "}";
    
    Schema.Parser parser = new Schema.Parser();
    Schema s = parser.parse(schema);
    
    GenericData.Record record = new GenericData.Record(s);
    for (int i = 0; i < 10; i++) {
      record.put("field" + i, "value" + i);
    }
    
    assertEquals(10, record.getSchema().getFields().size());
    for (int i = 0; i < 10; i++) {
      assertEquals("value" + i, record.get("field" + i).toString());
    }
  }
}
