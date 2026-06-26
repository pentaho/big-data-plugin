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
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.junit.Test;

/**
 * Test for Avro 1.12.1 compatibility with AvroInput map functionality.
 * These tests help identify issues with map handling that may occur in 1.12.1.
 */
public class AvroInputMapCompatibilityTest {

  @Test
  public void testSimpleMapParsing() throws IOException {
    String schema = "{\"type\": \"map\", \"values\": \"int\"}";
    
    Schema.Parser parser = new Schema.Parser();
    Schema s = parser.parse(schema);
    
    assertNotNull(s);
    assertEquals(Schema.Type.MAP, s.getType());
  }

  @Test
  public void testMapWithRecordValues() throws IOException {
    String schema = "{" +
        "\"type\": \"map\"," +
        "\"values\": {" +
        "  \"type\": \"record\"," +
        "  \"name\": \"Person\"," +
        "  \"fields\": [" +
        "    {\"name\": \"name\", \"type\": \"string\"}," +
        "    {\"name\": \"age\", \"type\": \"int\"}" +
        "  ]" +
        "}" +
        "}";
    
    Schema.Parser parser = new Schema.Parser();
    Schema s = parser.parse(schema);
    
    assertNotNull(s);
    assertEquals(Schema.Type.MAP, s.getType());
    Schema valueType = s.getValueType();
    assertEquals(Schema.Type.RECORD, valueType.getType());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMapDecodingWithRecordValues() throws IOException {
    String schema = "{" +
        "\"type\": \"map\"," +
        "\"values\": {" +
        "  \"type\": \"record\"," +
        "  \"name\": \"Person\"," +
        "  \"fields\": [" +
        "    {\"name\": \"name\", \"type\": \"string\"}," +
        "    {\"name\": \"age\", \"type\": \"int\"}" +
        "  ]" +
        "}" +
        "}";
    
    String jsonData = "{" +
        "\"alice\": {\"name\": \"alice\", \"age\": 25}," +
        "\"bob\": {\"name\": \"bob\", \"age\": 30}" +
        "}";
    
    Schema.Parser parser = new Schema.Parser();
    Schema s = parser.parse(schema);
    
    DecoderFactory factory = new DecoderFactory();
    Decoder decoder = factory.jsonDecoder(s, jsonData);
    
    GenericDatumReader<Object> reader = new GenericDatumReader<>(s);
    // In Avro 1.12.1, reading a map returns the map directly, not populating a passed-in map
    Object mapObj = reader.read(null, decoder);
    
    assertNotNull(mapObj);
    assertTrue(mapObj instanceof Map);
    
    Map<Object, Object> map = (Map<Object, Object>) mapObj;
    assertEquals(2, map.size());
    
    // Get bob's record
    Object bobObj = map.get(new Utf8("bob"));
    assertNotNull(bobObj);
    assertTrue(bobObj instanceof GenericData.Record);
    
    GenericData.Record bobRecord = (GenericData.Record) bobObj;
    Object ageObj = bobRecord.get("age");
    assertNotNull(ageObj);
    
    // In Avro 1.12.1, the age should be an Integer
    System.out.println("Age object type: " + ageObj.getClass().getName());
    System.out.println("Age value: " + ageObj);
    
    if (ageObj instanceof Integer) {
      assertEquals(Integer.valueOf(30), ageObj);
    } else if (ageObj instanceof Long) {
      assertEquals(Long.valueOf(30), ageObj);
    } else {
      assertEquals(30, ageObj);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMapAccessByKey() throws IOException {
    String schema = "{" +
        "\"type\": \"map\"," +
        "\"values\": {" +
        "  \"type\": \"record\"," +
        "  \"name\": \"Person\"," +
        "  \"fields\": [" +
        "    {\"name\": \"name\", \"type\": \"string\"}," +
        "    {\"name\": \"age\", \"type\": \"int\"}," +
        "    {\"name\": \"emails\", \"type\": {\"type\": \"array\", \"items\": \"string\"}}" +
        "  ]" +
        "}" +
        "}";
    
    String jsonData = "{" +
        "\"bob\": {" +
        "  \"name\": \"bob\"," +
        "  \"age\": 20," +
        "  \"emails\": [\"bob@example.com\", \"bob.work@example.com\"]" +
        "}," +
        "\"fred\": {" +
        "  \"name\": \"fred\"," +
        "  \"age\": 25," +
        "  \"emails\": [\"fred@example.com\"]" +
        "}" +
        "}";
    
    Schema.Parser parser = new Schema.Parser();
    Schema s = parser.parse(schema);
    
    DecoderFactory factory = new DecoderFactory();
    Decoder decoder = factory.jsonDecoder(s, jsonData);
    
    GenericDatumReader<Object> reader = new GenericDatumReader<>(s);
    // In Avro 1.12.1, reading a map returns the map directly, not populating a passed-in map
    Object mapObj = reader.read(null, decoder);
    
    Map<Object, Object> map = (Map<Object, Object>) mapObj;
    
    // Access bob by key
    GenericData.Record bobRecord = (GenericData.Record) map.get(new Utf8("bob"));
    assertNotNull(bobRecord);
    
    // Check name
    Object nameObj = bobRecord.get("name");
    assertNotNull(nameObj);
    assertEquals("bob", nameObj.toString());
    
    // Check age
    Object ageObj = bobRecord.get("age");
    assertNotNull(ageObj);
    System.out.println("Bob's age type: " + ageObj.getClass().getName());
    System.out.println("Bob's age value: " + ageObj);
    
    // Check emails
    Object emailsObj = bobRecord.get("emails");
    assertNotNull(emailsObj);
    assertTrue(emailsObj instanceof GenericData.Array);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMapKeyIteration() throws IOException {
    String schema = "{" +
        "\"type\": \"map\"," +
        "\"values\": \"int\"" +
        "}";
    
    String jsonData = "{\"key1\": 10, \"key2\": 20, \"key3\": 30}";
    
    Schema.Parser parser = new Schema.Parser();
    Schema s = parser.parse(schema);
    
    DecoderFactory factory = new DecoderFactory();
    Decoder decoder = factory.jsonDecoder(s, jsonData);
    
    GenericDatumReader<Object> reader = new GenericDatumReader<>(s);
    // In Avro 1.12.1, reading a map returns the map directly, not populating a passed-in map
    Object mapObj = reader.read(null, decoder);
    
    Map<Object, Object> map = (Map<Object, Object>) mapObj;
    
    assertNotNull(map);
    assertEquals(3, map.size());
    
    // Check that we can iterate and access values
    for (Map.Entry<Object, Object> entry : map.entrySet()) {
      assertNotNull(entry.getKey());
      assertNotNull(entry.getValue());
      System.out.println("Key: " + entry.getKey() + ", Value type: " + entry.getValue().getClass().getName() + ", Value: " + entry.getValue());
    }
  }

  @Test
  public void testMapValueTypeValidation() throws IOException {
    String schema = "{" +
        "\"type\": \"map\"," +
        "\"values\": {" +
        "  \"type\": \"record\"," +
        "  \"name\": \"TestRecord\"," +
        "  \"fields\": [{\"name\": \"value\", \"type\": \"int\"}]" +
        "}" +
        "}";
    
    String jsonData = "{\"test\": {\"value\": 42}}";
    
    Schema.Parser parser = new Schema.Parser();
    Schema s = parser.parse(schema);
    
    // Verify the map has the correct value schema
    Schema valueSchema = s.getValueType();
    assertNotNull(valueSchema);
    assertEquals(Schema.Type.RECORD, valueSchema.getType());
    assertEquals("TestRecord", valueSchema.getName());
    
    Schema.Field valueField = valueSchema.getField("value");
    assertNotNull(valueField);
    assertEquals(Schema.Type.INT, valueField.schema().getType());
  }
}
