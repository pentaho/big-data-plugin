/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package org.pentaho.di.trans.steps.avroinput;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.LongNode;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.row.value.ValueMetaBigNumber;
import org.pentaho.di.core.row.value.ValueMetaBinary;
import org.pentaho.di.core.row.value.ValueMetaBoolean;
import org.pentaho.di.core.row.value.ValueMetaDate;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaNumber;
import org.pentaho.di.core.row.value.ValueMetaPluginType;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.variables.VariableSpace;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 10/21/15.
 */
public class AvroInputMetaAvroFieldTest {
  private AvroInputMeta.AvroField avroField;
  private VariableSpace variableSpace;
  private Map<String, String> variableSpaceMap;

  @BeforeClass
  public static void before() throws KettlePluginException {
    PluginRegistry.addPluginType( ValueMetaPluginType.getInstance() );
    PluginRegistry.init( false );
  }

  @Before
  public void setup() throws KettleException {
    avroField = new AvroInputMeta.AvroField();
    variableSpace = mock( VariableSpace.class );
    variableSpaceMap = new HashMap<String, String>();
    when( variableSpace.environmentSubstitute( anyString() ) ).thenAnswer( new Answer<String>() {
      @Override public String answer( InvocationOnMock invocation ) throws Throwable {
        Object key = invocation.getArguments()[ 0 ];
        String result = variableSpaceMap.get( key );
        if ( result == null ) {
          return String.valueOf( key );
        }
        return result;
      }
    } );
    avroField.m_fieldPath = "testFieldPath";
  }

  @Test( expected = KettleException.class )
  public void testInitNoPathSet() throws KettleException {
    avroField = new AvroInputMeta.AvroField();
    avroField.init( 0 );
  }

  @Test
  public void testGetKettleValueBigNumber() throws KettleException {
    avroField.m_kettleType = "BigNumber";
    avroField.init( 0 );
    BigDecimal bigDecimal = new BigDecimal( 10 );
    BigDecimal expected = new ValueMetaBigNumber().getBigNumber( bigDecimal );
    assertEquals( expected, avroField.getKettleValue( bigDecimal ) );
  }

  @Test
  public void testGetKettleValueBinary() throws KettleException {
    avroField.m_kettleType = "Binary";
    avroField.init( 0 );
    byte[] bytes = new byte[] { 0, 2, 3 };
    assertEquals( new ValueMetaBinary().getBinary( bytes ), avroField.getKettleValue( bytes ) );
  }

  @Test
  public void testGetKettleValueBoolean() throws KettleException {
    avroField.m_kettleType = "Boolean";
    avroField.init( 0 );
    boolean value = false;
    Boolean expected = new ValueMetaBoolean().getBoolean( value );
    assertEquals( expected, avroField.getKettleValue( value ) );

    Schema schema = mock( Schema.class );
    when( schema.getType() ).thenReturn( Schema.Type.BOOLEAN );
    assertEquals( expected, avroField.getPrimitive( value, schema ) );
  }

  @Test
  public void testGetKettleValueDate() throws KettleException {
    avroField.m_kettleType = "Date";
    avroField.init( 0 );
    Date date = new Date();
    assertEquals( new ValueMetaDate().getDate( date ), avroField.getKettleValue( date ) );
  }

  @Test
  public void testGetKettleValueInteger() throws KettleException {
    avroField.m_kettleType = "Integer";
    avroField.init( 0 );
    long num = 12;
    Long expected = new ValueMetaInteger().getInteger( num );
    assertEquals( expected, avroField.getKettleValue( num ) );


    Schema schema = mock( Schema.class );
    when( schema.getType() ).thenReturn( Schema.Type.INT );
    assertEquals( expected, avroField.getPrimitive( Long.valueOf( num ).intValue(), schema ) );
  }

  @Test
  public void testGetKettleValueNumber() throws KettleException {
    avroField.m_kettleType = "Number";
    avroField.init( 0 );
    Double number = 2.5;
    Double expected = new ValueMetaNumber().getNumber( number );
    assertEquals( expected, avroField.getKettleValue( number ) );

    Schema schema = mock( Schema.class );
    when( schema.getType() ).thenReturn( Schema.Type.FLOAT );
    assertEquals( expected, avroField.getPrimitive( number.floatValue(), schema ) );
  }

  @Test
  public void testGetKettleValueString() throws KettleException {
    avroField.m_kettleType = "String";
    avroField.init( 0 );
    String string = "value: 2.54";
    assertEquals( new ValueMetaString().getString( string ), avroField.getKettleValue( string ) );
  }

  @Test
  public void testGetKettleValueInternetAddress() throws KettleException, UnknownHostException {
    avroField.m_kettleType = "Internet Address";
    avroField.init( 0 );
    InetAddress inetAddress = InetAddress.getByName( "192.168.1.1" );
    assertNull( avroField.getKettleValue( inetAddress ) );

    Schema schema = mock( Schema.class );
    when( schema.getType() ).thenReturn( Schema.Type.RECORD );
    assertNull( avroField.getPrimitive( inetAddress, schema ) );
  }

  @Test
  public void testGetPrimitiveNull() throws KettleException {
    avroField.init( 0 );
    assertNull( avroField.getPrimitive( null, null ) );
  }

  @Test
  public void testGetPrimitiveFixed() throws KettleException, UnsupportedEncodingException {
    Schema schema = mock( Schema.class );
    when( schema.getType() ).thenReturn( Schema.Type.FIXED );
    GenericData.Fixed fixed = new GenericData.Fixed( schema );
    String testBytes = "testBytes";
    fixed.bytes( testBytes.getBytes( "UTF-8" ) );
    assertEquals( testBytes, new String( (byte[]) avroField.getPrimitive( fixed, schema ), "UTF-8" ) );
  }

  @Test
  public void testConvertToKettleValueMapNull() throws KettleException {
    assertNull( avroField.convertToKettleValue( (Map<Utf8, Object>) null, null, null, false ) );
  }

  @Test( expected = KettleException.class )
  public void testConvertToKettleValueMapExceptionWithNoTempParts() throws KettleException {
    avroField.init( 0 );
    avroField.convertToKettleValue( Collections.<Utf8, Object>emptyMap(), mock( Schema.class ), mock( Schema.class ), false );
  }

  @Test( expected = KettleException.class )
  public void testConvertToKettleValueMapExceptionWithMalformedPath() throws KettleException {
    avroField.init( 0 );
    avroField.reset( variableSpace );
    avroField.convertToKettleValue( Collections.<Utf8, Object>emptyMap(), mock( Schema.class ), mock( Schema.class ), false );
  }

  @Test
  public void testConvertToKettleValueMapNullValue() throws KettleException {
    avroField.m_fieldPath = "[key]";
    avroField.init( 0 );
    avroField.reset( variableSpace );
    assertNull( avroField.convertToKettleValue( new HashMap<Utf8, Object>(), mock( Schema.class ), mock( Schema.class ), false ) );
  }

  @Test
  public void testConvertToKettleValueMapStringValue() throws KettleException {
    avroField.m_kettleType = "String";
    avroField.m_fieldPath = "[key]";
    avroField.init( 0 );
    avroField.reset( variableSpace );
    Map<Utf8, Object> map = new HashMap<Utf8, Object>();
    String testString = "testString";
    map.put( new Utf8( "key" ), testString );
    Schema schema = mock( Schema.class );
    Schema valueSchema = mock( Schema.class );
    when( schema.getValueType() ).thenReturn( valueSchema );
    when( valueSchema.getType() ).thenReturn( Schema.Type.STRING );
    assertEquals( testString, avroField.convertToKettleValue( map, schema, mock( Schema.class ), false ) );
  }

  @Test
  public void testConvertToKettleValueArrayNull() throws KettleException {
    assertNull( avroField.convertToKettleValue( (GenericData.Array) null, null, mock( Schema.class ), false ) );
  }

  @Test( expected = KettleException.class )
  public void testConvertToKettleValueArrayExceptionWithNoTempParts() throws KettleException {
    avroField.init( 0 );
    Schema schema = mock( Schema.class );
    when( schema.getType() ).thenReturn( Schema.Type.ARRAY );
    avroField.convertToKettleValue( new GenericData.Array( 0, schema ), mock( Schema.class ), mock( Schema.class ), false );
  }

  @Test( expected = KettleException.class )
  public void testConvertToKettleValueArrayWithMalformedPath() throws KettleException {
    avroField.init( 0 );
    avroField.reset( variableSpace );
    Schema schema = mock( Schema.class );
    when( schema.getType() ).thenReturn( Schema.Type.ARRAY );
    avroField.convertToKettleValue( new GenericData.Array( 0, schema ), mock( Schema.class ), mock( Schema.class ), false );
  }

  @Test( expected = KettleException.class )
  public void testConvertToKettleValueArrayNumberFormatException() throws KettleException {
    avroField.m_fieldPath = "[key]";
    avroField.init( 0 );
    avroField.reset( variableSpace );
    Schema schema = mock( Schema.class );
    when( schema.getType() ).thenReturn( Schema.Type.ARRAY );
    avroField.convertToKettleValue( new GenericData.Array( 0, schema ), mock( Schema.class ), mock( Schema.class ), false );
  }

  @Test
  public void testConvertToKettleValueArrayIndexLessThanZero() throws KettleException {
    avroField.m_fieldPath = "[-1]";
    avroField.init( 0 );
    avroField.reset( variableSpace );
    Schema schema = mock( Schema.class );
    when( schema.getType() ).thenReturn( Schema.Type.ARRAY );
    assertNull( avroField.convertToKettleValue( new GenericData.Array( 0, schema ), mock( Schema.class ), mock( Schema.class ), false ) );
  }

  @Test
  public void testConvertToKettleValueArrayIndexTooLarge() throws KettleException {
    avroField.m_fieldPath = "[1]";
    avroField.init( 0 );
    avroField.reset( variableSpace );
    Schema schema = mock( Schema.class );
    when( schema.getType() ).thenReturn( Schema.Type.ARRAY );
    assertNull( avroField.convertToKettleValue( new GenericData.Array( 1, schema ), mock( Schema.class ), mock( Schema.class ), false ) );
  }

  @Test
  public void testConvertToKettleValueArrayNullElement() throws KettleException {
    avroField.m_fieldPath = "[0]";
    avroField.init( 0 );
    avroField.reset( variableSpace );
    Schema schema = mock( Schema.class );
    when( schema.getType() ).thenReturn( Schema.Type.ARRAY );
    GenericData.Array array = new GenericData.Array( 1, schema );
    array.add( null );
    assertNull( avroField.convertToKettleValue( array, mock( Schema.class ), mock( Schema.class ), false ) );
  }

  @Test
  public void testConvertToKettleValueArrayStringValue() throws KettleException {
    avroField.m_kettleType = "String";
    avroField.m_fieldPath = "[0]";
    avroField.init( 0 );
    avroField.reset( variableSpace );
    String testString = "testString";
    Schema schema = mock( Schema.class );
    Schema valueSchema = mock( Schema.class );
    when( schema.getElementType() ).thenReturn( valueSchema );
    when( schema.getType() ).thenReturn( Schema.Type.ARRAY );
    when( valueSchema.getType() ).thenReturn( Schema.Type.STRING );
    GenericData.Array array = new GenericData.Array( 1, schema );
    array.add( testString );
    assertEquals( testString, avroField.convertToKettleValue( array, schema, mock( Schema.class ), false ) );
  }

  @Test
  public void testConvertToKettleValueRecordNull() throws KettleException {
    assertNull( avroField.convertToKettleValue( (GenericData.Record) null, null, mock( Schema.class ), false ) );
  }

  @Test( expected = KettleException.class )
  public void testConvertToKettleValueRecordExceptionWithNoTempParts() throws KettleException {
    avroField.init( 0 );
    Schema schema = mock( Schema.class );
    when( schema.getType() ).thenReturn( Schema.Type.RECORD );
    avroField.convertToKettleValue( new GenericData.Record( schema ), mock( Schema.class ), mock( Schema.class ), false );
  }

  @Test( expected = KettleException.class )
  public void testConvertToKettleValueRecordWithMalformedPath() throws KettleException {
    avroField.m_fieldPath = "[0]";
    avroField.init( 0 );
    avroField.reset( variableSpace );
    Schema schema = mock( Schema.class );
    when( schema.getType() ).thenReturn( Schema.Type.RECORD );
    avroField.convertToKettleValue( new GenericData.Record( schema ), mock( Schema.class ), mock( Schema.class ), false );
  }

  @Test( expected = KettleException.class )
  public void testConvertToKettleValueRecordNullSchema() throws KettleException {
    avroField.m_fieldPath = "key";
    avroField.init( 0 );
    avroField.reset( variableSpace );
    Schema schema = mock( Schema.class );
    when( schema.getType() ).thenReturn( Schema.Type.RECORD );
    assertNull( avroField.convertToKettleValue( new GenericData.Record( schema ), mock( Schema.class ), mock( Schema.class ), true ) );
    avroField.convertToKettleValue( new GenericData.Record( schema ), mock( Schema.class ), mock( Schema.class ), false );
  }

  @Test
  public void testConvertToKettleValueRecordStringValue() throws KettleException {
    avroField.m_kettleType = "String";
    avroField.m_fieldPath = "key";
    avroField.init( 0 );
    avroField.reset( variableSpace );
    String testString = "testString";
    Schema schema = mock( Schema.class );
    Schema.Field field = mock( Schema.Field.class );
    when( schema.getFields() ).thenReturn( new ArrayList<>( Arrays.asList( field ) ) );
    when( schema.getField( avroField.m_fieldPath ) ).thenReturn( field );
    when( schema.getType() ).thenReturn( Schema.Type.RECORD );
    Schema fieldSchema = mock( Schema.class );
    when( field.schema() ).thenReturn( fieldSchema );
    when( fieldSchema.getType() ).thenReturn( Schema.Type.STRING );
    GenericData.Record record = new GenericData.Record( schema );
    record.put( avroField.m_fieldPath, testString );
    assertEquals( testString, avroField.convertToKettleValue( record, schema, mock( Schema.class ), false ) );
  }

  @Test
  public void testGetDefaultValueFromDefaultSchemaIfNull() throws KettleException {
    avroField.m_kettleType = "Integer";
    avroField.m_fieldPath = "key";
    avroField.init( 0 );
    avroField.reset( variableSpace );
    IntNode node = new IntNode( 5 );
    Schema schemaToUse = mock( Schema.class );
    Schema defaultSchema = mock( Schema.class );
    Schema fieldSchema = mock( Schema.class );
    Schema.Field field = mock( Schema.Field.class );
    GenericData.Record record = mock( GenericData.Record.class );
    when( record.get( avroField.m_fieldPath ) ).thenReturn( null );
    when( defaultSchema.getField( avroField.m_fieldPath ) ).thenReturn( field );
    when( field.defaultValue() ).thenReturn( node ).thenReturn( node );
    when( field.schema() ).thenReturn( fieldSchema );
    when( fieldSchema.getType() ).thenReturn( Schema.Type.INT );
    assertEquals( 5L, avroField.convertToKettleValue( record, schemaToUse, defaultSchema, true ) );
  }

  @Test
  public void testGetPrimitiveFromConvertNode() throws KettleException {
    avroField.m_kettleType = "Integer";
    avroField.init( 0 );
    Schema schema = mock( Schema.class );
    LongNode node = new LongNode( 22 );
    when( schema.getType() ).thenReturn( Schema.Type.LONG );
    assertEquals( 22L,  avroField.getPrimitive( node, schema ) );
  }
}
