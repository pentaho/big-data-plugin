/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.kafka;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import static org.mockito.Mockito.when;

import org.pentaho.di.core.row.ValueMetaInterface;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by rfellows on 6/19/17.
 */
@RunWith( MockitoJUnitRunner.class )
public class KafkaConsumerFieldTest {
  KafkaConsumerField field;
  @Mock ValueMetaInterface vmi;

  @Test
  public void testEmptyConstructor() {
    field = new KafkaConsumerField();

    assertNull( field.getKafkaName() );
    assertNull( field.getOutputName() );
    assertEquals( KafkaConsumerField.Type.String, field.getOutputType() );
  }

  @Test
  public void testSettersGetters() {
    field = new KafkaConsumerField();
    field.setKafkaName( KafkaConsumerField.Name.MESSAGE );
    field.setOutputName( "MSG" );
    field.setOutputType( KafkaConsumerField.Type.Integer );

    assertEquals( KafkaConsumerField.Name.MESSAGE, field.getKafkaName() );
    assertEquals( "MSG", field.getOutputName() );
    assertEquals( KafkaConsumerField.Type.Integer, field.getOutputType() );
  }

  @Test
  public void testConstructor_noType() {
    field = new KafkaConsumerField( KafkaConsumerField.Name.KEY, "Test Name" );

    assertEquals( KafkaConsumerField.Name.KEY, field.getKafkaName() );
    assertEquals( "Test Name", field.getOutputName() );
    assertEquals( KafkaConsumerField.Type.String, field.getOutputType() );
  }

  @Test
  public void testConstructor_allProps() {
    field = new KafkaConsumerField( KafkaConsumerField.Name.KEY, "Test Name", KafkaConsumerField.Type.Binary );

    assertEquals( KafkaConsumerField.Name.KEY, field.getKafkaName() );
    assertEquals( "Test Name", field.getOutputName() );
    assertEquals( KafkaConsumerField.Type.Binary, field.getOutputType() );
  }

  @Test
  public void testSerializersSet() {
    field = new KafkaConsumerField( KafkaConsumerField.Name.KEY, "Test Name" );
    assertEquals( "class org.apache.kafka.common.serialization.StringSerializer", field.getOutputType().getKafkaSerializerClass().toString() );
    assertEquals( "class org.apache.kafka.common.serialization.StringDeserializer", field.getOutputType().getKafkaDeserializerClass().toString() );

    field = new KafkaConsumerField( KafkaConsumerField.Name.KEY, "Test Name", KafkaConsumerField.Type.Integer );
    assertEquals( "class org.apache.kafka.common.serialization.LongSerializer", field.getOutputType().getKafkaSerializerClass().toString() );
    assertEquals( "class org.apache.kafka.common.serialization.LongDeserializer", field.getOutputType().getKafkaDeserializerClass().toString() );

    field = new KafkaConsumerField( KafkaConsumerField.Name.KEY, "Test Name", KafkaConsumerField.Type.Binary );
    assertEquals( "class org.apache.kafka.common.serialization.ByteArraySerializer", field.getOutputType().getKafkaSerializerClass().toString() );
    assertEquals( "class org.apache.kafka.common.serialization.ByteArrayDeserializer", field.getOutputType().getKafkaDeserializerClass().toString() );

    field = new KafkaConsumerField( KafkaConsumerField.Name.KEY, "Test Name", KafkaConsumerField.Type.Number );
    assertEquals( "class org.apache.kafka.common.serialization.DoubleSerializer", field.getOutputType().getKafkaSerializerClass().toString() );
    assertEquals( "class org.apache.kafka.common.serialization.DoubleDeserializer", field.getOutputType().getKafkaDeserializerClass().toString() );
  }

  @Test
  public void testFromValueMetaInterface() {
    when( vmi.getType() ).thenReturn( ValueMetaInterface.TYPE_STRING );
    KafkaConsumerField.Type t = KafkaConsumerField.Type.fromValueMetaInterface( vmi );
    assertEquals( "String", t.toString() );

    when( vmi.getType() ).thenReturn( ValueMetaInterface.TYPE_INTEGER );
    t = KafkaConsumerField.Type.fromValueMetaInterface( vmi );
    assertEquals( "Integer", t.toString() );

    when( vmi.getType() ).thenReturn( ValueMetaInterface.TYPE_BINARY );
    t = KafkaConsumerField.Type.fromValueMetaInterface( vmi );
    assertEquals( "Binary", t.toString() );

    when( vmi.getType() ).thenReturn( ValueMetaInterface.TYPE_NUMBER );
    t = KafkaConsumerField.Type.fromValueMetaInterface( vmi );
    assertEquals( "Number", t.toString() );
  }
}
