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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.pentaho.di.core.bowl.DefaultBowl;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LoggingObjectInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaPluginType;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.loadsave.LoadSaveTester;
import org.pentaho.di.trans.steps.loadsave.validator.FieldLoadSaveValidator;
import org.pentaho.di.trans.steps.loadsave.validator.ListLoadSaveValidator;
import org.pentaho.di.trans.steps.loadsave.validator.StringLoadSaveValidator;
import org.pentaho.di.trans.steps.mock.StepMockHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 10/21/15.
 */
public class AvroInputMetaTest {
  private AvroInputMeta avroInputMeta;

  @BeforeClass
  public static void before() throws KettlePluginException {
    PluginRegistry.addPluginType( ValueMetaPluginType.getInstance() );
    PluginRegistry.init( false );
  }

  @Before
  public void setup() {
    avroInputMeta = new AvroInputMeta();
  }

  @Test
  public void testLoadSave() throws KettleException {
    List<String> commonAttributes = new ArrayList<>();
    commonAttributes.add( "avroInField" );
    commonAttributes.add( "avroFieldName" );
    commonAttributes.add( "schemaInField" );
    commonAttributes.add( "schemaFieldName" );
    commonAttributes.add( "schemaInFieldIsPath" );
    commonAttributes.add( "cacheSchemasInMemory" );
    commonAttributes.add( "filename" );
    commonAttributes.add( "schemaFilename" );
    commonAttributes.add( "avroIsJsonEncoded" );
    commonAttributes.add( "avroFields" );
    commonAttributes.add( "lookupFields" );
    commonAttributes.add( "dontComplainAboutMissingFields" );

    Map<String, FieldLoadSaveValidator<?>> fieldLoadSaveValidatorTypeMap = new HashMap<>();

    Map<String, FieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap = new HashMap<>();
    fieldLoadSaveValidatorAttributeMap.put( "avroFields", new ListLoadSaveValidator<AvroInputMeta.AvroField>(
      new FieldLoadSaveValidator<AvroInputMeta.AvroField>() {
        private final StringLoadSaveValidator stringLoadSaveValidator = new StringLoadSaveValidator();
        private final ListLoadSaveValidator<String> stringListLoadSaveValidator =
          new ListLoadSaveValidator<>( stringLoadSaveValidator );

        @Override public AvroInputMeta.AvroField getTestObject() {
          AvroInputMeta.AvroField avroField = new AvroInputMeta.AvroField();
          avroField.m_kettleType = stringLoadSaveValidator.getTestObject();
          avroField.m_fieldPath = stringLoadSaveValidator.getTestObject();
          avroField.m_fieldName = stringLoadSaveValidator.getTestObject();
          avroField.m_indexedVals = stringListLoadSaveValidator.getTestObject();
          return avroField;
        }

        @Override public boolean validateTestObject( AvroInputMeta.AvroField avroField, Object o ) {
          if ( !( o instanceof AvroInputMeta.AvroField ) ) {
            return false;
          }
          AvroInputMeta.AvroField avroField2 = (AvroInputMeta.AvroField) o;
          return stringLoadSaveValidator.validateTestObject( avroField.m_kettleType, avroField2.m_kettleType )
            && stringLoadSaveValidator.validateTestObject( avroField.m_fieldPath, avroField2.m_fieldPath )
            && stringLoadSaveValidator.validateTestObject( avroField.m_fieldName, avroField2.m_fieldName )
            && stringListLoadSaveValidator.validateTestObject( avroField.m_indexedVals, avroField2.m_indexedVals );
        }
      } ) );
    fieldLoadSaveValidatorAttributeMap.put( "lookupFields", new ListLoadSaveValidator<AvroInputMeta.LookupField>(
      new FieldLoadSaveValidator<AvroInputMeta.LookupField>() {
        private final StringLoadSaveValidator stringLoadSaveValidator = new StringLoadSaveValidator();

        @Override public AvroInputMeta.LookupField getTestObject() {
          AvroInputMeta.LookupField lookupField = new AvroInputMeta.LookupField();
          lookupField.m_fieldName = stringLoadSaveValidator.getTestObject();
          lookupField.m_variableName = stringLoadSaveValidator.getTestObject();
          lookupField.m_defaultValue = stringLoadSaveValidator.getTestObject();
          return lookupField;
        }

        @Override public boolean validateTestObject( AvroInputMeta.LookupField lookupField, Object o ) {
          if ( !( o instanceof AvroInputMeta.LookupField ) ) {
            return false;
          }
          AvroInputMeta.LookupField lookupField2 = (AvroInputMeta.LookupField) o;
          return stringLoadSaveValidator.validateTestObject( lookupField.m_fieldName, lookupField2.m_fieldName )
            && stringLoadSaveValidator.validateTestObject( lookupField.m_variableName, lookupField2.m_variableName )
            && stringLoadSaveValidator.validateTestObject( lookupField.m_defaultValue, lookupField2.m_defaultValue );
        }
      } ) );

    LoadSaveTester<AvroInputMeta> avroInputMetaLoadSaveTester =
      new LoadSaveTester<AvroInputMeta>( AvroInputMeta.class, commonAttributes, new HashMap<String, String>(),
        new HashMap<String, String>(), fieldLoadSaveValidatorAttributeMap, fieldLoadSaveValidatorTypeMap );

    avroInputMetaLoadSaveTester.testSerialization();
  }

  @Test
  public void testGetStepData() {
    assertTrue( avroInputMeta.getStepData() instanceof AvroInputData );
  }

  @Test
  public void testGetStep() {
    StepMockHelper<AvroInputMeta, AvroInputData> stepMockHelper =
      new StepMockHelper<AvroInputMeta, AvroInputData>( "avro", AvroInputMeta.class, AvroInputData.class );
    when( stepMockHelper.logChannelInterfaceFactory.create( any(), any( LoggingObjectInterface.class ) ) )
      .thenReturn( mock( LogChannelInterface.class ) );
    assertTrue( avroInputMeta
      .getStep( stepMockHelper.stepMeta, stepMockHelper.stepDataInterface, 0, stepMockHelper.transMeta,
        stepMockHelper.trans ) instanceof AvroInput );
  }

  @Test
  public void testIndexedValsList() {
    String val1 = "val1";
    String val2 = "val2";
    String val3 = "val3";
    List<String> indexedValsList = AvroInputMeta.indexedValsList( val1 + " , " + val2 + "," + val3 + " " );
    assertEquals( new ArrayList<>( Arrays.asList( val1, val2, val3 ) ), indexedValsList );
    assertEquals( val1 + "," + val2 + "," + val3, AvroInputMeta.indexedValsList( indexedValsList ) );
  }

  @Test
  public void testSetDefault() {
    // Doesn't do anything
    avroInputMeta.setDefault();
  }

  @Test
  public void testGetDialogClassName() {
    assertEquals( AvroInputDialog.class.getCanonicalName(), avroInputMeta.getDialogClassName() );
  }

  @Test
  public void testSupportsErrorHandling() {
    assertTrue( avroInputMeta.supportsErrorHandling() );
  }

  @Test
  public void testGetFieldsMFields() throws KettleStepException {
    AvroInputMeta.AvroField avroField = new AvroInputMeta.AvroField();
    avroField.m_fieldName = "testFieldName";
    avroField.m_kettleType = "String";
    String testOrigin = "testOrigin";
    String abc = "abc";
    avroField.m_indexedVals = new ArrayList<>( Arrays.asList( abc ) );
    avroInputMeta.setAvroFields( new ArrayList<>( Arrays.asList( avroField ) ) );
    RowMetaInterface rowMeta = mock( RowMetaInterface.class );
    avroInputMeta.getFields( DefaultBowl.getInstance(), rowMeta, testOrigin, new RowMetaInterface[ 0 ],
      mock( StepMeta.class ), mock( VariableSpace.class ) );
    ArgumentCaptor<ValueMetaInterface> valueMetaInterfaceArgumentCaptor = ArgumentCaptor.forClass( ValueMetaInterface.class );
    verify( rowMeta ).addValueMeta( valueMetaInterfaceArgumentCaptor.capture() );
    ValueMetaInterface valueMetaInterface = valueMetaInterfaceArgumentCaptor.getValue();
    assertEquals( avroField.m_fieldName, valueMetaInterface.getName() );
    assertEquals( testOrigin, valueMetaInterface.getOrigin() );
    assertEquals( ValueMetaInterface.TYPE_STRING, valueMetaInterface.getType() );
    assertArrayEquals( new Object[] { abc }, valueMetaInterface.getIndex() );
  }
}
