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
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.variables.VariableSpace;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 10/21/15.
 */
public class AvroInputMetaLookupFieldTest {

  private AvroInputMeta.LookupField lookupField;
  private RowMetaInterface rowMetaInterface;
  private VariableSpace variableSpace;
  private Map<String, String> variableSpaceMap;
  private ValueMetaInterface valueMetaInterface;

  @Before
  public void setup() {
    lookupField = new AvroInputMeta.LookupField();
    lookupField.m_fieldName = "testFieldName";
    lookupField.m_variableName = "testVariableName";
    rowMetaInterface = mock( RowMetaInterface.class );
    when( rowMetaInterface.indexOfValue( lookupField.m_fieldName ) ).thenReturn( 0 );
    valueMetaInterface = new ValueMetaString();
    when( rowMetaInterface.getValueMeta( 0 ) ).thenReturn( valueMetaInterface );
    when( rowMetaInterface.indexOfValue( lookupField.m_fieldName ) ).thenReturn( 0 );
    variableSpace = mock( VariableSpace.class );
    variableSpaceMap = new HashMap<>();
    when( variableSpace.environmentSubstitute( anyString() ) ).thenAnswer( new Answer<String>() {
      @Override public String answer( InvocationOnMock invocation ) throws Throwable {
        return variableSpaceMap.get( invocation.getArguments()[ 0 ] );
      }
    } );
  }

  @Test
  public void testInitNullRowMeta() {
    assertFalse( lookupField.init( null, null ) );
  }

  @Test
  public void testInitFieldMissing() {
    assertFalse( lookupField.init( new RowMeta(), null ) );
  }

  @Test
  public void testInitVariableNameEmpty() {
    lookupField.m_variableName = "";
    assertFalse( lookupField.init( rowMetaInterface, null ) );
  }

  @Test
  public void testInit() {
    assertTrue( lookupField.init( rowMetaInterface, variableSpace ) );
  }

  @Test
  public void setVariableInvalid() {
    lookupField.init( null, null );
    lookupField.setVariable( variableSpace, new Object[ 0 ] );
    verifyNoMoreInteractions( variableSpace );
  }

  @Test
  public void testSetVariableNull() {
    lookupField.init( rowMetaInterface, variableSpace );
    lookupField.setVariable( variableSpace, new Object[] { null } );
    verify( variableSpace ).setVariable( lookupField.m_cleansedVariableName, "null" );
  }

  @Test
  public void testSetVariableExceptionToNull() throws KettleValueException {
    valueMetaInterface = mock( ValueMetaInterface.class );
    rowMetaInterface = mock( RowMetaInterface.class );
    when( rowMetaInterface.indexOfValue( lookupField.m_variableName ) ).thenReturn( 0 );
    when( rowMetaInterface.getValueMeta( 0 ) ).thenReturn( valueMetaInterface );
    Object o = new Object();
    when( valueMetaInterface.isNull( o ) ).thenThrow( new KettleValueException() );
    lookupField.init( rowMetaInterface, variableSpace );
    lookupField.setVariable( variableSpace, new Object[] { o } );
    verify( variableSpace ).setVariable( lookupField.m_cleansedVariableName, "null" );
  }

  @Test
  public void testSetVariableNotNull() {
    lookupField.init( rowMetaInterface, variableSpace );
    String test = "test";
    lookupField.setVariable( variableSpace, new Object[] { test } );
    verify( variableSpace ).setVariable( lookupField.m_cleansedVariableName, test );
  }

  @Test
  public void testSetVariableNullDefault() {
    lookupField.init( rowMetaInterface, variableSpace );
    lookupField.m_resolvedDefaultValue = "resdef";
    lookupField.setVariable( variableSpace, new Object[] { null } );
    verify( variableSpace ).setVariable( lookupField.m_cleansedVariableName, lookupField.m_resolvedDefaultValue );
  }
}
