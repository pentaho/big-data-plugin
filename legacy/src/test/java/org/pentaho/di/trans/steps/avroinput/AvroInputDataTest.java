/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

import org.apache.avro.util.Utf8;
import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.variables.Variables;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by bryan on 10/21/15.
 */
public class AvroInputDataTest {
  @Test
  public void testCleansePath() {
    assertEquals( "const.name", AvroInputData.cleansePath( "const.name" ) );
    assertEquals( "const.${var_name}", AvroInputData.cleansePath( "const.${var.name}" ) );
    assertEquals( "const.${var.name", AvroInputData.cleansePath( "const.${var.name" ) );
    assertEquals( "const.${var_name}.${var_2_name}.const",
      AvroInputData.cleansePath( "const.${var.name}.${var.2.name}.const" ) );
  }

  @Test( expected = KettleException.class )
  public void testAvroArrayExpansionInitThrowsExceptionIfExpansionPathIsEmpty() throws KettleException {
    new AvroInputData.AvroArrayExpansion( Collections.<AvroInputMeta.AvroField>emptyList() ).init();
  }

  @Test
  public void testConvertToKettleValuesNullMap() throws KettleException {
    assertNull( new AvroInputData.AvroArrayExpansion( Collections.<AvroInputMeta.AvroField>emptyList() )
      .convertToKettleValues( (Map<Utf8, Object>) null, null, null, null, true ) );
  }

  @Test( expected = KettleException.class )
  public void testConvertToKettleValuesPartsMapMalformed() throws KettleException {
    AvroInputMeta.AvroField avroField = new AvroInputMeta.AvroField();
    avroField.m_fieldName = "a";
    avroField.m_fieldPath = "c.b.a";
    avroField.init( 0 );
    RowMeta rowMeta = new RowMeta();
    ValueMetaInterface valueMeta = new ValueMetaString();
    valueMeta.setName( avroField.m_fieldName );
    avroField.m_kettleType = valueMeta.getTypeDesc();
    rowMeta.addValueMeta( valueMeta );
    AvroInputData.AvroArrayExpansion avroArrayExpansion =
      new AvroInputData.AvroArrayExpansion( Arrays.asList( avroField ) );
    avroArrayExpansion.m_outputRowMeta = rowMeta;
    avroArrayExpansion.m_expansionPath = "c";
    avroArrayExpansion.init();
    assertNull( avroArrayExpansion
      .convertToKettleValues( new HashMap<Utf8, Object>(), null, null, null, true ) );
  }

  @Test( expected = KettleException.class )
  public void testConvertToKettleValuesMapMalformed2() throws KettleException {
    AvroInputMeta.AvroField avroField = new AvroInputMeta.AvroField();
    avroField.m_fieldName = "a";
    avroField.m_fieldPath = "c.b.a";
    avroField.init( 0 );
    RowMeta rowMeta = new RowMeta();
    ValueMetaInterface valueMeta = new ValueMetaString();
    valueMeta.setName( avroField.m_fieldName );
    avroField.m_kettleType = valueMeta.getTypeDesc();
    rowMeta.addValueMeta( valueMeta );
    AvroInputData.AvroArrayExpansion avroArrayExpansion =
      new AvroInputData.AvroArrayExpansion( Arrays.asList( avroField ) );
    avroArrayExpansion.m_outputRowMeta = rowMeta;
    avroArrayExpansion.m_expansionPath = "c";
    avroArrayExpansion.init();
    avroArrayExpansion.reset( new Variables() );
    avroArrayExpansion.convertToKettleValues( new HashMap<Utf8, Object>(), null, null, null, true );
  }

  public void testConvertToKettleValuesMapMalformed() throws KettleException {
    AvroInputMeta.AvroField avroField = new AvroInputMeta.AvroField();
    avroField.m_fieldName = "a";
    avroField.m_fieldPath = "c.b.a";
    avroField.init( 0 );
    RowMeta rowMeta = new RowMeta();
    ValueMetaInterface valueMeta = new ValueMetaString();
    valueMeta.setName( avroField.m_fieldName );
    avroField.m_kettleType = valueMeta.getTypeDesc();
    rowMeta.addValueMeta( valueMeta );
    AvroInputData.AvroArrayExpansion avroArrayExpansion =
      new AvroInputData.AvroArrayExpansion( Arrays.asList( avroField ) );
    avroArrayExpansion.m_outputRowMeta = rowMeta;
    avroArrayExpansion.m_expansionPath = "c";
    avroArrayExpansion.init();
    avroArrayExpansion.reset( new Variables() );
    avroArrayExpansion.convertToKettleValues( new HashMap<Utf8, Object>(), null, null, null, true );
  }
}
