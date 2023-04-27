/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.mapreduce.step.enter;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.kettle.plugins.mapreduce.ui.step.enter.HadoopEnterDialog;

import static org.junit.Assert.assertEquals;

/**
 * Created by bryan on 1/15/16.
 */
public class HadoopEnterMetaTest {

  private HadoopEnterMeta hadoopEnterMeta;

  @Before
  public void setup() throws Throwable {
    hadoopEnterMeta = new HadoopEnterMeta();
  }

  @Test
  public void testConstructor() {
    assertEquals( HadoopEnterMeta.KEY_FIELDNAME, hadoopEnterMeta.getFieldname()[ 0 ] );
    assertEquals( HadoopEnterMeta.VALUE_FIELDNAME, hadoopEnterMeta.getFieldname()[ 1 ] );
  }

  @Test
  public void testSetDefault() {
    hadoopEnterMeta.setFieldname( new String[ 0 ] );
    hadoopEnterMeta.setDefault();
    testConstructor();
  }

  @Test
  public void testGetDialogClassName() {
    assertEquals( HadoopEnterDialog.class.getCanonicalName(), hadoopEnterMeta.getDialogClassName() );
  }

  @Test
  public void testSetters() {
    hadoopEnterMeta.setKeyType( 1 );
    assertEquals( 1, hadoopEnterMeta.getType()[0] );
    hadoopEnterMeta.setKeyLength( 2 );
    assertEquals( 2, hadoopEnterMeta.getLength()[0] );
    hadoopEnterMeta.setKeyPrecision( 3 );
    assertEquals( 3, hadoopEnterMeta.getPrecision()[0] );
    hadoopEnterMeta.setValueType( 1 );
    assertEquals( 1, hadoopEnterMeta.getType()[1] );
    hadoopEnterMeta.setValueLength( 2 );
    assertEquals( 2, hadoopEnterMeta.getLength()[1] );
    hadoopEnterMeta.setValuePrecision( 3 );
    assertEquals( 3, hadoopEnterMeta.getPrecision()[1] );
  }
}
