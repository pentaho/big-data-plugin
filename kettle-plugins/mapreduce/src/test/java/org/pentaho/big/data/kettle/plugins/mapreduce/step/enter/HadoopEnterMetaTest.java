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
