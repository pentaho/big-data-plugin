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

package org.pentaho.big.data.kettle.plugins.mapreduce.step;

import static org.junit.Assert.*;

import org.junit.Test;
import org.pentaho.big.data.kettle.plugins.mapreduce.step.exit.HadoopExitMeta;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;

public class HadoopExitMetaTest {

  @Test
  public void getFields() throws Throwable {
    HadoopExitMeta meta = new HadoopExitMeta();
    meta.setOutKeyFieldname( "key" );
    meta.setOutValueFieldname( "value" );

    RowMeta rowMeta = new RowMeta();
    ValueMeta valueMeta0 = new ValueMeta( "key" );
    ValueMeta valueMeta1 = new ValueMeta( "value" );
    rowMeta.addValueMeta( valueMeta0 );
    rowMeta.addValueMeta( valueMeta1 );

    meta.getFields( rowMeta, null, null, null, null );

    assertEquals( 2, rowMeta.getValueMetaList().size() );
    ValueMetaInterface vmi = rowMeta.getValueMeta( 0 );
    assertEquals( "outKey", vmi.getName() );
    vmi = rowMeta.getValueMeta( 1 );
    assertEquals( "outValue", vmi.getName() );
  }

  @Test
  public void getFields_invalid_key() throws Throwable {
    HadoopExitMeta meta = new HadoopExitMeta();
    meta.setOutKeyFieldname( "invalid" );
    meta.setOutValueFieldname( "value" );

    RowMeta rowMeta = new RowMeta();
    ValueMeta valueMeta0 = new ValueMeta( "key" );
    ValueMeta valueMeta1 = new ValueMeta( "value" );
    rowMeta.addValueMeta( valueMeta0 );
    rowMeta.addValueMeta( valueMeta1 );

    try {
      meta.getFields( rowMeta, null, null, null, null );
      fail( "expected exception" );
    } catch ( Exception ex ) {
      assertEquals(
          BaseMessages.getString( HadoopExitMeta.class, "Error.InvalidKeyField", meta.getOutKeyFieldname() ).trim(),
          ex.getMessage().trim() );
    }

    // Check that the meta was not modified
    assertEquals( 2, rowMeta.getValueMetaList().size() );
    ValueMetaInterface vmi = rowMeta.getValueMeta( 0 );
    assertEquals( "key", vmi.getName() );
    vmi = rowMeta.getValueMeta( 1 );
    assertEquals( "value", vmi.getName() );
  }

  @Test
  public void getFields_invalid_value() throws Throwable {
    HadoopExitMeta meta = new HadoopExitMeta();
    meta.setOutKeyFieldname( "key" );
    meta.setOutValueFieldname( "invalid" );

    RowMeta rowMeta = new RowMeta();
    ValueMeta valueMeta0 = new ValueMeta( "key" );
    ValueMeta valueMeta1 = new ValueMeta( "value" );
    rowMeta.addValueMeta( valueMeta0 );
    rowMeta.addValueMeta( valueMeta1 );

    try {
      meta.getFields( rowMeta, null, null, null, null );
      fail( "expected exception" );
    } catch ( Exception ex ) {
      assertEquals(
          BaseMessages.getString( HadoopExitMeta.class, "Error.InvalidValueField", meta.getOutValueFieldname() ).trim(),
          ex.getMessage().trim() );
    }

    // Check that the meta was not modified
    assertEquals( 2, rowMeta.getValueMetaList().size() );
    ValueMetaInterface vmi = rowMeta.getValueMeta( 0 );
    assertEquals( "key", vmi.getName() );
    vmi = rowMeta.getValueMeta( 1 );
    assertEquals( "value", vmi.getName() );
  }

}
