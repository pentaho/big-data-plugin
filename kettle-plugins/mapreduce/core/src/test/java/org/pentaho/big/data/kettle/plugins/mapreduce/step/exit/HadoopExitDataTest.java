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


package org.pentaho.big.data.kettle.plugins.mapreduce.step.exit;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.bowl.DefaultBowl;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Created by bryan on 1/15/16.
 */
public class HadoopExitDataTest {
  private HadoopExitData hadoopExitData;
  private RowMetaInterface rowMetaInterface;

  @Before
  public void setup() {
    rowMetaInterface = mock( RowMetaInterface.class );
    hadoopExitData = new HadoopExitData();
  }

  @Test
  public void testInitNullRowMeta() throws KettleException {
    // This would npe if the rowmeta check wasn't there
    hadoopExitData.init( DefaultBowl.getInstance(), null, null, null );
  }

  @Test
  public void testInit() throws KettleException {
    String name = "hadoopExitMeta";
    String outKeyFieldName = "outKeyFieldName";
    String outValueFieldName = "outValueFieldName";

    RowMetaInterface outputRowMeta = mock( RowMetaInterface.class );
    HadoopExitMeta hadoopExitMeta = mock( HadoopExitMeta.class );
    when( hadoopExitMeta.getName() ).thenReturn( name );
    when( hadoopExitMeta.getOutKeyFieldname() ).thenReturn( outKeyFieldName );
    when( hadoopExitMeta.getOutValueFieldname() ).thenReturn( outValueFieldName );
    VariableSpace space = mock( VariableSpace.class );

    when( rowMetaInterface.clone() ).thenReturn( outputRowMeta );
    when( rowMetaInterface.indexOfValue( outKeyFieldName ) ).thenReturn( 5 );
    when( rowMetaInterface.indexOfValue( outValueFieldName ) ).thenReturn( 6 );

    hadoopExitData.init( DefaultBowl.getInstance(), rowMetaInterface, hadoopExitMeta, space );

    assertEquals( outputRowMeta, hadoopExitData.getOutputRowMeta() );
    verify( hadoopExitMeta ).getFields( DefaultBowl.getInstance(), outputRowMeta, name, null, null, space );
    assertEquals( 5, hadoopExitData.getInKeyOrdinal() );
    assertEquals( 6, hadoopExitData.getInValueOrdinal() );
  }

  @Test
  public void testGetOutKeyOrdinal() {
    assertEquals( HadoopExitData.outKeyOrdinal, HadoopExitData.getOutKeyOrdinal() );
  }

  @Test
  public void testGetOutValueOrdinal() {
    assertEquals( HadoopExitData.outValueOrdinal, HadoopExitData.getOutValueOrdinal() );
  }
}
