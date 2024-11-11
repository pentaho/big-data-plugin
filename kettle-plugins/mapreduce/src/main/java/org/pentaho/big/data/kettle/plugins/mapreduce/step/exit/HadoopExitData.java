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


package org.pentaho.big.data.kettle.plugins.mapreduce.step.exit;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class HadoopExitData extends BaseStepData implements StepDataInterface {
  private RowMetaInterface outputRowMeta = null;

  private int inKeyOrdinal = -1;
  private int inValueOrdinal = -1;

  public static final int outKeyOrdinal = 0;
  public static final int outValueOrdinal = 1;

  public HadoopExitData() {
    super();
  }

  public void init( RowMetaInterface rowMeta, HadoopExitMeta stepMeta, VariableSpace space ) throws KettleException {
    if ( rowMeta != null ) {
      outputRowMeta = rowMeta.clone();
      stepMeta.getFields( outputRowMeta, stepMeta.getName(), null, null, space );

      setInKeyOrdinal( rowMeta.indexOfValue( stepMeta.getOutKeyFieldname() ) );
      setInValueOrdinal( rowMeta.indexOfValue( stepMeta.getOutValueFieldname() ) );
    }
  }

  public RowMetaInterface getOutputRowMeta() {
    return outputRowMeta;
  }

  public void setInKeyOrdinal( int inKeyOrdinal ) {
    this.inKeyOrdinal = inKeyOrdinal;
  }

  public int getInKeyOrdinal() {
    return inKeyOrdinal;
  }

  public void setInValueOrdinal( int inValueOrdinal ) {
    this.inValueOrdinal = inValueOrdinal;
  }

  public int getInValueOrdinal() {
    return inValueOrdinal;
  }

  public static int getOutKeyOrdinal() {
    return outKeyOrdinal;
  }

  public static int getOutValueOrdinal() {
    return outValueOrdinal;
  }
}
