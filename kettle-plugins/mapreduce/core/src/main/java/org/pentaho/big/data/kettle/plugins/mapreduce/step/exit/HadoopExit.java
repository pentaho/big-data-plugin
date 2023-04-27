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

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

public class HadoopExit extends BaseStep implements StepInterface {
  private static final Class<?> PKG = HadoopExit.class;

  private HadoopExitMeta meta;
  private HadoopExitData data;

  public HadoopExit( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  public void runtimeInit() throws KettleException {
    data.init( getTransMeta().getBowl(), getInputRowMeta(), meta, this );
  }

  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    meta = (HadoopExitMeta) smi;
    data = (HadoopExitData) sdi;

    Object[] r = getRow();
    if ( r == null ) {
      // no more input to be expected...
      setOutputDone();
      return false;
    }

    if ( first ) {
      runtimeInit();
      first = false;
    }

    Object[] outputRow = new Object[2];
    outputRow[HadoopExitData.getOutKeyOrdinal()] = r[data.getInKeyOrdinal()];
    outputRow[HadoopExitData.getOutValueOrdinal()] = r[data.getInValueOrdinal()];

    putRow( data.getOutputRowMeta(), outputRow );

    if ( checkFeedback( getLinesRead() ) ) {
      logBasic( BaseMessages.getString( PKG, "HadoopExit.Linenr", getLinesRead() ) );
    }

    return true;
  }
}
