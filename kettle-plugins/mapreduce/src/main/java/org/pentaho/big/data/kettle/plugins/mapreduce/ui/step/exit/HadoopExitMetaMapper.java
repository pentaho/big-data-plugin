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

package org.pentaho.big.data.kettle.plugins.mapreduce.ui.step.exit;

import org.pentaho.big.data.kettle.plugins.mapreduce.step.exit.HadoopExitMeta;
import org.pentaho.ui.xul.XulEventSourceAdapter;

public class HadoopExitMetaMapper extends XulEventSourceAdapter {
  public static String OUT_KEY_FIELDNAME = "out-key-fieldname";
  public static String OUT_VALUE_FIELDNAME = "out-value-fieldname";

  protected String outKeyFieldname;
  protected String outValueFieldname;

  public void setOutKeyFieldname( String arg ) {
    String previousVal = outKeyFieldname;
    outKeyFieldname = arg;
    firePropertyChange( OUT_KEY_FIELDNAME, previousVal, outKeyFieldname );
  }

  public String getOutKeyFieldname() {
    return outKeyFieldname;
  }

  public void setOutValueFieldname( String arg ) {
    String previousVal = outValueFieldname;
    outValueFieldname = arg;
    firePropertyChange( OUT_VALUE_FIELDNAME, previousVal, outValueFieldname );
  }

  public String getOutValueFieldname() {
    return outValueFieldname;
  }

  /**
   * Load data into the MetaMapper from the HadoopExitMeta
   * 
   * @param meta
   */
  public void loadMeta( HadoopExitMeta meta ) {
    setOutKeyFieldname( meta.getOutKeyFieldname() );
    setOutValueFieldname( meta.getOutValueFieldname() );
  }

  /**
   * Save data from the MetaMapper into the HadoopExitMeta
   * 
   * @param meta
   */
  public void saveMeta( HadoopExitMeta meta ) {
    // Set outKey
    if ( meta.getOutKeyFieldname() == null && getOutKeyFieldname() != null ) {
      meta.setOutKeyFieldname( getOutKeyFieldname() );
      meta.setChanged();
    } else if ( meta.getOutKeyFieldname() != null && !meta.getOutKeyFieldname().equals( getOutKeyFieldname() ) ) {
      meta.setOutKeyFieldname( getOutKeyFieldname() );
      meta.setChanged();
    }

    // Set outValue
    if ( meta.getOutValueFieldname() == null && getOutValueFieldname() != null ) {
      meta.setOutValueFieldname( getOutValueFieldname() );
      meta.setChanged();
    } else if ( meta.getOutValueFieldname() != null && !meta.getOutValueFieldname().equals( getOutValueFieldname() ) ) {
      meta.setOutValueFieldname( getOutValueFieldname() );
      meta.setChanged();
    }
  }
}
