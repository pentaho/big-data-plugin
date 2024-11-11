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


package org.pentaho.big.data.kettle.plugins.hbase.mapping;

import org.pentaho.di.core.row.RowMetaInterface;

/**
 * Interface to something that can provide meta data on the fields that it is receiving
 * 
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision$
 * 
 */
public interface FieldProducer {

  /**
   * Get the incoming fields
   * 
   * @return the incoming fields
   */
  RowMetaInterface getIncomingFields();
}
