/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2002 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/


package org.pentaho.di.core.hadoop;
import org.pentaho.hadoop.shim.api.ConfigurationException;

/**
 * Created by bryan on 8/19/15.
 */
public class NoShimSpecifiedException extends ConfigurationException {
  public NoShimSpecifiedException( String message ) {
    super( message );
  }
}
