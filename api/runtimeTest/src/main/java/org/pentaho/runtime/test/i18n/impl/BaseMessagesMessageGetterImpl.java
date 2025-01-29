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


package org.pentaho.runtime.test.i18n.impl;

import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.runtime.test.i18n.MessageGetter;

/**
 * Created by bryan on 8/21/15.
 */
public class BaseMessagesMessageGetterImpl implements MessageGetter {
  private final Class<?> PKG;

  public BaseMessagesMessageGetterImpl( Class<?> PKG ) {
    this.PKG = PKG;
  }

  @Override public String getMessage( String key, String... parameters ) {
    if ( parameters != null && parameters.length > 0 ) {
      return BaseMessages.getString( PKG, key, parameters );
    } else {
      return BaseMessages.getString( PKG, key );
    }
  }
}
