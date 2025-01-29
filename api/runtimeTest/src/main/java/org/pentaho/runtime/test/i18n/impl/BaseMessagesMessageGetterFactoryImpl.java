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

import org.pentaho.runtime.test.i18n.MessageGetter;
import org.pentaho.runtime.test.i18n.MessageGetterFactory;

/**
 * Created by bryan on 8/21/15.
 */
public class BaseMessagesMessageGetterFactoryImpl implements MessageGetterFactory {
  @Override public MessageGetter create( Class<?> PKG ) {
    return new BaseMessagesMessageGetterImpl( PKG );
  }
}
