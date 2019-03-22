/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
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
