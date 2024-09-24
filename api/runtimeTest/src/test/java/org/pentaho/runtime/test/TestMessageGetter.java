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

package org.pentaho.runtime.test;

import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.runtime.test.i18n.MessageGetter;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by bryan on 8/21/15.
 */
public class TestMessageGetter implements MessageGetter {
  private final Class<?> PKG;

  public TestMessageGetter( Class<?> PKG ) {
    this.PKG = PKG;
  }

  @Override public String getMessage( String key, String... parameters ) {
    StringBuilder stringBuilder = new StringBuilder( "BaseMessages equivalent: BaseMessage.getMessage( " );
    stringBuilder.append( PKG );
    stringBuilder.append( ", \"" );
    stringBuilder.append( key );
    stringBuilder.append( "\"" );
    String realValue;
    boolean hasParameters = parameters != null && parameters.length > 0;
    if ( hasParameters ) {
      realValue = BaseMessages.getString( PKG, key, parameters );
      stringBuilder.append( ", \"" );
      for ( String parameter : parameters ) {
        stringBuilder.append( parameter );
        stringBuilder.append( "\", \"" );
      }
      stringBuilder.setLength( stringBuilder.length() - 3 );
    } else {
      realValue = BaseMessages.getString( PKG, key );
    }
    assertNotEquals( "!" + key + "!", realValue );
    stringBuilder.append( " )" );
    if ( hasParameters ) {
      for ( String parameter : parameters ) {
        assertTrue( "Expected " + realValue + " to contain \"" + parameter + "\"",
          realValue.contains( parameter ) );
      }
    }
    return stringBuilder.toString();
  }
}
