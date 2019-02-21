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

package org.pentaho.runtime.test.action.impl;

import org.pentaho.runtime.test.action.RuntimeTestActionPayload;
import org.pentaho.runtime.test.i18n.MessageGetter;
import org.pentaho.runtime.test.i18n.MessageGetterFactory;

/**
 * Created by bryan on 9/9/15.
 */
public class HelpUrlPayload implements RuntimeTestActionPayload {
  public static final String HELP_URL_PAYLOAD_MESSAGE = "HelpUrlPayload.Message";
  private final MessageGetter messageGetter;
  private final String title;
  private final String header;
  private final String url;

  public HelpUrlPayload( MessageGetterFactory messageGetterFactory, String title, String header, String url ) {
    this.messageGetter = messageGetterFactory.create( getClass() );
    this.title = title;
    this.header = header;
    this.url = url;
  }

  @Override public String getMessage() {
    return messageGetter.getMessage( HELP_URL_PAYLOAD_MESSAGE, url );
  }

  public String getTitle() {
    return title;
  }

  public String getHeader() {
    return header;
  }

  public String getUrl() {
    return url;
  }
}
