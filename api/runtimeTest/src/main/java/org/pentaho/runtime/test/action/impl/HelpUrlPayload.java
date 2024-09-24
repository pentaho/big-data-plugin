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
