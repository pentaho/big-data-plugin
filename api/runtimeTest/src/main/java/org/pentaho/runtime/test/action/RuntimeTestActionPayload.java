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


package org.pentaho.runtime.test.action;

/**
 * Created by bryan on 9/9/15.
 */
public interface RuntimeTestActionPayload {
  /**
   * This will be called and logged when the Action isn't handled by any registered handlers
   *
   * @return the message associated with the payload
   */
  String getMessage();
}
