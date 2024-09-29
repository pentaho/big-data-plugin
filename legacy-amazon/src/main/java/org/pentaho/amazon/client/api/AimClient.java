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


package org.pentaho.amazon.client.api;

import org.pentaho.ui.xul.util.AbstractModelList;

/**
 * Created by Aliaksandr_Zhuk on 2/5/2018.
 */
public interface AimClient {

  AbstractModelList<String> getEc2RolesFromAmazonAccount();

  AbstractModelList<String> getEmrRolesFromAmazonAccount();
}
