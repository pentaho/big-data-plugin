/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/



package org.pentaho.big.data.kettle.plugins.formats.impl.parquet.input;

public class VFSScheme {

  private final String scheme;

  private final String schemeName;

  public VFSScheme( String scheme, String schemeName ) {
    this.scheme = scheme;
    this.schemeName = schemeName;
  }

  public String getScheme() {
    return scheme;
  }

  public String getSchemeName() {
    return schemeName;
  }

}
