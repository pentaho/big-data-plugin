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


package org.pentaho.big.data.impl.vfs.hdfs;

import org.apache.commons.vfs2.provider.URLFileNameParser;

public class MapRFileNameParser extends URLFileNameParser {

  public static final String EMPTY_HOSTNAME = "";

  private static final MapRFileNameParser INSTANCE = new MapRFileNameParser();

  private MapRFileNameParser() {
    super( -1 );
  }

  public static MapRFileNameParser getInstance() {
    return INSTANCE;
  }

  /**
   * Extracts the hostname from a URI.
   *
   * @param name string buffer with the "scheme://[userinfo@]" part has been removed already. Will be modified.
   * @return the host name  or null.
   */
  @Override protected String extractHostName( StringBuilder name ) {
    final String hostname = super.extractHostName( name );
    // Trick the URLFileNameParser into thinking we have a hostname so we don't have to refactor it.
    return hostname == null ? EMPTY_HOSTNAME : hostname;
  }
}
