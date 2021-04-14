/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2021 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.impl.vfs.hdfs;

import org.apache.commons.vfs2.provider.URLFileNameParser;

@SuppressWarnings( "deprecation" )
public class AzureHdInsightsFileNameParser extends URLFileNameParser {

  public static final String EMPTY_HOSTNAME = "";

  private static final AzureHdInsightsFileNameParser INSTANCE = new AzureHdInsightsFileNameParser();

  private AzureHdInsightsFileNameParser() {
    super( -1 );
  }

  public static AzureHdInsightsFileNameParser getInstance() {
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
