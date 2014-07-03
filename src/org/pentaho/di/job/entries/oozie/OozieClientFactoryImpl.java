/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2014 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.job.entries.oozie;

import org.pentaho.hadoop.shim.ShimVersion;
import org.pentaho.oozie.shim.api.OozieClient;
import org.pentaho.oozie.shim.api.OozieClientFactory;

public class OozieClientFactoryImpl implements OozieClientFactory {

  @Override
  public ShimVersion getVersion() {
    return new ShimVersion( 0, 0 );
  }

  @Override
  public OozieClient create( String oozieUrl ) {
    return new OozieClientImpl( new org.apache.oozie.client.OozieClient( oozieUrl ) );
  }

}
