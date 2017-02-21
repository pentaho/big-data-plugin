/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

package org.pentaho.big.data.impl.shim.oozie;

import org.pentaho.bigdata.api.oozie.OozieServiceException;
import org.pentaho.bigdata.api.oozie.OozieJobInfo;
import org.pentaho.bigdata.api.oozie.OozieService;
import org.pentaho.oozie.shim.api.OozieClient;
import org.pentaho.oozie.shim.api.OozieClientException;
import org.pentaho.oozie.shim.api.OozieJob;

import static org.apache.oozie.client.OozieClient.APP_PATH;
import static org.apache.oozie.client.OozieClient.COORDINATOR_APP_PATH;
import static org.apache.oozie.client.OozieClient.BUNDLE_APP_PATH;

import java.util.Properties;

public class OozieServiceImpl implements OozieService {

  private final OozieClient delegate;

  public OozieServiceImpl( OozieClient oozieClient ) {
    this.delegate = oozieClient;
  }

  @Override
  public String getClientBuildVersion() {
    return delegate.getClientBuildVersion();
  }

  @Override
  public String getProtocolUrl() throws OozieServiceException {
    try {
      return delegate.getProtocolUrl();
    } catch ( OozieClientException e ) {
      throw new OozieServiceException( e, e.getErrorCode() );
    }
  }

  @Override
  public boolean hasAppPath( Properties props ) {
    return props.containsKey( APP_PATH )
      || props.containsKey( COORDINATOR_APP_PATH )
      || props.containsKey( BUNDLE_APP_PATH );
  }

  @Override
  public OozieJobInfo run( Properties props ) throws OozieServiceException {
    try {
      OozieJob job = delegate.run( props );
      return new OozieJobInfoDelegate( job );
    } catch ( OozieClientException e ) {
      throw new OozieServiceException( e, e.getErrorCode() );
    }
  }

  @Override
  public void validateWSVersion() throws OozieServiceException {
    try {
      delegate.validateWSVersion();
    } catch ( OozieClientException e ) {
      throw new OozieServiceException( e, e.getErrorCode() );
    }
  }

}
