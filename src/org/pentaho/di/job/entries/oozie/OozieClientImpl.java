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

import java.util.Properties;

import org.apache.oozie.client.OozieClient;
import org.pentaho.oozie.shim.api.OozieClientException;
import org.pentaho.oozie.shim.api.OozieJob;

public class OozieClientImpl implements org.pentaho.oozie.shim.api.OozieClient {
  private final OozieClient delegate;

  public OozieClientImpl( OozieClient delegate ) {
    this.delegate = delegate;
  }

  @Override
  public String getClientBuildVersion() {
    return delegate.getClientBuildVersion();
  }

  @Override
  public OozieJob getJob( String jobId ) {
    return new OozieJobInfoImpl( jobId, delegate );
  }

  @Override
  public String getProtocolUrl() throws OozieClientException {
    try {
      return delegate.getProtocolUrl();
    } catch ( org.apache.oozie.client.OozieClientException e ) {
      throw new OozieClientException( e.getCause(), e.getErrorCode() );
    }
  }

  @Override
  public boolean hasAppPath( Properties props ) {
    return props.containsKey( OozieClient.APP_PATH ) || props.containsKey( OozieClient.COORDINATOR_APP_PATH )
        || props.containsKey( OozieClient.BUNDLE_APP_PATH );
  }

  @Override
  public OozieJob run( Properties props ) throws OozieClientException {
    try {
      String jobId = delegate.run( props );
      return new OozieJobInfoImpl( jobId, delegate );
    } catch ( org.apache.oozie.client.OozieClientException e ) {
      throw new OozieClientException( e.getCause(), e.getErrorCode() );
    }
  }

  @Override
  public void validateWSVersion() throws OozieClientException {
    try {
      delegate.validateWSVersion();
    } catch ( org.apache.oozie.client.OozieClientException e ) {
      throw new OozieClientException( e.getCause(), e.getErrorCode() );
    }
  }

}
