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

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.pentaho.oozie.shim.api.OozieClientException;
import org.pentaho.oozie.shim.api.OozieJob;

public class OozieJobInfoImpl implements OozieJob {
  private final String id;
  private final OozieClient oozieClient;

  public OozieJobInfoImpl( String id, OozieClient oozieClient ) {
    this.id = id;
    this.oozieClient = oozieClient;
  }

  @Override
  public boolean didSucceed() throws OozieClientException {
    try {
      return oozieClient.getJobInfo( id ).getStatus().equals( WorkflowJob.Status.SUCCEEDED );
    } catch ( org.apache.oozie.client.OozieClientException e ) {
      throw new OozieClientException( e.getCause(), e.getErrorCode() );
    }
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getJobLog() throws OozieClientException {
    try {
      return oozieClient.getJobLog( id );
    } catch ( org.apache.oozie.client.OozieClientException e ) {
      throw new OozieClientException( e.getCause(), e.getErrorCode() );
    }
  }

  @Override
  public boolean isRunning() throws OozieClientException {
    try {
      return oozieClient.getJobInfo( id ).getStatus().equals( WorkflowJob.Status.RUNNING );
    } catch ( org.apache.oozie.client.OozieClientException e ) {
      throw new OozieClientException( e.getCause(), e.getErrorCode() );
    }
  }

}
