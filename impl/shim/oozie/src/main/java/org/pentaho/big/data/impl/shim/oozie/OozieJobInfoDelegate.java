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

import org.pentaho.bigdata.api.oozie.OozieJobInfo;
import org.pentaho.bigdata.api.oozie.OozieServiceException;
import org.pentaho.oozie.shim.api.OozieClientException;
import org.pentaho.oozie.shim.api.OozieJob;

public class OozieJobInfoDelegate implements OozieJobInfo {
  private final OozieJob oozieJob;

  public OozieJobInfoDelegate( OozieJob oozieJob ) {
    this.oozieJob = oozieJob;
  }

  @Override
  public boolean didSucceed() throws OozieServiceException {
    try {
      return oozieJob.didSucceed();
    } catch ( OozieClientException e ) {
      throw new OozieServiceException( e.getCause(), e.getErrorCode() );
    }
  }

  @Override
  public String getId() {
    return oozieJob.getId();
  }

  @Override
  public String getJobLog() throws OozieServiceException {
    try {
      return oozieJob.getJobLog();
    } catch ( OozieClientException e ) {
      throw new OozieServiceException( e.getCause(), e.getErrorCode() );
    }

  }

  @Override
  public boolean isRunning() throws OozieServiceException {
    try {
      return oozieJob.isRunning();
    } catch ( OozieClientException e ) {
      throw new OozieServiceException( e.getCause(), e.getErrorCode() );
    }
  }

}
