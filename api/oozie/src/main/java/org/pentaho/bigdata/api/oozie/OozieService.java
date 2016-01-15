/*******************************************************************************
 * Pentaho Big Data
 * <p/>
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
 * <p/>
 * ******************************************************************************
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 ******************************************************************************/

package org.pentaho.bigdata.api.oozie;


import java.util.Properties;

public interface OozieService {
  String getClientBuildVersion();

  String getProtocolUrl() throws OozieServiceException;

  boolean hasAppPath( Properties props );

  OozieJobInfo run( Properties props ) throws OozieServiceException;

  void validateWSVersion() throws OozieServiceException;
}
