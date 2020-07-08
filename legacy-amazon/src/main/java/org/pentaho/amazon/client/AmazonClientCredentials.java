/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2020 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.amazon.client;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.regions.RegionUtils;
import org.pentaho.amazon.AmazonRegion;
import org.pentaho.di.core.util.StringUtil;

/**
 * Created by Aliaksandr_Zhuk on 2/5/2018.
 */
public class AmazonClientCredentials {

  private AWSCredentials credentials;
  private String region;

  public AmazonClientCredentials( String accessKey, String secretKey, String sessionToken, String region ) {
    if ( !StringUtil.isEmpty( sessionToken ) ) {
      credentials = new BasicSessionCredentials( accessKey, secretKey, sessionToken );
    } else {
      credentials = new BasicAWSCredentials( accessKey, secretKey );
    }
    this.region = extractRegion( region );
  }

  public AWSCredentials getAWSCredentials() {
    return credentials;
  }

  public String getRegion() {
    return region;
  }

  private String extractRegion( String region ) {
    return RegionUtils.getRegion( AmazonRegion.extractRegionFromDescription( region ) ).getName();
  }
}
