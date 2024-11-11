/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
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
