/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

import com.amazonaws.auth.BasicAWSCredentials;
import org.pentaho.amazon.AmazonRegions;

/**
 * Created by Aliaksandr_Zhuk on 2/5/2018.
 */
public abstract class AbstractAmazonClient {

  private static BasicAWSCredentials credentials;
  private static String humanReadableRegion;
  private static String region;

  private static void setAWSCredentials( String accessKey, String secretKey ) {
    credentials = new BasicAWSCredentials( accessKey, secretKey );
  }

  public static BasicAWSCredentials getAWSCredentials() {
    return credentials;
  }

  private static void setRegion( String awsRegion ) {
    humanReadableRegion = awsRegion;
    region = extractRegionFromDescription();
  }

  public static String getHumanReadableRegion() {
    return humanReadableRegion;
  }

  public static String getRegion() {
    return region;
  }

  public static void initClientParameters( String accessKey, String secretKey, String region ) {
    setAWSCredentials( accessKey, secretKey );
    setRegion( region );
  }

  private static String extractRegionFromDescription() {
    for ( AmazonRegions region : AmazonRegions.values() ) {
      if ( humanReadableRegion.contains( region.getRegionIds().get( 2 ) ) ) {
        return region.toAWSRegion().getName();
      }
    }
    return AmazonRegions.US_EAST_1.name();
  }
}
