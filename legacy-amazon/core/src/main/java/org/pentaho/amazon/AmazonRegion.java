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

package org.pentaho.amazon;


/**
 * Created by Aliaksandr_Zhuk on 1/11/2018.
 */
public enum AmazonRegion {

  US_EAST_1( "us-east-1", "N. Virginia", "US East" ),
  US_EAST_2( "us-east-2", "Ohio", "US East" ),
  US_WEST_1( "us-west-1", "N. California", "US West" ),
  US_WEST_2( "us-west-2", "Oregon", "US West" ),
  AP_Mumbai( "ap-south-1", "Mumbai", "Asia Pacific" ),
  AP_Seoul( "ap-northeast-2", "Seoul", "Asia Pacific" ),
  AP_Singapore( "ap-southeast-1", "Singapore", "Asia Pacific" ),
  AP_Sydney( "ap-southeast-2", "Sydney", "Asia Pacific" ),
  AP_Tokyo( "ap-northeast-1", "Tokyo", "Asia Pacific" ),
  CA_CENTRAL( "ca-central-1", "Central", "Canada" ),
  EU_Frankfurt( "eu-central-1", "Frankfurt", "EU" ),
  EU_Ireland( "eu-west-1", "Ireland", "EU" ),
  EU_London( "eu-west-2", "London", "EU" ),
  EU_Paris( "eu-west-3", "Paris", "EU" ),
  SA_SaoPaulo( "sa-east-1", "Sao Paulo", "South America" ),
  US_GovCloud( "us-gov-west-1", "US", "AWS GovCloud" );

  private String regionId;
  private String city;
  private String region;

  private static final AmazonRegion DEFAULT_REGION = AmazonRegion.US_EAST_1;

  AmazonRegion( String regionId, String city, String region ) {
    this.regionId = regionId;
    this.city = city;
    this.region = region;
  }

  public String getHumanReadableRegion() {
    StringBuilder sb = new StringBuilder( this.region ).append( " (" ).append( this.city ).append( ")" );
    return sb.toString();
  }

  public static String extractRegionFromDescription( String humanReadableRegion ) {
    for ( AmazonRegion region : AmazonRegion.values() ) {
      if ( region.getHumanReadableRegion().equals( humanReadableRegion ) ) {
        return region.regionId;
      }
    }
    return DEFAULT_REGION.regionId;
  }
}
