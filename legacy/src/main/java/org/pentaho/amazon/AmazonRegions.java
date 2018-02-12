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

import com.amazonaws.regions.RegionUtils;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Aliaksandr_Zhuk on 1/11/2018.
 */
public enum AmazonRegions {

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

  private final List<String> regionIds;

  AmazonRegions( String... regionIds ) {
    this.regionIds = regionIds != null ? Arrays.asList( regionIds ) : null;
  }


  public List<String> getRegionIds() {
    return regionIds;
  }

  public String getHumanReadableRegion() {
    return getHumanReadableRegion0();
  }

  private String getHumanReadableRegion0() {
    return this.regionIds == null || regionIds.size() == 0
      ? null : this.regionIds.get( 2 ) + " (" + this.regionIds.get( 1 ) + ")";
  }

  public String getFirstRegionId() {
    return getFirstRegionId0();
  }

  private String getFirstRegionId0() {
    return this.regionIds == null || regionIds.size() == 0
      ? null : this.regionIds.get( 0 );
  }

  public com.amazonaws.regions.Region toAWSRegion() {
    String s3regionId = getFirstRegionId();
    if ( s3regionId == null ) { // US Standard
      return RegionUtils.getRegion( "us-east-1" );
    } else {
      return RegionUtils.getRegion( s3regionId );
    }
  }
}
