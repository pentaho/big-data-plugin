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
