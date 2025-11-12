/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.amazon.client.api;

import java.util.List;

/**
 * EC2 Client interface for interacting with AWS EC2 services
 */
public interface Ec2Client {

  /**
   * Retrieves a list of available VPC subnets in the configured region
   * 
   * @return List of subnet information containing subnet ID, name, VPC ID, availability zone, and CIDR block
   */
  List<SubnetInfo> getAvailableSubnets();

  /**
   * Represents information about an AWS VPC subnet
   */
  class SubnetInfo {
    private final String subnetId;
    private final String subnetName;
    private final String vpcId;
    private final String availabilityZone;
    private final String cidrBlock;
    private final String state;

    public SubnetInfo( String subnetId, String subnetName, String vpcId, 
                      String availabilityZone, String cidrBlock, String state ) {
      this.subnetId = subnetId;
      this.subnetName = subnetName;
      this.vpcId = vpcId;
      this.availabilityZone = availabilityZone;
      this.cidrBlock = cidrBlock;
      this.state = state;
    }

    public String getSubnetId() {
      return subnetId;
    }

    public String getSubnetName() {
      return subnetName != null && !subnetName.isEmpty() ? subnetName : subnetId;
    }

    public String getVpcId() {
      return vpcId;
    }

    public String getAvailabilityZone() {
      return availabilityZone;
    }

    public String getCidrBlock() {
      return cidrBlock;
    }

    public String getState() {
      return state;
    }

    /**
     * Returns a display string for UI dropdown
     * Format: "subnet-name (subnet-id) - AZ: az-name - CIDR: cidr-block"
     */
    public String getDisplayString() {
      StringBuilder display = new StringBuilder();
      display.append( getSubnetName() );
      if ( subnetName != null && !subnetName.isEmpty() && !subnetName.equals( subnetId ) ) {
        display.append( " (" ).append( subnetId ).append( ")" );
      }
      display.append( " - AZ: " ).append( availabilityZone );
      display.append( " - CIDR: " ).append( cidrBlock );
      return display.toString();
    }

    @Override
    public String toString() {
      return getDisplayString();
    }
  }
}
