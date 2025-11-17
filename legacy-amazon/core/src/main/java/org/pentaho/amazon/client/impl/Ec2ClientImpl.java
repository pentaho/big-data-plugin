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


package org.pentaho.amazon.client.impl;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeSubnetsRequest;
import com.amazonaws.services.ec2.model.DescribeSubnetsResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Subnet;
import com.amazonaws.services.ec2.model.Tag;
import java.util.ArrayList;
import java.util.List;
import org.pentaho.amazon.AmazonRegion;
import org.pentaho.amazon.client.api.Ec2Client;

/**
 * Implementation of EC2 Client for interacting with AWS EC2 services
 */
public class Ec2ClientImpl implements Ec2Client {

  private AmazonEC2 ec2Client;

  public Ec2ClientImpl( String awsAccessKeyId, String awsSecretKey, String awsSessionToken, String region ) {
    AmazonEC2ClientBuilder ec2ClientBuilder = AmazonEC2ClientBuilder.standard();

    // Set up credentials
    if ( awsSessionToken != null && !awsSessionToken.trim().isEmpty() ) {
      // Use session credentials if session token is provided
      BasicSessionCredentials sessionCredentials = 
        new BasicSessionCredentials( awsAccessKeyId, awsSecretKey, awsSessionToken );
      ec2ClientBuilder.withCredentials( new AWSStaticCredentialsProvider( sessionCredentials ) );
    } else {
      // Use basic credentials
      BasicAWSCredentials awsCredentials = new BasicAWSCredentials( awsAccessKeyId, awsSecretKey );
      ec2ClientBuilder.withCredentials( new AWSStaticCredentialsProvider( awsCredentials ) );
    }

    // Set region - convert from human-readable to AWS region code
    if ( region != null && !region.trim().isEmpty() ) {
      String regionCode = AmazonRegion.extractRegionFromDescription( region );
      ec2ClientBuilder.withRegion( RegionUtils.getRegion( regionCode ).getName() );
    }

    ec2Client = ec2ClientBuilder.build();
  }

  @Override
  public List<SubnetInfo> getAvailableSubnets() {
    List<SubnetInfo> subnetInfoList = new ArrayList<>();

    try {
      // Create request to describe all subnets
      DescribeSubnetsRequest request = new DescribeSubnetsRequest();
      
      // Optional: Filter to only show available subnets
      Filter availableFilter = new Filter( "state", java.util.Arrays.asList( "available" ) );
      request.withFilters( availableFilter );

      DescribeSubnetsResult result = ec2Client.describeSubnets( request );

      // Process each subnet
      for ( Subnet subnet : result.getSubnets() ) {
        String subnetId = subnet.getSubnetId();
        String vpcId = subnet.getVpcId();
        String availabilityZone = subnet.getAvailabilityZone();
        String cidrBlock = subnet.getCidrBlock();
        String state = subnet.getState();
        
        // Extract subnet name from tags
        String subnetName = extractNameFromTags( subnet.getTags() );

        SubnetInfo subnetInfo = new SubnetInfo( 
          subnetId, 
          subnetName, 
          vpcId, 
          availabilityZone, 
          cidrBlock, 
          state 
        );
        
        subnetInfoList.add( subnetInfo );
      }
    } catch ( Exception e ) {
      // Log error but don't throw - return empty list instead
      System.err.println( "Error retrieving subnets: " + e.getMessage() );
      e.printStackTrace();
    }

    return subnetInfoList;
  }

  /**
   * Extracts the Name tag value from a list of EC2 tags
   * 
   * @param tags List of EC2 tags
   * @return The value of the Name tag, or empty string if not found
   */
  private String extractNameFromTags( List<Tag> tags ) {
    if ( tags == null || tags.isEmpty() ) {
      return "";
    }

    for ( Tag tag : tags ) {
      if ( "Name".equals( tag.getKey() ) ) {
        return tag.getValue();
      }
    }

    return "";
  }
}
