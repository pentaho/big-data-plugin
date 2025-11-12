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

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.DescribeSubnetsRequest;
import com.amazonaws.services.ec2.model.DescribeSubnetsResult;
import com.amazonaws.services.ec2.model.Subnet;
import com.amazonaws.services.ec2.model.Tag;
import java.lang.reflect.Field;
import java.util.List;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import static org.mockito.Mockito.*;
import org.pentaho.amazon.client.api.Ec2Client;

/**
 * Unit tests for Ec2ClientImpl
 */
public class Ec2ClientImplTest {

  private AmazonEC2 mockEc2Client;
  private Ec2ClientImpl ec2Client;

  @Before
  public void setUp() throws Exception {
    mockEc2Client = mock( AmazonEC2.class );
    ec2Client = new Ec2ClientImpl( "accessKey", "secretKey", "", "US East (N. Virginia)" );
    
    // Inject mock EC2 client using reflection
    Field ec2ClientField = Ec2ClientImpl.class.getDeclaredField( "ec2Client" );
    ec2ClientField.setAccessible( true );
    ec2ClientField.set( ec2Client, mockEc2Client );
  }

  @Test
  public void testGetAvailableSubnets_Success() {
    // Arrange
    Subnet subnet1 = new Subnet()
      .withSubnetId( "subnet-12345" )
      .withVpcId( "vpc-abcde" )
      .withAvailabilityZone( "us-east-1a" )
      .withCidrBlock( "10.0.1.0/24" )
      .withState( "available" )
      .withTags( new Tag( "Name", "Test Subnet 1" ) );

    Subnet subnet2 = new Subnet()
      .withSubnetId( "subnet-67890" )
      .withVpcId( "vpc-fghij" )
      .withAvailabilityZone( "us-east-1b" )
      .withCidrBlock( "10.0.2.0/24" )
      .withState( "available" )
      .withTags( new Tag( "Name", "Test Subnet 2" ) );

    DescribeSubnetsResult result = new DescribeSubnetsResult()
      .withSubnets( subnet1, subnet2 );

    when( mockEc2Client.describeSubnets( any( DescribeSubnetsRequest.class ) ) )
      .thenReturn( result );

    // Act
    List<Ec2Client.SubnetInfo> subnets = ec2Client.getAvailableSubnets();

    // Assert
    assertNotNull( "Subnets list should not be null", subnets );
    assertEquals( "Should return 2 subnets", 2, subnets.size() );

    Ec2Client.SubnetInfo subnet1Info = subnets.get( 0 );
    assertEquals( "subnet-12345", subnet1Info.getSubnetId() );
    assertEquals( "vpc-abcde", subnet1Info.getVpcId() );
    assertEquals( "us-east-1a", subnet1Info.getAvailabilityZone() );
    assertEquals( "10.0.1.0/24", subnet1Info.getCidrBlock() );
    assertEquals( "available", subnet1Info.getState() );
    assertEquals( "Test Subnet 1", subnet1Info.getSubnetName() );

    // Verify the request has the correct filter
    ArgumentCaptor<DescribeSubnetsRequest> requestCaptor = 
      ArgumentCaptor.forClass( DescribeSubnetsRequest.class );
    verify( mockEc2Client ).describeSubnets( requestCaptor.capture() );
    
    DescribeSubnetsRequest capturedRequest = requestCaptor.getValue();
    assertNotNull( "Request should have filters", capturedRequest.getFilters() );
    assertEquals( "Should have one filter", 1, capturedRequest.getFilters().size() );
    assertEquals( "state", capturedRequest.getFilters().get( 0 ).getName() );
  }

  @Test
  public void testGetAvailableSubnets_NoNameTag() throws Exception {
    // Arrange - subnet without Name tag (empty tag list, no Name tag)
    Subnet subnet = new Subnet()
      .withSubnetId( "subnet-12345" )
      .withVpcId( "vpc-12345" )
      .withAvailabilityZone( "us-east-1a" )
      .withCidrBlock( "10.0.1.0/24" )
      .withState( "available" );
    // Don't set tags at all - this simulates a subnet with no tags

    DescribeSubnetsResult describeSubnetsResult = new DescribeSubnetsResult()
      .withSubnets( subnet );

    when( mockEc2Client.describeSubnets( any( DescribeSubnetsRequest.class ) ) )
      .thenReturn( describeSubnetsResult );

    // Act
    List<Ec2Client.SubnetInfo> subnets = ec2Client.getAvailableSubnets();

    // Assert
    assertEquals( "Should return 1 subnet", 1, subnets.size() );
    Ec2Client.SubnetInfo subnetInfo = subnets.get( 0 );
    assertEquals( "subnet-12345", subnetInfo.getSubnetId() );
    // When no Name tag exists, getSubnetName() returns the subnet ID as fallback
    assertEquals( "Subnet name should fallback to subnet ID when no Name tag", 
                  "subnet-12345", subnetInfo.getSubnetName() );
    assertEquals( "vpc-12345", subnetInfo.getVpcId() );
  }

  @Test
  public void testGetAvailableSubnets_EmptyResult() {
    // Arrange
    DescribeSubnetsResult result = new DescribeSubnetsResult()
      .withSubnets();

    when( mockEc2Client.describeSubnets( any( DescribeSubnetsRequest.class ) ) )
      .thenReturn( result );

    // Act
    List<Ec2Client.SubnetInfo> subnets = ec2Client.getAvailableSubnets();

    // Assert
    assertNotNull( "Subnets list should not be null", subnets );
    assertTrue( "Subnets list should be empty", subnets.isEmpty() );
  }

  @Test
  public void testGetAvailableSubnets_Exception() {
    // Arrange
    when( mockEc2Client.describeSubnets( any( DescribeSubnetsRequest.class ) ) )
      .thenThrow( new RuntimeException( "AWS error" ) );

    // Act
    List<Ec2Client.SubnetInfo> subnets = ec2Client.getAvailableSubnets();

    // Assert
    assertNotNull( "Should return empty list on exception", subnets );
    assertTrue( "Should return empty list on exception", subnets.isEmpty() );
  }

  @Test
  public void testSubnetInfo_GetDisplayString() {
    // Arrange
    Ec2Client.SubnetInfo subnetInfo = new Ec2Client.SubnetInfo(
      "subnet-12345",
      "Test Subnet",
      "vpc-abcde",
      "us-east-1a",
      "10.0.1.0/24",
      "available"
    );

    // Act
    String displayString = subnetInfo.getDisplayString();

    // Assert
    assertEquals( "Test Subnet (subnet-12345) - AZ: us-east-1a - CIDR: 10.0.1.0/24", 
                  displayString );
  }

  @Test
  public void testSubnetInfo_GetDisplayString_NoName() {
    // Arrange
    Ec2Client.SubnetInfo subnetInfo = new Ec2Client.SubnetInfo(
      "subnet-12345",
      null,
      "vpc-abcde",
      "us-east-1a",
      "10.0.1.0/24",
      "available"
    );

    // Act
    String displayString = subnetInfo.getDisplayString();

    // Assert
    assertEquals( "subnet-12345 - AZ: us-east-1a - CIDR: 10.0.1.0/24", 
                  displayString );
  }

  @Test
  public void testSubnetInfo_GetDisplayString_EmptyName() {
    // Arrange
    Ec2Client.SubnetInfo subnetInfo = new Ec2Client.SubnetInfo(
      "subnet-12345",
      "",
      "vpc-abcde",
      "us-east-1a",
      "10.0.1.0/24",
      "available"
    );

    // Act
    String displayString = subnetInfo.getDisplayString();

    // Assert
    assertEquals( "subnet-12345 - AZ: us-east-1a - CIDR: 10.0.1.0/24", 
                  displayString );
  }

  @Test
  public void testSubnetInfo_Getters() {
    // Arrange
    Ec2Client.SubnetInfo subnetInfo = new Ec2Client.SubnetInfo(
      "subnet-12345",
      "Test Subnet",
      "vpc-abcde",
      "us-east-1a",
      "10.0.1.0/24",
      "available"
    );

    // Assert
    assertEquals( "subnet-12345", subnetInfo.getSubnetId() );
    assertEquals( "Test Subnet", subnetInfo.getSubnetName() );
    assertEquals( "vpc-abcde", subnetInfo.getVpcId() );
    assertEquals( "us-east-1a", subnetInfo.getAvailabilityZone() );
    assertEquals( "10.0.1.0/24", subnetInfo.getCidrBlock() );
    assertEquals( "available", subnetInfo.getState() );
  }

  @Test
  public void testGetAvailableSubnets_MultipleTags() {
    // Arrange - subnet with multiple tags
    Subnet subnet = new Subnet()
      .withSubnetId( "subnet-12345" )
      .withVpcId( "vpc-abcde" )
      .withAvailabilityZone( "us-east-1a" )
      .withCidrBlock( "10.0.1.0/24" )
      .withState( "available" )
      .withTags( 
        new Tag( "Environment", "Production" ),
        new Tag( "Name", "Prod Subnet" ),
        new Tag( "Owner", "DevOps" )
      );

    DescribeSubnetsResult result = new DescribeSubnetsResult()
      .withSubnets( subnet );

    when( mockEc2Client.describeSubnets( any( DescribeSubnetsRequest.class ) ) )
      .thenReturn( result );

    // Act
    List<Ec2Client.SubnetInfo> subnets = ec2Client.getAvailableSubnets();

    // Assert
    assertNotNull( subnets );
    assertEquals( 1, subnets.size() );
    assertEquals( "Prod Subnet", subnets.get( 0 ).getSubnetName() );
  }
}
