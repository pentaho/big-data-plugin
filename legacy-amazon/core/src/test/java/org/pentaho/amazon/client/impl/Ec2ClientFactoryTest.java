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

import static org.junit.Assert.*;
import org.junit.Test;
import org.pentaho.amazon.client.api.Ec2Client;

/**
 * Unit tests for Ec2ClientFactory
 */
public class Ec2ClientFactoryTest {

  @Test
  public void testCreateClient_WithBasicCredentials() {
    // Arrange
    Ec2ClientFactory factory = new Ec2ClientFactory();
    String accessKey = "AKIAIOSFODNN7EXAMPLE";
    String secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    String region = "US East (N. Virginia)";

    // Act
    Ec2Client client = factory.createClient( accessKey, secretKey, null, region );

    // Assert
    assertNotNull( "Client should not be null", client );
    assertTrue( "Client should be instance of Ec2ClientImpl", 
                client instanceof Ec2ClientImpl );
  }

  @Test
  public void testCreateClient_WithSessionToken() {
    // Arrange
    Ec2ClientFactory factory = new Ec2ClientFactory();
    String accessKey = "AKIAIOSFODNN7EXAMPLE";
    String secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    String sessionToken = "FwoGZXIvYXdzEBYaDPCX3EXAMPLE";
    String region = "US West (Oregon)";

    // Act
    Ec2Client client = factory.createClient( accessKey, secretKey, sessionToken, region );

    // Assert
    assertNotNull( "Client should not be null", client );
    assertTrue( "Client should be instance of Ec2ClientImpl", 
                client instanceof Ec2ClientImpl );
  }

  @Test
  public void testCreateClient_WithEmptySessionToken() {
    // Arrange
    Ec2ClientFactory factory = new Ec2ClientFactory();
    String accessKey = "AKIAIOSFODNN7EXAMPLE";
    String secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    String sessionToken = "";
    String region = "US East (Ohio)";

    // Act
    Ec2Client client = factory.createClient( accessKey, secretKey, sessionToken, region );

    // Assert
    assertNotNull( "Client should not be null", client );
    assertTrue( "Client should be instance of Ec2ClientImpl", 
                client instanceof Ec2ClientImpl );
  }

  @Test
  public void testCreateClient_DifferentRegions() {
    // Arrange
    Ec2ClientFactory factory = new Ec2ClientFactory();
    String accessKey = "AKIAIOSFODNN7EXAMPLE";
    String secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

    String[] regions = {
      "US East (N. Virginia)",
      "US West (Oregon)",
      "EU (Ireland)",
      "Asia Pacific (Singapore)"
    };

    // Act & Assert
    for ( String region : regions ) {
      Ec2Client client = factory.createClient( accessKey, secretKey, null, region );
      assertNotNull( "Client should not be null for region: " + region, client );
    }
  }
}
