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

import org.pentaho.amazon.client.AbstractClientFactory;
import org.pentaho.amazon.client.api.Ec2Client;

/**
 * Factory for creating EC2 client instances
 */
public class Ec2ClientFactory extends AbstractClientFactory<Ec2Client> {

  @Override
  public Ec2Client createClient( String accessKey, String secretKey, String sessionToken, String region ) {
    return new Ec2ClientImpl( accessKey, secretKey, sessionToken, region );
  }
}
