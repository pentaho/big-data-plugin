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
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClientBuilder;
import org.pentaho.amazon.client.AmazonClientCredentials;
import org.pentaho.amazon.client.AbstractClientFactory;
import org.pentaho.amazon.client.api.AimClient;

/**
 * Created by Aliaksandr_Zhuk on 2/5/2018.
 */
public class AimClientFactory extends AbstractClientFactory<AimClient> {

  @Override
  public AimClient createClient( String accessKey, String secretKey, String sessionToken, String region ) {
    AmazonClientCredentials clientCredentials = new AmazonClientCredentials( accessKey, secretKey, sessionToken, region );

    AmazonIdentityManagement awsAimClient =
      AmazonIdentityManagementClientBuilder.standard().withRegion( clientCredentials.getRegion() )
        .withCredentials( new AWSStaticCredentialsProvider( clientCredentials.getAWSCredentials() ) ).build();

    AimClient aimClient = new AimClientImpl( awsAimClient );
    return aimClient;
  }
}
