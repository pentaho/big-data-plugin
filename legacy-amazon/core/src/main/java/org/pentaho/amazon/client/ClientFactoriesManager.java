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


package org.pentaho.amazon.client;

import org.pentaho.amazon.client.impl.AimClientFactory;
import org.pentaho.amazon.client.impl.EmrClientFactory;
import org.pentaho.amazon.client.impl.PricingClientFactory;
import org.pentaho.amazon.client.impl.S3ClientFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Aliaksandr_Zhuk on 2/5/2018.
 */
public class ClientFactoriesManager {

  private Map<ClientType, AbstractClientFactory> clientFactoryMap;
  private static ClientFactoriesManager instance;

  private ClientFactoriesManager() {
    clientFactoryMap = new HashMap<>();
    clientFactoryMap.put( ClientType.S3, new S3ClientFactory() );
    clientFactoryMap.put( ClientType.EMR, new EmrClientFactory() );
    clientFactoryMap.put( ClientType.AIM, new AimClientFactory() );
    clientFactoryMap.put( ClientType.PRICING, new PricingClientFactory() );
  }

  public static ClientFactoriesManager getInstance() {
    if ( instance == null ) {
      instance = new ClientFactoriesManager();
    }
    return instance;
  }

  public <T> T createClient( String accessKey, String secretKey, String sessionToken, String region, ClientType clientType ) {
    AbstractClientFactory clientFactory = getClientFactory( clientType );
    T amazonClient = (T) clientFactory.createClient( accessKey, secretKey, sessionToken, region );
    return amazonClient;
  }

  private AbstractClientFactory getClientFactory( ClientType clientType ) {
    if ( clientFactoryMap.containsKey( clientType ) ) {
      return clientFactoryMap.get( clientType );
    }
    return null;
  }
}
