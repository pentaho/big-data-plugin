/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2020 by Hitachi Vantara : http://www.pentaho.com
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
