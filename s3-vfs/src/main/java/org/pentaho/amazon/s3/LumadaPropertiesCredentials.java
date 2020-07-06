/*!
 * Copyright 2019 - 2020 Hitachi Vantara.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.pentaho.amazon.s3;

import com.amazonaws.auth.AWSCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class LumadaPropertiesCredentials implements AWSCredentials {
  private static final Logger logger = LoggerFactory.getLogger( LumadaPropertiesCredentials.class );

  public static final String ACCESS_KEY = "accessKey";
  public static final String SECRET_KEY = "secretKey";
  public static final String ENDPOINT_URL = "endpointUrl";
  public static final String API_SIGNATURE = "apiSignature";
  private final String accessKey;
  private final String secretAccessKey;
  private final String endpointUrl;
  private final String apiSignature;

  public LumadaPropertiesCredentials( File file ) throws IOException {
    if ( !file.exists() ) {
      throw new FileNotFoundException( "File doesn't exist:  " + file.getAbsolutePath() );
    } else {
      try ( FileInputStream stream = new FileInputStream( file ) ) {
        Properties accountProperties = new Properties();
        accountProperties.load( stream );
        if ( accountProperties.getProperty( ACCESS_KEY ) == null
          || accountProperties.getProperty( SECRET_KEY ) == null
          || accountProperties.getProperty( ENDPOINT_URL ) == null
          || accountProperties.getProperty( API_SIGNATURE ) == null ) {
          throw new IllegalArgumentException( "The specified file (" + file.getAbsolutePath()
            + ") doesn't contain the expected properties 'accessKey' and 'secretKey'." );
        }

        this.accessKey = accountProperties.getProperty( ACCESS_KEY );
        this.secretAccessKey = accountProperties.getProperty( SECRET_KEY );
        this.endpointUrl = accountProperties.getProperty( ENDPOINT_URL );
        this.apiSignature = accountProperties.getProperty( API_SIGNATURE );
      }
    }
  }

  public LumadaPropertiesCredentials( InputStream inputStream ) throws IOException {
    Properties accountProperties = new Properties();

    try {
      accountProperties.load( inputStream );
    } finally {
      try {
        inputStream.close();
      } catch ( Exception ex ) {
        logger.info( ex.getMessage() );
      }

    }

    if ( accountProperties.getProperty( ACCESS_KEY ) != null
      && accountProperties.getProperty( SECRET_KEY ) != null
      && accountProperties.getProperty( ENDPOINT_URL ) != null
      && accountProperties.getProperty( API_SIGNATURE ) != null ) {
      this.accessKey = accountProperties.getProperty( ACCESS_KEY );
      this.secretAccessKey = accountProperties.getProperty( SECRET_KEY );
      this.endpointUrl = accountProperties.getProperty( ENDPOINT_URL );
      this.apiSignature = accountProperties.getProperty( API_SIGNATURE );
    } else {
      throw new IllegalArgumentException(
        "The specified properties data doesn't contain the expected properties 'accessKey', 'secretKey', "
          + "'endpointUrl', and 'apiSignature'." );
    }
  }

  public String getAWSAccessKeyId() {
    return this.accessKey;
  }

  public String getAWSSecretKey() {
    return this.secretAccessKey;
  }

  public String getEndpointUrl() {
    return this.endpointUrl;
  }

  public String getApiSignature() {
    return this.apiSignature;
  }
}
