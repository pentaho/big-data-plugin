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

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;

import java.io.IOException;
import java.io.InputStream;

public class LumadaPropertiesFileCredentialsProvider extends ClasspathPropertiesFileCredentialsProvider {
  public static final String SLASH = "/";
  private static String defaultPropertiesFile = "AwsCredentials.properties";
  private final String credentialsFilePath;

  public LumadaPropertiesFileCredentialsProvider() {
    this( defaultPropertiesFile );
  }

  public LumadaPropertiesFileCredentialsProvider( String credentialsFilePath ) {
    if ( credentialsFilePath == null ) {
      throw new IllegalArgumentException( "Credentials file path cannot be null" );
    } else {
      if ( !credentialsFilePath.startsWith( "/" ) ) {
        this.credentialsFilePath = SLASH + credentialsFilePath;
      } else {
        this.credentialsFilePath = credentialsFilePath;
      }

    }
  }

  @Override
  public AWSCredentials getCredentials() {
    InputStream inputStream = this.getClass().getResourceAsStream( this.credentialsFilePath );
    if ( inputStream == null ) {
      throw new SdkClientException(
        "Unable to load AWS credentials from the " + this.credentialsFilePath + " file on the classpath" );
    } else {
      try {
        return new LumadaPropertiesCredentials( inputStream );
      } catch ( IOException var3 ) {
        throw new SdkClientException(
          "Unable to load AWS credentials from the " + this.credentialsFilePath + " file on the classpath", var3 );
      }
    }
  }
}
