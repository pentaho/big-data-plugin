/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.amazon.s3;

import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.textfileoutput.TextFileOutput;


public class S3FileOutput extends TextFileOutput {
  /** System property name for the AWS access key ID */
  public static final String ACCESS_KEY_SYSTEM_PROPERTY = "aws.accessKeyId";

  /** System property name for the AWS secret key */
  public  static final String SECRET_KEY_SYSTEM_PROPERTY = "aws.secretKey";
  public S3FileOutput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    S3FileOutputMeta s3Meta = (S3FileOutputMeta) smi;

    String accessKeySystemProperty = System.getProperty( ACCESS_KEY_SYSTEM_PROPERTY );
    String secretKeySystemProperty = System.getProperty( SECRET_KEY_SYSTEM_PROPERTY );

    if ( !isEmpty( s3Meta.getAccessKey() ) && isEmpty( accessKeySystemProperty ) ) {
      System.setProperty( ACCESS_KEY_SYSTEM_PROPERTY, s3Meta.getAccessKey() );
    }
    if ( !isEmpty( s3Meta.getSecretKey() ) && isEmpty( secretKeySystemProperty ) ) {
      System.setProperty( SECRET_KEY_SYSTEM_PROPERTY, s3Meta.getSecretKey() );
    }
    return super.init( smi, sdi );
  }

  @Override public void markStop() {
    super.markStop();
    String accessKeySystemProperty = System.getProperty( ACCESS_KEY_SYSTEM_PROPERTY );
    String secretKeySystemProperty = System.getProperty( SECRET_KEY_SYSTEM_PROPERTY );

    if ( !isEmpty( accessKeySystemProperty ) ) {
      System.setProperty( ACCESS_KEY_SYSTEM_PROPERTY, "" );
    }
    if ( !isEmpty( secretKeySystemProperty ) ) {
      System.setProperty( SECRET_KEY_SYSTEM_PROPERTY, "" );
    }
  }

  private boolean isEmpty( String value ) {
    return value == null || value.length() <= 0;
  }
}
