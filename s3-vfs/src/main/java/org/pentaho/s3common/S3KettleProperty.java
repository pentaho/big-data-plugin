/*!
 * Copyright 2020 Hitachi Vantara.  All rights reserved.
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

package org.pentaho.s3common;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.i18n.BaseMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Class that handles operations dealing with kettle property file.
 */
public class S3KettleProperty {
  private static final Class<?> PKG = S3KettleProperty.class;
  private static final Logger logger = LoggerFactory.getLogger( S3KettleProperty.class );
  public static final String S3VFS_PART_SIZE = "s3.vfs.partSize";

  public String getPartSize() {
    return getProperty( S3VFS_PART_SIZE );
  }

  public String getProperty( String property ) {
    String filename =  Const.getKettlePropertiesFilename();
    Properties properties;
    String partSizeString = "";
    try {
      properties = EnvUtil.readProperties( filename );
      partSizeString = properties.getProperty( property );
    } catch ( KettleException ke ) {
      logger.error( BaseMessages.getString( PKG, "WARN.S3Commmon.PropertyNotFound",
        property, filename ) );
    }
    return partSizeString;
  }
}
