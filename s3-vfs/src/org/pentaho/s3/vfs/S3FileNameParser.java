/*
 * Copyright 2010 Pentaho Corporation.  All rights reserved.
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
 * @author Michael D'Amour
 */
package org.pentaho.s3.vfs;

import org.apache.commons.vfs.provider.FileNameParser;
import org.apache.commons.vfs.provider.URLFileNameParser;

public class S3FileNameParser extends URLFileNameParser {

  private static final S3FileNameParser INSTANCE = new S3FileNameParser();

  public S3FileNameParser() {
    // S3 wants port 843, but the web service will use this by default
    super(843);
  }

  public static FileNameParser getInstance() {
    return INSTANCE;
  }
}
