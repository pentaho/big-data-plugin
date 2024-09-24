/*!
 * Copyright 2021 Hitachi Vantara.  All rights reserved.
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

import org.mockito.Mockito;

import static org.mockito.Mockito.doReturn;

public class S3CommonFileSystemTestUtil {

  public static S3CommonFileSystem stubRegionUnSet( S3CommonFileSystem fileSystem ) {
    S3CommonFileSystem fileSystemSpy = Mockito.spy( fileSystem );
    doReturn(false).when(fileSystemSpy).isRegionSet();
    return fileSystemSpy;
  }
}
