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

package com.pentaho.big.data.bundles.impl.shim.hbase;

public class FilePathModifierUtil {
  private static final String FILE_SCHEME_PREFIX = "file:///";
  private static final String WINDOWS_SPECIFIC_STRING = ":\\";
  private static final String PATH_SCHEME_STRING = "://";

  public static String modifyPathToConfigFileIfNecessary( String pathToFile ) {
    String modifiedPathToFile = modifyIfNullValue( pathToFile );

    return isWindowsSpecificPath( modifiedPathToFile ) && !hasPathScheme( modifiedPathToFile )
      ? addFileSchemeToPath( modifiedPathToFile ) : modifiedPathToFile;
  }

  private static boolean isWindowsSpecificPath( String pathToFile ) {
    return pathToFile.contains( WINDOWS_SPECIFIC_STRING );
  }

  private static boolean hasPathScheme( String pathToFile ) {
    return pathToFile.contains( PATH_SCHEME_STRING );
  }

  private static String addFileSchemeToPath( String pathToFile ) {
    return FILE_SCHEME_PREFIX.concat( pathToFile );
  }

  private static String modifyIfNullValue( String pathToFile ) {
    return pathToFile == null ? "" : pathToFile;
  }
}
