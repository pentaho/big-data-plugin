/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.impl.cluster.tests.hdfs;

import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystemLocator;
import org.pentaho.runtime.test.i18n.MessageGetterFactory;

/**
 * Created by bryan on 8/14/15.
 */
public class ListRootDirectoryTest extends ListDirectoryTest {
  public static final String HADOOP_FILE_SYSTEM_LIST_ROOT_DIRECTORY_TEST =
    "hadoopFileSystemListRootDirectoryTest";
  public static final String LIST_ROOT_DIRECTORY_TEST_NAME = "ListRootDirectoryTest.Name";
  private static final Class<?> PKG = ListRootDirectoryTest.class;

  public ListRootDirectoryTest( MessageGetterFactory messageGetterFactory,
                                HadoopFileSystemLocator hadoopFileSystemLocator ) {
    super( messageGetterFactory, hadoopFileSystemLocator, "/", HADOOP_FILE_SYSTEM_LIST_ROOT_DIRECTORY_TEST,
      messageGetterFactory.create( PKG ).getMessage( LIST_ROOT_DIRECTORY_TEST_NAME ) );
  }
}
