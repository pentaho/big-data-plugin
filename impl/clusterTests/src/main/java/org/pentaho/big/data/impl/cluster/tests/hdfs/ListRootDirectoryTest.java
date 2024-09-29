/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
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
