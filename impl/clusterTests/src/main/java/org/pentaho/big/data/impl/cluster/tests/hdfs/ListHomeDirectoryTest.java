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
public class ListHomeDirectoryTest extends ListDirectoryTest {
  public static final String HADOOP_FILE_SYSTEM_LIST_HOME_DIRECTORY_TEST =
    "hadoopFileSystemListHomeDirectoryTest";
  public static final String LIST_HOME_DIRECTORY_TEST_NAME = "ListHomeDirectoryTest.Name";
  private static final Class<?> PKG = ListHomeDirectoryTest.class;

  public ListHomeDirectoryTest( MessageGetterFactory messageGetterFactory,
                                HadoopFileSystemLocator hadoopFileSystemLocator ) {
    super( messageGetterFactory, hadoopFileSystemLocator, "", HADOOP_FILE_SYSTEM_LIST_HOME_DIRECTORY_TEST,
      messageGetterFactory.create( PKG ).getMessage( LIST_HOME_DIRECTORY_TEST_NAME ) );
  }
}
