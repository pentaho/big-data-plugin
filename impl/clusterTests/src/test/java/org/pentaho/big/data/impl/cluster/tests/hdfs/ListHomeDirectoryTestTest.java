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

import org.junit.Before;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystemLocator;
import org.pentaho.runtime.test.TestMessageGetterFactory;
import org.pentaho.runtime.test.i18n.MessageGetter;
import org.pentaho.runtime.test.i18n.MessageGetterFactory;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Created by bryan on 8/24/15.
 */
public class ListHomeDirectoryTestTest {
  private MessageGetterFactory messageGetterFactory;
  private HadoopFileSystemLocator hadoopFileSystemLocator;
  private ListHomeDirectoryTest listHomeDirectoryTest;
  private MessageGetter messageGetter;

  @Before
  public void setup() {
    messageGetterFactory = new TestMessageGetterFactory();
    messageGetter = messageGetterFactory.create( ListHomeDirectoryTest.class );
    hadoopFileSystemLocator = mock( HadoopFileSystemLocator.class );
    listHomeDirectoryTest = new ListHomeDirectoryTest( messageGetterFactory, hadoopFileSystemLocator );
  }

  @Test
  public void testGetName() {
    assertEquals( messageGetter.getMessage( ListHomeDirectoryTest.LIST_HOME_DIRECTORY_TEST_NAME ),
      listHomeDirectoryTest.getName() );
  }
}
