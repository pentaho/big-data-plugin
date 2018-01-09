/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.hdfs.trans;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HadoopFileInputDialogTest {

  @Test
  public void getFriendlyURIsUnsecure() {
    HadoopFileInputDialog dialog = mock( HadoopFileInputDialog.class );
    when( dialog.getFriendlyURIs( any() ) ).thenCallRealMethod();

    String[] files = new String[] {
      "hdfs://clouderaserver01.pentaho.com:8020/wordcount/parse/weblogsA.txt",
      "hdfs://clouderaserver02.pentaho.com:8020/wordcount/parse/weblogsB.txt",
      "hdfs://clouderaserver03.pentaho.com:8020/wordcount/parse/weblogsC.txt"
    };

    String[] friendly = dialog.getFriendlyURIs( files );

    assertEquals( files[0], friendly[0] );
    assertEquals( files[1], friendly[1] );
    assertEquals( files[2], friendly[2] );
  }

  @Test
  public void getFriendlyURIsSecure() {
    HadoopFileInputDialog dialog = mock( HadoopFileInputDialog.class );
    when( dialog.getFriendlyURIs( any() ) ).thenCallRealMethod();

    String[] files = new String[] {
      "hdfs://user01:pwd01@clouderaserver01.pentaho.com:8020/wordcount/parse/weblogsA.txt",
      "hdfs://user02@clouderaserver02.pentaho.com:8020/wordcount/parse/weblogsB.txt",
      "hdfs://user03:pwd03@clouderaserver03.pentaho.com:8020/wordcount/parse/weblogsC.txt"
    };

    String[] friendly = dialog.getFriendlyURIs( files );

    assertEquals( "hdfs://user01:***@clouderaserver01.pentaho.com:8020/wordcount/parse/weblogsA.txt", friendly[0] );
    assertEquals( files[1], friendly[1] );
    assertEquals( "hdfs://user03:***@clouderaserver03.pentaho.com:8020/wordcount/parse/weblogsC.txt", friendly[2] );
  }

}
