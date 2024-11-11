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
