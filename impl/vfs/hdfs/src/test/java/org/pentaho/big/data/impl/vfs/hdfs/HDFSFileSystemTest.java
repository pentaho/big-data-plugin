/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.pentaho.big.data.impl.vfs.hdfs;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystem;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Created by bryan on 8/7/15.
 */
public class HDFSFileSystemTest {
  private FileName rootName;
  private HadoopFileSystem hadoopFileSystem;
  private HDFSFileSystem hdfsFileSystem;

  @Before
  public void setup() {
    rootName = mock( FileName.class );
    hadoopFileSystem = mock( HadoopFileSystem.class );
    hdfsFileSystem = new HDFSFileSystem( rootName, null, hadoopFileSystem );
  }

  @Test
  public void testAddCapabilities() {
    Collection caps = mock( Collection.class );
    hdfsFileSystem.addCapabilities( caps );
    verify( caps ).addAll( HDFSFileProvider.capabilities );
  }

  @Test
  public void testCreateFile() throws Exception {
    assertTrue( hdfsFileSystem.createFile( mock( AbstractFileName.class ) ) instanceof HDFSFileObject );
  }

  @Test
  public void testGetHDFSFileSystem() throws FileSystemException {
    assertEquals( hadoopFileSystem, hdfsFileSystem.getHDFSFileSystem() );
  }
}
