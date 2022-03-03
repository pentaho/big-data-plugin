/*!
 * Copyright 2021 Hitachi Vantara. All rights reserved.
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
 */

package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.endpoints;

import org.apache.commons.fileupload.FileItemStream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogChannelInterfaceFactory;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.metastore.stores.delegate.DelegatingMetaStore;
import org.pentaho.osgi.metastore.locator.api.MetastoreLocator;
import org.pentaho.runtime.test.RuntimeTester;
import java.io.File;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class HadoopClusterEndpointsTest {

  @Mock private LogChannelInterfaceFactory logChannelFactory;
  @Mock private LogChannelInterface logChannel;

  @Mock private MetastoreLocator metaStoreLocator;
  @Mock private DelegatingMetaStore metaStore;
  @Mock private NamedClusterService namedClusterService;
  @Mock private RuntimeTester runtimeTester;

  @Mock private FileItemStream fileItemStream;

  private String internalShim = "";
  private boolean secureEnabled = false;

  @Before
  public void setUp() {
    KettleLogStore.setLogChannelInterfaceFactory( logChannelFactory );
    when( logChannelFactory.create( any(), any() ) ).thenReturn( logChannel );
    when( logChannelFactory.create( any() ) ).thenReturn( logChannel );
    when( metaStoreLocator.getMetastore() ).thenReturn( metaStore );
  }

  @Test
  public void testCopyAndUnzip() throws IOException {
    File zippedSiteFiles = new File( "src/test/resources/zippedSiteFiles/site.zip" );
    InputStream zippedInputStream = new FileInputStream( zippedSiteFiles );

    String name = zippedSiteFiles.getName();

    when( fileItemStream.openStream() ).thenReturn( zippedInputStream );
    when( fileItemStream.getName() ).thenReturn( name );
    when( fileItemStream.getFieldName() ).thenReturn( name );

    HadoopClusterEndpoints hce =
      new HadoopClusterEndpoints( metaStoreLocator, namedClusterService, runtimeTester, internalShim, secureEnabled );

    List<CachedFileItemStream> cachedFileItemStreams =
      hce.copyAndUnzip( fileItemStream, HadoopClusterEndpoints.FileType.CONFIGURATION, fileItemStream.getFieldName() );

    assertEquals( 6, cachedFileItemStreams.size() );

    Map<String, Integer> zipFileSizeByName = new HashMap<>();
    zipFileSizeByName.put( "core-site.xml", 3875 );
    zipFileSizeByName.put( "hbase-site.xml", 3121 );
    zipFileSizeByName.put( "hdfs-site.xml", 1778 );
    zipFileSizeByName.put( "hive-site.xml", 5918 );
    zipFileSizeByName.put( "mapred-site.xml", 5178 );
    zipFileSizeByName.put( "yarn-site.xml", 3689 );

    for ( CachedFileItemStream cachedFileItemStream : cachedFileItemStreams ) {
      int unzippedSize = zipFileSizeByName.get( cachedFileItemStream.getFieldName() );
      assertEquals( unzippedSize, cachedFileItemStream.getCachedOutputStream().size() );
      assertEquals( name, cachedFileItemStream.getName() );
    }
  }
}
