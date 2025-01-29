/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.endpoints;

import org.apache.commons.fileupload.FileItemStream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogChannelInterfaceFactory;
import org.pentaho.di.core.service.PluginServiceLoader;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.metastore.stores.delegate.DelegatingMetaStore;
import org.pentaho.metastore.locator.api.MetastoreLocator;
import org.pentaho.runtime.test.RuntimeTester;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.ArgumentMatchers.any;
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
    Collection<MetastoreLocator> providerCollection = new ArrayList<>();
    providerCollection.add( metaStoreLocator );
    try ( MockedStatic<PluginServiceLoader> pluginServiceLoaderMockedStatic = Mockito.mockStatic( PluginServiceLoader.class ) ) {
      pluginServiceLoaderMockedStatic.when( () -> PluginServiceLoader.loadServices( MetastoreLocator.class ) )
        .thenReturn( providerCollection );

      HadoopClusterEndpoints hce =
        new HadoopClusterEndpoints( namedClusterService, runtimeTester, internalShim, secureEnabled );

      List<CachedFileItemStream> cachedFileItemStreams =
        hce.copyAndUnzip( fileItemStream, HadoopClusterEndpoints.FileType.CONFIGURATION,
          fileItemStream.getFieldName() );

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
}
