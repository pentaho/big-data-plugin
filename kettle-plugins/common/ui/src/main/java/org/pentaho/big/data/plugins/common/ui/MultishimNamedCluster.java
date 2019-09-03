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

package org.pentaho.big.data.plugins.common.ui;

import org.apache.commons.io.FileUtils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.stores.delegate.DelegatingMetaStore;
import org.pentaho.metastore.stores.xml.XmlMetaStore;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public class MultishimNamedCluster {

  public static Class<?> PKG = MultishimNamedCluster.class;
  public static final String STRING_NAMED_CLUSTERS = BaseMessages.getString( PKG, "NamedClusterDialog.HadoopClusters" );

  private Spoon spoon = Spoon.getInstance();
  private NamedClusterService namedClusterService;
  private IMetaStore metaStore;
  private VariableSpace variableSpace;

  public MultishimNamedCluster( VariableSpace variableSpace, IMetaStore metaStore, NamedClusterService namedClusterService ) {
    this.namedClusterService = namedClusterService;
    this.metaStore = metaStore;
  }

  public String newNamedCluster( String name ) {

    NamedCluster nc = namedClusterService.getClusterTemplate();
    nc.setName( name );
    if ( variableSpace != null ) {
      nc.shareVariablesWith( (VariableSpace) variableSpace );
    } else {
      nc.initializeVariablesFrom( null );
    }

    try {
      if ( nc.getConfigId() != null ) {
        delNamedCluster( metaStore, nc );
      }
      String newClusterId = generateNewClusterId( null );
      nc.setConfigId( newClusterId );
      addConfigProperties( nc );
      saveNamedCluster( metaStore, nc );
    } catch ( Exception e ) {
      /*commonDialogFactory.createErrorDialog( spoon.getShell(),
          BaseMessages.getString( PKG, SPOON_DIALOG_ERROR_SAVING_NAMED_CLUSTER_TITLE ),
          BaseMessages.getString( PKG, SPOON_DIALOG_ERROR_SAVING_NAMED_CLUSTER_MESSAGE, nc.getName() ), e );*/
      spoon.refreshTree();
      return nc.getName();
    }


    return nc.getName();
  }

  private void saveNamedCluster( IMetaStore metaStore, NamedCluster namedCluster ) {
    try {
      namedClusterService.create( namedCluster, metaStore );
    } catch ( MetaStoreException e ) {
      /*commonDialogFactory.createErrorDialog( spoon.getShell(),
          BaseMessages.getString( PKG, SPOON_DIALOG_ERROR_SAVING_NAMED_CLUSTER_TITLE ),
          BaseMessages.getString( PKG, SPOON_DIALOG_ERROR_SAVING_NAMED_CLUSTER_MESSAGE, namedCluster.getName() ), e );*/
    }
  }


  private void addConfigProperties( NamedCluster namedCluster ) throws Exception {
    Path clusterConfigDirPath = Paths.get( getNamedClusterConfigsRootDir( null ) + "/" + namedCluster.getConfigId() );
    Path configPropertiesPath = Paths.get( getNamedClusterConfigsRootDir( null ) + "/" + namedCluster.getConfigId() + "/" + "config.properties" );
    Files.createDirectories( clusterConfigDirPath );
    String sampleConfigProperties = namedCluster.getShimIdentifier() + "sampleconfig.properties";
    InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream( sampleConfigProperties );
    if ( inputStream != null ) {
      Files.copy( inputStream, configPropertiesPath, StandardCopyOption.REPLACE_EXISTING );
    }
  }


  public void delNamedCluster( IMetaStore metaStore, NamedCluster namedCluster ) {
    if ( metaStore == null ) {
      metaStore = spoon.getMetaStore();
    }
    deleteNamedCluster( metaStore, namedCluster );
    spoon.refreshTree( STRING_NAMED_CLUSTERS );
    spoon.setShellText();
  }

  private XmlMetaStore getXmlMetastore( IMetaStore metaStore ) throws MetaStoreException {
    XmlMetaStore xmlMetaStore = null;

    if ( metaStore instanceof DelegatingMetaStore ) {
      IMetaStore activeMetastore = ( (DelegatingMetaStore) metaStore ).getActiveMetaStore();
      if ( activeMetastore instanceof XmlMetaStore ) {
        xmlMetaStore = (XmlMetaStore) activeMetastore;
      }
    } else if ( metaStore instanceof XmlMetaStore ) {
      xmlMetaStore = (XmlMetaStore) metaStore;
    }

    return xmlMetaStore;
  }


  private void deleteNamedCluster( IMetaStore metaStore, NamedCluster namedCluster ) {
    try {
      if ( namedClusterService.read( namedCluster.getName(), metaStore ) != null ) {
        namedClusterService.delete( namedCluster.getName(), metaStore );
        XmlMetaStore xmlMetaStore = getXmlMetastore( metaStore );
        if ( xmlMetaStore != null ) {
          String path = getNamedClusterConfigsRootDir( xmlMetaStore ) + "/" +  namedCluster.getConfigId();
          try {
            FileUtils.deleteDirectory( new File( path ) );
          } catch ( IOException e ) {
            // Do nothing. The config directory will be orphaned but functionality will not be impacted.
          }
        }
      }
    } catch ( MetaStoreException e ) {
      /*commonDialogFactory.createErrorDialog( spoon.getShell(),
          BaseMessages.getString( PKG, SPOON_DIALOG_ERROR_DELETING_NAMED_CLUSTER_TITLE ),
          BaseMessages.getString( PKG, SPOON_DIALOG_ERROR_DELETING_NAMED_CLUSTER_MESSAGE, namedCluster.getName() ), e );*/
    }
  }



  private String generateNewClusterId( XmlMetaStore xmlMetaStore ) {
    int newClusterId = 0;
    try {
      Path metaStorePath = Paths.get( getNamedClusterConfigsRootDir( xmlMetaStore ) );
      if ( Files.exists( metaStorePath ) ) {
        Object[] paths = Files.list( metaStorePath ).toArray();
        for ( int i = 0; i < paths.length; i++ ) {
          Path filePath = (Path) paths[i];
          try {
            int index = Integer.parseInt( filePath.getFileName().toString() );
            if ( index > newClusterId ) {
              newClusterId = Math.max( newClusterId, index );
            }
          } catch ( NumberFormatException ex ) {
          }
        }
      }
    } catch ( Exception e ) {
      return null;
    }
    return Integer.toString(newClusterId + 1 );
  }

  private String getNamedClusterConfigsRootDir( XmlMetaStore metaStore ) {
    return System.getProperty( "user.home" ) + File.separator + ".pentaho"  + File.separator + "metastore"  + File.separator + "pentaho" + File.separator + "NamedCluster" + File.separator + "Configs";
  }


}
