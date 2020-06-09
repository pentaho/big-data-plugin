/*!
 * HITACHI VANTARA PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2020 Hitachi Vantara. All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Hitachi Vantara and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Hitachi Vantara and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Hitachi Vantara is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Hitachi Vantara,
 * explicitly covering such access.
 */

package com.pentaho.di.plugins.catalog.metadata;

import com.pentaho.di.plugins.catalog.api.CatalogClient;
import com.pentaho.di.plugins.catalog.api.entities.DataResource;
import com.pentaho.di.plugins.catalog.api.entities.metadata.TagAssociation;
import com.pentaho.di.plugins.catalog.api.entities.metadata.TagResult;
import com.pentaho.di.plugins.catalog.provider.CatalogDetails;
import org.pentaho.di.connections.ConnectionManager;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

public class CatalogWriteMetadata extends BaseStep implements StepInterface {
  private CatalogWriteMetadataMeta meta;
  private CatalogWriteMetadataData data;
  private int inputFieldIndex = -1;
  private List<String> tagKeys;
  private RowMetaInterface previousStepRowMeta;

  Supplier<ConnectionManager> connectionManagerSupplier = ConnectionManager::getInstance;

  public CatalogWriteMetadata( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
    tagKeys = new ArrayList<>();
  }

  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    if ( !super.init( smi, sdi ) ) {
      return false;
    }

    meta = (CatalogWriteMetadataMeta) smi;
    data = (CatalogWriteMetadataData) sdi;

    CatalogDetails catalogDetails = (CatalogDetails) connectionManagerSupplier.get()
            .getConnectionDetails( CatalogDetails.CATALOG, environmentSubstitute( meta.getConnection() ) );

    try {
      previousStepRowMeta = getTransMeta().getPrevStepFields( getStepMeta() );
    } catch ( KettleStepException ex ) {
      return false;
    }

    if ( Boolean.TRUE.equals( meta.getResourceFromPrevious() ) ) {
      String[] fieldNames = previousStepRowMeta.getFieldNames();
      inputFieldIndex = Arrays.asList( fieldNames ).indexOf( meta.getInputFieldName() );
    }

    URL url;
    try {
      url = new URL( catalogDetails.getUrl() );
    } catch ( MalformedURLException mue ) {
      return false;
    }

    data.setCatalogClient( new CatalogClient( url.getHost(), String.valueOf( url.getPort() ),
            url.getProtocol().equals( CatalogClient.HTTPS ) ) );
    data.getCatalogClient().getAuthentication()
            .login( environmentSubstitute( catalogDetails.getUsername() ),
                    environmentSubstitute( catalogDetails.getPassword() ) );

    populateTagKeys( data.getCatalogClient().getTagDomains().doTags() );

    return ( data.getCatalogClient().getSessionId() != null );
  }

  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    if ( first ) {
      first = false;
      data.setOutputRowMeta( previousStepRowMeta );
      meta.getFields( data.getOutputRowMeta(), getStepname(), null, null,
              this, repository, metaStore );
    }

    if ( Boolean.TRUE.equals( meta.getResourceFromPrevious() ) ) {
      Object[] r = getRow();
      if ( r == null ) {
        setOutputDone();
        return false;
      }

      useResourceIdsFromPreviousStep( r );

      if ( Boolean.TRUE.equals( meta.getPassThroughFields() ) ) {
        putRow( data.getOutputRowMeta(), r );
      }

      return true;
    } else {
      useStepResourceId();
      setOutputDone();
    }

    return false;
  }

  private void useStepResourceId() {
    if ( !StringUtil.isEmpty( meta.getResourceId() ) && !StringUtil.isEmpty( meta.getDescription() ) ) {
      updateResourceById( meta.getResourceId() );
      addTagsToDataResource( meta.getResourceId() );
    }
  }

  private void useResourceIdsFromPreviousStep( Object[] row ) {
    //TODO: throw error if index is -1
    String resourceId = row[inputFieldIndex].toString();
    updateResourceById( resourceId );
    addTagsToDataResource( resourceId );
  }

  private void updateResourceById( String resourceId ) {
    DataResource update = new DataResource();
    update.setDescription( meta.getDescription() );
    data.getCatalogClient().getDataResources().update( resourceId, update );
  }

  private void addTagsToDataResource( String resourceId ) {
    List<TagAssociation> tagAssociations = new ArrayList<>( tagKeys.size() );
    tagKeys.stream().forEach( tk -> tagAssociations.add( new TagAssociation( tk, resourceId ) ) );
    data.getCatalogClient().getTagAssociations().write( tagAssociations );
  }

  private void populateTagKeys( TagResult tagResult ) {
    Arrays.stream( meta.getTags().split( "," ) ).forEach( t -> {
      String key = tagResult.get( t );
      if ( !StringUtil.isEmpty( key ) ) {
        tagKeys.add( key );
      }
    } );
  }
}
