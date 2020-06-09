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

package com.pentaho.di.plugins.catalog.write;

import com.pentaho.di.plugins.catalog.api.CatalogClient;
import com.pentaho.di.plugins.catalog.provider.CatalogDetails;
import org.pentaho.di.connections.ConnectionManager;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.function.Supplier;


/**
 * Describe your step plugin.
 */
public class WritePayload extends BaseStep implements StepInterface {

  @SuppressWarnings( "java:S1068" )
  private static Class<?> catalogMetaClass = WritePayloadMeta.class;
  // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  private WritePayloadMeta meta;
  private WritePayloadData data;

  @SuppressWarnings( "java:S1450" )
  private RowMetaInterface previousStepRowMeta;

  Supplier<ConnectionManager> connectionManagerSupplier = ConnectionManager::getInstance;

  public WritePayload( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                       Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }


  /**
   * Initialize and do work where other steps need to wait for...
   *
   * @param smi The metadata to work with
   * @param sdi The data to initialize
   */
  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {

    if ( !super.init( smi, sdi ) ) {
      return false;
    }

    meta = (WritePayloadMeta) smi;
    data = (WritePayloadData) sdi;

    CatalogDetails catalogDetails = (CatalogDetails) connectionManagerSupplier.get()
      .getConnectionDetails( CatalogDetails.CATALOG, environmentSubstitute( meta.getConnection() ) );

    try {
      previousStepRowMeta = getTransMeta().getPrevStepFields( getStepMeta() );
    } catch ( KettleStepException ex ) {
      return false;
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

    return ( data.getCatalogClient().getSessionId() != null );
  }

  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    meta = (WritePayloadMeta) smi;
    data = (WritePayloadData) sdi;

    Object[] row = getRow(); // This also waits for a row to be finished.

    // If we have no more rows to process
    if ( row == null ) {
      setOutputDone();
      return false;
    }

    // Get Input row metadata
    if ( first ) {
      data.setInputRowMeta( getInputRowMeta() );
      data.setOutputRowMeta( data.getInputRowMeta().clone() );
      first = false;
      meta.getFields( data.getOutputRowMeta(), getStepname(), null, null, this, repository, metaStore );
    }

    return writeOutPayload( row );
  }

  public boolean writeOutPayload( Object[] row ) throws KettleException {

    // TODO Process Individual rows aka write out Payload

    // Send Data it to down stream steps
    putRow( data.getOutputRowMeta(), row );
    return true;
  }

}

