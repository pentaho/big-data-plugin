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


package org.pentaho.amazon.s3;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;

import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Document;

import java.util.Collections;

public class S3FileOutputMetaTest {
  S3FileOutputMeta meta = new S3FileOutputMeta();

  @BeforeClass
  public static void setClassUp() throws KettleException {
    KettleEnvironment.init();
  }

  @AfterClass
  public static void tearDownClass() {
    KettleEnvironment.shutdown();

    // Clean up logs directory created by KettleEnvironment
    java.io.File logsDir = new java.io.File( "logs" );
    if ( logsDir.exists() && logsDir.isDirectory() ) {
      for ( java.io.File f : logsDir.listFiles() ) {
        f.delete();
      }
      logsDir.delete();
    }
  }

  @Before
  public void setUp() throws KettleXMLException {
    Document doc = XMLHandler.loadXMLFile( "./src/test/resources/s3OutputMetaTest.ktr" );
    meta.loadXML( doc.getFirstChild(), Collections.<DatabaseMeta>emptyList(), (IMetaStore) null );
  }

}
