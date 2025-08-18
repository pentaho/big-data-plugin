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

import org.apache.commons.fileupload2.core.FileItemInput;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class CachedFileItemStream {

  private ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
  private String name;
  private String fieldName;
  private long lastModified; //optional file last modified date

  public CachedFileItemStream( FileItemInput fileItemStream ) throws IOException {
    this( fileItemStream.getInputStream(), fileItemStream.getName(), fileItemStream.getFieldName() );
  }

  public CachedFileItemStream( InputStream inputStream, String name, String fieldName ) throws IOException {
    IOUtils.copy( inputStream, this.outputStream );
    this.name = name;
    this.fieldName = fieldName;
  }

  public ByteArrayOutputStream getCachedOutputStream() {
    return this.outputStream;
  }

  public ByteArrayInputStream getCachedInputStream() {
    return new ByteArrayInputStream( this.outputStream.toByteArray() );
  }

  public String getName() {
    return this.name;
  }

  public String getFieldName() {
    return this.fieldName;
  }

  public long getLastModified() {
    return lastModified;
  }

  public void setLastModified( long lastModified ) {
    this.lastModified = lastModified;
  }
}
