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


package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.endpoints;

import org.apache.commons.fileupload.FileItemStream;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Cached File Item Stream
 * <p>
 * {@link FileItemStream} interface is not extended because {@link FileItemStream#openStream()} doesn't represent
 * returning cached bytes. Additionally {@link FileItemStream} throws a
 * {@link org.apache.commons.fileupload.FileItemStream.ItemSkippedException}
 * when a previous stream is accessed after {@link FileItemIterator#next()} is called, which is not applicable here.
 */
public class CachedFileItemStream {

  private ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
  private String name;
  private String fieldName;
  private long lastModified; //optional file last modified date

  /**
   * Create a {@link CachedFileItemStream} from a {@link FileItemStream}
   * <p>
   * The {@link FileItemStream}'s {@link InputStream} is cached
   *
   * @param fileItemStream
   * @throws IOException
   */
  public CachedFileItemStream( FileItemStream fileItemStream ) throws IOException {
    this( fileItemStream.openStream(), fileItemStream.getName(), fileItemStream.getFieldName() );
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
