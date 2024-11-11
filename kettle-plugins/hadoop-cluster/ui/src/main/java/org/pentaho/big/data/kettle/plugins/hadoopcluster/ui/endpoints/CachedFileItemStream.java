/*!
 * Copyright 2024 Hitachi Vantara. All rights reserved.
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

import org.apache.commons.fileupload2.core.FileItem;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Cached File Item Stream
 * <p>
 * {@link FileItem} interface is not extended because {@link FileItem#getInputStream()} doesn't represent
 * returning cached bytes. Additionally {@link FileItem} throws a
 * {@link org.apache.commons.fileupload2.core.FileItemInput.ItemSkippedException}
 * when a previous stream is accessed after {@link org.apache.commons.fileupload2.core.FileItemInputIterator#next()} is called, which is not applicable here.
 */
public class CachedFileItemStream {

  private ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
  private String name;
  private String fieldName;
  private long lastModified; //optional file last modified date

  /**
   * Create a {@link CachedFileItemStream} from a {@link FileItem}
   * <p>
   * The {@link FileItem}'s {@link InputStream} is cached
   *
   * @param fileItemStream
   * @throws IOException
   */
  public CachedFileItemStream( FileItem fileItemStream ) throws IOException {
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
