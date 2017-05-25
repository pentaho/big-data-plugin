package org.pentaho.big.data.impl.vfs.knox;

import org.apache.commons.vfs2.FileType;

import com.fasterxml.jackson.databind.JsonNode;

public class KnoxFileStatus {
  JsonNode jsonFileStatusNode;

  public KnoxFileStatus( JsonNode jsonFileStatusNode ) {
    this.jsonFileStatusNode = jsonFileStatusNode;
  }

  public FileType getType() {
    String type = jsonFileStatusNode.get( "type" ).asText();
    return type == null? null : FileType.valueOf( type );
  }

  public Long getLastModifiedTime() {
    Long modTime = jsonFileStatusNode.get( "modificationTime" ).asLong();
    return modTime;
  }

  public Long getContentSize() {
    Long size = jsonFileStatusNode.get( "length" ).asLong();
    return size;
  }
}
