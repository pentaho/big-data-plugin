package org.pentaho.big.data.impl.vfs.hdfs;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.provider.FileNameParser;
import org.junit.Test;

import static org.junit.Assert.*;

public class MapRFileNameParserTest {

  @Test
  public void testDefaultPort() {
    assertEquals( -1, MapRFileNameParser.getInstance().getDefaultPort() );
  }

  @Test
  public void rootPathNoClusterName() throws FileSystemException {
    final String URI = "maprfs:///";

    FileNameParser parser = MapRFileNameParser.getInstance();
    FileName name = parser.parseUri( null, null, URI );

    assertEquals( URI, name.getURI() );
    assertEquals( "maprfs", name.getScheme() );
  }

  @Test
  public void withPath() throws FileSystemException {
    final String URI = "maprfs:///my/file/path";

    FileNameParser parser = MapRFileNameParser.getInstance();
    FileName name = parser.parseUri( null, null, URI );

    assertEquals( URI, name.getURI() );
    assertEquals( "maprfs", name.getScheme() );
    assertEquals( "/my/file/path", name.getPath() );
  }

  @Test
  public void withPathAndClusterName() throws FileSystemException {
    final String URI = "maprfs://cluster2/my/file/path";

    FileNameParser parser = MapRFileNameParser.getInstance();
    FileName name = parser.parseUri( null, null, URI );

    assertEquals( URI, name.getURI() );
    assertEquals( "maprfs", name.getScheme() );
    assertTrue( name.getURI().startsWith( "maprfs://cluster2/" ) );
    assertEquals( "/my/file/path", name.getPath() );
  }
}
