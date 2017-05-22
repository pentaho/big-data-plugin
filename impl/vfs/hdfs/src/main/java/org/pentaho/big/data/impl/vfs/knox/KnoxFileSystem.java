package org.pentaho.big.data.impl.vfs.knox;

import java.util.Collection;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileSystem;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystem;

public class KnoxFileSystem extends AbstractFileSystem implements FileSystem {

  private final HadoopFileSystem hdfs;

  protected KnoxFileSystem( final FileName rootName, final FileSystemOptions fileSystemOptions,
                            HadoopFileSystem hdfs ) {
    super( rootName, null, fileSystemOptions );
    this.hdfs = hdfs;
  }

  @Override
  @SuppressWarnings( { "unchecked", "rawtypes" } )
  protected void addCapabilities( Collection caps ) {
    //caps.addAll( KnoxFileProvider.capabilities );
    // Adding capabilities depending on configuration settings
//    if ( Boolean.parseBoolean( hdfs.getProperty( "dfs.support.append", "true" ) ) ) {
//      caps.add( Capability.APPEND_CONTENT );
//    }
  }

  @Override protected FileObject createFile( AbstractFileName name ) throws Exception {
    return new KnoxFileObject( name, this );
  }

  public HadoopFileSystem getKnoxFileSystem() throws FileSystemException {
    return hdfs;
  }
}
