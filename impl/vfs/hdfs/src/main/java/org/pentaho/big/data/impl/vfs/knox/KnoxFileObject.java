package org.pentaho.big.data.impl.vfs.knox;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import org.apache.commons.vfs2.provider.AbstractFileSystem;
import org.apache.hadoop.gateway.shell.BasicResponse;
import org.apache.hadoop.gateway.shell.Hadoop;
import org.apache.hadoop.gateway.shell.hdfs.Hdfs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KnoxFileObject extends AbstractFileObject<AbstractFileSystem> {

  private static final Logger LOGGER = LoggerFactory.getLogger( KnoxFileObject.class );

  private String gatewayUrl;
  private String gatewayUsername;
  private String gatewayPassword;
  private AbstractFileName name;
  private final String filePath;
  private KnoxFileSystem fileSystem;

  protected KnoxFileObject( final AbstractFileName name, final KnoxFileSystem fileSystem ) throws FileSystemException {
    super( name, fileSystem );
    URI uri = URI.create( fileSystem.getRootName().getURI() );
    String userInfo = uri.getUserInfo();
    this.gatewayUrl = "https://" + uri.getHost() + ":" + uri.getPort() + uri.getPath();
    this.gatewayUsername = userInfo.split( ":" )[0];
    this.gatewayPassword = userInfo.split( ":" )[1];
    this.fileSystem = fileSystem; // TODO: Delete this
    this.name = name;
    filePath = StringUtils.substringAfter( name.getPath(), "/" + KnoxFileProvider.KNOX_ROOT_MARKER );
  }

  @Override
  public boolean exists() throws FileSystemException {
    Hadoop hadoop = logInHadoop();
    BasicResponse response = Hdfs.status( hadoop ).file( filePath ).now();
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable( DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY );
    try {
      // will try to read status if successful than return true because we able to read status
      JsonNode rootNode = mapper.readTree( response.getString() );
      KnoxFileStatus knoxFileStatus = new KnoxFileStatus( rootNode.get( "FileStatus" ) );
      FileType type = knoxFileStatus.getType();
      if ( type != null ) {
        return true;  //Should Imaginary return true?
      }

    } catch ( IOException e ) {
      LOGGER.error( e.getMessage() );
      throw new FileSystemException( e );
    } finally {
      logOutHadoop( hadoop );
    }
    return false;
  }

  @Override
  protected InputStream doGetInputStream() throws Exception {
    Hadoop hadoop = logInHadoop();
    BasicResponse response = Hdfs.get( hadoop ).from( filePath ).now();
    logOutHadoop( hadoop );
    return response.getStream();
  }

  @Override
  public void doCreateFolder() throws Exception {
    Hadoop hadoop = logInHadoop();
    Hdfs.mkdir( hadoop ).dir( filePath ).now();
    logOutHadoop( hadoop );
  }

  @Override
  public void doDelete() throws Exception {
    Hadoop hadoop = logInHadoop();
    Hdfs.rm( hadoop ).file( filePath ).recursive( true ).now();
    logOutHadoop( hadoop );
  }

  @Override
  protected void doRename( FileObject newfile ) throws Exception {
    // TODO
  }

  @Override
  protected long doGetContentSize() throws Exception {
    Hadoop hadoop = logInHadoop();
    BasicResponse response = Hdfs.status( hadoop ).file( filePath ).now();
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable( DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY );
    // will try to read status if successful than return true because we able to read status
    try {
      try {
        JsonNode rootNode = mapper.readTree( response.getString() );
        KnoxFileStatus knoxFileStatus = new KnoxFileStatus( rootNode.get( "FileStatus" ) );
        return knoxFileStatus.getContentSize();
      } catch ( IOException e ) {
        LOGGER.error( e.getMessage() );
        throw new FileSystemException( e );
      } finally {
        logOutHadoop( hadoop );
      }
    } finally {
      logOutHadoop( hadoop );
    }
  }

  @Override
  protected FileType doGetType() throws Exception {
    // will try to read status if successful than return true because we able to read status
    Hadoop hadoop = logInHadoop();
    BasicResponse response = Hdfs.status( hadoop ).file( filePath ).now();
    ObjectMapper mapper = new ObjectMapper();
    try {
      JsonNode rootNode = mapper.readTree( response.getString() );
      KnoxFileStatus knoxFileStatus = new KnoxFileStatus( rootNode.get( "FileStatus" ) );
      FileType type = knoxFileStatus.getType();
      return type;
    } catch ( IOException e ) {
      LOGGER.error( e.getMessage() );
      throw new FileSystemException( e );
    } finally {
      logOutHadoop( hadoop );
    }
  }

  @Override
  protected long doGetLastModifiedTime() throws Exception {
    Hadoop hadoop = logInHadoop();
    BasicResponse response = Hdfs.status( hadoop ).file( filePath ).now();
    ObjectMapper mapper = new ObjectMapper();
    //mapper.enable( DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY );
    // will try to read status if successful than return true because we able to read status
    try {
      JsonNode rootNode = mapper.readTree( response.getString() );
      KnoxFileStatus knoxFileStatus = new KnoxFileStatus( rootNode.get( "FileStatus" ) );
      return knoxFileStatus.getLastModifiedTime();
    } catch ( IOException e ) {
      LOGGER.error( e.getMessage() );
      throw new FileSystemException( e );
    } finally {
      logOutHadoop( hadoop );
    }
  }

  @Override
  protected boolean doSetLastModifiedTime( long modtime ) throws Exception {
    // TODO Auto-generated method stub
    return true;
  }

  @Override
  protected String[] doListChildren() throws Exception {
    // TODO refactor it
    Hadoop hadoop = logInHadoop();
    BasicResponse response = Hdfs.ls( hadoop ).dir( filePath ).now();
    ObjectMapper mapper = new ObjectMapper();
    // FileStatusesWrapper readValues = mapper.readValue( response.getString(), FileStatusesWrapper.class );
    logOutHadoop( hadoop );
    // FileStatus[] statusList = readValues.getFileStatuses().getFileStatus().stream().toArray(FileStatus[]::new);
    // String[] children = new String[ statusList.length ];
    // for ( int i = 0; i < statusList.length; i++ ) {
    // children[ i ] = statusList[ i ].getPath().getName();
    // }
    // return children;
    return null;
  }

  /**
   * After call login we must to {@link #logOutHadoop(Hadoop)} to prevent resource leak
   *
   * @return instance from shell to access to knox
   */
  private Hadoop logInHadoop() {
    Hadoop hadoop = null;
    try {
      hadoop = Hadoop.login( gatewayUrl, gatewayUsername, gatewayPassword );
      return hadoop;
    } catch ( URISyntaxException e ) {
      LOGGER.error( e.getMessage() );
    }
    return hadoop;
  }

  private void logOutHadoop( Hadoop hadoop ) {
    try {
      if ( hadoop != null ) {
        hadoop.shutdown();
      }
    } catch ( InterruptedException e ) {
      LOGGER.error( "Do not able to close session to gateway" + e.getMessage() );
    }
  }
}
