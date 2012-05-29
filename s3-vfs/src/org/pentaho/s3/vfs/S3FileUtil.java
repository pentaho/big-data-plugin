package org.pentaho.s3.vfs;

import org.apache.commons.vfs.*;
import org.apache.commons.vfs.auth.StaticUserAuthenticator;
import org.apache.commons.vfs.impl.DefaultFileSystemConfigBuilder;
import org.jets3t.service.security.AWSCredentials;

/**
 * created by: rfellows
 * date:       5/21/12
 */
public class S3FileUtil {

  public static FileObject resolveFile(String fileUri, UserAuthenticator userAuthenticator) throws FileSystemException {
    FileSystemOptions opts = null;
    if (VFS.getManager().getBaseFile() != null) {
      opts = VFS.getManager().getBaseFile().getFileSystem().getFileSystemOptions();
    }
    if(opts == null) {
      opts = new FileSystemOptions();
    }
    DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, userAuthenticator);
    FileObject file = resolveFile(fileUri, opts);
    return file;
  }

  public static FileObject resolveFile(String fileUri, String username, String password) throws FileSystemException {
    StaticUserAuthenticator userAuthenticator = new StaticUserAuthenticator(null, username, password);
    return resolveFile(fileUri, userAuthenticator);
  }

  public static FileObject resolveFile(String fileUri, AWSCredentials credentials) throws FileSystemException {
    return resolveFile(fileUri, credentials.getAccessKey(), credentials.getSecretKey());
  }

  public static FileObject resolveFile(String fileUri, FileSystemOptions opts) throws FileSystemException {
    FileObject file = VFS.getManager().resolveFile(fileUri, opts);
    return file;
  }

}
