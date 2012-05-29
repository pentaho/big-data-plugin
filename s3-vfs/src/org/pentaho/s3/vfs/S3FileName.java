package org.pentaho.s3.vfs;

import org.apache.commons.vfs.FileName;
import org.apache.commons.vfs.FileType;
import org.apache.commons.vfs.provider.UriParser;
import org.apache.commons.vfs.provider.url.UrlFileName;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

public class S3FileName extends UrlFileName {

  private static final char[] USERNAME_RESERVED = {':', '@', '/'};
  private static final char[] PASSWORD_RESERVED = {'@', '/', '?', '+'};

  public S3FileName(final String scheme, final String hostName, final int port, final int defaultPort, final String userName, final String password, final String path, final FileType type, final String queryString)
  {
    super(scheme, hostName, port, defaultPort, userName, password, path, type, queryString);
  }

  @Override
  public String getFriendlyURI() {
    return super.getFriendlyURI();
  }

  public FileName createName(final String absPath, FileType type) {
    return new S3FileName(getScheme(),
        getHostName(),
        getPort(),
        getDefaultPort(),
        getUserName(),
        getPassword(),
        absPath,
        type,
        getQueryString());
  }


  /**
   * Builds the root URI for this file name.
   */
  protected void appendRootUri(final StringBuffer buffer, boolean addPassword)
  {
    buffer.append(getScheme());
    buffer.append("://");
    if (addPassword) {
      appendCredentials(buffer, addPassword);
    }
    buffer.append(getHostName());
    if (getPort() != getDefaultPort())
    {
      buffer.append(':');
      buffer.append(getPort());
    }
  }

  /**
   * append the user credentials
   */
  @Override
  protected void appendCredentials(StringBuffer buffer, boolean addPassword)
  {
    String userName = getUserName();
    String password = getPassword();

    if (addPassword && userName != null && userName.length() != 0)
    {
      try {
        userName = URLEncoder.encode(getUserName(), "UTF-8");
        buffer.append(userName);
      } catch (UnsupportedEncodingException e) {
        // fall back to the default
        UriParser.appendEncoded(buffer, userName, USERNAME_RESERVED);
      }

      if (password != null && password.length() != 0)
      {
        buffer.append(':');
        try {
          password = URLEncoder.encode(getPassword(), "UTF-8");
          buffer.append(password);
        } catch (UnsupportedEncodingException e) {
          // fall back to the default
          UriParser.appendEncoded(buffer, password, PASSWORD_RESERVED);
        }
      }
      buffer.append('@');
    }
  }


}
