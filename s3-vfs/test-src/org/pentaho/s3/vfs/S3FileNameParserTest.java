package org.pentaho.s3.vfs;

import org.apache.commons.vfs.*;
import org.apache.commons.vfs.auth.StaticUserAuthenticator;
import org.apache.commons.vfs.provider.FileNameParser;
import org.apache.commons.vfs.provider.FileReplicator;
import org.apache.commons.vfs.provider.TemporaryFileStore;
import org.apache.commons.vfs.provider.VfsComponentContext;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Properties;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * created by: rfellows
 * date:       5/25/12
 */
public class S3FileNameParserTest {

  public static String awsAccessKey;
  public static String awsSecretKey;

  public static final String HOST = "s3";
  public static final String SCHEME = "s3";
  public static final int PORT = 843;

  @BeforeClass
  public static void init() throws Exception {
    Properties settings = new Properties();
    settings.load(S3FileUtilTest.class.getResourceAsStream("/test-settings.properties"));
    awsAccessKey = settings.getProperty("awsAccessKey");
    awsSecretKey = settings.getProperty("awsSecretKey");
  }

  @Test
  public void testParseUri_withKeys() throws Exception {
    FileNameParser parser = S3FileNameParser.getInstance();
    String expected = buildS3URL("/rcf-emr-staging", true);

    FileName filename = parser.parseUri(null, null, "s3://" + awsAccessKey + ":" + awsSecretKey + "@" + HOST + "/rcf-emr-staging");
    assertEquals(expected, filename.getURI());

  }

  @Test
  public void testParseUri_withoutKeys() throws Exception {
    FileNameParser parser = S3FileNameParser.getInstance();
    String expected = buildS3URL("/", false);

    FileName filename = parser.parseUri(null, null, "s3://" + HOST + "/");
    assertEquals(expected, filename.getURI());

  }


  public static String buildS3URL(String path, boolean withUserInfo) throws UnsupportedEncodingException {
    if(withUserInfo) {
      return SCHEME + "://" + URLEncoder.encode(awsAccessKey, "UTF-8") + ":" + URLEncoder.encode(awsSecretKey, "UTF-8") + "@" + HOST + path;
    } else {
      return SCHEME +"://" + HOST + path;
    }
  }
}
