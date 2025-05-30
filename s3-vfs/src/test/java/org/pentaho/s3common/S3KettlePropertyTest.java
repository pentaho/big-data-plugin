package org.pentaho.s3common;

import org.junit.Test;
import org.mockito.MockedStatic;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.util.EnvUtil;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mockStatic;

public class S3KettlePropertyTest {

  @Test
  public void testGetPartSizeAndThreadPoolSize() {
    try ( MockedStatic<EnvUtil> envUtilMock = mockStatic( EnvUtil.class );
           MockedStatic<Const> constMock = mockStatic( Const.class ) ) {
      Properties props = new Properties();
      props.setProperty( S3KettleProperty.S3VFS_PART_SIZE, "10MB" );
      props.setProperty( S3KettleProperty.S3VFS_THREAD_POOL_SIZE, "5" );
      envUtilMock.when( () -> EnvUtil.readProperties( "kettle.properties" ) ).thenReturn( props );
      constMock.when( Const::getKettlePropertiesFilename ).thenReturn( "kettle.properties" );
      S3KettleProperty property = new S3KettleProperty();
      assertEquals( "10MB", property.getPartSize() );
      assertEquals( "5", property.getThreadPoolSize() );
    }
  }

  @Test
  public void testGetProperty_MissingProperty() {
    try ( MockedStatic<EnvUtil> envUtilMock = mockStatic( EnvUtil.class );
           MockedStatic<Const> constMock = mockStatic( Const.class ) ) {
      Properties props = new Properties();
      envUtilMock.when( () -> EnvUtil.readProperties( "kettle.properties" ) ).thenReturn( props );
      constMock.when( Const::getKettlePropertiesFilename ).thenReturn( "kettle.properties" );
      S3KettleProperty property = new S3KettleProperty();
      assertNull( property.getProperty( "not.a.real.property" ) );
    }
  }

  @Test
  public void testGetProperty_KettleException() {
    try ( MockedStatic<EnvUtil> envUtilMock = mockStatic( EnvUtil.class );
           MockedStatic<Const> constMock = mockStatic( Const.class ) ) {
      envUtilMock.when( () -> EnvUtil.readProperties( "kettle.properties" ) ).thenThrow( new KettleException( "fail" ) );
      constMock.when( Const::getKettlePropertiesFilename ).thenReturn( "kettle.properties" );
      S3KettleProperty property = new S3KettleProperty();
      // Should return empty string on exception
      assertEquals( "", property.getProperty( "any.property" ) );
    }
  }
}
