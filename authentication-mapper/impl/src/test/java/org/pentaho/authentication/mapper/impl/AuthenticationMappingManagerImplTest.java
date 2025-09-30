/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package org.pentaho.authentication.mapper.impl;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.authentication.mapper.api.AuthenticationMappingManager;
import org.pentaho.authentication.mapper.api.AuthenticationMappingService;
import org.pentaho.authentication.mapper.api.MappingException;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * @author nhudak
 */

@RunWith( MockitoJUnitRunner.class )
public class AuthenticationMappingManagerImplTest {
  @Rule public TemporaryFolder etc = new TemporaryFolder();
  @Rule public ExpectedException exception = ExpectedException.none();
  @Captor ArgumentCaptor<Map<String, Object>> mapArgumentCaptor;
  private AuthenticationMappingManagerImpl manager;

  @Before
  public void setUp() throws Exception {
    manager = new AuthenticationMappingManagerImpl();
  }

  @Test
  @SuppressWarnings( "unchecked" )
  public void mappingService() throws Exception {
    // Add mock service
    TestService service = new TestService( "cluster_security_mapping_configuration" );
    manager.onMappingServiceAdded( service, ImmutableMap.of( AuthenticationMappingManager.RANKING_CONFIG, 200 ) );

    // Also add decoy services with lower (default) priority an invalid input
    TestService defaultService = new TestService( "default" );
    manager.onMappingServiceAdded( defaultService, ImmutableMap.of() );
    TestService unused = new TestService( "unused" ) {
      @Override public boolean accepts( Object input ) {
        return false;
      }
    };
    manager.onMappingServiceAdded( unused, ImmutableMap.of( AuthenticationMappingManager.RANKING_CONFIG, 1000 ) );

    // Service called if input/output match
    Map<String, ?> result = manager.getMapping( String.class, "map this", Map.class );

    assertThat( result, allOf(
        hasEntry( "id", "cluster_security_mapping_configuration" ),
        hasEntry( "input", "map this" ) )
    );

    // Remove service, default will be used
    manager.onMappingServiceRemoved( service );
    result = manager.getMapping( String.class, "use the default", Map.class );
    assertThat( result, hasEntry( "id", "default" ) );
  }

  @Test
  public void noMappingAvailable() throws Exception {
    assertThat( manager.getMapping( String.class, "some value", List.class ), nullValue() );
  }

  class TestService implements AuthenticationMappingService<String, Map> {

    final String id;

    TestService( String id ) {
      this.id = id;
    }

    @Override public String getId() {
      return id;
    }

    @Override public Class<String> getInputType() {
      return String.class;
    }

    @Override public Class<Map> getOutputType() {
      return Map.class;
    }

    @Override public boolean accepts( Object input ) {
      return true;
    }

    @Override public Map getMapping( String input, Map<String, ?> config ) throws MappingException {
      return ImmutableMap.of( "id", id, "input", input );
    }
  }
}
