/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2024 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.amazon.client.impl;

import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.model.InstanceProfile;
import com.amazonaws.services.identitymanagement.model.Role;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.amazon.client.api.AimClient;
import org.pentaho.ui.xul.util.AbstractModelList;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;

/**
 * Created by Aliaksandr_Zhuk on 2/8/2018.
 */
@RunWith( MockitoJUnitRunner.class )
public class AimClientImplTest {

  private AimClient aimClient;
  private AmazonIdentityManagement amazonIdentityManagement;

  @Before
  public void setUp() {
    amazonIdentityManagement = Mockito.mock( AmazonIdentityManagement.class, RETURNS_DEEP_STUBS );
    aimClient = Mockito.spy( new AimClientImpl( amazonIdentityManagement ) );
  }

  @Test
  public void testGetEc2RolesFromAmazonAccount_getValidListOfRoles() {

    List<String> expectedEc2Roles = new ArrayList<>();

    List<InstanceProfile> ec2Profiles = new ArrayList<>();

    InstanceProfile defaultProfile = new InstanceProfile();
    defaultProfile.setInstanceProfileName( "defaulr_role" );

    InstanceProfile testProfile = new InstanceProfile();
    testProfile.setInstanceProfileName( "test_role" );

    ec2Profiles.add( defaultProfile );
    ec2Profiles.add( testProfile );

    expectedEc2Roles.add( "defaulr_role" );
    expectedEc2Roles.add( "test_role" );

    Mockito.when( amazonIdentityManagement.listInstanceProfiles().getInstanceProfiles() ).thenReturn( ec2Profiles );

    AbstractModelList<String> ec2Roles = aimClient.getEc2RolesFromAmazonAccount();

    assertEquals( expectedEc2Roles, ec2Roles );
  }

  @Test
  public void testGetEmrRolesFromAmazonAccount_getValidListOfRoles() {

    List<String> expectedEmrRoles = new ArrayList<>();
    expectedEmrRoles.add( "default_role" );
    expectedEmrRoles.add( "test_role" );

    List<Role> emrRolesList = new ArrayList<>();
    Role defaultRole = new Role();
    defaultRole.setRoleName( "default_role" );
    defaultRole.setAssumeRolePolicyDocument( "elasticmapreduce.amazonaws.com" );
    emrRolesList.add( defaultRole );

    Role testRole = new Role();
    testRole.setRoleName( "test_role" );
    testRole.setAssumeRolePolicyDocument( "elasticmapreduce.amazonaws.com" );
    emrRolesList.add( testRole );

    Role wrongRole = new Role();
    wrongRole.setRoleName( "wrong_role" );
    wrongRole.setAssumeRolePolicyDocument( "compute.amazonaws.com" );
    emrRolesList.add( wrongRole );

    Mockito.when( amazonIdentityManagement.listRoles().getRoles() ).thenReturn( emrRolesList );

    AbstractModelList<String> emrRoles = aimClient.getEmrRolesFromAmazonAccount();

    assertEquals( expectedEmrRoles, emrRoles );
  }
}
