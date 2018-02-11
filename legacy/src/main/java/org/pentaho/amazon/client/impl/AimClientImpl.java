/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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
import org.pentaho.amazon.client.api.AimClient;
import org.pentaho.ui.xul.util.AbstractModelList;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Aliaksandr_Zhuk on 2/5/2018.
 */
public class AimClientImpl implements AimClient {

  private AmazonIdentityManagement aim;

  public AimClientImpl( AmazonIdentityManagement aim ) {
    this.aim = aim;
  }

  @Override
  public AbstractModelList<String> getEc2RolesFromAmazonAccount() {
    List<InstanceProfile> ec2RolesList = aim.listInstanceProfiles().getInstanceProfiles();
    AbstractModelList<String> ec2List;
    ec2List = ec2RolesList.stream().map( e -> e.getInstanceProfileName() )
      .collect( Collectors.toCollection( AbstractModelList<String>::new ) );

    return ec2List;
  }

  @Override
  public AbstractModelList<String> getEmrRolesFromAmazonAccount() {
    List<Role> emrRolesList = aim.listRoles().getRoles();
    AbstractModelList<String> emrList;
    emrList =
      emrRolesList.stream().filter( e -> e.getAssumeRolePolicyDocument().contains( "elasticmapreduce" ) )
        .map( e -> e.getRoleName() ).collect( Collectors.toCollection( AbstractModelList<String>::new ) );

    return emrList;
  }
}
