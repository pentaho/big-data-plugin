/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
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
