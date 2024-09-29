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


package org.pentaho.amazon;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.amazon.client.ClientFactoriesManager;
import org.pentaho.amazon.client.ClientType;
import org.pentaho.amazon.client.api.PricingClient;
import org.pentaho.amazon.client.impl.AimClientImpl;
import org.pentaho.amazon.client.impl.PricingClientImpl;
import org.pentaho.amazon.hive.job.AmazonHiveJobExecutor;
import org.pentaho.amazon.hive.ui.AmazonHiveJobExecutorController;
import org.pentaho.di.ui.core.database.dialog.tags.ExtTextbox;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.components.XulButton;
import org.pentaho.ui.xul.util.AbstractModelList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by Aliaksandr_Zhuk on 2/8/2018.
 */
@RunWith( MockitoJUnitRunner.class )
public class AbstractAmazonJobExecutorControllerTest {

  private AmazonHiveJobExecutorController jobExecutorController;

  @Mock
  XulDomContainer container;

  @Mock
  AmazonHiveJobExecutor jobEntry;

  @Mock
  BindingFactory bindingFactory;

  @Before
  public void setUp() throws Exception {
    jobExecutorController =
      spy( new AmazonHiveJobExecutorController( container, jobEntry, bindingFactory ) );
  }

  @Test
  public void testPopulateRegions_getValidCountOfEnumElements() throws Exception {

    int expectedCountOfRegions = 16;

    AbstractModelList<String> listRegions = jobExecutorController.populateRegions();

    assertEquals( expectedCountOfRegions, listRegions.size() );
  }

  @Test
  public void testPopulateReleases_getValidCountOfEnumElements() throws Exception {

    int expectedCountOfReleases = 34;

    AbstractModelList<String> listReleases = jobExecutorController.populateReleases();

    assertEquals( expectedCountOfReleases, listReleases.size() );
  }

  @Test
  public void testPopulateReleases_setFirstEmrReleaseInJobEntry() throws Exception {

    String expectedEmrRelease = "emr-7.0.0";

    AmazonHiveJobExecutor jobEntry = spy( new AmazonHiveJobExecutor() );
    AmazonHiveJobExecutorController hiveJobExecutorController =
      new AmazonHiveJobExecutorController( container, jobEntry, bindingFactory );

    AbstractModelList<String> listReleases = hiveJobExecutorController.populateReleases();

    assertEquals( expectedEmrRelease, jobEntry.getEmrRelease() );
  }

  @Test
  public void testPopulateReleases_getValidEmrReleaseFormJobEntry() throws Exception {

    String expectedEmrRelease = "emr-5.11.0";

    AmazonHiveJobExecutor jobEntry = spy( new AmazonHiveJobExecutor() );
    AmazonHiveJobExecutorController hiveJobExecutorController =
      spy( new AmazonHiveJobExecutorController( container, jobEntry, bindingFactory ) );

    when( hiveJobExecutorController.getJobEntry().getEmrRelease() ).thenReturn( "emr-5.11.0" );

    hiveJobExecutorController.populateReleases();

    verify( hiveJobExecutorController, times( 2 ) ).getJobEntry();

    assertEquals( expectedEmrRelease, jobEntry.getEmrRelease() );
  }

  @Test
  public void testPopulateReleases_addNewEmrReleaseToReleasesList() throws Exception {

    int expectedCountOfReleases = 35;

    when( jobExecutorController.getJobEntry().getEmrRelease() ).thenReturn( "emr-5.12.0" );
    AbstractModelList<String> listReleases = jobExecutorController.populateReleases();

    verify( jobExecutorController, times( 2 ) ).getJobEntry();
    assertEquals( expectedCountOfReleases, listReleases.size() );
  }

  @Test
  public void testPopulateReleases_setEmrReleaseToFirstElementFromEmrReleasesList() throws Exception {

    String expectedEmrRelease = "emr-7.0.0";

    AmazonHiveJobExecutor jobEntry = spy( new AmazonHiveJobExecutor() );

    AmazonHiveJobExecutorController hiveJobExecutorController =
      spy( new AmazonHiveJobExecutorController( container, jobEntry, bindingFactory ) );
    jobEntry.setEmrRelease( null );

    AbstractModelList<String> listReleases = hiveJobExecutorController.populateReleases();

    verify( hiveJobExecutorController, times( 2 ) ).getJobEntry();
    verify( jobEntry, times( 2 ) ).setEmrRelease( listReleases.get( 0 ) );

    assertEquals( expectedEmrRelease, jobEntry.getEmrRelease() );
  }

  @Test
  public void testPopulateEc2Roles_ec2RoleNull() {

    AbstractModelList<String> roles = jobExecutorController.populateEc2Roles();

    assertEquals( 0, roles.size() );
  }

  @Test
  public void testPopulateEc2Roles_ec2RoleNotNull() {

    String expectedEc2Role = "ec2_role";

    when( jobExecutorController.getJobEntry().getEc2Role() ).thenReturn( expectedEc2Role );

    AbstractModelList<String> roles = jobExecutorController.populateEc2Roles();

    assertEquals( expectedEc2Role, roles.get( 0 ) );
  }

  @Test
  public void testPopulateEmrRoles_emrRoleNull() {

    AbstractModelList<String> roles = jobExecutorController.populateEmrRoles();

    assertEquals( 0, roles.size() );
  }

  @Test
  public void testPopulateEmrRoles_emrRoleNotNull() {

    String expectedEmrRole = "emr_role";

    when( jobExecutorController.getJobEntry().getEmrRole() ).thenReturn( expectedEmrRole );

    AbstractModelList<String> roles = jobExecutorController.populateEmrRoles();

    assertEquals( expectedEmrRole, roles.get( 0 ) );
  }

  @Test
  public void testPopulateMasterInstanceTypes_instanceTypeNull() {

    AbstractModelList<String> instanceTypes = jobExecutorController.populateMasterInstanceTypes();

    assertEquals( 0, instanceTypes.size() );
  }

  @Test
  public void testPopulateMasterInstanceTypes_instanceTypeNotNull() {

    String expectedInstanceType = "c1.medium";

    when( jobExecutorController.getJobEntry().getMasterInstanceType() ).thenReturn( expectedInstanceType );

    AbstractModelList<String> instanceTypes = jobExecutorController.populateMasterInstanceTypes();

    assertEquals( expectedInstanceType, instanceTypes.get( 0 ) );
  }

  @Test
  public void testPopulateSlaveInstanceTypes_instanceTypeNull() {

    AbstractModelList<String> instanceTypes = jobExecutorController.populateSlaveInstanceTypes();

    assertEquals( 0, instanceTypes.size() );
  }

  @Test
  public void testPopulateSlaveInstanceTypes_instanceTypeNotNull() {

    String expectedInstanceType = "c1.medium";

    when( jobExecutorController.getJobEntry().getSlaveInstanceType() ).thenReturn( expectedInstanceType );

    AbstractModelList<String> instanceTypes = jobExecutorController.populateSlaveInstanceTypes();

    assertEquals( expectedInstanceType, instanceTypes.get( 0 ) );
  }

  @Test
  public void testSetEc2RolesFromAmazonAccount_RolesListNotEmpty() throws Exception {

    AbstractModelList<String> expectedRolesList = new AbstractModelList<>();

    expectedRolesList.add( "default_role" );
    expectedRolesList.add( "ec2_role" );

    AmazonHiveJobExecutor jobEntry = spy( new AmazonHiveJobExecutor() );

    AmazonHiveJobExecutorController hiveJobExecutorController =
      new AmazonHiveJobExecutorController( container, jobEntry, bindingFactory );

    AimClientImpl aimClient = mock( AimClientImpl.class );
    when( aimClient.getEc2RolesFromAmazonAccount() ).thenReturn( expectedRolesList );

    hiveJobExecutorController.setEc2RolesFromAmazonAccount( aimClient );

    assertEquals( expectedRolesList.size(), hiveJobExecutorController.getEc2Roles().size() );
  }

  @Test
  public void testSetEc2RolesFromAmazonAccount_RolesListWithdefaultRole() throws Exception {

    String expectedEc2Role = "EMR_EC2_DefaultRole";

    AmazonHiveJobExecutor jobEntry = spy( new AmazonHiveJobExecutor() );

    AmazonHiveJobExecutorController hiveJobExecutorController =
      new AmazonHiveJobExecutorController( container, jobEntry, bindingFactory );

    AimClientImpl aimClient = mock( AimClientImpl.class );
    when( aimClient.getEc2RolesFromAmazonAccount() ).thenReturn( new AbstractModelList<>() );

    hiveJobExecutorController.setEc2RolesFromAmazonAccount( aimClient );

    assertEquals( expectedEc2Role, hiveJobExecutorController.getEc2Roles().get( 0 ) );
  }

  @Test
  public void testSetEmrRolesFromAmazonAccount_RolesListNotEmpty() throws Exception {

    AbstractModelList<String> expectedRolesList = new AbstractModelList<>();

    expectedRolesList.add( "default_role" );
    expectedRolesList.add( "emr_role" );

    AmazonHiveJobExecutor jobEntry = spy( new AmazonHiveJobExecutor() );

    AmazonHiveJobExecutorController hiveJobExecutorController =
      new AmazonHiveJobExecutorController( container, jobEntry, bindingFactory );

    AimClientImpl aimClient = mock( AimClientImpl.class );
    when( aimClient.getEmrRolesFromAmazonAccount() ).thenReturn( expectedRolesList );

    hiveJobExecutorController.setEmrRolesFromAmazonAccount( aimClient );

    assertEquals( expectedRolesList.size(), hiveJobExecutorController.getEmrRoles().size() );
  }

  @Test
  public void testSetEmrRolesFromAmazonAccount_RolesListWithdefaultRole() throws Exception {

    String expectedEmrRole = "EMR_DefaultRole";

    AmazonHiveJobExecutor jobEntry = spy( new AmazonHiveJobExecutor() );

    AmazonHiveJobExecutorController hiveJobExecutorController =
      new AmazonHiveJobExecutorController( container, jobEntry, bindingFactory );

    AimClientImpl aimClient = mock( AimClientImpl.class );
    when( aimClient.getEmrRolesFromAmazonAccount() ).thenReturn( new AbstractModelList<>() );

    hiveJobExecutorController.setEmrRolesFromAmazonAccount( aimClient );

    assertEquals( expectedEmrRole, hiveJobExecutorController.getEmrRoles().get( 0 ) );
  }

  @Test
  public void testPopulateInstanceTypesForSelectedRegion_instanceTypesListNotEmpty() throws Exception {

    List<String> instanceTypes = new ArrayList<>();
    instanceTypes.add( "c1.medium" );
    instanceTypes.add( "c1.large" );

    AmazonHiveJobExecutor jobEntry = spy( new AmazonHiveJobExecutor() );

    AmazonHiveJobExecutorController hiveJobExecutorController =
      new AmazonHiveJobExecutorController( container, jobEntry, bindingFactory );

    PricingClient pricingClient = mock( PricingClientImpl.class );
    when( pricingClient.populateInstanceTypesForSelectedRegion() ).thenReturn( instanceTypes );

    hiveJobExecutorController.populateInstanceTypesForSelectedRegion( pricingClient );

    assertEquals( instanceTypes.size(), hiveJobExecutorController.getMasterInstanceTypes().size() );
    assertEquals( instanceTypes.size(), hiveJobExecutorController.getSlaveInstanceTypes().size() );
  }

  @Test
  public void testPopulateInstanceTypesForSelectedRegion_instanceTypesListNull() throws Exception {

    AmazonHiveJobExecutor jobEntry = spy( new AmazonHiveJobExecutor() );

    AmazonHiveJobExecutorController hiveJobExecutorController =
      new AmazonHiveJobExecutorController( container, jobEntry, bindingFactory );

    PricingClient pricingClient = mock( PricingClientImpl.class );
    when( pricingClient.populateInstanceTypesForSelectedRegion() ).thenReturn( null );

    hiveJobExecutorController.populateInstanceTypesForSelectedRegion( pricingClient );

    assertEquals( 0, hiveJobExecutorController.getMasterInstanceTypes().size() );
    assertEquals( 0, hiveJobExecutorController.getSlaveInstanceTypes().size() );
  }

  @Test
  public void testPopulateInstanceTypesForSelectedRegion_catchException() throws Exception {

    AmazonHiveJobExecutor jobEntry = spy( new AmazonHiveJobExecutor() );

    AmazonHiveJobExecutorController hiveJobExecutorController =
      spy( new AmazonHiveJobExecutorController( container, jobEntry, bindingFactory ) );

    PricingClient pricingClient = mock( PricingClientImpl.class );
    when( pricingClient.populateInstanceTypesForSelectedRegion() ).thenThrow( new IOException() );
    doNothing().when( hiveJobExecutorController ).showErrorDialog( nullable( String.class ), nullable( String.class ) );
    doCallRealMethod().when( hiveJobExecutorController ).populateInstanceTypesForSelectedRegion( any() );


    hiveJobExecutorController.populateInstanceTypesForSelectedRegion( pricingClient );

    verify( hiveJobExecutorController, times( 1 ) ).showErrorDialog( nullable( String.class ), nullable( String.class ) );
  }

  @Test
  public void testGetEmrSettings_catchExceptionWhenKeysAreNull() {

    ExtTextbox textBox = mock( ExtTextbox.class );
    XulButton btn = mock( XulButton.class );

    XulDomContainer container = mock( XulDomContainer.class, RETURNS_DEEP_STUBS );

    AmazonHiveJobExecutorController hiveJobExecutorController =
      spy( new AmazonHiveJobExecutorController( container, jobEntry, bindingFactory ) );

    when( hiveJobExecutorController.getXulDomContainer() ).thenReturn( container );
    when( container.getDocumentRoot().getElementById( "access-key" ) ).thenReturn( textBox );
    when( container.getDocumentRoot().getElementById( "secret-key" ) ).thenReturn( textBox );
    when( container.getDocumentRoot().getElementById( "session-token" ) ).thenReturn( textBox );
    when( container.getDocumentRoot().getElementById( "emr-settings" ) ).thenReturn( btn );

    doNothing().when( btn ).setDisabled( true );
    doNothing().when( hiveJobExecutorController ).showErrorDialog( anyString(), anyString() );

    hiveJobExecutorController.getEmrSettings();

    verify( hiveJobExecutorController, times( 1 ) ).showErrorDialog( anyString(), anyString() );
  }

  @Test
  public void testGetEmrSettings_catchExceptionWhenRegionIsNull() {

    ExtTextbox textBox = mock( ExtTextbox.class );
    XulButton btn = mock( XulButton.class );

    XulDomContainer container = mock( XulDomContainer.class, RETURNS_DEEP_STUBS );

    AmazonHiveJobExecutorController hiveJobExecutorController =
      spy( new AmazonHiveJobExecutorController( container, jobEntry, bindingFactory ) );

    when( hiveJobExecutorController.getXulDomContainer() ).thenReturn( container );
    when( container.getDocumentRoot().getElementById( "access-key" ) ).thenReturn( textBox );
    when( container.getDocumentRoot().getElementById( "secret-key" ) ).thenReturn( textBox );
    when( container.getDocumentRoot().getElementById( "session-token" ) ).thenReturn( textBox );
    when( container.getDocumentRoot().getElementById( "emr-settings" ) ).thenReturn( btn );
    when( textBox.getValue() ).thenReturn( "testing" );

    doNothing().when( btn ).setDisabled( true );
    doNothing().when( hiveJobExecutorController ).showErrorDialog( anyString(), anyString() );

    hiveJobExecutorController.getEmrSettings();

    verify( hiveJobExecutorController, times( 1 ) ).showErrorDialog( anyString(), anyString() );
  }

  @Test
  public void testGetEmrSettings_setAllValidParameters() throws Exception {

    AbstractModelList<String> rolesList = new AbstractModelList<>();
    List<String> typesList = new ArrayList<>();
    typesList.add( "c1.medium" );

    ExtTextbox textBox = mock( ExtTextbox.class );
    XulButton btn = mock( XulButton.class );
    try ( MockedStatic<ClientFactoriesManager> clientFactoriesManagerMockedStatic =
            Mockito.mockStatic( ClientFactoriesManager.class ) ) {
      ClientFactoriesManager manager = mock( ClientFactoriesManager.class );
      clientFactoriesManagerMockedStatic.when( () -> ClientFactoriesManager.getInstance() ).thenReturn( manager );

      AimClientImpl aimClient = mock( AimClientImpl.class );
      PricingClient pricingClient = mock( PricingClientImpl.class );

      when( aimClient.getEc2RolesFromAmazonAccount() ).thenReturn( rolesList );
      when( aimClient.getEmrRolesFromAmazonAccount() ).thenReturn( rolesList );
      when( pricingClient.populateInstanceTypesForSelectedRegion() ).thenReturn( typesList );

      XulDomContainer container = mock( XulDomContainer.class, RETURNS_DEEP_STUBS );
      AmazonHiveJobExecutorController hiveJobExecutorController =
        spy( new AmazonHiveJobExecutorController( container, jobEntry, bindingFactory ) );

      when( hiveJobExecutorController.getXulDomContainer() ).thenReturn( container );
      when( container.getDocumentRoot().getElementById( "access-key" ) ).thenReturn( textBox );
      when( container.getDocumentRoot().getElementById( "secret-key" ) ).thenReturn( textBox );
      when( container.getDocumentRoot().getElementById( "session-token" ) ).thenReturn( textBox );
      when( container.getDocumentRoot().getElementById( "num-instances" ) ).thenReturn( textBox );
      when( container.getDocumentRoot().getElementById( "emr-settings" ) ).thenReturn( btn );
      when( textBox.getValue() ).thenReturn( "testing" );
      when( hiveJobExecutorController.getJobEntry().getRegion() ).thenReturn( "invalid Region" );

      doNothing().when( btn ).setDisabled( true );
      doNothing().when( hiveJobExecutorController ).setXulMenusDisabled( false );
      doNothing().when( hiveJobExecutorController ).setSelectedItemForEachMenu();

      doReturn( aimClient ).when( manager )
        .createClient( anyString(), anyString(), anyString(), anyString(), eq( ClientType.AIM ) );
      doReturn( pricingClient ).when( manager )
        .createClient( anyString(), anyString(), anyString(), anyString(), eq( ClientType.PRICING ) );

      hiveJobExecutorController.getEmrSettings();

      verify( hiveJobExecutorController, never() ).showErrorDialog( nullable( String.class ), nullable( String.class ) );
    }
  }
}
