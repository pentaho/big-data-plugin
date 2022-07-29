package org.pentaho.amazon.s3;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.pentaho.di.connections.vfs.VFSDetailsComposite;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.vfs.connections.ui.dialog.VFSDetailsCompositeHelper;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.widget.CheckBoxVar;
import org.pentaho.di.ui.core.widget.ComboVar;
import org.pentaho.di.ui.core.widget.FileChooserVar;
import org.pentaho.di.ui.core.widget.PasswordTextVar;
import org.pentaho.di.ui.core.widget.TextVar;

import java.util.Arrays;
import java.util.HashSet;
import java.util.stream.IntStream;

public class S3DetailComposite implements VFSDetailsComposite {
  private static final Class<?> PKG = S3DetailComposite.class;
  private final Composite wComposite; //The content for the parentComposite
  private final S3Details details;
  private final VFSDetailsCompositeHelper helper;
  private final PropsUI props;

  private static final String[] S3_CONNECTION_TYPE_CHOICES = new String[] { "Amazon", "Minio" };
  private static final String[] AUTH_TYPE_CHOICES = new String[] { "Access Key/Secret Key", "Credentials File" };
  private static final int TEXT_VAR_FLAGS = SWT.SINGLE | SWT.LEFT | SWT.BORDER;
  private final String[] regionChoices;
  private CCombo wS3ConnectionType;
  private CCombo wAuthType;

  private Composite wBottomHalf;
  private Composite wWidgetHolder;

  private ComboVar wRegion;
  private PasswordTextVar wAccessKey;
  private PasswordTextVar wSecretKey;
  private PasswordTextVar wSessionToken;
  private CheckBoxVar wDefaultS3Config;
  private TextVar wProfileName;
  private FileChooserVar wCredentialsFilePath;
  private TextVar wEndpoint;
  private TextVar wSignatureVersion;
  private CheckBoxVar wPathStyleAccess;

  private VariableSpace variableSpace = Variables.getADefaultVariableSpace();
  private HashSet<Control> skipControls = new HashSet<>();

  public S3DetailComposite( Composite composite, S3Details details, PropsUI props ) {
    helper = new VFSDetailsCompositeHelper( PKG, props );
    this.props = props;
    this.wComposite = composite;
    this.details = details;
    regionChoices = details.getRegions().toArray( new String[ 0 ] );
  }

  public Object open() {
    FormLayout genLayout = new FormLayout();
    genLayout.marginWidth = Const.FORM_MARGIN;
    genLayout.marginHeight = Const.FORM_MARGIN;
    wComposite.setLayout( genLayout );

    //Title
    Label wlTitle = helper.createTitleLabel( wComposite, "ConnectionDialog.s3.Details.Title", skipControls );

    // S3 Connection Type
    createLabel( "ConnectionDialog.s3.ConnectionType.Label", wlTitle, wComposite );
    wS3ConnectionType = createCCombo( wlTitle, 200, wComposite );
    wS3ConnectionType.setItems( S3_CONNECTION_TYPE_CHOICES );
    wS3ConnectionType.select( Integer.parseInt( Const.NVL( details.getConnectionType(), "0" ) ) );

    // This composite will fill dynamically based on connection type and auth type
    wBottomHalf = new Composite( wComposite, SWT.NONE );
    FormLayout bottomHalfLayout = new FormLayout();
    wBottomHalf.setLayout( bottomHalfLayout );

    FormData wfdBottomHalf = new FormData();
    wfdBottomHalf.top = new FormAttachment( wS3ConnectionType, 0 );
    wfdBottomHalf.left = new FormAttachment( 0, 0 );
    wfdBottomHalf.right = new FormAttachment( 100, 0 );
    wBottomHalf.setLayoutData( wfdBottomHalf );
    props.setLook( wBottomHalf );

    //This composite holds controls that are not currently in use.  No layout needed here
    wWidgetHolder = new Composite( wComposite, SWT.NONE );
    wWidgetHolder.setVisible( false );

    wAuthType = createStandbyCombo();
    wAuthType.setItems( AUTH_TYPE_CHOICES );
    wAuthType.select( Integer.parseInt( Const.NVL( details.getAuthType(), "0" ) ) );
    wRegion = createStandbyComboVar();
    wRegion.setItems( regionChoices );
    wRegion.select( 0 );
    wAccessKey = createStandbyPasswordTextVar();
    wSecretKey = createStandbyPasswordTextVar();
    wSessionToken = createStandbyPasswordTextVar();
    wDefaultS3Config = createStandByCheckBoxVar();
    wProfileName = createStandByTextVar();
    wCredentialsFilePath = new FileChooserVar( variableSpace, wWidgetHolder, TEXT_VAR_FLAGS, "Browse File" );
    wEndpoint = createStandByTextVar();
    wSignatureVersion = createStandByTextVar();
    wPathStyleAccess = createStandByCheckBoxVar();

    skipControls.add( wBottomHalf );
    skipControls.add( wWidgetHolder );

    setupBottomHalf();

    helper.adjustPlacement( wComposite, skipControls );
    helper.adjustPlacement( wBottomHalf, skipControls );

    // Set the data
    populateWidgets();

    //add Listeners
    for ( CCombo cCombo : Arrays.asList( wS3ConnectionType, wAuthType ) ) {
      cCombo.addSelectionListener( new SelectionAdapter() {
        @Override public void widgetSelected( SelectionEvent selectionEvent ) {
          setupBottomHalf();
        }
      } );
    }
    wRegion.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent selectionEvent ) {
        details.setRegion( wRegion.getText() );
      }
    } );
    wAccessKey.addModifyListener( modifyEvent -> details.setAccessKey( wAccessKey.getText() ) );
    wSecretKey.addModifyListener( modifyEvent -> details.setSecretKey( wSecretKey.getText() ) );
    wSessionToken.addModifyListener(
      modifyEvent -> details.setSessionToken( wSessionToken.getText() ) );
    wDefaultS3Config.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        details.setDefaultS3Config( Boolean.toString( wDefaultS3Config.getSelection() ) );
      }
    } );
    wCredentialsFilePath.addModifyListener(
      modifyEvent -> details.setCredentialsFilePath( wCredentialsFilePath.getText() ) );
    wProfileName.addModifyListener( modifyEvent -> details.setProfileName( wProfileName.getText() ) );
    wEndpoint.addModifyListener( modifyEvent -> details.setEndpoint( wEndpoint.getText() ) );
    wSignatureVersion.addModifyListener( modifyEvent -> details.setSignatureVersion( wSignatureVersion.getText() ) );
    wPathStyleAccess.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        details.setPathStyleAccess( Boolean.toString( wPathStyleAccess.getSelection() ) );
      }
    } );

    wComposite.getParent().addListener( SWT.Resize, arg0 -> {
      Rectangle r = wComposite.getParent().getClientArea();
      wComposite.setSize( r.width, r.height );
    } );
    return wComposite;
  }

  private void setupBottomHalf() {
    setupBottomHalf( computeComboIndex( wS3ConnectionType.getText(), S3_CONNECTION_TYPE_CHOICES ),
      computeComboIndex( wAuthType.getText(), AUTH_TYPE_CHOICES ) );
  }

  private void setupBottomHalf( int s3ConnectionType, int authType ) {
    if ( s3ConnectionType != stringToInteger( details.getConnectionType() ) ) {
      details.setConnectionType( String.valueOf( s3ConnectionType ) );
    }
    if ( authType != stringToInteger( details.getAuthType() ) ) {
      details.setAuthType( String.valueOf( authType ) );
    }

    for ( Control c : wBottomHalf.getChildren() ) {
      if ( c instanceof Label ) {
        c.dispose(); //Dispose any labels
      } else {
        c.setParent( wWidgetHolder ); //Other widgets go back to the wWidgetHolder (they hold the current value)
      }
    }

    switch ( s3ConnectionType * 10 + authType ) {
      case 0: // Amazon with Access Key
        moveWidgetToBottomHalf( wAuthType, "ConnectionDialog.s3.AuthType.Label", null, 200 );
        moveWidgetToBottomHalf( wRegion, "ConnectionDialog.s3.Region.Label", wAuthType, 200 );
        moveWidgetToBottomHalf( wAccessKey, "ConnectionDialog.s3.AccessKey.Label", wRegion );
        moveWidgetToBottomHalf( wSecretKey, "ConnectionDialog.s3.SecretKey.Label", wAccessKey );
        moveWidgetToBottomHalf( wSessionToken, "ConnectionDialog.s3.sessionToken.Label", wSecretKey );
        moveWidgetToBottomHalf( wDefaultS3Config, "ConnectionDialog.s3.DefaultS3Config.Label", wSessionToken );
        break;
      case 1: // Amazon with Credentials File
        moveWidgetToBottomHalf( wAuthType, "ConnectionDialog.s3.AuthType.Label", null, 200 );
        moveWidgetToBottomHalf( wRegion, "ConnectionDialog.s3.Region.Label", wAuthType, 200 );
        moveWidgetToBottomHalf( wProfileName, "ConnectionDialog.s3.ProfileName.Label", wRegion );
        moveWidgetToBottomHalf( wCredentialsFilePath, "ConnectionDialog.s3.CredentialsFilePath.Label", wProfileName );
        break;
      case 10: // Mineo
      case 11:
        moveWidgetToBottomHalf( wAccessKey, "ConnectionDialog.s3.AccessKey.Label", null );
        moveWidgetToBottomHalf( wSecretKey, "ConnectionDialog.s3.SecretKey.Label", wAccessKey );
        moveWidgetToBottomHalf( wEndpoint, "ConnectionDialog.s3.Endpoint.Label", wSecretKey );
        moveWidgetToBottomHalf( wSignatureVersion, "ConnectionDialog.s3.SignatureVersion.Label", wEndpoint );
        moveWidgetToBottomHalf( wPathStyleAccess, "ConnectionDialog.s3.PathStyleAccess.Label", wSignatureVersion );
        moveWidgetToBottomHalf( wDefaultS3Config, "ConnectionDialog.s3.DefaultS3Config.Label", wPathStyleAccess );
        break;
      default:

    }
    helper.adjustPlacement( wComposite, skipControls );
    helper.adjustPlacement( wBottomHalf, skipControls );

    wBottomHalf.layout();
    wComposite.pack();
  }

  private void populateWidgets() {
    wS3ConnectionType.select( stringToInteger( details.getConnectionType() ) );
    wAuthType.select( stringToInteger( details.getAuthType() ) );
    wAccessKey.setText( Const.NVL( details.getAccessKey(), "" ) );
    wSecretKey.setText( Const.NVL( details.getSecretKey(), "" ) );
    wSessionToken.setText( Const.NVL( details.getSessionToken(), "" ) );
    wDefaultS3Config.setSelection( Boolean.parseBoolean( Const.NVL( details.getDefaultS3Config(), "false" ) ) );
    wRegion.select( computeComboIndex( Const.NVL( details.getRegion(), regionChoices[ 0 ] ), regionChoices ) );
    wProfileName.setText( Const.NVL( details.getProfileName(), "" ) );
    wCredentialsFilePath.setText( Const.NVL( details.getCredentialsFilePath(), "" ) );
    wEndpoint.setText( Const.NVL( details.getEndpoint(), "" ) );
    wSignatureVersion.setText( Const.NVL( details.getSignatureVersion(), "" ) );
    wPathStyleAccess.setSelection( Boolean.parseBoolean( Const.NVL( details.getPathStyleAccess(), "false" ) ) );
  }

  private Label createLabel( String key, Control topWidget, Composite composite ) {
    return helper.createLabel( composite, SWT.RIGHT | SWT.WRAP, key, topWidget );
  }

  private PasswordTextVar createStandbyPasswordTextVar() {
    return new PasswordTextVar( variableSpace, wWidgetHolder, TEXT_VAR_FLAGS );
  }

  private TextVar createStandByTextVar() {
    return new TextVar( variableSpace, wWidgetHolder, TEXT_VAR_FLAGS );
  }

  private CCombo createCCombo( Control topWidget, int width, Composite composite ) {
    return helper.createCCombo( composite, TEXT_VAR_FLAGS, topWidget, width );
  }

  private CCombo createStandbyCombo() {
    return new CCombo( wWidgetHolder, TEXT_VAR_FLAGS );
  }

  private ComboVar createStandbyComboVar() {
    return new ComboVar( variableSpace, wWidgetHolder, TEXT_VAR_FLAGS );
  }

  private CheckBoxVar createStandByCheckBoxVar() {
    return new CheckBoxVar( variableSpace, wWidgetHolder, SWT.CHECK );
  }

  private void moveWidgetToBottomHalf( Control targetWidget, String labelKey, Control topWidget ) {
    moveWidgetToBottomHalf( targetWidget, labelKey, topWidget, 0 );
  }

  private void moveWidgetToBottomHalf( Control targetWidget, String labelKey, Control topWidget, int width ) {
    createLabel( labelKey, topWidget, wBottomHalf );
    targetWidget.setParent( wBottomHalf );
    props.setLook( targetWidget );
    FormData formData = helper.getFormDataField( topWidget );
    formData.left = new FormAttachment( helper.getMaxLabelWidth() + helper.getMargin() * 2 );
    targetWidget.setLayoutData( helper.getFormDataField( topWidget, width ) );
  }

  @Override
  public void close() {
    wComposite.dispose();
    if ( wBottomHalf != null ) {
      wBottomHalf.dispose();
    }

  }

  private int stringToInteger( String value ) {
    return Integer.parseInt( Const.NVL( value, "0" ) );
  }

  private int computeComboIndex( String targetValue, String[] choices ) {
    return IntStream.range( 0, choices.length )
      .filter( i -> choices[ i ].equals( targetValue ) )
      .findFirst().orElse( 0 );
  }

}
