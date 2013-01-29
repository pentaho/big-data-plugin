/*******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2012 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.ui.trans.steps.mongodbinput;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransPreviewFactory;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputData;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputMeta;
import org.pentaho.di.ui.core.dialog.EnterNumberDialog;
import org.pentaho.di.ui.core.dialog.EnterTextDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.dialog.PreviewRowsDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.StyledTextComp;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.dialog.TransPreviewProgressDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.MongoClient;

public class MongoDbInputDialog extends BaseStepDialog implements
    StepDialogInterface {
  private static Class<?> PKG = MongoDbInputMeta.class; // for i18n purposes,
                                                        // needed by
                                                        // Translator2!!
                                                        // $NON-NLS-1$

  private CTabFolder m_wTabFolder;
  private CTabItem m_wConfigTab;
  private CTabItem m_wMongoQueryTab;
  private CTabItem m_wMongoFieldsTab;

  private TextVar wHostname;
  private TextVar wPort;
  private CCombo wDbName;
  private Button m_getDbsBut;
  private TextVar wFieldsName;
  private CCombo wCollection;
  private Button m_getCollectionsBut;
  private TextVar wJsonField;

  private StyledTextComp wJsonQuery;
  private Label wlJsonQuery;
  private Button m_queryIsPipelineBut;

  private TextVar wAuthUser;
  private TextVar wAuthPass;

  private Button m_outputAsJson;
  private TableView m_fieldsView;

  private TextVar m_connectionTimeout;
  private TextVar m_socketTimeout;
  private CCombo m_readPreference;

  private final MongoDbInputMeta input;

  public MongoDbInputDialog(Shell parent, Object in, TransMeta tr, String sname) {
    super(parent, (BaseStepMeta) in, tr, sname);
    input = (MongoDbInputMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    setShellImage(shell, input);

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText(ModifyEvent e) {
        input.setChanged();
      }
    };
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell
        .setText(BaseMessages.getString(PKG, "MongoDbInputDialog.Shell.Title")); //$NON-NLS-1$

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Stepname line
    wlStepname = new Label(shell, SWT.RIGHT);
    wlStepname.setText(BaseMessages.getString(PKG,
        "MongoDbInputDialog.Stepname.Label")); //$NON-NLS-1$
    props.setLook(wlStepname);
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment(0, 0);
    fdlStepname.right = new FormAttachment(middle, -margin);
    fdlStepname.top = new FormAttachment(0, margin);
    wlStepname.setLayoutData(fdlStepname);
    wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wStepname.setText(stepname);
    props.setLook(wStepname);
    wStepname.addModifyListener(lsMod);
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment(middle, 0);
    fdStepname.top = new FormAttachment(0, margin);
    fdStepname.right = new FormAttachment(100, 0);
    wStepname.setLayoutData(fdStepname);
    Control lastControl = wStepname;

    m_wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(m_wTabFolder, Props.WIDGET_STYLE_TAB);
    m_wTabFolder.setSimple(false);

    // start of the config tab
    m_wConfigTab = new CTabItem(m_wTabFolder, SWT.NONE);
    m_wConfigTab.setText(BaseMessages.getString(PKG,
        "MongoDbInputDialog.ConfigTab.TabTitle"));

    Composite wConfigComp = new Composite(m_wTabFolder, SWT.NONE);
    props.setLook(wConfigComp);

    FormLayout configLayout = new FormLayout();
    configLayout.marginWidth = 3;
    configLayout.marginHeight = 3;
    wConfigComp.setLayout(configLayout);

    // Hostname(s) input ...
    //
    Label wlHostname = new Label(wConfigComp, SWT.RIGHT);
    wlHostname.setText(BaseMessages.getString(PKG,
        "MongoDbInputDialog.Hostname.Label")); //$NON-NLS-1$
    wlHostname.setToolTipText(BaseMessages.getString(PKG,
        "MongoDbInputDialog.Hostname.Label.TipText"));
    props.setLook(wlHostname);
    FormData fdlHostname = new FormData();
    fdlHostname.left = new FormAttachment(0, 0);
    fdlHostname.right = new FormAttachment(middle, -margin);
    fdlHostname.top = new FormAttachment(0, margin);
    wlHostname.setLayoutData(fdlHostname);
    wHostname = new TextVar(transMeta, wConfigComp, SWT.SINGLE | SWT.LEFT
        | SWT.BORDER);
    props.setLook(wHostname);
    wHostname.addModifyListener(lsMod);
    FormData fdHostname = new FormData();
    fdHostname.left = new FormAttachment(middle, 0);
    fdHostname.top = new FormAttachment(0, margin);
    fdHostname.right = new FormAttachment(100, 0);
    wHostname.setLayoutData(fdHostname);
    lastControl = wHostname;

    // Port input ...
    //
    Label wlPort = new Label(wConfigComp, SWT.RIGHT);
    wlPort
        .setText(BaseMessages.getString(PKG, "MongoDbInputDialog.Port.Label")); //$NON-NLS-1$
    wlPort.setToolTipText(BaseMessages.getString(PKG,
        "MongoDbInputDialog.Port.Label.TipText"));
    props.setLook(wlPort);
    FormData fdlPort = new FormData();
    fdlPort.left = new FormAttachment(0, 0);
    fdlPort.right = new FormAttachment(middle, -margin);
    fdlPort.top = new FormAttachment(lastControl, margin);
    wlPort.setLayoutData(fdlPort);
    wPort = new TextVar(transMeta, wConfigComp, SWT.SINGLE | SWT.LEFT
        | SWT.BORDER);
    props.setLook(wPort);
    wPort.addModifyListener(lsMod);
    FormData fdPort = new FormData();
    fdPort.left = new FormAttachment(middle, 0);
    fdPort.top = new FormAttachment(lastControl, margin);
    fdPort.right = new FormAttachment(100, 0);
    wPort.setLayoutData(fdPort);
    lastControl = wPort;

    // DbName input ...
    //
    Label wlDbName = new Label(wConfigComp, SWT.RIGHT);
    wlDbName.setText(BaseMessages.getString(PKG,
        "MongoDbInputDialog.DbName.Label")); //$NON-NLS-1$
    props.setLook(wlDbName);
    FormData fdlDbName = new FormData();
    fdlDbName.left = new FormAttachment(0, 0);
    fdlDbName.right = new FormAttachment(middle, -margin);
    fdlDbName.top = new FormAttachment(lastControl, margin);
    wlDbName.setLayoutData(fdlDbName);

    m_getDbsBut = new Button(wConfigComp, SWT.PUSH | SWT.CENTER);
    props.setLook(m_getDbsBut);
    m_getDbsBut.setText(BaseMessages.getString(PKG,
        "MongoDbInputDialog.DbName.Button"));
    FormData fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(lastControl, 0);
    m_getDbsBut.setLayoutData(fd);

    m_getDbsBut.addSelectionListener(new SelectionAdapter() {
      @Override
      public void widgetSelected(SelectionEvent e) {
        setupDBNames();
      }
    });

    wDbName = new CCombo(wConfigComp, SWT.BORDER);
    props.setLook(wDbName);
    wDbName.addModifyListener(lsMod);
    FormData fdDbName = new FormData();
    fdDbName.left = new FormAttachment(middle, 0);
    fdDbName.top = new FormAttachment(lastControl, margin);
    fdDbName.right = new FormAttachment(m_getDbsBut, 0);
    wDbName.setLayoutData(fdDbName);
    lastControl = wDbName;

    wDbName.addModifyListener(new ModifyListener() {
      public void modifyText(ModifyEvent e) {
        input.setChanged();
        wDbName.setToolTipText(transMeta.environmentSubstitute(wDbName
            .getText()));
      }
    });

    wDbName.addSelectionListener(new SelectionAdapter() {
      @Override
      public void widgetSelected(SelectionEvent e) {
        setupCollectionNamesForDB();
      }

      @Override
      public void widgetDefaultSelected(SelectionEvent e) {
        setupCollectionNamesForDB();
      }
    });

    wDbName.addFocusListener(new FocusListener() {
      public void focusGained(FocusEvent e) {

      }

      public void focusLost(FocusEvent e) {
        setupCollectionNamesForDB();
      }
    });

    // Collection input ...
    //
    Label wlCollection = new Label(wConfigComp, SWT.RIGHT);
    wlCollection.setText(BaseMessages.getString(PKG,
        "MongoDbInputDialog.Collection.Label")); //$NON-NLS-1$
    props.setLook(wlCollection);
    FormData fdlCollection = new FormData();
    fdlCollection.left = new FormAttachment(0, 0);
    fdlCollection.right = new FormAttachment(middle, -margin);
    fdlCollection.top = new FormAttachment(lastControl, margin);
    wlCollection.setLayoutData(fdlCollection);

    m_getCollectionsBut = new Button(wConfigComp, SWT.PUSH | SWT.CENTER);
    props.setLook(m_getCollectionsBut);
    m_getCollectionsBut.setText(BaseMessages.getString(PKG,
        "MongoDbInputDialog.GetCollections.Button"));
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(lastControl, 0);
    m_getCollectionsBut.setLayoutData(fd);

    m_getCollectionsBut.addSelectionListener(new SelectionAdapter() {
      @Override
      public void widgetSelected(SelectionEvent e) {
        setupCollectionNamesForDB();
      }
    });

    wCollection = new CCombo(wConfigComp, SWT.BORDER);
    props.setLook(wCollection);
    wCollection.addModifyListener(lsMod);
    FormData fdCollection = new FormData();
    fdCollection.left = new FormAttachment(middle, 0);
    fdCollection.top = new FormAttachment(lastControl, margin);
    fdCollection.right = new FormAttachment(m_getCollectionsBut, 0);
    wCollection.setLayoutData(fdCollection);
    lastControl = wCollection;

    wCollection.addSelectionListener(new SelectionAdapter() {
      @Override
      public void widgetSelected(SelectionEvent e) {
        updateQueryTitleInfo();
      }
    });

    wCollection.addFocusListener(new FocusListener() {
      public void focusGained(FocusEvent e) {

      }

      public void focusLost(FocusEvent e) {
        updateQueryTitleInfo();
      }
    });

    // Authentication...
    //
    // AuthUser line
    Label wlAuthUser = new Label(wConfigComp, SWT.RIGHT);
    wlAuthUser.setText(BaseMessages.getString(PKG,
        "MongoDbInputDialog.AuthenticationUser.Label"));
    props.setLook(wlAuthUser);
    FormData fdlAuthUser = new FormData();
    fdlAuthUser.left = new FormAttachment(0, -margin);
    fdlAuthUser.top = new FormAttachment(lastControl, margin);
    fdlAuthUser.right = new FormAttachment(middle, -margin);
    wlAuthUser.setLayoutData(fdlAuthUser);

    wAuthUser = new TextVar(transMeta, wConfigComp, SWT.BORDER | SWT.READ_ONLY);
    wAuthUser.setEditable(true);
    props.setLook(wAuthUser);
    wAuthUser.addModifyListener(lsMod);
    FormData fdAuthUser = new FormData();
    fdAuthUser.left = new FormAttachment(middle, 0);
    fdAuthUser.top = new FormAttachment(lastControl, margin);
    fdAuthUser.right = new FormAttachment(100, 0);
    wAuthUser.setLayoutData(fdAuthUser);
    lastControl = wAuthUser;

    // AuthPass line
    Label wlAuthPass = new Label(wConfigComp, SWT.RIGHT);
    wlAuthPass.setText(BaseMessages.getString(PKG,
        "MongoDbInputDialog.AuthenticationPassword.Label"));
    props.setLook(wlAuthPass);
    FormData fdlAuthPass = new FormData();
    fdlAuthPass.left = new FormAttachment(0, -margin);
    fdlAuthPass.top = new FormAttachment(lastControl, margin);
    fdlAuthPass.right = new FormAttachment(middle, -margin);
    wlAuthPass.setLayoutData(fdlAuthPass);

    wAuthPass = new TextVar(transMeta, wConfigComp, SWT.BORDER | SWT.READ_ONLY);
    wAuthPass.setEditable(true);
    wAuthPass.setEchoChar('*');
    props.setLook(wAuthPass);
    wAuthPass.addModifyListener(lsMod);
    FormData fdAuthPass = new FormData();
    fdAuthPass.left = new FormAttachment(middle, 0);
    fdAuthPass.top = new FormAttachment(wAuthUser, margin);
    fdAuthPass.right = new FormAttachment(100, 0);
    wAuthPass.setLayoutData(fdAuthPass);
    lastControl = wAuthPass;

    // connection timeout
    Label connectTimeoutL = new Label(wConfigComp, SWT.RIGHT);
    connectTimeoutL.setText(BaseMessages.getString(PKG,
        "MongoDbInputDialog.ConnectionTimeout.Label"));
    props.setLook(connectTimeoutL);
    connectTimeoutL.setToolTipText(BaseMessages.getString(PKG,
        "MongoDbInputDialog.ConnectionTimeout.TipText"));

    fd = new FormData();
    fd.left = new FormAttachment(0, -margin);
    fd.top = new FormAttachment(lastControl, margin);
    fd.right = new FormAttachment(middle, -margin);
    connectTimeoutL.setLayoutData(fd);

    m_connectionTimeout = new TextVar(transMeta, wConfigComp, SWT.SINGLE
        | SWT.LEFT | SWT.BORDER);
    props.setLook(m_connectionTimeout);
    m_connectionTimeout.addModifyListener(lsMod);
    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(lastControl, margin);
    fd.right = new FormAttachment(100, 0);
    m_connectionTimeout.setLayoutData(fd);
    lastControl = m_connectionTimeout;

    // socket timeout
    Label socketTimeoutL = new Label(wConfigComp, SWT.RIGHT);
    socketTimeoutL.setText(BaseMessages.getString(PKG,
        "MongoDbInputDialog.SocketTimeout.Label"));
    props.setLook(connectTimeoutL);
    socketTimeoutL.setToolTipText(BaseMessages.getString(PKG,
        "MongoDbInputDialog.SocketTimeout.TipText"));

    fd = new FormData();
    fd.left = new FormAttachment(0, -margin);
    fd.top = new FormAttachment(lastControl, margin);
    fd.right = new FormAttachment(middle, -margin);
    socketTimeoutL.setLayoutData(fd);

    m_socketTimeout = new TextVar(transMeta, wConfigComp, SWT.SINGLE | SWT.LEFT
        | SWT.BORDER);
    props.setLook(m_socketTimeout);
    m_socketTimeout.addModifyListener(lsMod);
    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(lastControl, margin);
    fd.right = new FormAttachment(100, 0);
    m_socketTimeout.setLayoutData(fd);
    lastControl = m_socketTimeout;

    // read preference
    Label readPrefL = new Label(wConfigComp, SWT.RIGHT);
    readPrefL.setText(BaseMessages.getString(PKG,
        "MongoDbInputDialog.ReadPreferenceLabel"));
    props.setLook(readPrefL);
    fd = new FormData();
    fd.left = new FormAttachment(0, -margin);
    fd.top = new FormAttachment(lastControl, margin);
    fd.right = new FormAttachment(middle, -margin);
    readPrefL.setLayoutData(fd);

    m_readPreference = new CCombo(wConfigComp, SWT.BORDER);
    props.setLook(m_readPreference);
    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(lastControl, margin);
    fd.right = new FormAttachment(100, 0);
    m_readPreference.setLayoutData(fd);
    m_readPreference.addModifyListener(new ModifyListener() {
      public void modifyText(ModifyEvent e) {
        input.setChanged();
        m_readPreference.setToolTipText(transMeta
            .environmentSubstitute(m_readPreference.getText()));
      }
    });
    m_readPreference.add("Primary");
    m_readPreference.add("Primary preferred");
    m_readPreference.add("Secondary");
    m_readPreference.add("Secondary preferred");
    m_readPreference.add("Nearest");

    lastControl = m_readPreference;

    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(100, 0);
    wConfigComp.setLayoutData(fd);

    wConfigComp.layout();
    m_wConfigTab.setControl(wConfigComp);

    // Query tab -----
    m_wMongoQueryTab = new CTabItem(m_wTabFolder, SWT.NONE);
    m_wMongoQueryTab.setText(BaseMessages.getString(PKG,
        "MongoDbInputDialog.QueryTab.TabTitle"));
    Composite wQueryComp = new Composite(m_wTabFolder, SWT.NONE);
    props.setLook(wQueryComp);
    FormLayout queryLayout = new FormLayout();
    formLayout.marginWidth = 3;
    formLayout.marginHeight = 3;
    wQueryComp.setLayout(queryLayout);

    // fields input ...
    //
    Label wlFieldsName = new Label(wQueryComp, SWT.RIGHT);
    wlFieldsName.setText(BaseMessages.getString(PKG,
        "MongoDbInputDialog.FieldsName.Label")); //$NON-NLS-1$
    props.setLook(wlFieldsName);
    FormData fdlFieldsName = new FormData();
    fdlFieldsName.left = new FormAttachment(0, 0);
    fdlFieldsName.right = new FormAttachment(middle, -margin);
    fdlFieldsName.bottom = new FormAttachment(100, -margin);
    wlFieldsName.setLayoutData(fdlFieldsName);
    wFieldsName = new TextVar(transMeta, wQueryComp, SWT.SINGLE | SWT.LEFT
        | SWT.BORDER);
    props.setLook(wFieldsName);
    wFieldsName.addModifyListener(lsMod);
    FormData fdFieldsName = new FormData();
    fdFieldsName.left = new FormAttachment(middle, 0);
    fdFieldsName.bottom = new FormAttachment(100, -margin);
    fdFieldsName.right = new FormAttachment(100, 0);
    wFieldsName.setLayoutData(fdFieldsName);
    lastControl = wFieldsName;

    Label queryIsPipelineL = new Label(wQueryComp, SWT.RIGHT);
    queryIsPipelineL.setText(BaseMessages.getString(PKG,
        "MongoDbInputDialog.Pipeline.Label"));
    props.setLook(queryIsPipelineL);
    fd = new FormData();
    fd.bottom = new FormAttachment(lastControl, -margin);
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(middle, -margin);
    queryIsPipelineL.setLayoutData(fd);

    m_queryIsPipelineBut = new Button(wQueryComp, SWT.CHECK);
    props.setLook(m_queryIsPipelineBut);
    fd = new FormData();
    fd.bottom = new FormAttachment(lastControl, -margin);
    fd.left = new FormAttachment(middle, 0);
    fd.right = new FormAttachment(100, 0);
    m_queryIsPipelineBut.setLayoutData(fd);
    lastControl = m_queryIsPipelineBut;

    m_queryIsPipelineBut.addSelectionListener(new SelectionAdapter() {
      @Override
      public void widgetSelected(SelectionEvent e) {
        updateQueryTitleInfo();
      }
    });

    // JSON Query input ...
    //
    wlJsonQuery = new Label(wQueryComp, SWT.NONE);
    wlJsonQuery.setText(BaseMessages.getString(PKG,
        "MongoDbInputDialog.JsonQuery.Label")); //$NON-NLS-1$
    props.setLook(wlJsonQuery);
    FormData fdlJsonQuery = new FormData();
    fdlJsonQuery.left = new FormAttachment(0, 0);
    fdlJsonQuery.right = new FormAttachment(middle, -margin);
    fdlJsonQuery.top = new FormAttachment(0, margin);
    wlJsonQuery.setLayoutData(fdlJsonQuery);

    wJsonQuery = new StyledTextComp(transMeta, wQueryComp, SWT.MULTI | SWT.LEFT
        | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL, "");
    props.setLook(wJsonQuery, props.WIDGET_STYLE_FIXED);
    wJsonQuery.addModifyListener(lsMod);

    /*
     * wJsonQuery = new TextVar(transMeta, wQueryComp, SWT.SINGLE | SWT.LEFT |
     * SWT.BORDER); props.setLook(wJsonQuery);
     * wJsonQuery.addModifyListener(lsMod);
     */
    FormData fdJsonQuery = new FormData();
    fdJsonQuery.left = new FormAttachment(0, 0);
    fdJsonQuery.top = new FormAttachment(wlJsonQuery, margin);
    fdJsonQuery.right = new FormAttachment(100, -2 * margin);
    fdJsonQuery.bottom = new FormAttachment(lastControl, -margin);
    // wJsonQuery.setLayoutData(fdJsonQuery);
    wJsonQuery.setLayoutData(fdJsonQuery);
    // lastControl = wJsonQuery;
    lastControl = wJsonQuery;

    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(100, 0);
    wQueryComp.setLayoutData(fd);

    wQueryComp.layout();
    m_wMongoQueryTab.setControl(wQueryComp);

    // fields tab -----
    m_wMongoFieldsTab = new CTabItem(m_wTabFolder, SWT.NONE);
    m_wMongoFieldsTab.setText(BaseMessages.getString(PKG,
        "MongoDbInputDialog.FieldsTab.TabTitle"));
    Composite wFieldsComp = new Composite(m_wTabFolder, SWT.NONE);
    props.setLook(wFieldsComp);
    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = 3;
    fieldsLayout.marginHeight = 3;
    wFieldsComp.setLayout(fieldsLayout);

    // Output as Json check box
    Label outputJLab = new Label(wFieldsComp, SWT.RIGHT);
    outputJLab.setText(BaseMessages.getString(PKG,
        "MongoDbInputDialog.OutputJson.Label"));
    props.setLook(outputJLab);
    fd = new FormData();
    fd.top = new FormAttachment(0, 0);
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(middle, -margin);
    outputJLab.setLayoutData(fd);
    m_outputAsJson = new Button(wFieldsComp, SWT.CHECK);
    props.setLook(m_outputAsJson);
    fd = new FormData();
    fd.top = new FormAttachment(0, 0);
    fd.left = new FormAttachment(middle, 0);
    fd.right = new FormAttachment(100, 0);
    m_outputAsJson.setLayoutData(fd);
    lastControl = m_outputAsJson;
    m_outputAsJson.addSelectionListener(new SelectionAdapter() {
      @Override
      public void widgetSelected(SelectionEvent e) {
        input.setChanged();
        wGet.setEnabled(!m_outputAsJson.getSelection());
        wJsonField.setEnabled(m_outputAsJson.getSelection());
      }
    });

    // JsonField input ...
    //
    Label wlJsonField = new Label(wFieldsComp, SWT.RIGHT);
    wlJsonField.setText(BaseMessages.getString(PKG,
        "MongoDbInputDialog.JsonField.Label")); //$NON-NLS-1$
    props.setLook(wlJsonField);
    FormData fdlJsonField = new FormData();
    fdlJsonField.left = new FormAttachment(0, 0);
    fdlJsonField.right = new FormAttachment(middle, -margin);
    fdlJsonField.top = new FormAttachment(lastControl, margin);
    wlJsonField.setLayoutData(fdlJsonField);
    wJsonField = new TextVar(transMeta, wFieldsComp, SWT.SINGLE | SWT.LEFT
        | SWT.BORDER);
    props.setLook(wJsonField);
    wJsonField.addModifyListener(lsMod);
    FormData fdJsonField = new FormData();
    fdJsonField.left = new FormAttachment(middle, 0);
    fdJsonField.top = new FormAttachment(lastControl, margin);
    fdJsonField.right = new FormAttachment(100, 0);
    wJsonField.setLayoutData(fdJsonField);
    lastControl = wJsonField;

    // get fields button
    wGet = new Button(wFieldsComp, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG,
        "MongoDbInputDialog.Button.GetFields"));
    props.setLook(wGet);
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(100, 0);
    wGet.setLayoutData(fd);
    wGet.addSelectionListener(new SelectionAdapter() {
      @Override
      public void widgetSelected(SelectionEvent e) {
        // populate table from schema
        MongoDbInputMeta newMeta = (MongoDbInputMeta) input.clone();
        getFields(newMeta, transMeta);
      }
    });

    // fields stuff
    final ColumnInfo[] colinf = new ColumnInfo[] {
        new ColumnInfo(BaseMessages.getString(PKG,
            "MongoDbInputDialog.Fields.FIELD_NAME"),
            ColumnInfo.COLUMN_TYPE_TEXT, false),
        new ColumnInfo(BaseMessages.getString(PKG,
            "MongoDbInputDialog.Fields.FIELD_PATH"),
            ColumnInfo.COLUMN_TYPE_TEXT, false),
        new ColumnInfo(BaseMessages.getString(PKG,
            "MongoDbInputDialog.Fields.FIELD_TYPE"),
            ColumnInfo.COLUMN_TYPE_CCOMBO, false),
        new ColumnInfo(BaseMessages.getString(PKG,
            "MongoDbInputDialog.Fields.FIELD_INDEXED"),
            ColumnInfo.COLUMN_TYPE_TEXT, false),
        new ColumnInfo(BaseMessages.getString(PKG,
            "MongoDbInputDialog.Fields.SAMPLE_ARRAYINFO"),
            ColumnInfo.COLUMN_TYPE_CCOMBO, false),
        new ColumnInfo(BaseMessages.getString(PKG,
            "MongoDbInputDialog.Fields.SAMPLE_PERCENTAGE"),
            ColumnInfo.COLUMN_TYPE_TEXT, false),
        new ColumnInfo(BaseMessages.getString(PKG,
            "MongoDbInputDialog.Fields.SAMPLE_DISPARATE_TYPES"),
            ColumnInfo.COLUMN_TYPE_TEXT, false), };

    colinf[2].setComboValues(ValueMeta.getTypes());
    colinf[4].setReadOnly(true);
    colinf[5].setReadOnly(true);
    colinf[6].setReadOnly(true);

    m_fieldsView = new TableView(transMeta, wFieldsComp, SWT.FULL_SELECTION
        | SWT.MULTI, colinf, 1, lsMod, props);

    fd = new FormData();
    fd.top = new FormAttachment(lastControl, margin * 2);
    fd.bottom = new FormAttachment(wGet, -margin * 2);
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    m_fieldsView.setLayoutData(fd);

    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(100, 0);
    wFieldsComp.setLayoutData(fd);

    wFieldsComp.layout();
    m_wMongoFieldsTab.setControl(wFieldsComp);

    // --------------

    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(wStepname, margin);
    fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(100, -50);
    m_wTabFolder.setLayoutData(fd);

    // Some buttons
    wOK = new Button(shell, SWT.PUSH);
    wOK.setText(BaseMessages.getString(PKG, "System.Button.OK")); //$NON-NLS-1$
    wPreview = new Button(shell, SWT.PUSH);
    wPreview.setText(BaseMessages.getString(PKG, "System.Button.Preview")); //$NON-NLS-1$
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel")); //$NON-NLS-1$

    setButtonPositions(new Button[] { wOK, wPreview, wCancel }, margin,
        m_wTabFolder);

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent(Event e) {
        cancel();
      }
    };
    lsPreview = new Listener() {
      public void handleEvent(Event e) {
        preview();
      }
    };
    lsOK = new Listener() {
      public void handleEvent(Event e) {
        ok();
      }
    };

    wCancel.addListener(SWT.Selection, lsCancel);
    wPreview.addListener(SWT.Selection, lsPreview);
    wOK.addListener(SWT.Selection, lsOK);

    lsDef = new SelectionAdapter() {
      @Override
      public void widgetDefaultSelected(SelectionEvent e) {
        ok();
      }
    };

    wStepname.addSelectionListener(lsDef);
    wHostname.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(new ShellAdapter() {
      @Override
      public void shellClosed(ShellEvent e) {
        cancel();
      }
    });

    getData(input);
    input.setChanged(changed);

    m_wTabFolder.setSelection(0);
    // Set the shell size, based upon previous time...
    setSize();

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch())
        display.sleep();
    }
    return stepname;
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData(MongoDbInputMeta meta) {
    wHostname.setText(Const.NVL(meta.getHostnames(), "")); //$NON-NLS-1$
    wPort.setText(Const.NVL(meta.getPort(), "")); //$NON-NLS-1$
    wDbName.setText(Const.NVL(meta.getDbName(), "")); //$NON-NLS-1$
    wFieldsName.setText(Const.NVL(meta.getFieldsName(), "")); //$NON-NLS-1$
    wCollection.setText(Const.NVL(meta.getCollection(), "")); //$NON-NLS-1$
    wJsonField.setText(Const.NVL(meta.getJsonFieldName(), "")); //$NON-NLS-1$
    wJsonQuery.setText(Const.NVL(meta.getJsonQuery(), "")); //$NON-NLS-1$

    wAuthUser.setText(Const.NVL(meta.getAuthenticationUser(), "")); // $NON-NLS-1$
    wAuthPass.setText(Const.NVL(meta.getAuthenticationPassword(), "")); // $NON-NLS-1$
    m_connectionTimeout.setText(Const.NVL(meta.getConnectTimeout(), ""));
    m_socketTimeout.setText(Const.NVL(meta.getSocketTimeout(), ""));
    m_readPreference.setText(Const.NVL(meta.getReadPreference(), ""));
    m_queryIsPipelineBut.setSelection(meta.getQueryIsPipeline());
    m_outputAsJson.setSelection(meta.getOutputJson());

    setTableFields(meta.getMongoFields());

    wJsonField.setEnabled(meta.getOutputJson());
    wGet.setEnabled(!meta.getOutputJson());

    updateQueryTitleInfo();

    wStepname.selectAll();
  }

  private void updateQueryTitleInfo() {
    if (m_queryIsPipelineBut.getSelection()) {
      wlJsonQuery.setText(BaseMessages.getString(PKG,
          "MongoDbInputDialog.JsonQuery.Label2")
          + ": db."
          + Const.NVL(wCollection.getText(), "n/a") + ".aggregate(...");
      wFieldsName.setEnabled(false);
    } else {
      wlJsonQuery.setText(BaseMessages.getString(PKG,
          "MongoDbInputDialog.JsonQuery.Label"));
      wFieldsName.setEnabled(true);
    }
  }

  private void cancel() {
    stepname = null;
    input.setChanged(changed);
    dispose();
  }

  private void getInfo(MongoDbInputMeta meta) {

    meta.setHostnames(wHostname.getText());
    meta.setPort(wPort.getText());
    meta.setDbName(wDbName.getText());
    meta.setFieldsName(wFieldsName.getText());
    meta.setCollection(wCollection.getText());
    meta.setJsonFieldName(wJsonField.getText());
    meta.setJsonQuery(wJsonQuery.getText());

    meta.setAuthenticationUser(wAuthUser.getText());
    meta.setAuthenticationPassword(wAuthPass.getText());
    meta.setConnectTimeout(m_connectionTimeout.getText());
    meta.setSocketTimeout(m_socketTimeout.getText());
    meta.setReadPreference(m_readPreference.getText());
    meta.setOutputJson(m_outputAsJson.getSelection());
    meta.setQueryIsPipeline(m_queryIsPipelineBut.getSelection());

    int numNonEmpty = m_fieldsView.nrNonEmpty();
    if (numNonEmpty > 0) {
      List<MongoDbInputData.MongoField> outputFields = new ArrayList<MongoDbInputData.MongoField>();
      for (int i = 0; i < numNonEmpty; i++) {
        TableItem item = m_fieldsView.getNonEmpty(i);
        MongoDbInputData.MongoField newField = new MongoDbInputData.MongoField();

        newField.m_fieldName = item.getText(1).trim();
        newField.m_fieldPath = item.getText(2).trim();
        newField.m_kettleType = item.getText(3).trim();

        if (!Const.isEmpty(item.getText(4))) {
          newField.m_indexedVals = MongoDbInputData.indexedValsList(item
              .getText(4).trim());
        }

        outputFields.add(newField);
      }

      meta.setMongoFields(outputFields);
    }
  }

  private void ok() {
    if (Const.isEmpty(wStepname.getText()))
      return;

    stepname = wStepname.getText(); // return value

    getInfo(input);

    dispose();
  }

  private void setTableFields(List<MongoDbInputData.MongoField> fields) {
    if (fields == null) {
      return;
    }

    m_fieldsView.clearAll();
    for (MongoDbInputData.MongoField f : fields) {
      TableItem item = new TableItem(m_fieldsView.table, SWT.NONE);

      if (!Const.isEmpty(f.m_fieldName)) {
        item.setText(1, f.m_fieldName);
      }

      if (!Const.isEmpty(f.m_fieldPath)) {
        item.setText(2, f.m_fieldPath);
      }

      if (!Const.isEmpty(f.m_kettleType)) {
        item.setText(3, f.m_kettleType);
      }

      if (f.m_indexedVals != null && f.m_indexedVals.size() > 0) {
        item.setText(4, MongoDbInputData.indexedValsList(f.m_indexedVals));
      }

      if (!Const.isEmpty(f.m_arrayIndexInfo)) {
        item.setText(5, f.m_arrayIndexInfo);
      }

      if (!Const.isEmpty(f.m_occurenceFraction)) {
        item.setText(6, f.m_occurenceFraction);
      }

      if (f.m_disparateTypes) {
        item.setText(7, "Y");
      }
    }

    m_fieldsView.removeEmptyRows();
    m_fieldsView.setRowNums();
    m_fieldsView.optWidth(true);
  }

  private void getFields(MongoDbInputMeta meta, TransMeta transMeta) {
    if (!Const.isEmpty(wHostname.getText()) && !Const.isEmpty(wPort.getText())
        && !Const.isEmpty(wDbName.getText())
        && !Const.isEmpty(wCollection.getText())) {
      EnterNumberDialog end = new EnterNumberDialog(shell, 100,
          BaseMessages.getString(PKG,
              "MongoDbInputDialog.SampleDocuments.Title"),
          BaseMessages.getString(PKG,
              "MongoDbInputDialog.SampleDocuments.Message"));
      int samples = end.open();
      if (samples > 0) {
        try {
          getInfo(meta);
          boolean result = MongoDbInputData.discoverFields(meta, transMeta,
              samples);

          if (!result) {
            new ErrorDialog(shell, stepname, BaseMessages.getString(PKG,
                "MongoDbInputDialog.ErrorMessage.NoFieldsFound"),
                new KettleException(
                    "MongoDbInputDialog.ErrorMessage.NoFieldsFound"));
          } else {
            getData(meta);
          }
        } catch (KettleException e) {
          new ErrorDialog(shell, stepname, BaseMessages.getString(PKG,
              "MongoDbInputDialog.ErrorMessage.ErrorDuringSampling"), e);
        }
      }
    }
  }

  // Preview the data
  private void preview() {
    // Create the XML input step
    MongoDbInputMeta oneMeta = new MongoDbInputMeta();
    getInfo(oneMeta);

    TransMeta previewMeta = TransPreviewFactory.generatePreviewTransformation(
        transMeta, oneMeta, wStepname.getText());

    EnterNumberDialog numberDialog = new EnterNumberDialog(shell,
        props.getDefaultPreviewSize(), BaseMessages.getString(PKG,
            "MongoDbInputDialog.PreviewSize.DialogTitle"),
        BaseMessages.getString(PKG,
            "MongoDbInputDialog.PreviewSize.DialogMessage"));
    int previewSize = numberDialog.open();
    if (previewSize > 0) {
      TransPreviewProgressDialog progressDialog = new TransPreviewProgressDialog(
          shell, previewMeta, new String[] { wStepname.getText() },
          new int[] { previewSize });
      progressDialog.open();

      Trans trans = progressDialog.getTrans();
      String loggingText = progressDialog.getLoggingText();

      if (!progressDialog.isCancelled()) {
        if (trans.getResult() != null && trans.getResult().getNrErrors() > 0) {
          EnterTextDialog etd = new EnterTextDialog(
              shell,
              BaseMessages.getString(PKG, "System.Dialog.PreviewError.Title"),
              BaseMessages.getString(PKG, "System.Dialog.PreviewError.Message"),
              loggingText, true);
          etd.setReadOnly();
          etd.open();
        }
      }

      PreviewRowsDialog prd = new PreviewRowsDialog(shell, transMeta, SWT.NONE,
          wStepname.getText(), progressDialog.getPreviewRowsMeta(wStepname
              .getText()), progressDialog.getPreviewRows(wStepname.getText()),
          loggingText);
      prd.open();
    }
  }

  private void setupDBNames() {
    wDbName.removeAll();

    String hostname = transMeta.environmentSubstitute(wHostname.getText());

    if (!Const.isEmpty(hostname)) {

      MongoDbInputMeta meta = new MongoDbInputMeta();
      getInfo(meta);
      try {
        MongoClient conn = MongoDbInputData.initConnection(meta, transMeta);
        List<String> dbNames = conn.getDatabaseNames();

        for (String s : dbNames) {
          wDbName.add(s);
        }

        conn.close();
        conn = null;
      } catch (Exception e) {
        logError(BaseMessages.getString(PKG,
            "MongoDbInputDialog.ErrorMessage.UnableToConnect"), e);
        new ErrorDialog(shell, BaseMessages.getString(PKG,
            "MongoDbInputDialog.ErrorMessage." + "UnableToConnect"),
            BaseMessages.getString(PKG,
                "MongoDbInputDialog.ErrorMessage.UnableToConnect"), e);
      }
    }
  }

  private void setupCollectionNamesForDB() {

    String hostname = transMeta.environmentSubstitute(wHostname.getText());
    String dB = transMeta.environmentSubstitute(wDbName.getText());
    String username = transMeta.environmentSubstitute(wAuthUser.getText());
    String realPass = Encr.decryptPasswordOptionallyEncrypted(transMeta
        .environmentSubstitute(wAuthPass.getText()));

    if (Const.isEmpty(dB)) {
      return;
    }

    wCollection.removeAll();

    if (!Const.isEmpty(hostname)) {

      MongoDbInputMeta meta = new MongoDbInputMeta();
      getInfo(meta);
      try {
        MongoClient conn = MongoDbInputData.initConnection(meta, transMeta);
        DB theDB = conn.getDB(dB);

        if (!Const.isEmpty(username) || !Const.isEmpty(realPass)) {
          CommandResult comResult = theDB.authenticateCommand(username,
              realPass.toCharArray());
          if (!comResult.ok()) {
            throw new Exception(BaseMessages.getString(PKG,
                "MongoDbInput.ErrorAuthenticating.Exception",
                comResult.getErrorMessage()));
          }
        }

        Set<String> collections = theDB.getCollectionNames();
        for (String c : collections) {
          wCollection.add(c);
        }

        conn.close();
        conn = null;
      } catch (Exception e) {
        logError(BaseMessages.getString(PKG,
            "MongoDbInputDialog.ErrorMessage.UnableToConnect"), e);
        new ErrorDialog(shell, BaseMessages.getString(PKG,
            "MongoDbInputDialog.ErrorMessage." + "UnableToConnect"),
            BaseMessages.getString(PKG,
                "MongoDbInputDialog.ErrorMessage.UnableToConnect"), e);
      }
    }
  }
}
