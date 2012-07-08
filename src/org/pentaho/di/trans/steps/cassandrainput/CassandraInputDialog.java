/*******************************************************************************
 *
 * Pentaho Big Data
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

package org.pentaho.di.trans.steps.cassandrainput;

import java.util.ArrayList;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.cassandra.CassandraColumnMetaData;
import org.pentaho.cassandra.CassandraConnection;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.dialog.ShowMessageDialog;
import org.pentaho.di.ui.core.widget.StyledTextComp;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.trans.steps.tableinput.SQLValuesHighlight;

/**
 * Dialog class for the CassandraInput step
 * 
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision$
 */
public class CassandraInputDialog extends BaseStepDialog implements
    StepDialogInterface {

  private static final Class<?> PKG = CassandraInputMeta.class;

  private final CassandraInputMeta m_currentMeta;
  private final CassandraInputMeta m_originalMeta;

  /** various UI bits and pieces for the dialog */
  private Label m_stepnameLabel;
  private Text m_stepnameText;

  private Label m_hostLab;
  private TextVar m_hostText;
  private Label m_portLab;
  private TextVar m_portText;

  private Label m_userLab;
  private TextVar m_userText;
  private Label m_passLab;
  private TextVar m_passText;

  private Label m_keyspaceLab;
  private TextVar m_keyspaceText;

  private Label m_compressionLab;
  private Button m_useCompressionBut;

  private Label m_outputTuplesLab;
  private Button m_outputTuplesBut;

  private Label m_useThriftLab;
  private Button m_useThriftBut;

  private Label m_timeoutLab;
  private TextVar m_timeoutText;

  private Label m_positionLab;

  private Button m_showSchemaBut;

  private Label m_cqlLab;
  private StyledTextComp m_cqlText;

  public CassandraInputDialog(Shell parent, Object in, TransMeta tr, String name) {

    super(parent, (BaseStepMeta) in, tr, name);

    m_currentMeta = (CassandraInputMeta) in;
    m_originalMeta = (CassandraInputMeta) m_currentMeta.clone();
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);

    props.setLook(shell);
    setShellImage(shell, m_currentMeta);

    // used to listen to a text field (m_wStepname)
    ModifyListener lsMod = new ModifyListener() {
      public void modifyText(ModifyEvent e) {
        m_currentMeta.setChanged();
      }
    };

    changed = m_currentMeta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG,
        "CassandraInputDialog.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Stepname line
    m_stepnameLabel = new Label(shell, SWT.RIGHT);
    m_stepnameLabel.setText(BaseMessages.getString(PKG,
        "CassandraInputDialog.StepName.Label"));
    props.setLook(m_stepnameLabel);

    FormData fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(middle, -margin);
    fd.top = new FormAttachment(0, margin);
    m_stepnameLabel.setLayoutData(fd);
    m_stepnameText = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    m_stepnameText.setText(stepname);
    props.setLook(m_stepnameText);
    m_stepnameText.addModifyListener(lsMod);

    // format the text field
    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(0, margin);
    fd.right = new FormAttachment(100, 0);
    m_stepnameText.setLayoutData(fd);

    // host line
    m_hostLab = new Label(shell, SWT.RIGHT);
    props.setLook(m_hostLab);
    m_hostLab.setText(BaseMessages.getString(PKG,
        "CassandraInputDialog.Hostname.Label"));
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(m_stepnameText, margin);
    fd.right = new FormAttachment(middle, -margin);
    m_hostLab.setLayoutData(fd);

    m_hostText = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT
        | SWT.BORDER);
    props.setLook(m_hostText);
    m_hostText.addModifyListener(new ModifyListener() {
      public void modifyText(ModifyEvent e) {
        m_hostText.setToolTipText(transMeta.environmentSubstitute(m_hostText
            .getText()));
      }
    });
    m_hostText.addModifyListener(lsMod);
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(m_stepnameText, margin);
    fd.left = new FormAttachment(middle, 0);
    m_hostText.setLayoutData(fd);

    // port line
    m_portLab = new Label(shell, SWT.RIGHT);
    props.setLook(m_portLab);
    m_portLab.setText(BaseMessages.getString(PKG,
        "CassandraInputDialog.Port.Label"));
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(m_hostText, margin);
    fd.right = new FormAttachment(middle, -margin);
    m_portLab.setLayoutData(fd);

    m_portText = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT
        | SWT.BORDER);
    props.setLook(m_portText);
    m_portText.addModifyListener(new ModifyListener() {
      public void modifyText(ModifyEvent e) {
        m_portText.setToolTipText(transMeta.environmentSubstitute(m_portText
            .getText()));
      }
    });
    m_portText.addModifyListener(lsMod);
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(m_hostText, margin);
    fd.left = new FormAttachment(middle, 0);
    m_portText.setLayoutData(fd);

    // timeout line
    m_timeoutLab = new Label(shell, SWT.RIGHT);
    props.setLook(m_timeoutLab);
    m_timeoutLab.setText(BaseMessages.getString(PKG,
        "CassandraInputDialog.Timeout.Label"));
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(m_portText, margin);
    fd.right = new FormAttachment(middle, -margin);
    m_timeoutLab.setLayoutData(fd);

    m_timeoutText = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT
        | SWT.BORDER);
    props.setLook(m_timeoutText);
    m_timeoutText.addModifyListener(new ModifyListener() {
      public void modifyText(ModifyEvent e) {
        m_timeoutText.setToolTipText(transMeta
            .environmentSubstitute(m_timeoutText.getText()));
      }
    });
    m_timeoutText.addModifyListener(lsMod);
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(m_portText, margin);
    fd.left = new FormAttachment(middle, 0);
    m_timeoutText.setLayoutData(fd);

    // username line
    m_userLab = new Label(shell, SWT.RIGHT);
    props.setLook(m_userLab);
    m_userLab.setText(BaseMessages.getString(PKG,
        "CassandraInputDialog.User.Label"));
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(m_timeoutText, margin);
    fd.right = new FormAttachment(middle, -margin);
    m_userLab.setLayoutData(fd);

    m_userText = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT
        | SWT.BORDER);
    props.setLook(m_userText);
    m_userText.addModifyListener(new ModifyListener() {
      public void modifyText(ModifyEvent e) {
        m_userText.setToolTipText(transMeta.environmentSubstitute(m_userText
            .getText()));
      }
    });
    m_userText.addModifyListener(lsMod);
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(m_timeoutText, margin);
    fd.left = new FormAttachment(middle, 0);
    m_userText.setLayoutData(fd);

    // password line
    m_passLab = new Label(shell, SWT.RIGHT);
    props.setLook(m_passLab);
    m_passLab.setText(BaseMessages.getString(PKG,
        "CassandraInputDialog.Password.Label"));
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(m_userText, margin);
    fd.right = new FormAttachment(middle, -margin);
    m_passLab.setLayoutData(fd);

    m_passText = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT
        | SWT.BORDER);
    props.setLook(m_passText);
    m_passText.setEchoChar('*');
    // If the password contains a variable, don't hide it.
    m_passText.getTextWidget().addModifyListener(new ModifyListener() {
      public void modifyText(ModifyEvent e) {
        checkPasswordVisible();
      }
    });

    m_passText.addModifyListener(lsMod);

    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(m_userText, margin);
    fd.left = new FormAttachment(middle, 0);
    m_passText.setLayoutData(fd);

    // keyspace line
    m_keyspaceLab = new Label(shell, SWT.RIGHT);
    props.setLook(m_keyspaceLab);
    m_keyspaceLab.setText(BaseMessages.getString(PKG,
        "CassandraInputDialog.Keyspace.Label"));
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(m_passText, margin);
    fd.right = new FormAttachment(middle, -margin);
    m_keyspaceLab.setLayoutData(fd);

    m_keyspaceText = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT
        | SWT.BORDER);
    props.setLook(m_keyspaceText);
    m_keyspaceText.addModifyListener(new ModifyListener() {
      public void modifyText(ModifyEvent e) {
        m_keyspaceText.setToolTipText(transMeta
            .environmentSubstitute(m_keyspaceText.getText()));
      }
    });
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(m_passText, margin);
    fd.left = new FormAttachment(middle, 0);
    m_keyspaceText.setLayoutData(fd);

    // output key, column, timestamp tuples line
    m_outputTuplesLab = new Label(shell, SWT.RIGHT);
    props.setLook(m_outputTuplesLab);
    m_outputTuplesLab.setText(BaseMessages.getString(PKG,
        "CassandraInputDialog.OutputTuples.Label"));
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(m_keyspaceText, margin);
    fd.right = new FormAttachment(middle, -margin);
    m_outputTuplesLab.setLayoutData(fd);

    m_outputTuplesBut = new Button(shell, SWT.CHECK);
    props.setLook(m_outputTuplesBut);
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(m_keyspaceText, margin);
    fd.left = new FormAttachment(middle, 0);
    m_outputTuplesBut.setLayoutData(fd);

    m_outputTuplesBut.addSelectionListener(new SelectionAdapter() {
      @Override
      public void widgetSelected(SelectionEvent e) {
        // check tuple/thrift mode
        checkWidgets();
      }
    });

    m_useThriftLab = new Label(shell, SWT.RIGHT);
    props.setLook(m_useThriftLab);
    m_useThriftLab.setText(BaseMessages.getString(PKG,
        "CassandraInputDialog.Thrift.Label"));
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(m_outputTuplesBut, margin);
    fd.right = new FormAttachment(middle, -margin);
    m_useThriftLab.setLayoutData(fd);

    m_useThriftBut = new Button(shell, SWT.CHECK);
    props.setLook(m_useThriftBut);
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(m_outputTuplesBut, margin);
    fd.left = new FormAttachment(middle, 0);
    m_useThriftBut.setLayoutData(fd);

    // compression check box
    m_compressionLab = new Label(shell, SWT.RIGHT);
    props.setLook(m_compressionLab);
    m_compressionLab.setText(BaseMessages.getString(PKG,
        "CassandraInputDialog.UseCompression.Label"));
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(m_useThriftBut, margin);
    fd.right = new FormAttachment(middle, -margin);
    m_compressionLab.setLayoutData(fd);

    m_useCompressionBut = new Button(shell, SWT.CHECK);
    props.setLook(m_useCompressionBut);
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(m_useThriftBut, margin);
    m_useCompressionBut.setLayoutData(fd);
    m_useCompressionBut.addSelectionListener(new SelectionAdapter() {
      @Override
      public void widgetSelected(SelectionEvent e) {
        m_currentMeta.setChanged();
      }
    });

    // Buttons inherited from BaseStepDialog
    wOK = new Button(shell, SWT.PUSH);
    wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));

    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

    setButtonPositions(new Button[] { wOK, wCancel }, margin, m_cqlText);

    // position label
    m_positionLab = new Label(shell, SWT.NONE);
    props.setLook(m_positionLab);
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(middle, -margin);
    fd.bottom = new FormAttachment(wOK, -margin);
    m_positionLab.setLayoutData(fd);

    m_showSchemaBut = new Button(shell, SWT.PUSH);
    m_showSchemaBut.setText(BaseMessages.getString(PKG,
        "CassandraInputDialog.Schema.Button"));
    props.setLook(m_showSchemaBut);
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(wOK, -margin);
    m_showSchemaBut.setLayoutData(fd);

    m_showSchemaBut.addSelectionListener(new SelectionAdapter() {
      @Override
      public void widgetSelected(SelectionEvent e) {
        RowMeta outputF = new RowMeta();
        CassandraConnection conn = null;
        try {
          String hostS = transMeta.environmentSubstitute(m_hostText.getText());
          String portS = transMeta.environmentSubstitute(m_portText.getText());
          String userS = m_userText.getText();
          String passS = m_passText.getText();
          if (!Const.isEmpty(userS) && !Const.isEmpty(passS)) {
            userS = transMeta.environmentSubstitute(userS);
            passS = transMeta.environmentSubstitute(passS);
          }
          String keyspaceS = transMeta.environmentSubstitute(m_keyspaceText
              .getText());
          String cqlText = transMeta.environmentSubstitute(m_cqlText.getText());

          conn = CassandraInputData.getCassandraConnection(hostS,
              Integer.parseInt(portS), userS, passS);
          try {
            conn.setKeyspace(keyspaceS);
          } catch (InvalidRequestException ire) {
            logError(
                BaseMessages
                    .getString(PKG,
                        "CassandraInputDialog.Error.ProblemGettingSchemaInfo.Message")
                    + ":\n\n" + ire.why, ire);
            new ErrorDialog(
                shell,
                BaseMessages
                    .getString(PKG,
                        "CassandraInputDialog.Error.ProblemGettingSchemaInfo.Title"),
                BaseMessages
                    .getString(PKG,
                        "CassandraInputDialog.Error.ProblemGettingSchemaInfo.Message")
                    + ":\n\n" + ire.why, ire);
            return;
          }

          String colFam = CassandraInputData
              .getColumnFamilyNameFromCQLSelectQuery(cqlText);
          if (Const.isEmpty(colFam)) {
            throw new Exception(BaseMessages.getString(PKG,
                "CassandraInput.Error.NoFromClauseInQuery"));
          }

          if (!CassandraColumnMetaData.columnFamilyExists(conn, colFam)) {
            throw new Exception(BaseMessages.getString(PKG,
                "CassandraInput.Error.NonExistentColumnFamily", colFam,
                keyspaceS));
          }

          CassandraColumnMetaData cassMeta = new CassandraColumnMetaData(conn,
              colFam);
          String schemaDescription = cassMeta.getSchemaDescription();
          ShowMessageDialog smd = new ShowMessageDialog(shell,
              SWT.ICON_INFORMATION | SWT.OK, "Schema info", schemaDescription,
              true);
          smd.open();
        } catch (Exception e1) {
          logError(
              BaseMessages
                  .getString(PKG,
                      "CassandraInputDialog.Error.ProblemGettingSchemaInfo.Message")
                  + ":\n\n" + e1.getMessage(), e1);
          new ErrorDialog(
              shell,
              BaseMessages.getString(PKG,
                  "CassandraInputDialog.Error.ProblemGettingSchemaInfo.Title"),
              BaseMessages
                  .getString(PKG,
                      "CassandraInputDialog.Error.ProblemGettingSchemaInfo.Message")
                  + ":\n\n" + e1.getMessage(), e1);
        } finally {
          if (conn != null) {
            conn.close();
          }
        }
      }
    });

    // cql stuff
    m_cqlLab = new Label(shell, SWT.NONE);
    props.setLook(m_cqlLab);
    m_cqlLab.setText(BaseMessages.getString(PKG,
        "CassandraInputDialog.CQL.Label"));
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(m_useCompressionBut, margin);
    fd.right = new FormAttachment(middle, -margin);
    m_cqlLab.setLayoutData(fd);

    m_cqlText = new StyledTextComp(transMeta, shell, SWT.MULTI | SWT.LEFT
        | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL, "");
    props.setLook(m_cqlText, props.WIDGET_STYLE_FIXED);
    m_cqlText.addModifyListener(lsMod);
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(m_cqlLab, margin);
    fd.right = new FormAttachment(100, -2 * margin);
    fd.bottom = new FormAttachment(m_showSchemaBut, -margin);
    m_cqlText.setLayoutData(fd);
    m_cqlText.addModifyListener(new ModifyListener() {
      public void modifyText(ModifyEvent e) {
        setPosition();
        m_cqlText.setToolTipText(transMeta.environmentSubstitute(m_cqlText
            .getText()));
      }
    });

    // Text Highlighting
    m_cqlText.addLineStyleListener(new SQLValuesHighlight());

    m_cqlText.addKeyListener(new KeyAdapter() {
      @Override
      public void keyPressed(KeyEvent e) {
        setPosition();
      }

      @Override
      public void keyReleased(KeyEvent e) {
        setPosition();
      }
    });

    m_cqlText.addFocusListener(new FocusAdapter() {
      @Override
      public void focusGained(FocusEvent e) {
        setPosition();
      }

      @Override
      public void focusLost(FocusEvent e) {
        setPosition();
      }
    });

    m_cqlText.addMouseListener(new MouseAdapter() {
      @Override
      public void mouseDoubleClick(MouseEvent e) {
        setPosition();
      }

      @Override
      public void mouseDown(MouseEvent e) {
        setPosition();
      }

      @Override
      public void mouseUp(MouseEvent e) {
        setPosition();
      }
    });

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent(Event e) {
        cancel();
      }
    };

    lsOK = new Listener() {
      public void handleEvent(Event e) {
        ok();
      }
    };

    wCancel.addListener(SWT.Selection, lsCancel);
    wOK.addListener(SWT.Selection, lsOK);

    lsDef = new SelectionAdapter() {
      @Override
      public void widgetDefaultSelected(SelectionEvent e) {
        ok();
      }
    };

    m_stepnameText.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(new ShellAdapter() {
      @Override
      public void shellClosed(ShellEvent e) {
        cancel();
      }
    });

    setSize();

    getData();

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }

    return stepname;
  }

  private void checkWidgets() {
    if (m_outputTuplesBut.getSelection()) {
      m_useThriftBut.setEnabled(true);
    } else {
      m_useThriftBut.setSelection(false);
      m_useThriftBut.setEnabled(false);
    }
  }

  protected void ok() {
    if (Const.isEmpty(m_stepnameText.getText())) {
      return;
    }

    stepname = m_stepnameText.getText();
    m_currentMeta.setCassandraHost(m_hostText.getText());
    m_currentMeta.setCassandraPort(m_portText.getText());
    m_currentMeta.setSocketTimeout(m_timeoutText.getText());
    m_currentMeta.setUsername(m_userText.getText());
    m_currentMeta.setPassword(m_passText.getText());
    m_currentMeta.setCassandraKeyspace(m_keyspaceText.getText());
    m_currentMeta.setUseCompression(m_useCompressionBut.getSelection());
    m_currentMeta.setOutputKeyValueTimestampTuples(m_outputTuplesBut
        .getSelection());
    m_currentMeta.setUseThriftIO(m_useThriftBut.getSelection());
    m_currentMeta.setCQLSelectQuery(m_cqlText.getText());

    if (!m_originalMeta.equals(m_currentMeta)) {
      m_currentMeta.setChanged();
      changed = m_currentMeta.hasChanged();
    }

    dispose();
  }

  protected void cancel() {
    stepname = null;
    m_currentMeta.setChanged(changed);

    dispose();
  }

  protected void getData() {
    if (!Const.isEmpty(m_currentMeta.getCassandraHost())) {
      m_hostText.setText(m_currentMeta.getCassandraHost());
    }

    if (!Const.isEmpty(m_currentMeta.getCassandraPort())) {
      m_portText.setText(m_currentMeta.getCassandraPort());
    }

    if (!Const.isEmpty(m_currentMeta.getSocketTimeout())) {
      m_timeoutText.setText(m_currentMeta.getSocketTimeout());
    }

    if (!Const.isEmpty(m_currentMeta.getUsername())) {
      m_userText.setText(m_currentMeta.getUsername());
    }

    if (!Const.isEmpty(m_currentMeta.getPassword())) {
      m_passText.setText(m_currentMeta.getPassword());
    }

    if (!Const.isEmpty(m_currentMeta.getCassandraKeyspace())) {
      m_keyspaceText.setText(m_currentMeta.getCassandraKeyspace());
    }

    m_useCompressionBut.setSelection(m_currentMeta.getUseCompression());

    m_outputTuplesBut.setSelection(m_currentMeta
        .getOutputKeyValueTimestampTuples());
    m_useThriftBut.setSelection(m_currentMeta.getUseThriftIO());

    if (!Const.isEmpty(m_currentMeta.getCQLSelectQuery())) {
      m_cqlText.setText(m_currentMeta.getCQLSelectQuery());
    }

    checkWidgets();
  }

  protected void setPosition() {
    String scr = m_cqlText.getText();
    int linenr = m_cqlText.getLineAtOffset(m_cqlText.getCaretOffset()) + 1;
    int posnr = m_cqlText.getCaretOffset();

    // Go back from position to last CR: how many positions?
    int colnr = 0;
    while (posnr > 0 && scr.charAt(posnr - 1) != '\n'
        && scr.charAt(posnr - 1) != '\r') {
      posnr--;
      colnr++;
    }
    m_positionLab.setText(BaseMessages.getString(PKG,
        "CassandraInputDialog.Position.Label", "" + linenr, "" + colnr));
  }

  private void checkPasswordVisible() {
    String password = m_passText.getText();
    ArrayList<String> list = new ArrayList<String>();
    StringUtil.getUsedVariables(password, list, true);
    if (list.size() == 0) {
      m_passText.setEchoChar('*');
    } else {
      m_passText.setEchoChar('\0'); // show everything
    }
  }
}
