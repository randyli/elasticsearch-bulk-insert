/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.ui.trans.steps.elasticsearchbulk;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.*;

import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.steps.elasticsearchbulk.ElasticSearchBulkMeta;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.LabelComboVar;
import org.pentaho.di.ui.core.widget.LabelTextVar;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public class ElasticSearchBulkDialog extends BaseStepDialog implements StepDialogInterface {

  private ElasticSearchBulkMeta model;

  private static Class<?> PKG = ElasticSearchBulkMeta.class;
  private CTabFolder wTabFolder;
  private FormData fdTabFolder;
  private CTabItem wGeneralTab;
  private Composite wGeneralComp;
  private FormData fdGeneralComp;
  private Label wlBatchSize;

  private TextVar wBatchSize;
  private LabelTextVar wIdOutField;
  private Group wIndexGroup;
  private FormData fdIndexGroup;
  private Group wSettingsGroup;
  private FormData fdSettingsGroup;

  private String[] fieldNames;

  private CTabItem wFieldsTab;

  private LabelTextVar wIndex;
  private LabelTextVar wType;

  private ModifyListener lsMod;

  private Button wIsJson;

  private Label wlIsJson;

  private Label wlUseOutput;

  private Button wUseOutput;

  private LabelComboVar wJsonField;

  private TableView wFields;

  private CTabItem wServersTab;

  private Text wUserName;

  private String userName;

  private Text wPassword;

  private String password;

  private Text wConnectTimeout;

  private int connectTimeout = ElasticSearchBulkMeta.DEFAULT_CONNECT_TIMEOUT;

  private Text wSocketTimeout;

  private int socketTimeout = ElasticSearchBulkMeta.DEFAULT_SOCKET_TIMEOUT;

  private Text wMaxRetryTimeout;

  private int maxRetryTimeout = ElasticSearchBulkMeta.DEFAULT_MAX_RETRY_TIMEOUT;

  private TableView wServers;

  //private Group wAuthGroup;

  private CTabItem wSettingsTab;

  private TableView wSettings;

  private Label wlStopOnError;

  private Button wStopOnError;

  private LabelComboVar wIdInField;

  private Button wIsOverwrite;

  private Label wlIsOverwrite;

  public ElasticSearchBulkDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    model = (ElasticSearchBulkMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, model );

    lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        model.setChanged();
      }
    };

    changed = model.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "ElasticSearchBulkDialog.DialogTitle" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "System.Label.StepName" ) );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.top = new FormAttachment( 0, margin );
    fdlStepname.right = new FormAttachment( middle, -margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, 0 );
    fdStepname.top = new FormAttachment( 0, margin );
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );

    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );

    // GENERAL TAB
    addGeneralTab();

    // Servers TAB
    addServersTab();

    // Fields TAB
    addFieldsTab();

    // Settings TAB
    addSettingsTab();

    // ////////////
    // BUTTONS //
    // //////////
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[]{ wOK, wCancel }, margin, null );

    fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wStepname, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( wOK, -margin );
    wTabFolder.setLayoutData( fdTabFolder );

    // //////////////////
    // Std Listeners //
    // ////////////////
    addStandardListeners();

    wTabFolder.setSelection( 0 );

    // Set the shell size, based upon previous time...
    setSize();
    getData( model );
    model.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }

  private void addStandardListeners() {
    // Add listeners
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    lsCancel = new Listener() {

      public void handleEvent( Event e ) {
        cancel();
      }
    };

    lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent event ) {
        model.setChanged();
      }
    };

    wOK.addListener( SWT.Selection, lsOK );
    wCancel.addListener( SWT.Selection, lsCancel );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );

    // window close
    shell.addShellListener( new ShellAdapter() {

      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );
  }

  /**
   */
  private void addGeneralTab() {
    wGeneralTab = new CTabItem( wTabFolder, SWT.NONE );
    wGeneralTab.setText( BaseMessages.getString( PKG, "ElasticSearchBulkDialog.General.Tab" ) );

    wGeneralComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wGeneralComp );

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;
    wGeneralComp.setLayout( generalLayout );

    // Index GROUP
    fillIndexGroup( wGeneralComp );

    // Options GROUP
    fillOptionsGroup( wGeneralComp );

    fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment( 0, 0 );
    fdGeneralComp.top = new FormAttachment( wStepname, Const.MARGIN );
    fdGeneralComp.right = new FormAttachment( 100, 0 );
    fdGeneralComp.bottom = new FormAttachment( 100, 0 );
    wGeneralComp.setLayoutData( fdGeneralComp );

    wGeneralComp.layout();
    wGeneralTab.setControl( wGeneralComp );
  }

  private void fillIndexGroup( Composite parentTab ) {
    wIndexGroup = new Group( parentTab, SWT.SHADOW_NONE );
    props.setLook( wIndexGroup );
    wIndexGroup.setText( BaseMessages.getString( PKG, "ElasticSearchBulkDialog.IndexGroup.Label" ) );

    FormLayout indexGroupLayout = new FormLayout();
    indexGroupLayout.marginWidth = 10;
    indexGroupLayout.marginHeight = 10;
    wIndexGroup.setLayout( indexGroupLayout );

    // Index
    wIndex =
      new LabelTextVar( transMeta, wIndexGroup,
        BaseMessages.getString( PKG, "ElasticSearchBulkDialog.Index.Label" ),
        BaseMessages.getString( PKG, "ElasticSearchBulkDialog.Index.Tooltip" ) );
    wIndex.addModifyListener( lsMod );

    // Type
    wType =
      new LabelTextVar( transMeta, wIndexGroup,
        BaseMessages.getString( PKG, "ElasticSearchBulkDialog.Type.Label" ),
        BaseMessages.getString( PKG, "ElasticSearchBulkDialog.Type.Tooltip" ) );
    wType.addModifyListener( lsMod );

    Control[] connectionControls = new Control[]{ wIndex, wType };
    placeControls( wIndexGroup, connectionControls );

    //BaseStepDialog.positionBottomButtons( wIndexGroup, new Button[]{ wTest }, Const.MARGIN, wType );

    fdIndexGroup = new FormData();
    fdIndexGroup.left = new FormAttachment( 0, Const.MARGIN );
    fdIndexGroup.top = new FormAttachment( wStepname, Const.MARGIN );
    fdIndexGroup.right = new FormAttachment( 100, -Const.MARGIN );
    wIndexGroup.setLayoutData( fdIndexGroup );
  }

  private void addServersTab() {
    wServersTab = new CTabItem( wTabFolder, SWT.NONE );
    wServersTab.setText( BaseMessages.getString( PKG, "ElasticSearchBulkDialog.ServersTab.TabTitle" ) );


    Composite wServersComp = new Composite( wTabFolder, SWT.NONE );
    wServersComp.setLayout( new GridLayout(2, false) );
    props.setLook( wServersComp );

    Group grpAuth = new Group(wServersComp, SWT.NONE);
    grpAuth.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 2, 1));
    grpAuth.setText("Auth");
    grpAuth.setLayout(new GridLayout(2, false));
    props.setLook( grpAuth );

    Label lblUserName = new Label(grpAuth, SWT.NONE);
    lblUserName.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false, 1, 1));
    lblUserName.setText("User Name:");

    wUserName = new Text(grpAuth, SWT.BORDER);
    wUserName.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 1, 1));

    Label lblPassword = new Label(grpAuth, SWT.NONE);
    lblPassword.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false, 1, 1));
    lblPassword.setText("Password:");

    wPassword = new Text(grpAuth, SWT.BORDER);
    wPassword.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 1, 1));
    //fillAuthGroup(wServersComp);

    Group grpTimeout = new Group(wServersComp, SWT.NONE);
    grpTimeout.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 2, 1));
    grpTimeout.setText("Timeout");
    grpTimeout.setLayout(new GridLayout(2, false));

    Label lblConnectTimeout = new Label(grpTimeout, SWT.NONE);
    lblConnectTimeout.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false, 1, 1));
    lblConnectTimeout.setText("Connect Timeout:");

    wConnectTimeout = new Text(grpTimeout, SWT.BORDER);
    wConnectTimeout.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 1, 1));

    Label lblSocketTimeout = new Label(grpTimeout, SWT.NONE);
    lblSocketTimeout.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false, 1, 1));
    lblSocketTimeout.setText("Socket Timeout:");

    wSocketTimeout = new Text(grpTimeout, SWT.BORDER);
    wSocketTimeout.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 1, 1));

    Label lblMaxRetryTimeout = new Label(grpTimeout, SWT.NONE);
    lblMaxRetryTimeout.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false, 1, 1));
    lblMaxRetryTimeout.setText("Max Retry Timeout:");

    wMaxRetryTimeout = new Text(grpTimeout, SWT.BORDER);
    wMaxRetryTimeout.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 1, 1));

    Group grpHosts = new Group(wServersComp, SWT.NONE);
    grpHosts.setLayout(new FillLayout(SWT.HORIZONTAL));
    grpHosts.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true, 1, 4));
    grpHosts.setText("Hosts");
    props.setLook( grpHosts );


    ColumnInfo[] columnsMeta = new ColumnInfo[3];
    columnsMeta[0] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ElasticSearchBulkDialog.ServersTab.Address.Column" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false );
    columnsMeta[1] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ElasticSearchBulkDialog.ServersTab.Port.Column" ),
        ColumnInfo.COLUMN_TYPE_TEXT, true );
    columnsMeta[2] =
            new ColumnInfo(
                    "Status",
                    ColumnInfo.COLUMN_TYPE_TEXT, false );
    wServers =
      new TableView(
        transMeta, grpHosts, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, columnsMeta, 1, lsMod, props );

    Button btnTestAll = new Button(wServersComp, SWT.CENTER);
    btnTestAll.setText("Test All");
    btnTestAll.addSelectionListener(new SelectionAdapter() {
      @Override
      public void widgetSelected(SelectionEvent selectionEvent) {
          testAll();
      }
    });
    wServersTab.setControl( wServersComp );
  }

  private void addSettingsTab() {
    wSettingsTab = new CTabItem( wTabFolder, SWT.NONE );
    wSettingsTab.setText( BaseMessages.getString( PKG, "ElasticSearchBulkDialog.SettingsTab.TabTitle" ) );

    FormLayout serversLayout = new FormLayout();
    serversLayout.marginWidth = Const.FORM_MARGIN;
    serversLayout.marginHeight = Const.FORM_MARGIN;

    Composite wSettingsComp = new Composite( wTabFolder, SWT.NONE );
    wSettingsComp.setLayout( serversLayout );
    props.setLook( wSettingsComp );

    ColumnInfo[] columnsMeta = new ColumnInfo[2];
    columnsMeta[0] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ElasticSearchBulkDialog.SettingsTab.Property.Column" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false );
    columnsMeta[1] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ElasticSearchBulkDialog.SettingsTab.Value.Column" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false );

    wSettings =
      new TableView(
        transMeta, wSettingsComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, columnsMeta, 1, lsMod, props );
    FormData fdServers = new FormData();
    fdServers.left = new FormAttachment( 0, Const.MARGIN );
    fdServers.top = new FormAttachment( 0, Const.MARGIN );
    fdServers.right = new FormAttachment( 100, -Const.MARGIN );
    fdServers.bottom = new FormAttachment( 100, -Const.MARGIN );
    wSettings.setLayoutData( fdServers );

    FormData fdServersComp = new FormData();
    fdServersComp.left = new FormAttachment( 0, 0 );
    fdServersComp.top = new FormAttachment( 0, 0 );
    fdServersComp.right = new FormAttachment( 100, 0 );
    fdServersComp.bottom = new FormAttachment( 100, 0 );
    wSettingsComp.setLayoutData( fdServersComp );

    wSettingsComp.layout();
    wSettingsTab.setControl( wSettingsComp );
  }

  private void addFieldsTab() {

    wFieldsTab = new CTabItem( wTabFolder, SWT.NONE );
    wFieldsTab.setText( BaseMessages.getString( PKG, "ElasticSearchBulkDialog.FieldsTab.TabTitle" ) );

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = Const.FORM_MARGIN;
    fieldsLayout.marginHeight = Const.FORM_MARGIN;

    Composite wFieldsComp = new Composite( wTabFolder, SWT.NONE );
    wFieldsComp.setLayout( fieldsLayout );
    props.setLook( wFieldsComp );

    wGet = new Button( wFieldsComp, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "System.Button.GetFields" ) );
    wGet.setToolTipText( BaseMessages.getString( PKG, "System.Tooltip.GetFields" ) );

    lsGet = new Listener() {
      public void handleEvent( Event e ) {
        getPreviousFields( wFields );
      }
    };
    wGet.addListener( SWT.Selection, lsGet );

    setButtonPositions( new Button[]{ wGet }, Const.MARGIN, null );

    final int fieldsRowCount = model.getFields().size();

    String[] names = this.fieldNames != null ? this.fieldNames : new String[]{ "" };
    ColumnInfo[] columnsMeta = new ColumnInfo[2];
    columnsMeta[0] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ElasticSearchBulkDialog.NameColumn.Column" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, names, false );
    columnsMeta[1] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ElasticSearchBulkDialog.TargetNameColumn.Column" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false );

    wFields =
      new TableView(
        transMeta, wFieldsComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, columnsMeta, fieldsRowCount,
        lsMod, props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, Const.MARGIN );
    fdFields.top = new FormAttachment( 0, Const.MARGIN );
    fdFields.right = new FormAttachment( 100, -Const.MARGIN );
    fdFields.bottom = new FormAttachment( wGet, -Const.MARGIN );
    wFields.setLayoutData( fdFields );

    FormData fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment( 0, 0 );
    fdFieldsComp.top = new FormAttachment( 0, 0 );
    fdFieldsComp.right = new FormAttachment( 100, 0 );
    fdFieldsComp.bottom = new FormAttachment( 100, 0 );
    wFieldsComp.setLayoutData( fdFieldsComp );

    wFieldsComp.layout();
    wFieldsTab.setControl( wFieldsComp );
  }

  private void fillOptionsGroup( Composite parentTab ) {

    int margin = Const.MARGIN;

    wSettingsGroup = new Group( parentTab, SWT.SHADOW_NONE );
    props.setLook( wSettingsGroup );
    wSettingsGroup.setText( BaseMessages.getString( PKG, "ElasticSearchBulkDialog.SettingsGroup.Label" ) );

    FormLayout settingGroupLayout = new FormLayout();
    settingGroupLayout.marginWidth = 10;
    settingGroupLayout.marginHeight = 10;
    wSettingsGroup.setLayout( settingGroupLayout );


    // BatchSize
    wlBatchSize = new Label( wSettingsGroup, SWT.RIGHT );
    wlBatchSize.setText( BaseMessages.getString( PKG, "ElasticSearchBulkDialog.BatchSize.Label" ) );
    props.setLook( wlBatchSize );

    wBatchSize = new TextVar( transMeta, wSettingsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );

    props.setLook( wBatchSize );
    wBatchSize.addModifyListener( lsMod );

    // Stop on error
    wlStopOnError = new Label( wSettingsGroup, SWT.RIGHT );
    wlStopOnError.setText( BaseMessages.getString( PKG, "ElasticSearchBulkDialog.StopOnError.Label" ) );

    wStopOnError = new Button( wSettingsGroup, SWT.CHECK | SWT.RIGHT );
    wStopOnError.setToolTipText( BaseMessages.getString( PKG, "ElasticSearchBulkDialog.StopOnError.Tooltip" ) );

    // ID input
    wIdInField =
      new LabelComboVar( transMeta, wSettingsGroup,
        BaseMessages.getString( PKG, "ElasticSearchBulkDialog.IdField.Label" ),
        BaseMessages.getString( PKG, "ElasticSearchBulkDialog.IdField.Tooltip" ) );
    props.setLook( wIdInField );
    wIdInField.getComboWidget().setEditable( true );
    wIdInField.addModifyListener( lsMod );
    wIdInField.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        getPreviousFields( wIdInField );
      }
    } );
    getPreviousFields( wIdInField );

    wlIsOverwrite = new Label( wSettingsGroup, SWT.RIGHT );
    wlIsOverwrite.setText( BaseMessages.getString( PKG, "ElasticSearchBulkDialog.Overwrite.Label" ) );

    wIsOverwrite = new Button( wSettingsGroup, SWT.CHECK | SWT.RIGHT );
    wIsOverwrite.setToolTipText( BaseMessages.getString( PKG, "ElasticSearchBulkDialog.Overwrite.Tooltip" ) );

    // Output rows
    wlUseOutput = new Label( wSettingsGroup, SWT.RIGHT );
    wlUseOutput.setText( BaseMessages.getString( PKG, "ElasticSearchBulkDialog.UseOutput.Label" ) );

    wUseOutput = new Button( wSettingsGroup, SWT.CHECK | SWT.RIGHT );
    wUseOutput.setToolTipText( BaseMessages.getString( PKG, "ElasticSearchBulkDialog.UseOutput.Tooltip" ) );

    wUseOutput.addSelectionListener( new SelectionListener() {

      public void widgetDefaultSelected( SelectionEvent arg0 ) {
        widgetSelected( arg0 );
      }

      public void widgetSelected( SelectionEvent arg0 ) {
        wIdOutField.setEnabled( wUseOutput.getSelection() );
      }

    } );

    // ID out field
    wIdOutField =
      new LabelTextVar( transMeta, wSettingsGroup,
        BaseMessages.getString( PKG, "ElasticSearchBulkDialog.IdOutField.Label" ),
        BaseMessages.getString( PKG, "ElasticSearchBulkDialog.IdOutField.Tooltip" ) );
    props.setLook( wIdOutField );
    wIdOutField.setEnabled( wUseOutput.getSelection() );
    wIdOutField.addModifyListener( lsMod );

    // use json
    wlIsJson = new Label( wSettingsGroup, SWT.RIGHT );
    wlIsJson.setText( BaseMessages.getString( PKG, "ElasticSearchBulkDialog.IsJson.Label" ) );

    wIsJson = new Button( wSettingsGroup, SWT.CHECK | SWT.RIGHT );
    wIsJson.setToolTipText( BaseMessages.getString( PKG, "ElasticSearchBulkDialog.IsJson.Tooltip" ) );

    wIsJson.addSelectionListener( new SelectionListener() {
      public void widgetDefaultSelected( SelectionEvent arg0 ) {
        widgetSelected( arg0 );
      }

      public void widgetSelected( SelectionEvent arg0 ) {
        wJsonField.setEnabled( wIsJson.getSelection() );
        wFields.setEnabled( !wIsJson.getSelection() );
        wFields.setVisible( !wIsJson.getSelection() );
        wGet.setEnabled( !wIsJson.getSelection() );
      }
    } );

    // Json field
    wJsonField =
      new LabelComboVar( transMeta, wSettingsGroup,
        BaseMessages.getString( PKG, "ElasticSearchBulkDialog.JsonField.Label" ),
        BaseMessages.getString( PKG, "ElasticSearchBulkDialog.JsonField.Tooltip" ) );
    wJsonField.getComboWidget().setEditable( true );
    props.setLook( wJsonField );
    wJsonField.addModifyListener( lsMod );
    wJsonField.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        getPreviousFields( wJsonField );
      }
    } );
    getPreviousFields( wJsonField );
    wJsonField.setEnabled( wIsJson.getSelection() );

    Control[] settingsControls =
      new Control[]{
        wlBatchSize, wBatchSize, wlStopOnError, wStopOnError, wIdInField, wlIsOverwrite,
        wIsOverwrite, wlUseOutput, wUseOutput, wIdOutField, wlIsJson, wIsJson, wJsonField };
    placeControls( wSettingsGroup, settingsControls );

    fdSettingsGroup = new FormData();
    fdSettingsGroup.left = new FormAttachment( 0, margin );
    fdSettingsGroup.top = new FormAttachment( wIndexGroup, margin );
    fdSettingsGroup.right = new FormAttachment( 100, -margin );
    wSettingsGroup.setLayoutData( fdSettingsGroup );
  }

  private void getPreviousFields( LabelComboVar combo ) {
    String value = combo.getText();
    combo.removeAll();
    combo.setItems( getInputFieldNames() );
    if ( value != null ) {
      combo.setText( value );
    }
  }

  private String[] getInputFieldNames() {
    if ( this.fieldNames == null ) {
      try {
        RowMetaInterface r = transMeta.getPrevStepFields( stepname );
        if ( r != null ) {
          fieldNames = r.getFieldNames();
        }
      } catch ( KettleException ke ) {
        new ErrorDialog( shell,
          BaseMessages.getString( PKG, "ElasticSearchBulkDialog.FailedToGetFields.DialogTitle" ),
          BaseMessages.getString( PKG, "ElasticSearchBulkDialog.FailedToGetFields.DialogMessage" ), ke );
        return new String[0];
      }
    }

    return fieldNames;
  }

  private void getPreviousFields( TableView table ) {
    try {
      RowMetaInterface r = transMeta.getPrevStepFields( stepname );
      if ( r != null ) {
        BaseStepDialog.getFieldsFromPrevious( r, table, 1, new int[]{ 1, 2 }, null, 0, 0, null );
      }
    } catch ( KettleException ke ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Title" ), BaseMessages
        .getString( PKG, "System.Dialog.GetFieldsFailed.Message" ), ke );
    }
  }

  private void placeControls( Group group, Control[] controls ) {

    Control previousAbove = group;
    Control previousLeft = group;

    for ( Control control : controls ) {
      if ( control instanceof Label ) {
        addLabelAfter( control, previousAbove );
        previousLeft = control;
      } else {
        addWidgetAfter( control, previousAbove, previousLeft );
        previousAbove = control;
        previousLeft = group;
      }
    }

  }

  private void addWidgetAfter( Control widget, Control widgetAbove, Control widgetLeft ) {
    props.setLook( widget );
    FormData fData = new FormData();
    fData.left = new FormAttachment( widgetLeft, Const.MARGIN );
    fData.top = new FormAttachment( widgetAbove, Const.MARGIN );
    fData.right = new FormAttachment( 100, -Const.MARGIN );
    widget.setLayoutData( fData );
  }

  private void addLabelAfter( Control widget, Control widgetAbove ) {
    props.setLook( widget );
    FormData fData = new FormData();
    fData.top = new FormAttachment( widgetAbove, Const.MARGIN );
    fData.right = new FormAttachment( Const.MIDDLE_PCT, -Const.MARGIN );
    widget.setLayoutData( fData );
  }

  /**
   * Read the data from the ElasticSearchBulkMeta object and show it in this dialog.
   *
   * @param in The ElasticSearchBulkMeta object to obtain the data from.
   */
  public void getData( ElasticSearchBulkMeta in ) {
    wIndex.setText( Const.NVL( in.getIndex(), "" ) );
    wType.setText( Const.NVL( in.getType(), "" ) );

    wUserName.setText(Const.NVL(in.getUserName(), ""));
    wPassword.setText(Const.NVL(in.getPassword(), ""));

    wConnectTimeout.setText(Const.NVL(""+in.getConnectTimeout(), "" + ElasticSearchBulkMeta.DEFAULT_CONNECT_TIMEOUT));
    wSocketTimeout.setText(Const.NVL(""+in.getSocketTimeout(), "" + ElasticSearchBulkMeta.DEFAULT_SOCKET_TIMEOUT));
    wMaxRetryTimeout.setText(Const.NVL(""+in.getMaxRetryTimeout(), "" + ElasticSearchBulkMeta.DEFAULT_MAX_RETRY_TIMEOUT));

    wBatchSize.setText( Const.NVL( in.getBatchSize(), "" + ElasticSearchBulkMeta.DEFAULT_BATCH_SIZE ) );

    wStopOnError.setSelection( in.isStopOnError() );

    wIdInField.setText( Const.NVL( in.getIdInField(), "" ) );

    wIsOverwrite.setSelection( in.isOverWriteIfSameId() );

    wIsJson.setSelection( in.isJsonInsert() );

    wJsonField.setText( Const.NVL( in.getJsonField(), "" ) );
    wJsonField.setEnabled( wIsJson.getSelection() ); // listener not working here

    wUseOutput.setSelection( in.isUseOutput() );

    wIdOutField.setText( Const.NVL( in.getIdOutField(), "" ) );
    wIdOutField.setEnabled( wUseOutput.getSelection() ); // listener not working here

    // Fields
    mapToTableView( model.getFieldsMap(), wFields );

    // Servers
    for ( ElasticSearchBulkMeta.Server server : model.getServers() ) {
      wServers.add( server.address, "" + server.port );
    }
    wServers.removeEmptyRows();
    wServers.setRowNums();

    // Settings
    mapToTableView( model.getSettingsMap(), wSettings );

    wStepname.selectAll();
    wStepname.setFocus();
  }

  private void mapToTableView( Map<String, String> map, TableView table ) {
    for ( String key : map.keySet() ) {
      table.add( key, map.get( key ) );
    }
    table.removeEmptyRows();
    table.setRowNums();
  }

  private void cancel() {
    stepname = null;
    model.setChanged( changed );
    dispose();
  }

  private void ok() {
    toModel( model );
    dispose();
  }

  private void toModel( ElasticSearchBulkMeta in ) { // copy info to ElasticSearchBulkMeta
    stepname = wStepname.getText();

    in.setType( wType.getText() );
    in.setIndex( wIndex.getText() );

    in.setBatchSize( wBatchSize.getText() );

    in.setIdInField( wIdInField.getText() );
    //in.setOverWriteIfSameId( StringUtils.isNotBlank( wIdInField.getText() ) && wIsOverwrite.getSelection() );
    in.setStopOnError( wStopOnError.getSelection() );
    in.setJsonInsert( wIsJson.getSelection() );
    in.setJsonField( wIsJson.getSelection() ? wJsonField.getText() : null );
    in.setIdOutField( wIdOutField.getText() );
    in.setUseOutput( wUseOutput.getSelection() );

    in.setUserName(wUserName.getText());
    in.setPassword(wPassword.getText());

    in.setConnectTimeout(wConnectTimeout.getText());
    in.setSocketTimeout(wSocketTimeout.getText());
    in.setMaxRetryTimeout(wMaxRetryTimeout.getText());

    in.clearFields();
    if ( !wIsJson.getSelection() ) {
      for ( int i = 0; i < wFields.getItemCount(); i++ ) {
        String[] row = wFields.getItem( i );
        if ( StringUtils.isNotBlank( row[0] ) ) {
          in.addField( row[0], row[1] );
        }
      }
    }

    in.clearServers();
    for ( int i = 0; i < wServers.getItemCount(); i++ ) {
      String[] row = wServers.getItem( i );
      if ( StringUtils.isNotBlank( row[0] ) ) {
        try {
          in.addServer( row[0], Integer.parseInt( row[1] ) );
        } catch ( NumberFormatException nfe ) {
          in.addServer( row[0], ElasticSearchBulkMeta.DEFAULT_PORT );
        }
      }
    }

    in.clearSettings();
    for ( int i = 0; i < wSettings.getItemCount(); i++ ) {
      String[] row = wSettings.getItem( i );
      in.addSetting( row[0], row[1] );
    }
  }


  private void testAll( ) {

    // Save off the thread's context class loader to restore after the test
    ClassLoader originalClassloader = Thread.currentThread().getContextClassLoader();
    // Now ensure that the thread's context class loader is the plugin's classloader
    Thread.currentThread().setContextClassLoader( this.getClass().getClassLoader() );

    ElasticSearchBulkMeta tempMeta = new ElasticSearchBulkMeta();
    toModel( tempMeta );
    if ( !tempMeta.getServers().isEmpty() ) {
      List<ElasticSearchBulkMeta.Server> servers = tempMeta.getServers();
      HttpHost[] hosts = new HttpHost[servers.size()];
      for ( int i=0; i<hosts.length; i++ ) {
        String res = testOne(servers.get(i).address, servers.get(i).port);
        wServers.setText(res, 3,  i);
      }
    }
    // Restore the original classloader
    Thread.currentThread().setContextClassLoader( originalClassloader );
  }

  private String testOne(String host, int port) {
    RestClient restClient;
    userName = wUserName.getText();
    password = wPassword.getText();

    try {
      connectTimeout = Integer.parseInt(wConnectTimeout.getText());
      socketTimeout  = Integer.parseInt(wSocketTimeout.getText());
      maxRetryTimeout = Integer.parseInt(wMaxRetryTimeout.getText());
    }catch(NumberFormatException e) {

    }
    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));
    RestClientBuilder builder = RestClient.builder(new HttpHost(host, port))
            .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
              @Override
              public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
              }
            });
    RestClientBuilder.RequestConfigCallback requestConfigCallback = new RestClientBuilder.RequestConfigCallback() {
      @Override
      public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
        return requestConfigBuilder.setConnectTimeout(connectTimeout).setSocketTimeout(socketTimeout);
      }
    };
    builder.setRequestConfigCallback(requestConfigCallback);
    builder.setMaxRetryTimeoutMillis(maxRetryTimeout);
    restClient = builder.build();
    try {
      Response response = restClient.performRequest("GET", "/");
      if(response.getStatusLine().getStatusCode() == 200) {
         return "OK";
      }
      restClient.close();

    } catch (IOException e) {
        return e.getMessage();
      //e.printStackTrace();
    }
    return "Unknown Error";
  }

  private void showError( String message ) {
    MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
    mb.setMessage( message );
    mb.setText( BaseMessages.getString( PKG, "System.Dialog.Error.Title" ) );
    mb.open();
  }

  private void showMessage( String message ) {
    MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_INFORMATION );
    mb.setMessage( message );
    mb.setText( BaseMessages.getString( PKG, "ElasticSearchBulkDialog.Test.TestOKTitle" ) );
    mb.open();
  }

  @Override
  public String toString() {
    return this.getClass().getName();
  }
}
