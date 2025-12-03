package org.pentaho.big.data.kettle.plugins.druid.input.output.output;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.http.HttpEntity;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.DateTime;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;

/**
 * @Title: 对话框类
 * @Package plugin.template
 * @Description: TODO(用一句话描述该文件做什么)
 * @author http://www.ahuoo.com
 * @date 2010-8-8 下午05:10:26
 * @version V1.0
 */

public class DruidOutputStepDialog extends BaseStepDialog implements StepDialogInterface {

	private static Class<?> PKG = DruidOutputStepMeta.class; // for i18n purposes

	private DruidOutputStepMeta input;

	// output field name
	private Text text;
	private Label lbljson;
	private Table table;
	private Text UTC_Offset;
	private Text Druid_Table;
	private DateTime dateTime;
	private DateTime dateTime_1;

	public DruidOutputStepDialog(Shell parent, Object in, TransMeta transMeta, String sname) {
		super(parent, (BaseStepMeta) in, transMeta, sname);
		input = (DruidOutputStepMeta) in;
	}

	public String open() {
		Display display = Display.getDefault();

		shell = new Shell();
		// head title
		shell.setSize(425, 518);
		shell.setText("SWT Application");

		// 输入url的文字
		Label lblNewLabel = new Label(shell, SWT.NONE);
		lblNewLabel.setBounds(10, 10, 61, 17);
		lblNewLabel.setText("http Druid");
		props.setLook(lblNewLabel);

		// url input
		text = new Text(shell, SWT.BORDER);
		text.setBounds(77, 7, 320, 23);
		props.setLook(text);

		// 输入的文字
		lbljson = new Label(shell, SWT.NONE);
		lbljson.setBounds(10, 47, 61, 17);
		lbljson.setText("Druid_Table");
		props.setLook(lbljson);

		// table inout
		Druid_Table = new Text(shell, SWT.BORDER);
		Druid_Table.setBounds(77, 44, 159, 23);
		props.setLook(Druid_Table);

		// time space
		dateTime = new DateTime(shell, SWT.BORDER);
		dateTime.setBounds(77, 87, 88, 24);
		props.setLook(dateTime);

		// time space
		dateTime_1 = new DateTime(shell, SWT.BORDER);
		dateTime_1.setBounds(269, 87, 88, 24);
		props.setLook(dateTime_1);

		// start time
		Label lblStarttime = new Label(shell, SWT.NONE);
		lblStarttime.setBounds(10, 94, 61, 17);
		lblStarttime.setText("Start_Time");

		// end time
		Label lblEndtime = new Label(shell, SWT.NONE);
		lblEndtime.setBounds(210, 94, 52, 17);
		lblEndtime.setText("End_time");

		// 偏移时区
		Label lblUtc = new Label(shell, SWT.NONE);
		lblUtc.setBounds(10, 132, 76, 17);
		lblUtc.setText("UTC_Offset");

		// 偏移输入框
		UTC_Offset = new Text(shell, SWT.BORDER);
		UTC_Offset.setBounds(92, 129, 73, 23);
		props.setLook(UTC_Offset);

		// 获取元数据的按钮
		Button btnTest = new Button(shell, SWT.NONE);
		btnTest.setBounds(10, 161, 61, 27);
		btnTest.setText("G_metaD");
		// props.setLook(btnTest);

		// 显示表格的文字
		Label label = new Label(shell, SWT.NONE);
		label.setBounds(10, 224, 61, 17);
		label.setText("view data");
		// props.setLook(label);
		// 多行文本输入框

		// 显示的表格
		table = new Table(shell, SWT.MULTI | SWT.FULL_SELECTION | SWT.BORDER);
		table.setBounds(77, 158, 320, 249);
		table.setHeaderVisible(true);
		table.setLinesVisible(true);

		// 每一列
		TableColumn idColumn = new TableColumn(table, SWT.LEFT);
		idColumn.setText("Id");
		idColumn.setWidth(43);
		TableColumn usernameColumn = new TableColumn(table, SWT.LEFT);
		usernameColumn.setText("Key");
		usernameColumn.setWidth(63);
		TableColumn passwordColumn = new TableColumn(table, SWT.LEFT);
		passwordColumn.setText("Value");
		passwordColumn.setWidth(207);

		// props.setLook(table);
		// 尾部信息

		Label lblNewLabel_1 = new Label(shell, SWT.NONE);
		lblNewLabel_1.setBounds(124, 458, 179, 17);
		lblNewLabel_1.setText("BigData-Druid-Export");

		// OK and cancel buttons
		wOK = new Button(shell, SWT.PUSH);
		wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));
		wOK.setBounds(100, 423, 80, 27);
		props.setLook(wOK);
		wCancel = new Button(shell, SWT.PUSH);
		wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
		wCancel.setBounds(223, 423, 80, 27);
		props.setLook(wCancel);

		// test

		btnTest.addSelectionListener(new SelectionListener() {
			@Override
			public void widgetSelected(SelectionEvent arg0) {

				String URL = text.getText();
				String xmlInfo = gojson(Druid_Table.getText(),replace_string(dateTime.toString()), replace_string(dateTime_1.toString())).toJSONString();
				JSONObject jsStr = (JSONObject) JSONObject.parse(xmlInfo);
				JSONObject pagingSpec_content = jsStr.getJSONObject("pagingSpec");
				pagingSpec_content.put("threshold", "1");
				jsStr.put("pagingSpec", pagingSpec_content);
				String selectM = doHttpPost(JSON.toJSONString(jsStr), URL);
				JSONArray test_json = (JSONArray) JSON.parse(selectM);
				JSONObject jieg = test_json.getJSONObject(0).getJSONObject("result").getJSONArray("events")
						.getJSONObject(0).getJSONObject("event");
				table.removeAll();
				for (int i = 0; i < jieg.keySet().size(); i++) {
					TableItem item = new TableItem(table, SWT.LEFT);
					String[] valueStrings = { "" + i, (String) jieg.keySet().toArray()[i],
							jieg.getString((String) jieg.keySet().toArray()[i]) };

					item.setText(valueStrings);
				}
			}

			@Override
			public void widgetDefaultSelected(SelectionEvent arg0) {

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
			public void widgetDefaultSelected(SelectionEvent e) {
				ok();
			}
		};

		// 设置监听后就保存
		text.addSelectionListener(lsDef);
		Druid_Table.addSelectionListener(lsDef);
		dateTime.addSelectionListener(lsDef);
		dateTime_1.addSelectionListener(lsDef);
		UTC_Offset.addSelectionListener(lsDef);

		// Detect X or ALT-F4 or something that kills this window...
		shell.addShellListener(new ShellAdapter() {
			public void shellClosed(ShellEvent e) {
				cancel();
			}
		});

		// Set the shell size, based upon previous time...
		// setSize();

		getData();
		input.setChanged(changed);

		shell.open();
		shell.layout();
		while (!shell.isDisposed()) {
			if (!display.readAndDispatch())
				display.sleep();
		}
		return stepname;
	}

	// Read data and place it in the dialog
	public void getData() {
		// wStepname.selectAll();
		text.setText(input.getOutputField());
		Druid_Table.setText(input.getOutputvalue());
		dateTime.setYear(Integer.valueOf(split_string(input.getOutputSTime())[2]));
		dateTime.setMonth(Integer.valueOf(split_string(input.getOutputSTime())[0]) - 1);
		dateTime.setDay(Integer.valueOf(split_string(input.getOutputSTime())[1]));

		dateTime_1.setYear(Integer.valueOf(split_string(input.getOutputETime())[2]));
		dateTime_1.setMonth(Integer.valueOf(split_string(input.getOutputETime())[0]) - 1);
		dateTime_1.setDay(Integer.valueOf(split_string(input.getOutputETime())[1]));

		UTC_Offset.setText(input.getOutputUtcOf());
		if (input.getOutputList().length > 2) {

			for (int row = 0; row < input.getOutputList().length; row++) {
				TableItem item = new TableItem(table, SWT.LEFT);
				String[] valueStrings = { input.getOutputList()[row][0].toString(),
						input.getOutputList()[row][1].toString(), input.getOutputList()[row][2].toString() };
				item.setText(valueStrings);
			}
			;
		} else {
			System.out.println(input.getOutputList().length);

		}
	}

	private void cancel() {
		stepname = null;
		input.setChanged(changed);
		dispose();
	}

	// let the plugin know about the entered data
	private void ok() {
		// stepname = wStepname.getText(); // return value
		input.setOutputField(text.getText());
		input.setOutputvalue(Druid_Table.getText());
		input.setOutputSTime(replace_string(dateTime.toString()));
		input.setOutputETime(replace_string(dateTime_1.toString()));
		input.setOutputUtcOf(UTC_Offset.getText());

		TableItem[] items = table.getItems();
		String outputList[][] = new String[items.length][table.getColumnCount()];
		for (int i = 0; i < items.length; i++) {
			for (int j = 0; j < table.getColumnCount(); j++)
				outputList[i][j] = items[i].getText(j);
		}
		input.setOutputList(outputList);
		dispose();
	}

	public static String doHttpPost(String xmlInfo, String URL) {
		System.out.println("发起的数据:" + xmlInfo);

		final String CONTENT_TYPE_TEXT_JSON = "application/json";
		CloseableHttpClient client = HttpClients.createDefault();

		HttpPost httpPost = new HttpPost(URL);
		httpPost.setHeader("Content-Type", "application/json;charset=UTF-8");

		StringEntity se = null;
		try {
			se = new StringEntity(xmlInfo);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		se.setContentType(CONTENT_TYPE_TEXT_JSON);

		httpPost.setEntity(se);

		CloseableHttpResponse response2 = null;

		try {
			response2 = client.execute(httpPost);
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		HttpEntity entity2 = null;
		entity2 = response2.getEntity();
		String s2 = null;
		try {
			s2 = EntityUtils.toString(entity2, "UTF-8");
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return s2;
	}

	public String replace_string(String stime) {
		stime = stime.replace("DateTime {", "").replace("}", "");
		return stime;

	}

	public String ortime(String stime) {
		String year = split_string(stime)[2];
		String day = split_string(stime)[1];
		String Month = split_string(stime)[0];
		stime = year + "-" + Month + "-" + day;
		return stime;

	}
	
//	//限制查询性能  只是为了前端不崩溃  并且可以显示元数据 跟流程无关
//	public String or1time(String stime) {
//		String year = split_string(stime)[2];
//		String day = (Integer.valueOf(split_string(stime)[1])+1)+"";
//		String Month = split_string(stime)[0];
//		stime = year + "-" + Month + "-" + day;
//		return stime;
//
//	}

	public String[] split_string(String stime) {
		String[] sstime = stime.split("/");
		return sstime;

	}

	public JSONObject gojson(String tables, String stime, String etime) {
		String gojson = "{\n" + "   \"queryType\": \"select\",\n" + "   \"dataSource\": \"HATest\",\n"
				+ "   \"granularity\": \"all\",\n" + "   \"intervals\": [\n" + "     \"2017-05-01/2017-05-02\"\n"
				+ "   ],\n" + "   \"pagingSpec\":{\"pagingIdentifiers\": {}, \"threshold\":1}   \n" + "}";
		JSONObject jsStr = (JSONObject) JSONObject.parse(gojson); // 将字符串{“id”：1}
		jsStr.put("dataSource", Druid_Table.getText());
		jsStr.put("intervals", ortime(stime) + "/" + ortime(etime));
		return jsStr;
	}

}
