package org.pentaho.big.data.kettle.plugins.druid.input.output.output;

import org.apache.http.HttpEntity;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Title: 步骤�?
 * @Package plugin.template
 * @Description: TODO(插件导出)
 * @author http://www.Teld.cn
 * @date 2010-8-8 下午05:10:26
 * @version V1.0
 */

public class DruidOutputStep extends BaseStep implements StepInterface {

	private DruidOutputStepData data;
	private DruidOutputStepMeta meta;

	public DruidOutputStep(StepMeta s, StepDataInterface stepDataInterface, int c, TransMeta t, Trans dis) {
		super(s, stepDataInterface, c, t, dis);

	}

	public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
		meta = (DruidOutputStepMeta) smi;
		data = (DruidOutputStepData) sdi;

		super.dispose(smi, sdi);
	}

	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
		meta = (DruidOutputStepMeta) smi;
		data = (DruidOutputStepData) sdi;
		String URL = meta.getOutputField();
		String table = meta.getOutputvalue();
		String stime = meta.getOutputSTime();
		String etime = meta.getOutputETime();
		String utcif = Integer.valueOf(meta.getOutputUtcOf())*60+"";
	   
		String gojson = "{\n" + "   \"queryType\": \"select\",\n" + "   \"dataSource\": \"HATest\",\n"
				+ "   \"granularity\": \"all\",\n" + "   \"intervals\": [\n" + "     \"2017-05-01/2017-05-02\"\n"
				+ "   ],\n" + "   \"pagingSpec\":{\"pagingIdentifiers\": {}, \"threshold\":1000000000}   \n" + "}";
		JSONObject jsStr = (JSONObject) JSONObject.parse(gojson); // 将字符串{“id”：1}
		jsStr.put("dataSource", table);
		jsStr.put("intervals", ortime(stime) + "/" + ortime(etime));
		String getTime = jsStr.get("intervals").toString()
				.replace("\"", "").split("/")[0];
		String UTF_8 = getCutTime(getTime, utcif);

		String endTime = jsStr.get("intervals").toString()
				.replace("\"", "").split("/")[1];

		String end_UTF_8 = getCutTime(endTime, utcif);
		if (first) {
			first = false;
			// data.outputRowMeta = (RowMetaInterface) getInputRowMeta().clone();
			// //只能在getrow之后再用 ming
			data.outputRowMeta = new RowMeta();
			meta.getFields(data.outputRowMeta, getStepname(), null, null, this);
			logBasic("template step initialized successfully");
		}
		Ontime(UTF_8, end_UTF_8, jsStr.toJSONString(), URL, data.outputRowMeta);
		setOutputDone();
		return false;
	}
	public String ortime(String stime) {
		String year = split_string(stime)[2];
		String day = split_string(stime)[1];
		String Month = split_string(stime)[0];
		stime = year + "-" + Month + "-" + day+"T00:00:00";
		return stime;

	}
	public String[] split_string(String stime) {
		String[] sstime = stime.split("/");
		return sstime;

	}
	public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
		meta = (DruidOutputStepMeta) smi;
		data = (DruidOutputStepData) sdi;
		return super.init(smi, sdi);

	}

	public void stop() {
		dispose(meta, data);
		logBasic("Finished, processing " + getLinesRead() + " rows");
		markStop();
	}

	// Run is were the action happens!
	public void run() {
		logBasic("Starting to run...");
		try {
			processRow(meta, data);
		} catch (Exception e) {
			logError("Unexpected error : " + e.toString());
			logError(Const.getStackTracker(e));
			setErrors(1);
			stopAll();
		} finally {
			dispose(meta, data);
			logBasic("Finished, processing " + getLinesRead() + " rows");
			markStop();
		}
	}

	public static Date strToDateLong(String strDate) {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		ParsePosition pos = new ParsePosition(0);
		Date strtodate = formatter.parse(strDate, pos);
		return strtodate;
	}

	public static String getPreTime(String sj1, String jj) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		String mydate1 = "";
		try {
			Date date1 = format.parse(sj1);
			long Time = (date1.getTime() / 1000) + Integer.parseInt(jj) * 60;
			date1.setTime(Time * 1000);
			mydate1 = format.format(date1);
		} catch (Exception e) {
		}
		return mydate1;
	}

	public static String getCutTime(String sj1, String jj) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		String mydate1 = "";
		try {
			Date date1 = format.parse(sj1);
			long Time = (date1.getTime() / 1000) - Integer.parseInt(jj) * 60;
			date1.setTime(Time * 1000);
			mydate1 = format.format(date1);
		} catch (Exception e) {
		}
		return mydate1;
	}

	public static String doHttpPost(String xmlInfo, String URL) {
		System.out.println("发起的数据:" + xmlInfo);
		byte[] xmlData = xmlInfo.getBytes();
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

	public void Ontime(String UTF_8, String end_UTF_8, String jsonc, String URL, RowMetaInterface outputRowMeta) {
		int maxThread = Thread.activeCount() + 4;
		while (true) {
			if (strToDateLong(UTF_8).getTime() < strToDateLong(end_UTF_8).getTime()) {
				UTF_8 = getPreTime(UTF_8, "1");
				String UTF_8_1 = getCutTime(UTF_8, "1");

				String query_time = "{" + "\"" + "intervals" + "\"" + ":" + "[" + "\"" + UTF_8_1 + "/" + UTF_8 + "\""
						+ "]" + "}";
				JSONObject test1 = (JSONObject) JSON.parse(query_time);
				JSONObject jsStr = (JSONObject) JSONObject.parse(jsonc);
				jsStr.put("intervals", test1.get("intervals"));

				try {
					while (Thread.activeCount() > maxThread) {
						Thread.sleep(200);
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				try {
					AsyncPost_BatchInputSize(JSON.toJSONString(jsStr), URL);
				} catch (Exception e) {
					e.printStackTrace();
				}
			} else {
				if (Thread.activeCount() <= (maxThread - 4)) {
					try {
						Thread thread = new Thread();
						if (thread.isAlive()) {
							Thread.sleep(1000);
						}
						;

					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					break;
				}

			}
		}

	}

	public void AsyncPost_BatchInputSize(String c_json, String c_url) {
		Runnable task = new Runnable() {
			@Override
			public void run() {
				try {
					Stored_procedure(c_json, c_url);
				} catch (Exception e) {
					e.printStackTrace();
					try {
						Thread.sleep(50);
						System.out.println("出现问题，已经等待了0.05秒钟，尝试重新请求" + c_json);
						Stored_procedure(c_json, c_url);
					} catch (KettleStepException | InterruptedException e1) {
						e1.printStackTrace();
					}
				}
			}
		};
		Thread thread = new Thread(task);
		thread.start();
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	public void Stored_procedure(String c_json, String c_url) throws KettleStepException {

		Object[] outputRow = new Object[meta.getOutputList().length];

		String postResult = doHttpPost(c_json, c_url);
		JSONArray test_json = (JSONArray) JSON.parse(postResult);
		JSONArray jieg = test_json.getJSONObject(0).getJSONObject("result").getJSONArray("events");
		Object[] temp = jieg.getJSONObject(0).getJSONObject("event").keySet().toArray();
		for (int i = 0; i < jieg.size(); i++) {
			for (int j = 0; j < outputRow.length; j++) {
				outputRow[j] = jieg.getJSONObject(i).getJSONObject("event").get(temp[j]);
			}
			data.outputRowMeta = new RowMeta();
			meta.getFields(data.outputRowMeta, getStepname(), null, null, this);
			putRow(data.outputRowMeta, outputRow);

		}
	}

}
