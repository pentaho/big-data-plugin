package org.pentaho.big.data.kettle.plugins.druid.input.output.output;

import java.util.List;
import java.util.Map;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.*;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.*;
import org.pentaho.di.core.row.*;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.*;
import org.pentaho.di.trans.*;
import org.pentaho.di.trans.step.*;
import org.w3c.dom.Node;
import org.pentaho.di.core.annotations.Step;

/**
 * @Title: 元数据类
 * @Package plugin.template
 * @Description: TODO(用一句话描述该文件做什么)
 * @author http://www.ahuoo.com
 * @date 2010-8-8 下午05:10:26
 * @version V1.0
 */
@Step(id = "DruidOutput", image = "druido.png", name = "DruidOutput.Name", description = "DruidOutput.Description",
		categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.BigData",
		documentationUrl = "http://wiki.pentaho.com/display/EAI/Druid+Output",
		i18nPackageName = "org.pentaho.di.trans.steps.druidoutput")
public class DruidOutputStepMeta extends BaseStepMeta implements StepMetaInterface {

	private static Class<?> PKG = DruidOutputStepMeta.class; // for i18n purposes
	private String outputField = "url";
	private String outputValue = "table";
	private String outputSTime = "str";
	private String outputETime = "estr";
	private String outputUtcOf = "0";
	private String outputList[][] = new String[1][3];

	// private String[] outputList = { "" + 1, (String) "test", (String) "test" };

	public String getOutputField() {
		return outputField;
	}

	public String getOutputUtcOf() {
		return outputUtcOf;
	}

	public void setOutputUtcOf(String outputUtcOf) {
		this.outputUtcOf = outputUtcOf;
	}

	public String getOutputValue() {
		return outputValue;
	}

	public void setOutputValue(String outputValue) {
		this.outputValue = outputValue;
	}

	public String getOutputSTime() {
		return outputSTime;
	}

	public void setOutputSTime(String outputSTime) {
		this.outputSTime = outputSTime;
	}

	public String getOutputETime() {
		return outputETime;
	}

	public void setOutputETime(String outputETime) {
		this.outputETime = outputETime;
	}

	public void setOutputField(String outputField) {
		this.outputField = outputField;
	}

	public String getOutputvalue() {
		return outputValue;
	}

	public void setOutputvalue(String outputvalue) {
		this.outputValue = outputvalue;
	}

	public String[][] getOutputList() {
		return outputList;
	}

	public void setOutputList(String[][] outputList) {
		this.outputList = outputList;
	}

	public String getXML() throws KettleValueException {
		String retval = "";
		retval += "		<outputfield>" + getOutputField() + "</outputfield>" + Const.CR;
		retval += "		<outputvalue>" + getOutputvalue() + "</outputvalue>" + Const.CR;
		retval += "		<outputstime>" + getOutputSTime() + "</outputstime>" + Const.CR;
		retval += "		<outputetime>" + getOutputETime() + "</outputetime>" + Const.CR;
		retval += "		<outpututcof>" + getOutputUtcOf() + "</outpututcof>" + Const.CR;
		int row = getRow(getOutputList());
		int col = getCol(getOutputList());
		String str = convertToString(getOutputList(), row, col);
		retval += "		<outputrow>" + row + "</outputrow>" + Const.CR;
		retval += "		<outputcol>" + col + "</outputcol>" + Const.CR;
		retval += "		<outputList>" + str + "</outputList>" + Const.CR;

		return retval;
	}

	public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep,
			VariableSpace space) {

		ValueMetaInterface v0 = null;

		for (int i = 0; i < outputList.length; i++) {
			v0 = new ValueMeta();
			v0.setName(outputList[i][1]);
			v0.setType(ValueMeta.TYPE_STRING);
			v0.setTrimType(ValueMeta.TRIM_TYPE_BOTH);
			v0.setOrigin(origin);
			r.addValueMeta(v0);
		}
	}

	public Object clone() {
		Object retval = super.clone();
		return retval;
	}

	public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters)
			throws KettleXMLException {

		try {
			setOutputField(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, "outputfield")));
			setOutputvalue(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, "outputvalue")));
			setOutputSTime(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, "outputstime")));
			setOutputETime(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, "outputetime")));
			setOutputUtcOf(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, "outpututcof")));

			String temp = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, "outputlist"));
			String row = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, "outputrow"));
			String col = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, "outputcol"));
			String[][] arrayConvert = new String[Integer.parseInt(row)][Integer.parseInt(col)];
			arrayConvert = convertToArray(temp, Integer.parseInt(row), Integer.parseInt(col));
			setOutputList(arrayConvert);
		} catch (Exception e) {
			throw new KettleXMLException("Template Plugin Unable to read step info from XML node", e);
		}

	}

	public void setDefault() {
		outputField = "http://xxxx:8082/druid/v2/?pretty";
		outputValue = "HATest";
		outputList[0][0] = "" + 1;
		outputList[0][1] = "" + "test";
		outputList[0][2] = "" + "test1";
		
		outputSTime = "9/29/2017";
		outputETime = "9/29/2017";
		outputUtcOf = "0";
	}

	public void check(List<CheckResultInterface> remarks, TransMeta transmeta, StepMeta stepMeta, RowMetaInterface prev,
			String input[], String output[], RowMetaInterface info) {
		CheckResult cr;

		// See if we have input streams leading to this step!
		if (input.length > 0) {
			cr = new CheckResult(CheckResult.TYPE_RESULT_OK, "Step is receiving info from other steps.", stepMeta);
			remarks.add(cr);
		} else {
			cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, "No input received from other steps!", stepMeta);
			remarks.add(cr);
		}

	}

	public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String name) {
		return new DruidOutputStepDialog(shell, meta, transMeta, name);
	}

	public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
			Trans disp) {
		return new DruidOutputStep(stepMeta, stepDataInterface, cnr, transMeta, disp);
	}

	public StepDataInterface getStepData() {
		return new DruidOutputStepData();
	}

	public void readRep(Repository rep, ObjectId id_step, List<DatabaseMeta> databases, Map<String, Counter> counters)
			throws KettleException {
		try {
			outputField = rep.getStepAttributeString(id_step, "outputfield"); //$NON-NLS-1$
			outputValue = rep.getStepAttributeString(id_step, "outputvalue"); //$NON-NLS-1$
		} catch (Exception e) {
			throw new KettleException(
					BaseMessages.getString(PKG, "TemplateStep.Exception.UnexpectedErrorInReadingStepInfo"), e);
		}
	}

	public void saveRep(Repository rep, ObjectId id_transformation, ObjectId id_step) throws KettleException {
		try {
			rep.saveStepAttribute(id_transformation, id_step, "outputfield", outputField); //$NON-NLS-1$
			rep.saveStepAttribute(id_transformation, id_step, "outputvalue", outputValue); //$NON-NLS-1$
		} catch (Exception e) {
			throw new KettleException(
					BaseMessages.getString(PKG, "TemplateStep.Exception.UnableToSaveStepInfoToRepository") + id_step,
					e);
		}
	}

	// -------序列化组件-----------
	public int getRow(String[][] array) {
		int row = 0;
		if (array != null) {
			row = array.length; // 行
		}
		return row;
	}

	public int getCol(String[][] array) {
		int col = 0;
		if (array != null) {
			col = array[0].length; // 列
		}
		return col;
	}

	public String convertToString(String[][] array, int row, int col) {
		String str = "";
		String tempStr = null;
		for (int i = 0; i < row; i++) {
			for (int j = 0; j < col; j++) {
				tempStr = String.valueOf(array[i][j]);
				str = str + tempStr + ",";
			}
		}
		return str;
	}

	public String[][] convertToArray(String str, int row, int col) {
		String[][] arrayConvert = new String[row][col];
		int count = 0;
		String[] strArray = str.split(",");
		for (int i = 0; i < row; i++) {
			for (int j = 0; j < col; j++) {
				arrayConvert[i][j] = (String) strArray[count];
				++count;
			}
		}
		return arrayConvert;
	}
}
