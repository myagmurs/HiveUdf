package com.hive.udtf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class ListProcessorUdtf extends GenericUDTF {

	private transient ObjectInspector inputOI = null;

	private transient final Object[] forwardListObj = new Object[2];

	@Override
	public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {

		if (args.length != 1) {
			throw new UDFArgumentException("this udtf takes only one argument");
		}

		ArrayList<String> fieldNames = new ArrayList<String>();
		ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

		inputOI = args[0];
		fieldNames.add("value");
		fieldOIs.add(((ListObjectInspector) inputOI).getListElementObjectInspector());

		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	}

	@Override
	public void process(Object[] args) throws HiveException {
		ListObjectInspector listOI = (ListObjectInspector) inputOI;
		List<?> list = listOI.getList(args[0]);
		if (list == null) {
			return;
		}
		for (Object r : list) {
			forwardListObj[0] = r;
			forward(forwardListObj);
		}
	}

	@Override
	public void close() throws HiveException {

	}

}

