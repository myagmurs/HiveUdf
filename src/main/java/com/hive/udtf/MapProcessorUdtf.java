package com.hive.udtf;

import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class MapProcessorUdtf extends GenericUDTF {

	private transient ObjectInspector inputOI = null;

	private transient final Object[] forwardMapObj = new Object[2];

	@Override
	public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {

		if (args.length != 1) {
			throw new UDFArgumentException("this udtf takes only one argument");
		}

		ArrayList<String> fieldNames = new ArrayList<String>();
		ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

		inputOI = args[0];
		fieldNames.add("hotel");
		fieldNames.add("value");
		fieldOIs.add(((MapObjectInspector) inputOI).getMapKeyObjectInspector());
		fieldOIs.add(((MapObjectInspector) inputOI).getMapValueObjectInspector());

		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	}

	@Override
	public void process(Object[] args) throws HiveException {
		MapObjectInspector mapOI = (MapObjectInspector) inputOI;
		Map<?, ?> map = mapOI.getMap(args[0]);
		if (map == null) {
			return;
		}
		for (Entry<?, ?> r : map.entrySet()) {
			forwardMapObj[0] = r.getKey();
			forwardMapObj[1] = r.getValue();
			forward(forwardMapObj);
		}
	}

	@Override
	public void close() throws HiveException {

	}

}
