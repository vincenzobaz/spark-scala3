package scala3udf;

import java.lang.reflect.InvocationTargetException;

public class UdfHelper {
	public static Object createUdf(Object f, Object dataType, Object inputEncoders, Object outputEncoder, Object name,
			boolean nullable, boolean deterministic) throws ClassNotFoundException, InstantiationException,
			IllegalAccessException, IllegalArgumentException, InvocationTargetException, SecurityException {
		final Class<?> clz = Class.forName("org.apache.spark.sql.expressions.SparkUserDefinedFunction");
		return clz.getDeclaredConstructors()[0].newInstance(f, dataType, inputEncoders, outputEncoder, name, nullable,
				deterministic);
	}

}
