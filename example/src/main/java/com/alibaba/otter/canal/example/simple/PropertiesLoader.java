package com.alibaba.otter.canal.example.simple;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Properties;

/**
 * By neo.chen{neocxf@gmail.com} on 2017/10/10.
 */
@Slf4j
public final class PropertiesLoader {

	/**
	 * load the properties file into a properties
	 *
	 * @param fileName the properties file name, should ends with '.properties'
	 * @return the constructed properties
	 */
	public static Properties loadProps(String fileName) {
		if (!fileName.endsWith(".properties"))
			throw new IllegalArgumentException(String.format("must supply a properties file, but found %s.", fileName));

		InputStream inputStream = PropertiesLoader.class.getClassLoader().getResourceAsStream(fileName);

		Properties properties = new Properties();

		try {
			properties.load(inputStream);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return properties;
	}

	/**
	 * map the properties to the related Java bean
	 *
	 * @param fileName properties file name
	 * @param prefix   property prefix
	 * @param clazz    config bean Class
	 * @param <T>      the config bean type
	 * @return instance of config bean
	 */
	public static <T> T loadForConfigBean(String fileName, String prefix, Class<T> clazz) {
		final String _prefix = prefix.trim();

		Properties properties = loadProps(fileName);

		T result = null;

		try {

			final T configBean = clazz.newInstance();

			properties.stringPropertyNames()
				.stream()
				.filter(key -> key.startsWith(_prefix))
				.map(key -> key.substring(_prefix.length() + 1))
				.forEach(_property -> {
					try {
						Field field = clazz.getDeclaredField(_property);
						field.setAccessible(true);

						String property = "";

						if ("".equals(_prefix)) {
							property = _property;
						} else {
							property = _prefix + "." + _property;
						}

						log.trace(String.format("field %s\'s type is %s", property, field.getType()));

						String fieldValStr = properties.getProperty(property);

						Object fieldVal = castPrimitive(field, fieldValStr);

						field.set(configBean, fieldVal);

					} catch (NoSuchFieldException | IllegalAccessException e) {
						if (e instanceof NoSuchFieldException) {
							log.error(String.format("can not find field of [%s] in class %s", _property, clazz.getName()));
						} else {
							e.printStackTrace();
						}
					}
				});

			result = configBean;

		} catch (IllegalAccessException | InstantiationException e) {
			e.printStackTrace();
		}

		return result;
	}

	/**
	 * cast the string representation to the target val
	 *
	 * @param field       field
	 * @param fieldValStr string representation
	 * @return the object
	 */
	public static Object castPrimitive(Field field, String fieldValStr) {
		Class type = field.getType();

		Object fieldVal;

		if (type.equals(byte.class) || type.equals(Byte.class)) {
			fieldVal = Byte.valueOf(fieldValStr);
		} else if (type.equals(long.class) || type.equals(Long.class)) {
			fieldVal = Long.valueOf(fieldValStr);
		} else if (type.equals(int.class) || type.equals(Integer.class)) {
			fieldVal = Integer.valueOf(fieldValStr);
		} else if (type.equals(boolean.class) || type.equals(Boolean.class)) {
			fieldVal = Boolean.valueOf(fieldValStr);
		} else if (type.equals(float.class) || type.equals(Float.class)) {
			fieldVal = Float.valueOf(fieldValStr);
		} else if (type.equals(short.class) || type.equals(Short.class)) {
			fieldVal = Short.valueOf(fieldValStr);
		} else if (type.equals(double.class) || type.equals(Double.class)) {
			fieldVal = Double.valueOf(fieldValStr);
		} else {
			fieldVal = fieldValStr;
		}

		return fieldVal;
	}
}
