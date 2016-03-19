package com.autonavi.mapart.service;

import static org.junit.Assert.assertEquals;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.autonavi.mapart.service.WashDataConfig;

public class WashDataConfigTest {

	@Test
	public void testGetProperty() throws FileNotFoundException, IOException {
		WashDataConfig instance = WashDataConfig.getInstance();
		instance.loadConfig("src/main/resources/config.properties");
		Map<String, String> env = System.getenv();
		Map<String, String> env2 = new HashMap<String, String>(env);
		for (String key : env.keySet()) {
			System.out.println(key + "\t" + env.get(key));
		}
//		env.put("PROCESSOR_LEVEL", "10");

		env2.put("PROCESSOR_LEVEL", "10");
		setEnv(env2);
		assertEquals("", System.getenv("PROCESSOR_LEVEL"));
		assertEquals("", instance.getProperty("replaceMergeFile"));

	}

	protected static void setEnv(Map<String, String> newenv) {
		try {
			Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
			Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
			theEnvironmentField.setAccessible(true);
			Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
			env.putAll(newenv);
			Field theCaseInsensitiveEnvironmentField = processEnvironmentClass
					.getDeclaredField("theCaseInsensitiveEnvironment");
			theCaseInsensitiveEnvironmentField.setAccessible(true);
			Map<String, String> cienv = (Map<String, String>) theCaseInsensitiveEnvironmentField.get(null);
			cienv.putAll(newenv);
		} catch (NoSuchFieldException e) {
			try {
				Class[] classes = Collections.class.getDeclaredClasses();
				Map<String, String> env = System.getenv();
				for (Class cl : classes) {
					if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
						Field field = cl.getDeclaredField("m");
						field.setAccessible(true);
						Object obj = field.get(env);
						Map<String, String> map = (Map<String, String>) obj;
						map.clear();
						map.putAll(newenv);
					}
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}
}
