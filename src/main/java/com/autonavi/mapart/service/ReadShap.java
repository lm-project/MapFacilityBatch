package com.autonavi.mapart.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
/**
 * 读取shape文件头<br>
 * 头信息：http://blog.csdn.net/ruoyuseu/article/details/3981744
 * @author qiang.cai
 *
 */
public class ReadShap {
	private static double xMin;
	private static double yMin;
	private static double xMax;
	private static double yMax;

	private static double read8(FileInputStream input) {
		byte[] b = new byte[8];
		for (int i = 0; i < 8; i++) {
			try {
				b[i] = (byte) input.read();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return arr2double(b);
	}

	public static double arr2double(byte[] arr) {
		int i = 0;
		int len = 8;
		int cnt = 0;
		byte[] tmp = new byte[len];
		for (i = 0; i < len; i++) {
			tmp[cnt] = arr[i];
			cnt++;
		}
		long accum = 0;
		i = 0;
		for (int shiftBy = 0; shiftBy < 64; shiftBy += 8) {
			accum |= ((long) (tmp[i] & 0xff)) << shiftBy;
			i++;
		}
		return Double.longBitsToDouble(accum);
	}

	/**
	 * 获取shapefile的box
	 * 
	 * @param fileName
	 */
	public static String read(String fileName) {
		int index = 0;
		File file = new File(fileName);
		if (file.exists()) {
			try (FileInputStream input = new FileInputStream(file)) {
				while (input.available() > 0) {
					input.read();
					index++;
					if (index == 36)
						break;
				}
				xMin = read8(input);
				yMin = read8(input);
				xMax = read8(input);
				yMax = read8(input);

				// return "POLYGON((9 0, 9 11, 10 11, 10 0, 9 0))";
				return "POLYGON((" + xMin + " " + yMin + ", " + xMin + " "
						+ yMax + "," + xMax + " " + yMax + ", " + xMax + " "
						+ yMin + "," + xMin + " " + yMin + "))";
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			System.out.println("文件不存在");
		}
		return "";
	}

	public static void main(String[] args) throws Exception {
		String filePath = "D:/EWorspase/MapFacilityBatch/data/2014-09-11[09_12_40]JointDBF/ANMESH/浙江省/杭州市(150)/H50F020044/Joint_FacilityArea_Dissolve.shp";
		H2gisServer.getInstance().startup();
		String sql = " create table MESHBOX(THE_GEOM POLYGON)";
		H2gisServer.getInstance().execute(sql);
		String sql2 = "insert into MESHBOX values(' " + read(filePath) + "')";
		System.out.println(sql2);
		H2gisServer.getInstance().execute(sql2);
		System.out.println(H2gisServer.getInstance().count("MESHBOX"));
		H2gisServer.getInstance().outputShape("d:/B/box.shp", "MESHBOX");
		H2gisServer.getInstance().shutdown();
	}
}
