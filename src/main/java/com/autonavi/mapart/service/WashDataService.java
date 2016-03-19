package com.autonavi.mapart.service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import org.apache.log4j.Logger;

/**
 * 清洗数据
 */
public abstract class WashDataService {

	public Logger log = Logger.getLogger(getClass());

	public static String AUTONAVI_TNAME = "AN_POLYGON";// 导入后的autonavi的表
	public static String BD_TNAME = "BD_POLYGON";// 导入bd的表
	public static String POI_TNAME = "POI_POINT";// 导入POI的表
	public static String GB_TNAME = "GB_POLYGON";// 导入BD垃圾的表
	// public static String GB_AN_TNAME = "GB_AN_POLYGON";//导入AN垃圾的表

	/**
	 * 数据清洗
	 * 
	 * @param outFilePath
	 *            洗完数据输出目录
	 * @param areaCoverPercent
	 * @param diffCoverPercent
	 * @return WashedData
	 */
	public abstract WashedData wash();

	/**
	 * 统计数据表中数据量
	 * 
	 * @param tableName
	 *            数据表名
	 * @throws SQLException
	 */
	public int count(String tableName, boolean... isShow) {
		try {
			ResultSet rs = H2gisServer.getInstance().executeQuery("select count(1) from " + tableName);
			if (rs.next()) {
				if (isShow == null || (isShow != null && isShow.length > 0 && isShow[0])) {
					log.debug("table " + tableName + " row num: " + rs.getInt(1));
				}
			}
			log.debug(tableName+"的数量："+rs.getInt(1));
			return rs.getInt(1);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void output(String tableName, String outputPath) throws SQLException {
		log.debug("output : " + tableName + "," + outputPath);
		H2gisServer.getInstance().outputShape(
				outputPath.replace("${outputPath}", WashDataConfig.getInstance().getProperty("outputPath")), tableName);
	}

	protected String createBdTmpTable(Collection<String> bdGeomData, String bdTname, String garbageTableName) {
		log.debug("insert table :" + garbageTableName+"数量为："+bdGeomData.size());
		for (String geom : bdGeomData) {
			H2gisServer.getInstance().execute(
					"insert into " + garbageTableName + "(THE_GEOM) select THE_GEOM FROM " + bdTname
							+ " where ST_Equals(THE_GEOM, ST_GeomFromText('" + geom + "')) = true");
		}
		return garbageTableName;
	}

	protected String createAnTmpTable(Collection<String> geoms, String anTname, String tableName) {
		for (String geom : geoms) {
			H2gisServer
					.getInstance()
					.execute(
							"insert into "
									+ tableName
									+ "(THE_GEOM,NAME_CHN,MESH,POI_ID,FA_TYPE,AREA_FLAG) "
									+ "select THE_GEOM,NAME_CHN,MESH,POI_ID,FA_TYPE,AREA_FLAG FROM "
									+ anTname + " where ST_Equals(THE_GEOM ,ST_GeomFromText('" + geom + "')) = true");
		}
		return tableName;
	}

	public void createEmptyTable(String sourceTname, String targetTname) {
		log.debug("sourceTname=="+sourceTname+"     "+"targetTname=="+targetTname);
		H2gisServer.getInstance().execute(
				"create table " + targetTname + " as select * from " + sourceTname + " where 1=2 ");

	}

	@SuppressWarnings("rawtypes")
	protected boolean isCoveredNumOver2(Collection polygon) {
		return polygon.size() >= 2;
	}

	protected boolean isAreaPercentOk(double area, Collection<Double> multiData) {
		return calAreaSum(multiData) / area >= 0.8;
	}

	private double calAreaSum(Collection<Double> multiData) {
		double area_sum = 0;
		for (double intsec_area : multiData) {
			area_sum += intsec_area;
		}
		return area_sum;
	}

	protected String selectAllAnDataByGeom(String tmpAnTable, String finalAnTable) throws SQLException {
		String sql = "create table " + finalAnTable + " as select an.* from " + AUTONAVI_TNAME + " an," + tmpAnTable
				+ " tAn " + "where ST_Equals(an.THE_GEOM,tAn.THE_GEOM) = true";
		H2gisServer.getInstance().execute(sql);
		return finalAnTable;
	}

	public static class WashedData {
		String garbageFile;// 垃圾 （处理完成数据）
		String resultFile;// 成果（进行下一步数据处理）
		String garbageTableName;
		String resultTableName;

		public WashedData(String garbageFile) {
			super();
			this.garbageFile = garbageFile;
		}

		public WashedData(String garbageFile, String resultFile, String garbageTableName, String resultTableName) {
			super();
			this.garbageFile = garbageFile;
			this.resultFile = resultFile;
			this.garbageTableName = garbageTableName;
			this.resultTableName = resultTableName;
		}

		public String getGarbageFile() {
			return garbageFile;
		}

		public String getResultFile() {
			return resultFile;
		}

		public String getGarbageTableName() {
			return garbageTableName;
		}

		public String getResultTableName() {
			return resultTableName;
		}

	}

	public class Polygon {
		private String polygon;
		private Double area;

		public Polygon(String polygon, Double area) {
			super();
			this.polygon = polygon;
			this.area = area;
		}

		public String getPolygon() {
			return polygon;
		}

		public void setPolygon(String polygon) {
			this.polygon = polygon;
		}

		public Double getArea() {
			return area;
		}

		public void setArea(Double area) {
			this.area = area;
		}

		@Override
		public String toString() {
			return "Polygon [polygon=" + polygon + ", area=" + area + "]";
		}
//		hashCode()方法被用来获取给定对象的唯一整数。这个整数被用来确定对象被存储在HashTable类似的结构中的位置。
//		默认的，Object类的hashCode()方法返回这个对象存储的内存地址的编号。
//		如果重写equals()方法必须要重写hashCode()方法
//		  1.如果两个对象相同，那么它们的hashCode值一定要相同；
//		  2.如果两个对象的hashCode相同，它们并不一定相同（这里说的对象相同指的是用eqauls方法比较）。  
//		        如不按要求去做了，会发现相同的对象可以出现在Set集合中，同时，增加新元素的效率会大大下降。
//		  3.equals()相等的两个对象，hashcode()一定相等；equals()不相等的两个对象，却并不能证明他们的hashcode()不相等。
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + ((area == null) ? 0 : area.hashCode());
			result = prime * result + ((polygon == null) ? 0 : polygon.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Polygon other = (Polygon) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (area == null) {
				if (other.area != null)
					return false;
			} else if (!area.equals(other.area))
				return false;
			if (polygon == null) {
				if (other.polygon != null)
					return false;
			} else if (!polygon.equals(other.polygon))
				return false;
			return true;
		}

		private WashDataService getOuterType() {
			return WashDataService.this;
		}

	}
	
}
