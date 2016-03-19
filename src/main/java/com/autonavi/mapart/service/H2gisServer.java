package com.autonavi.mapart.service;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.h2gis.drivers.shp.SHPRead;
import org.h2gis.drivers.shp.SHPWrite;
import org.h2gis.h2spatialext.CreateSpatialExtension;

/**
 * 
 * h2gis 内存空间数据库
 */
public class H2gisServer {
	private static final String ENCODING = "cp936";

	private Logger log = Logger.getLogger(getClass());

	private Connection connection;
	private Statement st;

	private static H2gisServer server = new H2gisServer();

	private H2gisServer() {
	}

	public static H2gisServer getInstance() {
		return server;
	}

	// 获取h2gis连接
	public void startup() throws ClassNotFoundException, SQLException {
		// Open memory H2 table
		Class.forName("org.h2.Driver");
		connection = DriverManager.getConnection("jdbc:h2:mem:syntax", "sa", "sa");
		st = connection.createStatement();
		// Import spatial functions, domains and drivers
		// If you are using a file database, you have to do only that once.
		CreateSpatialExtension.initSpatialExtension(connection);
	}

	/**
	 * 导入shap数据:</br> CALL SHPRead(VARCHAR path, VARCHAR tableName, VARCHAR
	 * fileEncoding);</br> 创建空间索引(Spatial indices):</br> CREATE SPATIAL INDEX
	 * [index_name] ON table_name(geometry_column);</br>
	 * 
	 * @param filePath
	 *            文件路径
	 * @param tableName
	 *            创建数据表名
	 * @throws SQLException
	 */
	public void importData(String filePath, String tableName) {
		if( StringUtils.isBlank(filePath)) {
			return;
		}
		File file  = new File(filePath);
		if (!file.exists()) {
			log.error("找不到该shape文件！");
			return;
		}
		
		try {
			SHPRead.readShape(connection, filePath, tableName, ENCODING);
			st.execute("CREATE SPATIAL INDEX IDX_" + tableName + " ON " + tableName + "(THE_GEOM);");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		log.debug("import file :" + filePath + " to talbe :" + tableName +"   size : "+count(tableName));
	}

	/**
	 * 导出shap数据:</br> CALL SHPWrite(VARCHAR path, VARCHAR tableName, VARCHAR
	 * fileEncoding);</br> 创建空间索引(Spatial indices):</br> CREATE SPATIAL INDEX
	 * [index_name] ON table_name(geometry_column);</br>
	 * 
	 * @param filePath
	 *            文件路径
	 * @param tableName
	 *            创建数据表名
	 * @throws SQLException
	 */
	public void outputShape(String outputFile, String tableName) {
		if (!new File(outputFile).getParentFile().exists()) {
			new File(outputFile).getParentFile().mkdirs();
		}

		try {
			SHPWrite.exportTable(connection, outputFile, tableName, "cp936");
		} catch (Exception e) {
//			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	/**
	 * 关闭数据库
	 */
	public void shutdown() {
		if (st != null) {
			try {
				st.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 查询表的记录数
	 * @param tablename
	 * @return
	 */
	public int count(String tablename) {
		String sql = "select count(*) from " + tablename;
		ResultSet result = executeQuery(sql);
		int count = 0;
		try {
			if(result.next()) {
				count = result.getInt(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return count;
	}
	
	/**
	 * 处理(新增、修改和删除) sql
	 * @param sql
	 */
	public void execute(String sql) {
		try {
			st.execute(sql);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * 处理查询语句
	 * @param sql
	 * @return
	 */
	public ResultSet executeQuery(String sql) {
		try {
			return st.executeQuery(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	public void showTableMeta(String tableName) throws SQLException {
		ResultSet rs = this.executeQuery("select * from " + tableName+" where rownum = 1");
		ResultSetMetaData meta = rs.getMetaData();
		while (rs.next()) {
			  for (int i = 0; i < meta.getColumnCount(); i++) {
				  log.debug(
	                    meta.getColumnLabel(i + 1) + ": " +
	                    rs.getString(i + 1));
	            }
		}
	}
	public int queryForInt(String sql) {
		ResultSet rrs = H2gisServer.getInstance().executeQuery(sql);
		try {
			if (rrs.next()) {
				return rrs.getInt(1);
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			try {
				rrs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return 0;
	}
}
