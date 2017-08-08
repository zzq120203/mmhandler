package iie.mm.dao;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MppJdbcPool_dbcp {

	private static GlobalConfig gconf = GlobalConfig.getConfig();
	private static List<Connection> sessionList = new ArrayList<Connection>();

	static {
		try {
			Class.forName(gconf.mppDriver);
		} catch (Throwable e) {
			throw new ExceptionInInitializerError(e);
		}
	}

	public static Connection getConnection() throws SQLException {
		Connection s = null;
		synchronized (sessionList) {
			if (sessionList.size() > 0)
				s = sessionList.remove(0);
		}
		if (s == null || s.isClosed()) {
			s = DriverManager.getConnection(gconf.mppUrl, gconf.mppUser, gconf.mppPwd);
		}
		return s;
	}

	public static void closeConn(Connection session) {
		synchronized (sessionList) {
			try {
				if (sessionList.size() >= 5) {
					session.close();
				} else if (!session.isClosed()) {
					sessionList.add(session);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
