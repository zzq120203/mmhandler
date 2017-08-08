package iie.mm.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import iie.mm.tools.SimHashTools;

public class DBUtils {
	
	private static GlobalConfig gconf = GlobalConfig.getConfig();

	public static boolean updateMpp(String txt, String g_id) {
		Connection dbConn = null;
		PreparedStatement pstmt = null;
		try {
			dbConn = MppJdbcPool_dbcp.getConnection();	
			String sql = "update tp_wxq_target_v1 set m_mm_audio_txt = ?,m_simhash = ? where g_id = ?";
			pstmt = dbConn.prepareStatement(sql);
			pstmt.setString(1, txt);
			pstmt.setLong(2, SimHashTools.genSimHashCode(txt));
			pstmt.setString(3, g_id);
			pstmt.executeUpdate();
		}catch (SQLException e) {
			e.printStackTrace();
			try {
				dbConn.close();
				return false;
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				pstmt.close();
				MppJdbcPool_dbcp.closeConn(dbConn);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return true;
	}

}
	
