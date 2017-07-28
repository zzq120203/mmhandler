package iie.mm.dao;

import java.util.ArrayList;
import java.util.List;
/**
 * 初始化，模拟加载所有的配置文件
 * @author Ran
 *
 */
public class DBInitInfo {
	private static GlobalConfig gconf = GlobalConfig.getConfig();
	public  static List<DBbean>  beans = null;
	static{
		beans = new ArrayList<DBbean>();
//		DBbean beanOracle = new DBbean();
//		beanOracle.setDriverName("oracle.jdbc.driver.OracleDriver");
//		beanOracle.setUrl("jdbc:oracle:thin:@7MEXGLUY95W1Y56:1521:orcl");
//		beanOracle.setUserName("mmsoa");
//		beanOracle.setPassword("password1234");
//		
//		beanOracle.setMinConnections(5);
//		beanOracle.setMaxConnections(100);
//		
//		beanOracle.setPoolName("testPool");
//		beans.add(beanOracle);
		DBbean mpp = new DBbean(gconf.mppDriver, gconf.mppUrl, gconf.mppUser, gconf.mppPwd, gconf.mppDriver);
		mpp.setCheakPool(false);
		beans.add(mpp);
		
		
		
	}
}
