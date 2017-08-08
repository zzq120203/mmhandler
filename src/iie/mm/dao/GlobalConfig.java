package iie.mm.dao;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import iie.mm.client.ClientAPI;

public class GlobalConfig {
	private static GlobalConfig config = new GlobalConfig();
	
	public static GlobalConfig getConfig() {
		return config;
	}

	public String apiUrl;
	public String getUrl;
	public String callback;
	public int httpprot;
	
	
	public int maxBlockQueue = 10;
	public int maxTransmitThreadCount = 10;
	public int maxBlockingQueueCapacity = 100;
	public int maxLogicProcThreadCount = 10;
	public long syncTime = 60;
	public String sRedisURL;
	public String dRedisURL;
	public String setPath;

	public String mppDriver;
	public String mppUrl;
	public String mppUser;
	public String mppPwd;	
	public boolean startUpdateMpp = false;
 	
	private DateFormat df = new SimpleDateFormat(" yyyy-MM-dd HH:mm:ss ");

	public final ClientAPI scp = new ClientAPI();
	public final ClientAPI dcp = new ClientAPI();
	public int healthTime;

	public GlobalConfig() {
	}

	@Override
	public String toString() {
		return "GlobalConfig ["														+ "\n"
				+ "maxTransmitThreadCount=" 	+ maxTransmitThreadCount			+ "\n"
				+ "maxBlockQueue=" 				+ maxBlockQueue                		+ "\n"
				+ "maxBlockingQueueCapacity=" 	+ maxBlockingQueueCapacity          + "\n"
				+ "maxLogicProcThreadCount=" 	+ maxLogicProcThreadCount           + "\n"
				+ "sRedisURL=" 					+ sRedisURL                   		+ "\n"
				+ "dRedisURL=" 					+ dRedisURL                    		+ "\n"
				+ "syncTime=" 					+ syncTime                    		+ "\n"
				+ "setPath=" 					+ setPath                    		+ "\n"
				+ "mppDriver=" 					+ mppDriver                    		+ "\n"
				+ "mppUrl=" 					+ mppUrl                    		+ "\n"
				+ "mppUser=" 					+ mppUser                    		+ "\n"
				+ "mppPwd=" 					+ mppPwd                    		+ "\n"
				+ "startUpdateMpp=" 			+ startUpdateMpp                   	+ "\n"
				+ "apiUrl=" 					+ apiUrl          		         	+ "\n"
				+ "getUrl=" 					+ getUrl              		     	+ "\n"
				+ "callback=" 					+ callback              	     	+ "\n"
				+ "httpprot=" 					+ httpprot              	     	+ "\n"
				+ "healthTime=" 				+ healthTime              	     	+ "\n"
				+ "]";
	}
	
	public String getTime() {
		return df.format(System.currentTimeMillis());
	}
	
}
