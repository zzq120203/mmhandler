package iie.mm.asr;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;

import com.alibaba.fastjson.JSONArray;

import iie.mm.dao.GlobalConfig;
import iie.mm.datasync.DataSyncPut;

public class AsrHealth implements Runnable{
	private GlobalConfig gconf;
	public static boolean isOK = false;

	public AsrHealth(GlobalConfig gconf) {
		super();
		this.gconf = gconf;
	}
	
	@Override
	public void run() {
		try {
			while (true) {
				isOK = false;
				SrcEntity health = new SrcEntity("health", 0, "YP", "health.wav", gconf.callback, gconf.getUrl + "health.wav", "opStt");
				
				URL url = new URL(gconf.apiUrl);
				HttpURLConnection conn = (HttpURLConnection) url.openConnection();
				conn.setRequestMethod("POST");
				conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");
				conn.setDoInput(true);
				conn.setDoOutput(true);
				JSONArray samples = new JSONArray(Arrays.asList(health));
		        String str = samples.toString();
//			        System.out.println("sample= " + str);
				OutputStream out = conn.getOutputStream();
				out.write(str.getBytes());
				out.flush();
				out.close();
				
				printResponse(conn);
				
				Thread.sleep(gconf.healthTime*1000);
				if(!isOK){
					DataSyncPut.asrHealth = false;
					System.out.println(Thread.currentThread().getName() + gconf.getTime() + " [ERR] ASR NOT OK");
				} else {
					DataSyncPut.asrHealth = true;
					System.out.println(Thread.currentThread().getName() + gconf.getTime() + " [INFO] ASR IS OK");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
    private static String printResponse(HttpURLConnection conn) throws Exception {
        if (conn.getResponseCode() != 200) {
            // request error
            return "";
        }
        InputStream is = conn.getInputStream();
        BufferedReader rd = new BufferedReader(new InputStreamReader(is));
        String line;
        StringBuffer response = new StringBuffer();
        while ((line = rd.readLine()) != null) {
            response.append(line);
            response.append('\r');
        }
        rd.close();
        
        return response.toString();
    }

}
