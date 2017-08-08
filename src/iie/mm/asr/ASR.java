package iie.mm.asr;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.fastjson.JSONArray;

import iie.mm.dao.GlobalConfig;

public class ASR{
	
	private GlobalConfig gconf;
	private ArrayList<LinkedBlockingQueue<String>> asrQueueArr;
	public ASR(GlobalConfig gconf, ArrayList<LinkedBlockingQueue<String>> asrQueueArr) {
		super();
		this.asrQueueArr = asrQueueArr;
		this.gconf = gconf;
	}

	public void runThrow() {
		for (int i = 0; i < this.gconf.maxTransmitThreadCount; i++) {
			AsrThread t = new AsrThread((LinkedBlockingQueue<String>) this.asrQueueArr.get(i % this.gconf.maxBlockQueue));
			Thread tTrhead = new Thread(t);
			tTrhead.start();
		}
	}
	
	public String putSample(String gid) {
		return putSample(Arrays.asList(gid));
	}
	
	public String putSample(List<String> gids) {
		Object[] asrArr = gids.stream().map(key->SrcEntity.getSrcEntity(key,gconf)).toArray();
		List<Object> asrList = Arrays.asList(asrArr);
		
		String returnStr = null;
		try {
		
			URL url = new URL(gconf.apiUrl);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");
			conn.setDoInput(true);
			conn.setDoOutput(true);
			JSONArray samples = new JSONArray(asrList);
	        String str = samples.toString();
//	        System.out.println("sample= " + str);
			OutputStream out = conn.getOutputStream();
			out.write(str.getBytes());
			out.flush();
			out.close();
			
			returnStr = printResponse(conn);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return returnStr;
		
	}
	
    private String printResponse(HttpURLConnection conn) throws Exception {
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
    
    class AsrThread  implements Runnable {

		private LinkedBlockingQueue<String> asrQueue;

		public AsrThread(LinkedBlockingQueue<String> asrQueue) {
			this.asrQueue = asrQueue;
		}
    	
		@Override
		public void run() {
			while (true) {
				try {
					String g_id = asrQueue.take();
					if (g_id != null) {
						putSample(g_id);
					}
				}catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
    	
    }

}
