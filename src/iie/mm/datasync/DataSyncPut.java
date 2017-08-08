package iie.mm.datasync;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import iie.mm.asr.AsrHealth;
import iie.mm.dao.DBUtils;
import iie.mm.dao.GlobalConfig;
import iie.mm.stt.AsrClient;
import redis.clients.jedis.Jedis;

public class DataSyncPut {

	private GlobalConfig gconf;
	private Convert convert;
	private ArrayList<LinkedBlockingQueue<String>> asrQueueArr;
	private ArrayList<LinkedBlockingQueue<Entry<String, String>>> arrSrcQueue;
	private ArrayList<LinkedBlockingQueue<Entry<String, String>>> bigArrSrcQueue;
	private ConcurrentHashMap<String,String> kg = new ConcurrentHashMap<String, String>();
	private AsrClient sample;
	private static DataSyncPut dp = null;
	
	public static boolean asrHealth = true;
	
	public static DataSyncPut getDataSyncPut(){
		if (dp == null)
			dp = new DataSyncPut();
		return dp;
	}
	
	public void init(GlobalConfig gc, ArrayList<LinkedBlockingQueue<Entry<String, String>>> setQueue) {
		this.gconf = gc;
		this.convert = new Convert();
		this.asrQueueArr = new ArrayList<LinkedBlockingQueue<String>>();
		this.bigArrSrcQueue = new ArrayList<LinkedBlockingQueue<Entry<String, String>>>();
		this.arrSrcQueue = setQueue;
		this.sample = new AsrClient("http://api.hivoice.cn/USCService/WebApi");
		runThrow();
	}

	private void runThrow() {
		for (int i = 0; i < this.gconf.maxTransmitThreadCount; i++) {
			DSThread t = new DSThread((LinkedBlockingQueue<Entry<String, String>>) this.arrSrcQueue.get(i % this.gconf.maxBlockQueue), i);
			Thread tTrhead = new Thread(t);
			tTrhead.start();
		}
		
		for (int i = 0; i < gconf.maxBlockQueue; i++) {
			LinkedBlockingQueue<Entry<String, String>> dxoBQueue = new LinkedBlockingQueue<Entry<String, String>>(
					gconf.maxBlockingQueueCapacity);
			bigArrSrcQueue.add(dxoBQueue);
		}
		for (int i = 0; i < gconf.maxBlockQueue; i++) {
			LinkedBlockingQueue<String> dxoBQueue = new LinkedBlockingQueue<String>(
					gconf.maxBlockingQueueCapacity);
			asrQueueArr.add(dxoBQueue);
		}

	}
	
	public ConcurrentHashMap<String, String> getKg() {
		return kg;
	}

	public void updateMMS(String g_id, String txt) {
		String key = kg.get(g_id);
		try {
			String tstr = gconf.dcp.put("t" + key.substring(1), txt.getBytes());
			if (tstr != null)
				System.out.println(Thread.currentThread().getName() + gconf.getTime() + " [PUT] " + key + " " + txt);
			else {
				System.out.println(Thread.currentThread().getName() + gconf.getTime() + " [ERR] " + key + " " + txt);
			}
			kg.remove(g_id);
			rmFile(g_id, key);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void rmFile(String g_id, String key) {
        File file = null;
		if(key.startsWith("a"))
			file = new File("get/"+g_id+".wav");
		if(key.startsWith("v"))
			file = new File("get/"+g_id+".mp4");
        if (file.exists() && file.isFile()) {
            if(file.delete())
            	System.out.println("[DEL FILE] " + file.getPath());
        }
		
	}

	public ArrayList<LinkedBlockingQueue<String>> getAsrQueueArr() {
		return asrQueueArr;
	}
	
	public ArrayList<LinkedBlockingQueue<Entry<String, String>>> getBigArrSrcQueue() {
		return bigArrSrcQueue;
	}

	class DSThread implements Runnable {

		private LinkedBlockingQueue<Entry<String, String>> srcQueue;
		private int id;

		public DSThread(LinkedBlockingQueue<Entry<String, String>> arrSrcQueue, int threadid) {
			this.srcQueue = arrSrcQueue;
			this.id = threadid;
			File dir = new File(gconf.setPath + "/" + id);
			if (!dir.exists()) {
				dir.mkdirs();
				System.out.println("mkdir " + dir.getPath());
			}
		}

		private boolean mkFile(byte[] afile, String g_id, int type) {
			boolean status = false;
			if (g_id.length() <= 1) {
				return status;
			}
			FileOutputStream out = null;
			File f = null;
			try {
				if(type == 1)
					f = new File("get/"+g_id+".wav");
				if(type == 2)
					f = new File("get/"+g_id+".mp4");
				out = new FileOutputStream(f);
				out.write(afile);
				out.flush();
				status = true;
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					out.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			return status;
		}
		
		@Override
		public void run() {
			int count = 0;
			Entry<String, String> kv = null;
			while (true) {
				Jedis jedis = gconf.scp.getPc().getRpL1().getResource();
				try {
					kv = srcQueue.take();
					if (kv != null) {
						String key = kv.getKey();
						String value = kv.getValue();
						if (gconf.scp.getFile(key).getFlength() > 1024* 1024* 1024) {
							System.out.println(Thread.currentThread().getName() + gconf.getTime() + " [PUT] " + key + " is big file");
							count++;
							bigArrSrcQueue.get(count % gconf.maxBlockQueue).put(kv);
							if (count == gconf.maxBlockQueue)
								count = 0;
						}

						String str;
						byte[] outContent = gconf.scp.get(key);
						if (asrHealth) {
							if (key.charAt(0) == 'a') {
								byte[] inContent = convert.convertAudioFiles(outContent);
								str = gconf.dcp.put(key, inContent);
								System.out.println(Thread.currentThread().getName() + gconf.getTime() + " [PUT] " + key + " " + str);
								count++;
								kg.put(value, key);
								asrQueueArr.get(count % gconf.maxBlockQueue).put(value);
								if (count == gconf.maxBlockQueue)
									count = 0;
								mkFile(inContent, value, 1);
							} else if (key.charAt(0) == 'v'){
								str = gconf.dcp.put(key, outContent);
								System.out.println(Thread.currentThread().getName() + gconf.getTime() + " [PUT] " + key + " " + str);
								count++;
								kg.put(value, key);
								asrQueueArr.get(count % gconf.maxBlockQueue).put(value);
								if (count == gconf.maxBlockQueue)
									count = 0;
								mkFile(outContent, value, 2);
							} else {
								str = gconf.dcp.put(key, outContent);
								System.out.println(Thread.currentThread().getName() + gconf.getTime() + " [PUT] " + key + " " + str);
							}
						} else {
							
							if (key.charAt(0) == 'a') {
								byte[] inContent = convert.convertAudioFiles(outContent);
								str = gconf.dcp.put(key, inContent);
								System.out.println(Thread.currentThread().getName() + gconf.getTime() + " [PUT] " + key + " " + str);
								String txt = sample.parseAudio(inContent);
								if (txt == null || txt.length() <= 0)
									txt = "未识别";
								String tstr = gconf.dcp.put("t" + key.substring(1), txt.getBytes());
								if (tstr == null)
									System.out.println(Thread.currentThread().getName() + gconf.getTime() + " [ERR] " + key + " " + txt);
								else {
									System.out.println(Thread.currentThread().getName() + gconf.getTime() + " [PUT] " + key + " " + txt);
									if (gconf.startUpdateMpp) {
										boolean upok = false;
										int con = 0;
										while (!upok) {
											upok = DBUtils.updateMpp(txt, value);
											con++;
											if(con >= 3) {
												upok = true;
												System.out.println(Thread.currentThread().getName() + gconf.getTime() + " [ERR] update mpp " + key + " " + txt);
											}
										}
									}
								}
							} else {
								str = gconf.dcp.put(key, outContent);
								System.out.println(Thread.currentThread().getName() + gconf.getTime() + " [PUT] " + key + " " + str);
							}
						}
						if (str == null) {
							jedis.hset("ds.set", key, value);
							System.out.println(Thread.currentThread().getName() + gconf.getTime() + " [ERR] " + key + " put is failed");
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					gconf.scp.getPc().getRpL1().putInstance(jedis);
				}
			}
		}
	}
}
