package iie.mm.datasync;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;

import iie.mm.dao.ConnectionPoolManager;
import iie.mm.dao.GlobalConfig;
import iie.mm.stt.AsrClient;
import redis.clients.jedis.Jedis;

public class DataSyncPut {

	private GlobalConfig gconf;
	private Convert convert;
	private ArrayList<LinkedBlockingQueue<Entry<String, String>>> arrSrcQueue;
	private ArrayList<LinkedBlockingQueue<Entry<String, String>>> bigArrSrcQueue;
	private AsrClient sample;
	
	public DataSyncPut(GlobalConfig gc, ArrayList<LinkedBlockingQueue<Entry<String, String>>> setQueue) {
		this.gconf = gc;
		this.convert = new Convert();
		this.sample = new AsrClient("http://api.hivoice.cn/USCService/WebApi");
		this.bigArrSrcQueue = new ArrayList<LinkedBlockingQueue<Entry<String, String>>>();
		this.arrSrcQueue = setQueue;
	}

	public void runThrow() {
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

		private boolean moveFile(File afile) {
	        File nfile = new File(gconf.setPath);
	        boolean success = afile.renameTo(new File(nfile, afile.getName()));
	        return success;
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
										upok = updateMpp(txt, value);
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

		private boolean updateMpp(String txt, String g_id) {
			Connection dbConn = null;
			PreparedStatement pstmt = null;
			try {
				dbConn = ConnectionPoolManager.getInstance().getConnection(gconf.mppDriver);	
				String sql = "update tp_wxq_target_v1 set m_mm_audio_txt = ? where g_id = ?";
				pstmt = dbConn.prepareStatement(sql);
				pstmt.setString(1, txt);
				pstmt.setString(2, g_id);
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
					ConnectionPoolManager.getInstance().close(gconf.mppDriver, dbConn);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			return true;
		}

	}

}
