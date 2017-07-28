package iie.mm.datasync;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;

import iie.mm.dao.GlobalConfig;

public class DataSyncPutdown {

	private GlobalConfig gconf;

	private ArrayList<LinkedBlockingQueue<Entry<String, String>>> arrSrcQueue;

	public DataSyncPutdown(GlobalConfig gc, ArrayList<LinkedBlockingQueue<Entry<String, String>>> setQueue) {
		this.gconf = gc;
		this.arrSrcQueue = setQueue;
	}

	public void runThrow() {
		for (int i = 0; i < this.gconf.maxTransmitThreadCount; i++) {
			DSThread t = new DSThread((LinkedBlockingQueue<Entry<String, String>>) this.arrSrcQueue.get(i % this.gconf.maxBlockQueue), i);
			Thread tTrhead = new Thread(t);
			tTrhead.start();
		}
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
			FileOutputStream fos = null;
			while (true) {
				try {
					Entry<String, String> kv = srcQueue.take();
					if (kv != null) {
						String key = kv.getKey();
						int blen = 100 * 1024 * 1024;
						byte[] content = null;
						long len = gconf.scp.getFile(key).getFlength();
						if (len > blen) {
							File file = new File(key);
							fos = new FileOutputStream(file);
							for (long i = 0; i <= len / blen; i++) {
								if (i != len / blen) {
									content = gconf.scp.downloadFile(key, i * blen, blen);
								} else {
									content = gconf.scp.downloadFile(key, i * blen, (int) (len % blen));
								}
								fos.write(content);
							}
							if (moveFile(file))
								file.delete();
						} else {
							fos = new FileOutputStream(gconf.setPath + "/" + id + "/" + key);
							content = gconf.scp.downloadFile(key, 0, (int)len);
							fos.write(content);
						}
					}
				} catch (IOException e) {
					System.out.println(e.getMessage());
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					try {
						fos.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}

	}

}
