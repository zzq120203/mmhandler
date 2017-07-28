package iie.mm.datasync;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import iie.mm.client.DeleteSet;
import iie.mm.dao.GlobalConfig;

public class DataSyncDel {

	private GlobalConfig gconf;
	private DeleteSet delset;
	private ArrayList<LinkedBlockingQueue<String>> arrSrcQueue;
	
	public DataSyncDel(GlobalConfig gc, ArrayList<LinkedBlockingQueue<String>> arrDelQueue) {
		this.gconf = gc;
		this.arrSrcQueue = arrDelQueue;
		this.delset = new DeleteSet(gconf.dcp);
	}
	
	public void runThrow() {
		for (int i = 0; i < this.gconf.maxTransmitThreadCount; i++) {
			DSThread t = new DSThread( 
					(LinkedBlockingQueue<String>) this.arrSrcQueue.get(i % this.gconf.maxBlockQueue));
			Thread tTrhead = new Thread(t);
			tTrhead.start();
		}
	}
	
	class DSThread implements Runnable {

		private LinkedBlockingQueue<String> srcQueue;
		
		public DSThread(LinkedBlockingQueue<String> arrSrcQueue) {
			this.srcQueue = arrSrcQueue;
		}
		
		@Override
		public void run() {
			while (true) {
				try {
					String key = srcQueue.take();
					if (key != null && key.contains("@")) {
						gconf.dcp.deleteFile(key);
					} else if (key != null) {
						delset.delSet(key);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	
}
