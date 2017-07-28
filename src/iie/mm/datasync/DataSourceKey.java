package iie.mm.datasync;

import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import iie.mm.dao.GlobalConfig;
import redis.clients.jedis.Jedis;

public class DataSourceKey implements Runnable{
	private GlobalConfig gconf;
	
	private static ArrayList<LinkedBlockingQueue<Entry<String, String>>> putQueue;
	private static ArrayList<LinkedBlockingQueue<String>> delQueue;
	
	public DataSourceKey(GlobalConfig gc) {
		gconf = gc;
		putQueue = new ArrayList<LinkedBlockingQueue<Entry<String, String>>>();
		delQueue = new ArrayList<LinkedBlockingQueue<String>>();
		
		Jedis jedis = gconf.scp.getPc().getRpL1().getResource();
		try {
			if (jedis != null)
				jedis.hset("mm.client.conf", "ds", "1");
		} finally {
			gconf.scp.getPc().getRpL1().putInstance(jedis);
		}
		
		for (int i = 0; i < gconf.maxBlockQueue; i++) {
			LinkedBlockingQueue<Entry<String, String>> dxoBQueue = new LinkedBlockingQueue<Entry<String, String>>(
					gconf.maxBlockingQueueCapacity);
			putQueue.add(dxoBQueue);
		}
		
		for (int i = 0; i < gconf.maxBlockQueue; i++) {
			LinkedBlockingQueue<String> dxoBQueue = new LinkedBlockingQueue<String>(
					gconf.maxBlockingQueueCapacity);
			delQueue.add(dxoBQueue);
		}
	}
	
	public ArrayList<LinkedBlockingQueue<Entry<String, String>>> getPutQueue() {
		return putQueue;
	}
	
	public ArrayList<LinkedBlockingQueue<String>> getDelQueue() {
		return delQueue;
	}
	
	@Override
	public void run(){
		do {
			Jedis jedis = gconf.scp.getPc().getRpL1().getResource();
			try {
				if (jedis != null) {
					//get redis
					Map<String, String> skeys = jedis.hgetAll("ds.set");
					
					if (skeys != null && skeys.size() > 0) {
						System.out.println(Thread.currentThread().getName() + gconf.getTime() + " ds.set size :" + skeys.size());
						int scount = 0;
						for (Entry<String, String> skey : skeys.entrySet()) {
							//System.out.println(skey);
							while (putQueue.get(scount % gconf.maxBlockQueue).remainingCapacity() <= 0) {
								Thread.sleep(10);
							}
							putQueue.get(scount % gconf.maxBlockQueue).put(skey);
							jedis.hdel("ds.set", skey.getKey());
							scount++;
						}
					}
					//del redis
					Set<String> dkeys = jedis.hkeys("ds.del");
					if (dkeys != null && dkeys.size() > 0) {
						System.out.println(Thread.currentThread().getName() + gconf.getTime() + " ds.del size :" + dkeys.size());
						int dcount = 0;
						for (String dkey : dkeys) {
							while (delQueue.get(dcount % gconf.maxBlockQueue).remainingCapacity() <= 0) {
								Thread.sleep(10);
							}
							delQueue.get(dcount % gconf.maxBlockQueue).put(dkey);
							jedis.hdel("ds.del", dkey);
							dcount++;
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				gconf.scp.getPc().getRpL1().putInstance(jedis);
			}
			try {
				Thread.sleep(gconf.syncTime * 1000L);
			} catch (Exception e) {
				e.printStackTrace();
			}
		} while(true);
	}

}
