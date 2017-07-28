package devmap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class DevMap {

	public static class DevStat {
		public String mount_point;
		public long read_nr;
		public long write_nr;
		public long err_nr;
	
		public DevStat(String mp, long read_nr, long write_nr, long err_nr) {
			this.mount_point = mp;
			this.read_nr = read_nr;
			this.write_nr = write_nr;
			this.err_nr = err_nr;
		}
	
		public static DevStat convertToDevStat(String str) {
			String[] ls = str.split(",");
			if (ls.length == 4) {
				return new DevStat(ls[0], Long.parseLong(ls[1]), Long.parseLong(ls[2]), Long.parseLong(ls[3]));
			}
			return null;
		}
	};
	
	public static enum VFSOperation {
		READ_BEGIN, WRITE_BEGIN, READ_END, WRITE_END, ERROR,
	};
	
	private ConcurrentHashMap<String, DevStat> devmap = new ConcurrentHashMap<String, DevStat>();
	private long last_refresh_ts = -1;
	
    static { 
    	System.loadLibrary("devmap");
    }

    public static native boolean isValid();

    public static native String getDevMaps();
    
    public static native synchronized boolean appendAuditLog(String auditLog);
    
    public DevMap() {
    	refreshDevMap();
    }
    
    public void refreshDevMap() {
    	synchronized (this) {
    		if (last_refresh_ts + 5 * 1000  < System.currentTimeMillis()) {
    			System.out.println("[DEVMAP NOTICE] Current devmap might be out-of-date, refresh it firstly!");
    			last_refresh_ts = System.currentTimeMillis();
    		} else {
    			return;
    		}
    	}
    	String content = getDevMaps();
    	String ls[]	= content.split("\n");
    	
    	for (String line : ls) {
    		String[] r = line.split(":");
    		if (r.length == 2) {
    			DevStat v = DevStat.convertToDevStat(r[1]);
    			if (v != null) {
    				devmap.put(r[0], v);
    			}
    		}
    	}
    }
    
    public String dumpDevMap() {
    	String s = "";
    	
    	for (String k : devmap.keySet()) {
    		DevStat v = devmap.get(k);
    		s += "Key " + k + ", Value: " + v.mount_point + "," + v.read_nr + "," + 
    				v.write_nr + "," + v.err_nr + "\n";
    	}
    	
    	return s;
    }
    
    public DevStat findDev(String devid) {
    	refreshDevMap();
    	return devmap.get(devid);
    }
    
    /**
     * getPath(), thread-safe
     * 
     * @param devid
     * @param location
     * @return
     * @throws IOException
     */
    public String getPath(String devid, String location) throws IOException {
    	DevStat ds = findDev(devid);
    	if (ds == null) {
    		throw new IOException("Unknown Device " + devid + ", can't translate it to Path. " + 
    				"Reference map: \n" + dumpDevMap());
    	}
    	return ds.mount_point + "/" + location; 
    }
    
    /**
     * audit(), thread(in one jvm) and process safe
     * 
     * @param devid
     * @param location
     * @param op
     * @param tag: should not contain ','. Generally, it should be something 
     *             like 'mpp.r' or 'rzx.r' or 'swmid.w' or 'ulss.w'.
     */
    public void audit(String devid, String location, VFSOperation op, String tag) {
    	String auditLog = "+A:" + tag + "," + System.currentTimeMillis() + "," + 
    			devid + "," + location + ",";
    	
    	switch (op) {
    	case READ_BEGIN:
    		auditLog += "RDB";
    		break;
    	case READ_END:
    		auditLog += "RDE";
    		break;
    	case WRITE_BEGIN:
    		auditLog += "WRB";
    		break;
    	case WRITE_END:
    		auditLog += "WRE";
    		break;
    	case ERROR:
    		auditLog += "ERR";
    		break;
    	default:
    	}
    	if (!appendAuditLog(auditLog + "\n")) {
    		System.out.println("[DEVMAP WARN] append audit log '" + auditLog + "' failed.");
    	}
    }
    
    public static void main(String[] args) {
    	ArrayList<TestThread> ttal = new ArrayList<TestThread>();
    	DevMap dm = new DevMap();
    	int thread_nr = 1, loop_nr = 1;
    	
    	if (args.length >= 1)
    		thread_nr = Integer.parseInt(args[0]);
    	if (args.length >= 2)
    		loop_nr = Integer.parseInt(args[1]);
    	
    	long begin = System.currentTimeMillis();
    	for (int i = 0; i < thread_nr; i++) {
    		TestThread tt = dm.new TestThread("TT" + i, dm, loop_nr);
    		ttal.add(tt);
    	}
    	for (int i = 0; i < thread_nr; i++) {
    		try {
				ttal.get(i).runner.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    	}
    	System.out.println("Avg latency is: " + 
    			(double)(System.currentTimeMillis() - begin) / (thread_nr * loop_nr * 5) + " ms.");
    }
    
    public class TestThread implements Runnable {
    	Thread runner;
    	DevMap dm;
    	int nr = 1;
    	
    	public TestThread(String threadName, DevMap dm, int nr) {
    		this.nr = nr;
    		this.dm = new DevMap();
    		runner = new Thread(this, threadName);
    		runner.start();
    	}
    	
    	public void run() {
    		int i = 0;
    		
    		while (i < nr) {
    			/*
    			System.out.println(runner.getName() + " -> " + dm.dumpDevMap());

    			try {
    				System.out.println(dm.getPath("ST9500420AS_5VJFBJ3K-part6", "/hello/world"));
    			} catch (IOException e) {
    				e.printStackTrace();
    			}
    			*/
    			dm.audit("ST9500420AS_5VJFBJ3K-part6", "/hello/world1", VFSOperation.READ_BEGIN, "search.r");
    			dm.audit("ST9500420AS_5VJFBJ3K-part6", "/hello/world1", VFSOperation.READ_END, "search.r");
    			dm.audit("ST9500420AS_5VJFBJ3K-part6", "/hello/world1", VFSOperation.WRITE_BEGIN, "load.w");
    			dm.audit("ST9500420AS_5VJFBJ3K-part6", "/hello/world1", VFSOperation.WRITE_END, "load.w");
    			dm.audit("ST9500420AS_5VJFBJ3K-part6", "/hello/world2", VFSOperation.ERROR, "load.w");
    			i++;
    		}
    	}
    	
    }
}

