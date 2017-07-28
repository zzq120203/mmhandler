package iie.mm.tools;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

public class MMMoveDedupInfo {
	
	public static class Option {
	     String flag, opt;
	     public Option(String flag, String opt) { this.flag = flag; this.opt = opt; }
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		List<String> argsList = new ArrayList<String>();  
	    List<Option> optsList = new ArrayList<Option>();
	    List<String> doubleOptsList = new ArrayList<String>();
	    
	    // parse the args
		for (int i = 0; i < args.length; i++) {
			System.out.println("Args " + i + ", " + args[i]);
			switch (args[i].charAt(0)) {
	        case '-':
	            if (args[i].length() < 2)
	                throw new IllegalArgumentException("Not a valid argument: "+args[i]);
	            if (args[i].charAt(1) == '-') {
	                if (args[i].length() < 3)
	                    throw new IllegalArgumentException("Not a valid argument: "+args[i]);
	                doubleOptsList.add(args[i].substring(2, args[i].length()));
	            } else {
	                if (args.length-1 > i)
	                    if (args[i + 1].charAt(0) == '-') {
	                    	optsList.add(new Option(args[i], null));
	                    } else {
	                    	optsList.add(new MMMoveDedupInfo.Option(args[i], args[i+1]));
	                    	i++;
	                    }
	                else {
	                	optsList.add(new Option(args[i], null));
	                }
	            }
	            break;
	        default:
	            // arg
	            argsList.add(args[i]);
	            break;
	        }
		}
		
		String fromRP = null, toRP = null;
		int kdays = 3;
		long cday = System.currentTimeMillis() / 86400000 * 86400;
		long bTs;
		long iter = Long.MAX_VALUE - 1, j = 0;
		
		for (Option o : optsList) {
			if (o.flag.equals("-h")) {
				// print help message
				System.out.println("-h    : print this help.");
				System.out.println("-stl  : sentinels <host:port;host:port>.");
				
				System.exit(0);
			}
			if (o.flag.equals("-from")) {
				// parse uri
				if (o.opt == null) {
					System.out.println("-from redis_server:port");
					System.exit(0);
				}
				fromRP = o.opt;
			}
			if (o.flag.equals("-to")) {
				// parse uri
				if (o.opt == null) {
					System.out.println("-to redis_server:port");
					System.exit(0);
				}
				toRP = o.opt;
			}
			if (o.flag.equals("-kdays")) {
				// keep days nr
				if (o.opt == null) {
					System.out.println("-kdays DAY_NR");
					System.exit(0);
				}
				kdays = Integer.parseInt(o.opt);
			}
			if (o.flag.equals("-iter")) {
				// iter number
				if (o.opt == null) {
					System.out.println("-iter NR");
					System.exit(0);
				}
				iter = Long.parseLong(o.opt);
			}
		}
		
		bTs = cday - (kdays * 86400);
		System.out.println("Begin day time is " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(bTs * 1000)));
		if (fromRP == null || toRP == null) {
			System.out.println("Invalid arguments.");
			System.exit(0);
		}
		
		Jedis fromR, toR;
		
		String[] frp = fromRP.split(":");
		String[] trp = toRP.split(":");
		
		if (frp.length < 2 || trp.length < 2) {
			System.out.println("Invalid arguments.");
			System.exit(0);
		}
		fromR = new Jedis(frp[0], Integer.parseInt(frp[1]), 120 * 1000);
		toR = new Jedis(trp[0], Integer.parseInt(trp[1]), 60 * 1000);
		
		Map<String, String> infos = new HashMap<String, String>(); 
		ScanParams sp = new ScanParams();
		sp.match("*");
		boolean isDone = false;
		String cursor = ScanParams.SCAN_POINTER_START;
		
		while (!isDone) {
			ScanResult<Entry<String, String>> r = fromR.hscan("mm.dedup.info", cursor, sp);
			for (Entry<String, String> entry : r.getResult()) {
				infos.put(entry.getKey(), entry.getValue());
			}
			cursor = r.getStringCursor();
			if (cursor.equalsIgnoreCase("0")) {
				isDone = true;
			}
			j++;
			if (j > iter)
				break;
		}
		
		if (infos != null && infos.size() > 0) {
			for (Map.Entry<String, String> entry : infos.entrySet()) {
				String[] k = entry.getKey().split("@");
				long ts = -1;
				
				if (k.length == 2) {
					try {
						if (Character.isDigit(k[0].charAt(0))) {
							ts = Long.parseLong(k[0]);
						} else {
							ts = Long.parseLong(k[0].substring(1));
						}
					} catch (Exception e) {
						// ignore it
						System.out.println("Ignore set '" + k[0] + "'.");
					}
				}
				if (ts >= 0 && ts < bTs) {
					String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts * 1000));
					toR.hset("mm.dedup." + date, entry.getKey(), entry.getValue());
					fromR.hdel("mm.dedup.info", entry.getKey());
				}
			}
		}
		
		fromR.quit();
		toR.quit();
	}
}
