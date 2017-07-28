package iie.mm.tools;

import iie.mm.client.ClientAPI;
import iie.mm.client.Feature.FeatureTypeString;
import iie.mm.server.FeatureSearch;
import iie.mm.server.ServerConf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MMObjectImporter {
	public static ClientAPI ca;
	
	public static class Option {
	     String flag, opt;
	     public Option(String flag, String opt) { this.flag = flag; this.opt = opt; }
	}

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
	                    	optsList.add(new Option(args[i], args[i+1]));
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
	    
	    String uri = null, index_path = "./";
	    long mget_begin_time = 0;
	    
	    for (Option o : optsList) {
	    	if (o.flag.equals("-h")) {
	    		// print help message
	    		System.out.println("-h    : print this help.");
	    		System.out.println();
	    		System.exit(0);
	    	}
	    	if (o.flag.equals("-uri")) {
	    		// set uri
	    		if (o.opt == null) {
	    			System.out.println("-uri SERVER_URI");
	    			System.exit(0);
	    		}
	    		uri = o.opt;
	    	}
	    	if (o.flag.equals("-mget_begin_time")) {
				if (o.opt == null) {
					System.out.println("-mget_begin_time TIME");
					System.exit(0);
				}
				mget_begin_time = Long.parseLong(o.opt);
			}
	    	if (o.flag.equals("-index_path")) {
	    		if (o.opt == null) {
	    			System.out.println("-index_path PATH");
	    			System.exit(0);
	    		}
	    		index_path = o.opt;
	    	}
	    }
	    
	    List<String> r;
	    try {
	    	ca = new ClientAPI();
	    	ca.init(uri, "TEST");
	    	
	    	//计算图片hash值的线程
	    	ServerConf conf = new ServerConf(-1);
	    	conf.setFeatureIndexPath(index_path);
	    	conf.addToFeatures(FeatureTypeString.IMAGE_PHASH_ES);
			conf.addToFeatures(FeatureTypeString.IMAGE_LIRE);
			conf.setIndexFeatures(true);
	    	
	    	FeatureSearch im = new FeatureSearch(conf);
	    	im.startWork(4);

	    	String rset = null;
	    	Map<String, String> cookies = new HashMap<String, String>();
	    	long begin, end;

	    	begin = System.nanoTime();
	    	r = ca.getkeys("image", mget_begin_time);
	    	end = System.nanoTime();
	    	if (r.size() > 0)
	    		rset = r.get(0).split("@")[0];
	    	System.out.println("Got nr " + r.size() + " keys from Set " + rset + " in " + ((end - begin) / 1000.0) + " us.");

	    	long tlen = 0;
	    	long ttime = 0;
	    	do {
	    		begin = System.nanoTime();
	    		List<byte[]> b = ca.mget(r, cookies);
	    		end = System.nanoTime();
	    		ttime += (end - begin);

	    		long len = 0;
	    		for (byte[] v : b) {
	    			if (v != null)
	    				len += v.length;
	    		}
	    		System.out.println("Got nr " + b.size() + " vals len " + len + "B in " + ((end - begin) / 1000.0) + " us, BW is " + (len / ((end - begin) / 1000000.0)) + " KB/s.");
	    		System.out.flush();
	    		tlen += len;

	    		// do check now
	    		int idx = Integer.parseInt(cookies.get("idx"));
	    		for (int i = 0; i < b.size(); i++) {
	    			// do index now
	    			String md5 = r.get(idx - b.size() + i).split("@")[1];
	    			System.out.println("Indexing " + rset + "@" + md5);
	    			FeatureSearch.add(conf, new FeatureSearch.ImgKeyEntry(
	    					conf.getFeatures(), b.get(i), 0, b.get(i).length, 
	    					rset, md5));
	    		}
	    		// wait for indexing
	    		while (FeatureSearch.getQueueLength() > 100) {
	    			try {
	    				Thread.sleep(1000);
	    			} catch (InterruptedException ie) {
	    			}
	    		}
	    		//if (idx >= r.size() - 1)
	    		if (b.size() == 0)
	    			break;
	    	} while (true);

	    	System.out.println(" -> cookies: " + cookies);
	    	System.out.println("MGET nr " + r.size() + " size " + tlen + "B : BW " + 
	    			(tlen / (ttime / 1000000.0)) + " KB/s");

	    } catch (Exception e) {
	    	e.printStackTrace();
	    }
	    
	    while (true) {
	    	if (!FeatureSearch.isEmpty() || FeatureSearch.isActive.get() > 0) {
	    		try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
	    	} else
	    		System.exit(0);
	    }
	}
}
