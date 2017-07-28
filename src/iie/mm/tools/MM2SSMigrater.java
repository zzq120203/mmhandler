package iie.mm.tools;

import iie.mm.client.ClientAPI;
import iie.mm.common.RedisPool;
import iie.mm.common.RedisPoolSelector.RedisConnection;
import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.fusesource.lmdbjni.*;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisException;
import static org.fusesource.lmdbjni.Constants.*;

public class MM2SSMigrater {
	Env env = new Env();
	Database db = null;
	public String db_path = "./lmdb";
	
	public boolean open(String prefix) {
		boolean r = true;
		try {
			env.setMapSize(1024L * 1024 * 1024 * 1024);
			File f = new File(prefix + "/" + db_path);
			f.mkdirs();
			env.open(prefix + "/" + db_path);
			db = env.openDatabase("mm-master", org.fusesource.lmdbjni.Constants.CREATE);
		} catch (Exception e) {
			e.printStackTrace();
			r = false;
		}
		return r;
	}
	
	public void write(String key, String value) {
		db.put(bytes(key), bytes(value));
	}
	
	public String read(String key) {
		return string(db.get(bytes(key)));
	}
	
	public void scan() {
		Transaction tx = env.createTransaction(true);
		try {
			Cursor cursor = db.openCursor(tx);
			try {
				for (Entry entry = cursor.get(FIRST); 
						entry !=null; 
						entry = cursor.get(NEXT) ) {
					String key = string(entry.getKey());
					String value = string(entry.getValue());
					System.out.println(key+" = "+value);
				}
			} finally {
				// Make sure you close the cursor to avoid leaking reasources.
				cursor.close();
			}

		} finally {
			// Make sure you commit the transaction to avoid resource leaks.
			tx.commit();
		}
	}
	
	public void close() {
		try {
			if (db != null)
				db.close();
		} finally {
			env.close();
		}
	}
	
	public static void do_scan(String prefix) {
		MM2SSMigrater mdb = new MM2SSMigrater();
		
		if (mdb.open(prefix)) {
			mdb.scan();
			mdb.close();
		}
	}
	
	public static class Option {
	     String flag, opt;
	     public Option(String flag, String opt) { this.flag = flag; this.opt = opt; }
	}
	
	public static void main(String[] args) throws Exception {
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

	    String uri = null;
	    String daystr = null;
	    String prefix = ".";
	    boolean do_delete = false;
	    
	    for (Option o : optsList) {
	    	if (o.flag.equals("-h")) {
	    		// print help message
	    		System.out.println("-h     : print this help.");
	    		System.out.println("-uri   : unified uri for SENTINEL and STANDALONE.");
	    		System.out.println("-daystr: set YYYY-MM-DD.");
	    		System.out.println("-dump  : dump the LMDB database.");
	    		System.out.println();
	    		
	    		System.exit(0);
	    	}
	    	if (o.flag.equals("-uri")) {
	    		// parse uri
	    		if (o.opt == null) {
	    			System.out.println("-uri URI");
	    			System.exit(0);
	    		}
	    		uri = o.opt;
	    	}
	    	if (o.flag.equals("-daystr")) {
	    		// parse daystr
	    		if (o.opt == null) {
	    			System.out.println("-daystr DAY");
	    			System.exit(0);
	    		}
	    		daystr = o.opt;
	    	}
	    	if (o.flag.equals("-dump")) {
	    		do_scan(prefix);
	    		System.exit(0);
	    	}
	    	if (o.flag.equals("-del")) {
	    		// set delete flag
	    		do_delete = true;
	    	}
	    	if (o.flag.equals("-prefix")) {
	    		// set prefix path
	    		if (o.opt == null) {
	    			System.out.println("-prefix LMDB path");
	    			System.exit(0);
	    		}
	    		prefix = o.opt;
	    	}
	    }
	    
	    System.out.println("LMDB path prefix: " + prefix);
	    
	    // try to connect to redis master
	    ClientAPI ca = new ClientAPI();
	    try {
			ca.init(uri, "TEST");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	    if (daystr == null) {
	    	System.out.println("Please set -daystr DAY");
	    	System.exit(0);
	    }

	    // open local mdb file
	    MM2SSMigrater mdb = new MM2SSMigrater();
	    if (mdb.open(prefix)) {
	    	try {
				recycleSet(ca, mdb, daystr, do_delete);
			} catch (Exception e) {
				e.printStackTrace();
			}
	    	// close local mdb file
	    	mdb.close();
	    }
	    ca.quit();
	}

	private static void recycleSet(ClientAPI ca, MM2SSMigrater mdb, String dstr, 
			boolean do_delete) throws Exception {
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date d = df.parse(dstr);
		Jedis jedis = null;
		
		int err = 0;
		
		// get all sets
		TreeSet<String> temp = new TreeSet<String>();
		for (Map.Entry<String, RedisPool> entry : ca.getPc().getRPS().getRpL2().entrySet()) {
			jedis = entry.getValue().getResource();
			if (jedis != null) {
				try {
					Set<String> keys = jedis.keys("*.blk.*");

					if (keys != null && keys.size() > 0) {
						String[] keya = keys.toArray(new String[0]);

						for (int i = 0; i < keya.length; i++) {
							String set = keya[i].split("\\.")[0];

							if (Character.isDigit(set.charAt(0))) {
								temp.add(set);
							} else {
								temp.add(set.substring(1));
							}
						}
					}
				} finally {
					entry.getValue().putInstance(jedis);
				}
			}
		}
		
		try {
			String[] prefixs = new String[]{"", "i", "t", "a", "v", "o", "s"};
			boolean should_stop = false;
			
			jedis = ca.getPc().getRpL1().getResource();
			if (jedis == null) {
				throw new Exception("Cound not get avaliable L1 Jedis instance.");
			}
			for (String set : temp) {
				if (should_stop)
					break;
				try {
					long thistime = Long.parseLong(set);
					if (d.getTime() > thistime * 1000) {
						// ok, delete it
						System.out.println("Migrate Set:\t" + set + "\t" + 
								df2.format(new Date(thistime * 1000)));
						
						for (String prefix : prefixs) {
							try {
								if (!migrateSet(ca, prefix + set, mdb, do_delete)) {
									// we should stop here, admin need to handle the error
									System.out.println("Migrate should stop because of " +
											"some errors: " + set);
									should_stop = true;
									break;
								}
							} catch (Exception e) {
								System.out.println("MEE: on set " + prefix + set + "," + 
										e.getMessage());
							}
						}
						// ok, update ckpt timestamp: if > ts, check redis, else check lmdb.
						String saved = jedis.get("mm.ckpt.ts");
						long saved_ts = 0, this_ts = 0;
						if (saved != null) {
							try {
								saved_ts = Long.parseLong(saved);
							} catch (Exception e) {
							}
						}
						try {
							this_ts = Long.parseLong(set);
						} catch (Exception e) {
						}
						if (this_ts > saved_ts)
							jedis.set("mm.ckpt.ts", set);
						System.out.println("saved=" + saved_ts + ", this=" + this_ts);
					}
				} catch (NumberFormatException nfe) {
					System.out.println("NFE: on set " + set);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			temp = null;
		} finally {
			ca.getPc().getRpL1().putInstance(jedis);
		}
		
	}

	private static boolean migrateSet(ClientAPI ca, String set, MM2SSMigrater mdb, 
			boolean do_delete) throws Exception {
		boolean r = false;
		RedisConnection rc = ca.getPc().getRPS().getL2(set, false);
		int nr = 0;
		
		if (rc.rp == null || rc.jedis == null) {
			throw new Exception("Could not get avaliable L2 pool " + rc.id + 
					" instance.");
		}
		Jedis jedis = rc.jedis;

		try {
			// Save all the server info for this set
			Iterator<String> ir = jedis.smembers(set + ".srvs").iterator();
			String info;
			int idx = 0;
			
			while (ir.hasNext()) {
				info = ir.next();
				mdb.write("S#" + set + ".srvs$" + idx, info);
				idx++;
			}
			if (idx > 0)
				mdb.write("S#" + set + ".srvs~size", "" + idx);
			if (do_delete)
				jedis.del(set + ".srvs");

			// Save all the block info for this set
			Iterator<String> ikeys1 = jedis.keys(set + ".blk.*").iterator();
			
			while (ikeys1.hasNext()) {
				String key1 = ikeys1.next();
				try {
					String value = jedis.get(key1);
					mdb.write("B#" + key1, value);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			if (do_delete) {
				ikeys1 = jedis.keys(set + ".blk.*").iterator();
				Pipeline pipeline1 = jedis.pipelined();
				
				while (ikeys1.hasNext()) {
					String key1 = ikeys1.next();
					pipeline1.del(key1);
				}
				pipeline1.sync();
			}

			// Save the entries of this set
			Map<String, String> hall = jedis.hgetAll(set);
			nr = hall.size();
			for (Map.Entry<String, String> entry : hall.entrySet()) {
				mdb.write("H#" + set + "." + entry.getKey(), entry.getValue());
			}
			if (do_delete)
				jedis.del(set);
			r = true;
		} catch (LMDBException le) {
			System.out.println("LMDBException: " + le.getLocalizedMessage());
		} catch (JedisException je) {
			je.printStackTrace();
		} finally {
			ca.getPc().getRPS().putL2(rc);
		}
		System.out.println("Mig Set '" + set + "' complete, mig=" + nr);
		
		return r;
	}
}
