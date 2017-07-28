package iie.mm.tools;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import redis.clients.jedis.Jedis;

import iie.mm.client.ClientAPI;

public class RedisInfoMonitor {
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
	    
	    String logpath = "log";
	    String uri = null;
	    String host = "localhost";
	    int port = 26379;
	    
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
	    	if (o.flag.equals("-p")) {
	    		// set log file path
	    		if (o.opt == null) {
	    			System.out.println("-p   : log file path.");
	    			System.exit(0);
	    		}
	    		logpath = o.opt;
	    	}
	    }
	    
	    Date d = new Date(System.currentTimeMillis());
	    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	    File reportFile = new File(logpath + "/redisinfo-" + sdf.format(d));
	    if (reportFile.getParentFile() != null)
	    	if (!reportFile.getParentFile().exists() && !reportFile.getParentFile().mkdirs()) {
	    		return;
	    	}
	    
	    String ANSI_RESET = "\u001B[0m";
	    String ANSI_RED = "\u001B[31m";
	    String ANSI_GREEN = "\u001B[32m";
	    
	    try {
	    	// parset uri
	    	if (uri != null && uri.startsWith("STL://")) {
	    		// sentinel mode, ok
	    		uri = uri.substring(6);
	    		String[] sentinels = uri.split(";|,");
	    		if (sentinels.length <= 0) {
	    			throw new Exception("[ERROR] Invalid URI: " + uri);
	    		} else {
	    			String[] hap = sentinels[0].split(":");
	    			if (hap.length == 2) {
	    				host = hap[0];
	    				port = Integer.parseInt(hap[1]);
	    			}
	    		}
	    	} else {
	    		throw new Exception("[ERROR] You must supply a valid URI STL://...");
	    	}
	    	Jedis j = new Jedis(host, port);
	    	List<Map<String, String>> masters = j.sentinelMasters();
	    	Map<String, String> servers = new TreeMap<String, String>();
	    	
	    	if (masters != null) {
	    		for (Map<String, String> m : masters) {
	    			String mline = "";
	    			System.out.println("Service Instance: " + m.get("name"));
	    			
	    			servers.put(m.get("name"), m.get("ip") + ":" + m.get("port"));
	    			
	    			mline += " -> Master\t" + m.get("ip") + ":" + m.get("port") + 
	    					" flags " + ANSI_GREEN + m.get("flags") + ANSI_RESET +
	    					" last-ping-reply " + ANSI_GREEN + m.get("last-ping-reply") + ANSI_RESET;
	    			if (m.get("s-down-time") != null)
	    				mline += " s-down-time " + ANSI_RED + m.get("s-down-time") + ANSI_RESET;
	    			if (m.get("o-down-time") != null)
	    				mline += " o-down-time " + ANSI_RED + m.get("o-down-time") + ANSI_RESET;
	    			System.out.println(mline);
	    			
	    			for (Map.Entry<String, String> e1 : m.entrySet()) {
	    				//System.out.println(e1.getKey() + " -> " + e1.getValue());
	    			}
	    			List<Map<String, String>> slaves = j.sentinelSlaves(m.get("name"));
	    			if (slaves != null) {
	    				for (Map<String, String> s : slaves) {
	    					if (s != null) {
	    						String sline = "";
	    						
	    						servers.put(s.get("name"), s.get("ip") + ":" + s.get("port"));
	    						
	    						sline += " -> Slave\t" + s.get("ip") + ":" + 
	    								s.get("port") +
	    								" flags " + ANSI_GREEN + s.get("flags") + ANSI_RESET +  
	    								" last-ping-reply " + ANSI_GREEN + s.get("last-ping-reply") + ANSI_RESET + 
	    								" master-link-status " + (s.get("master-link-status").equals("ok") ? ANSI_GREEN : ANSI_RED) + s.get("master-link-status") + ANSI_RESET;
	    								//" master " + s.get("master-host") + ":" + s.get("master-port") +
	    						if (s.get("s-down-time") != null)
	    								sline += " s-down-time " + ANSI_RED + s.get("s-down-time") + ANSI_RESET;
	    						if (s.get("o-down-time") != null)
	    								sline += " o-down-time " + ANSI_RED + s.get("o-down-time") + ANSI_RESET;
	    						System.out.println(sline);
	    						for (Map.Entry<String, String> e2 : s.entrySet()) {
	    							//System.out.println(e2.getKey() + " -> " + e2.getValue());
	    						}
	    					}
	    				}
	    			}
	    		}
	    	}
	    	j.close();
	    	
	    	// handle redis servers, call info to gather stats
	    	List<String> filterKeys = new ArrayList<String>(), dup;
	    	filterKeys.add("total_commands_processed");
	    	filterKeys.add("instantaneous_ops_per_sec");
	    	filterKeys.add("used_memory");
	    	filterKeys.add("used_memory_rss");
	    	filterKeys.add("used_memory_peak");
	    	filterKeys.add("rdb_changes_since_last_save");
	    	filterKeys.add("rdb_bgsave_in_progress");
	    	filterKeys.add("rdb_last_bgsave_status");
	    	filterKeys.add("sync_full");
	    	filterKeys.add("sync_partial_ok");
	    	filterKeys.add("sync_partial_err");
	    	filterKeys.add("connected_slaves");
	    	filterKeys.add("master_repl_offset");
	    	filterKeys.add("latest_fork_usec");
	    	filterKeys.add("master_link_status");
	    	
	    	System.out.println("Redis Instances: ");
	    	for (Map.Entry<String, String> redis : servers.entrySet()) {
	    		System.out.println(" -> " + redis.getValue() + " {");
	    		String cts = System.currentTimeMillis() / 1000 + ",";
	    		String xhost, line = "" , logline = redis.getValue() + "," + cts;
	    		dup = new ArrayList<String>(filterKeys);
	    		int xport;

	    		try {
	    			String[] hap = redis.getValue().split(":");
	    			if (hap.length != 2) {
	    				throw new Exception("Invalid redis host and port.");
	    			} else {
	    				xhost = hap[0];
	    				xport = Integer.parseInt(hap[1]);
	    			}
	    			j = new Jedis(xhost, xport);
	    			// Stats
	    			String stats = j.info();
	    			String[] l = stats.split("\r\n");
	    			for (String x : l) {
	    				for (int i = 0; i < dup.size(); i++) {
	    					if (x.startsWith(dup.get(i) + ":")) {
	    						line += dup.get(i) + " -> " + x.substring(dup.get(i).length() + 1) + "\n";
	    						//logline += x.substring(y.length() + 1) + ",";
	    						String r = x.substring(dup.get(i).length() + 1);
	    						if (r.equals("ok") || r.equals("up")) {
	    							r = "1";
	    						} else if (r.equals("err") || r.equals("down")) {
	    							r = "0";
	    						}
	    						dup.set(i, "" + r);
	    					}
	    				}
	    			}
	    			if (redis.getKey().contains(":")) {
	    				logline += "-1,";
	    			} else 
	    				logline += "1,";
	    			for (int i = 0; i < dup.size(); i++) {
	    				if (!dup.get(i).equals(filterKeys.get(i)))
	    					logline += dup.get(i) + ",";
	    				else
	    					logline += "-1,";
	    			}
	    			j.close();
	    		} catch (Exception e) {
	    			line = ANSI_RED + "LINK ERROR" + ANSI_RESET;
	    		}
	    		System.out.println(line);
	    		System.out.println(logline);
	    		System.out.println("}");
	    		
	    		// ok, write to file
	    		try {
	    			if (!reportFile.exists()) {
	    				reportFile.createNewFile();
	    			}
	    			FileWriter fw = new FileWriter(reportFile.getAbsoluteFile(), true);
	    			BufferedWriter bw = new BufferedWriter(fw);
	    			bw.write(logline);
	    			bw.write("\n");
	    			bw.close();
	    		} catch (IOException e) {
	    			e.printStackTrace();
	    		}
	    	}
	    } catch (Exception e) {
	    	e.printStackTrace();
	    }
	}

}
