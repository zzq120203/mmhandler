package iie.mm.tools;

import iie.mm.client.ClientAPI;
import iie.mm.common.MMConf;
import iie.mm.common.RedisPool;
import iie.mm.common.RedisPoolSelector;
import iie.mm.common.RedisPoolSelector.RedisConnection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

/**
 * 多媒体元数据扫描工具
 * 检查副本分布情况，检查各个节点的数据情况，各个节点故障后可能丢失的数据量有多少
 */
public class MMRepChecker {
	private MMConf conf;
	private RedisPool rpL1;
	private RedisPoolSelector rps;
	private HashMap<String, String> sidname = new HashMap<String, String>();
	private HashMap<String, Long> sidnum = new HashMap<String, Long>();
	private ClientAPI ca;
	
	public MMRepChecker(String uri) throws Exception {
		this.conf = new MMConf();
		init(uri);

		Jedis jedis = rpL1.getResource();
		
		if (jedis != null) {
			try {
				Set<Tuple> re = jedis.zrangeWithScores("mm.active", 0, -1);

				if (re != null) {
					for (Tuple t : re) {
						String ipport = jedis.hget("mm.dns", t.getElement());
						if (ipport != null)
							sidname.put(((int)t.getScore()) + "", ipport);
						else 
							sidname.put(((int)t.getScore()) + "", t.getElement());
						sidnum.put(((int)t.getScore()) + "", 0l);
						System.out.println((int)t.getScore() + "  " + t.getElement());
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				rpL1.putInstance(jedis);
			}
		}
	}

	public static class Option {
		String flag, opt;

		public Option(String flag, String opt) {
			this.flag = flag;
			this.opt = opt;
		}
	}

	private static List<Option> parseArgs(String[] args) {
		List<Option> optsList = new ArrayList<Option>();

		// parse the args
		for (int i = 0; i < args.length; i++) {
			System.out.println("Args " + i + ", " + args[i]);
			switch (args[i].charAt(0)) {
			case '-':
				if (args[i].length() < 2) {
					throw new IllegalArgumentException("Not a valid argument: "
							+ args[i]);
				}
				if (args[i].charAt(1) == '-') {
					if (args[i].length() < 3) {
						throw new IllegalArgumentException(
								"Not a valid argument: " + args[i]);
					}
				} else {
					if (args.length - 1 > i) {
						if (args[i + 1].charAt(0) == '-') {
							optsList.add(new Option(args[i], null));
						} else {
							optsList.add(new Option(args[i], args[i + 1]));
							i++;
						}
					} else {
						optsList.add(new Option(args[i], null));
					}
				}
				break;
			default:
				// arg
				break;
			}
		}

		return optsList;
	}
	
	public void __init() throws Exception {
		switch (conf.getRedisMode()) {
		case SENTINEL:
			rpL1 = new RedisPool(conf, "l1.master");
			break;
		case STANDALONE:
			rpL1 = new RedisPool(conf, "nomaster");
			break;
		}
		rps = new RedisPoolSelector(conf, rpL1);
	}
	
	public void quit() {
		rps.quit();
		rpL1.quit();
	}
	
	private int init_by_sentinel(MMConf conf, String urls) throws Exception {
		if (conf.getRedisMode() != MMConf.RedisMode.SENTINEL) {
			return -1;
		}
		// iterate the sentinel set, get master IP:port, save to sentinel set
		if (conf.getSentinels() == null) {
			if (urls == null) {
				throw new Exception("Invalid URL(null) or sentinels.");
			}
			HashSet<String> sens = new HashSet<String>();
			String[] s = urls.split(";");
			
			for (int i = 0; i < s.length; i++) {
				sens.add(s[i]);
			}
			conf.setSentinels(sens);
		}
		__init();

		return 0;
	}
	
	private int init_by_standalone(MMConf conf, String urls) throws Exception {
		if (conf.getRedisMode() != MMConf.RedisMode.STANDALONE) {
			return -1;
		}
		// get IP:port, save it to HaP
		if (urls == null) {
			throw new Exception("Invalid URL: null");
		}
		String[] s = urls.split(":");
		if (s != null && s.length == 2) {
			try {
				HostAndPort hap = new HostAndPort(s[0], 
						Integer.parseInt(s[1]));
				conf.setHap(hap);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		__init();

		return 0;
	}
	
	public void init(String urls) throws Exception {
		if (urls == null) {
			throw new Exception("The url can not be null.");
		}
		if (urls.startsWith("STL://")) {
			urls = urls.substring(6);
			conf.setRedisMode(MMConf.RedisMode.SENTINEL);
		} else if (urls.startsWith("STA://")) {
			urls = urls.substring(6);
			conf.setRedisMode(MMConf.RedisMode.STANDALONE);
		}
		switch (conf.getRedisMode()) {
		case SENTINEL:
			init_by_sentinel(conf, urls);
			break;
		case STANDALONE:
			init_by_standalone(conf, urls);
			break;
		case CLUSTER:
			System.out.println("MMS do NOT support CLUSTER mode now, " +
					"use STL/STA instead.");
			break;
		default:
			break;
		}
	}

	public static void main(String[] args) {
		String uri = null;
		boolean doFix = false;
		int fn = 1;
		
		if (args.length >= 1) {
			List<Option> ops = parseArgs(args);

			for (Option o : ops) {
				if (o.flag.equals("-h")) {
					// print help message
					System.out.println("-h    : print this help.");
					System.out.println("-uri  : unified uri for SENTINEL and STANDALONE.");
					System.exit(0);
				}
				if (o.flag.equals("-uri")) {
					if (o.opt == null) {
						System.out.println("-uri URI");
						System.exit(0);
					}
					uri = o.opt;
				}
				if (o.flag.equals("-fix")) {
					doFix = true;
					if (o.opt != null) {
						fn = Integer.parseInt(o.opt);
					}
					System.out.println("Use " + fn + " threads to do fix");
				}
			}
		}

		if (uri == null) {
			System.out.println("No valid URI provided.");
			System.exit(0);
		}
		try {
			MMRepChecker checker = new MMRepChecker(uri);
			Map<String, String> undup = checker.check();
			System.out.println("Total unreplicated objects: " + undup.size());
			if (doFix) {
				checker.fix_parallel(uri, undup, fn);
			}
			checker.quit();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void fix(String uri, Map<String, String> toRep, int id) throws Exception {
		for (Map.Entry<String, String> entry : toRep.entrySet()) {
			String info = ca.xput(entry.getKey(), entry.getValue(), "X");
			System.out.println("[" + id + "] Fix " + entry.getKey() + " -> " + info);
		}
	}

	private class FixThread extends Thread {
		public MMRepChecker checker;
		public String uri;
		public Map<String, String> toRep;
		public int id;

		public FixThread(int id, MMRepChecker checker, String uri, 
				Map<String, String> toRep) {
			this.id = id;
			this.checker = checker;
			this.uri = uri;
			this.toRep = toRep;
		}

		public void run() {
			try {
				checker.fix(uri, toRep, id);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public void fix_parallel(String uri, Map<String, String> toRep, int fn) 
			throws Exception {
		ArrayList<TreeMap<String, String>> sn = 
				new ArrayList<TreeMap<String, String>>();
		int i;

		ca = new ClientAPI();
		ca.init(uri, "TEST");
		for (i = 0; i < fn; i++) {
			sn.add(new TreeMap<String, String>());
		}
		i = 0;
		for (Map.Entry<String, String> e : toRep.entrySet()) {
			sn.get(i % fn).put(e.getKey(), e.getValue());
			i++;
		}
		ArrayList<FixThread> fts = new ArrayList<FixThread>();
		for (i = 0; i < fn; i++) {
			fts.add(new FixThread(i, this, uri, sn.get(i)));
		}
		for (FixThread ft : fts) {
			ft.start();
		}
		for (FixThread ft : fts) {
			try {
				ft.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		ca.quit();
	}

	public TreeMap<String, String> check() throws Exception {
		TreeMap<String, String> undup = new TreeMap<String, String>();
		System.out.println("A: only one copy");
		System.out.println("B: all copies on the same server\n");
		
		for (Map.Entry<String, RedisPool> entry : rps.getRpL2().entrySet()) {
			undup.putAll(check(entry.getValue().getPid()));
		}
		return undup;
	}

	public class SetStats {
		String set;
		long total_nr;
		long dup_nr;
		
		public SetStats(String set) {
			this.set = set;
			total_nr = 0;
			dup_nr = 0;
		}
		
		public String toString() {
			return "Set " + set + " -> Total " + total_nr + " nonRep " + dup_nr + 
					" Health Ratio " + ((double)(total_nr - dup_nr) / total_nr * 100) + "%";
		}
	}
	
	public HashMap<String, String> check(String pid) throws Exception {
		long start = System.currentTimeMillis();
		int totalm = 0;
		HashMap<String, String> notdup = new HashMap<String, String>();
		TreeMap<String, SetStats> setInfo = new TreeMap<String, SetStats>();

		RedisConnection rc = rps.getL2ByPid(pid);

		if (rc == null || rc.rp == null || rc.jedis == null) {
			throw new Exception("Get redis connection by pid " + pid + " failed.");
		}
		Jedis jedis = rc.jedis;

		Set<String> keys = new HashSet<String>();

		try {
			for (String s : jedis.keys("*.srvs")) {
				s = s.replaceAll("\\.srvs", "");
				keys.add(s);
			}

			for (String set : keys) {
				long tnr = jedis.hlen(set);

				totalm += tnr;

				SetStats ss = setInfo.get(set);
				if (ss == null)
					ss = new SetStats(set);
				ss.total_nr = tnr;

				Map<String, String> setentrys = jedis.hgetAll(set);
				if (setentrys != null) {
					for (Map.Entry<String, String> en : setentrys.entrySet()) {
						String[] infos = en.getValue().split("#");
						if (infos.length == 1) {
							notdup.put(set + "@" + en.getKey(), en.getValue());
							System.out.println("A: " + set + "@"	+ en.getKey() + 
									" --> " + en.getValue());
							String id = MMRepChecker.getServerid(infos[0]);
							long n = sidnum.get(id) + 1;
							sidnum.put(id, n);
							ss.dup_nr++;
						} else {
							String id = MMRepChecker.getServerid(infos[0]);
							boolean duped = true;
							for (int i = 1; i < infos.length; i++) {
								if (!MMRepChecker.getServerid(infos[i]).equals(id)) {
									duped = false;
									break;
								}
							}
							if (duped) {
								notdup.put(set + "@" + en.getKey(), en.getValue());
								System.out.println("B: " + set + "@" + en.getKey() + 
										" --> " + en.getValue());
								long n = sidnum.get(id) + 1;
								sidnum.put(id, n);
								ss.dup_nr++;
							}
						}
					}
				}
				setInfo.put(set, ss);
			}

			System.out.println();
			System.out.println("Check meta data takes " + 
					(System.currentTimeMillis() - start) + " ms");
			System.out.println("Checked objects num: " + totalm + ", unreplicated: " + 
					notdup.size() + ", healthy ratio " + 
					((double)(totalm - notdup.size()) / totalm * 100) + "%");

			for (Map.Entry<String, Long> e : sidnum.entrySet()) {
				System.out.println("If server " + sidname.get(e.getKey()) + " down, " + 
						e.getValue() + " mm objects may be lost.");
			}
			System.out.println();
			System.out.println("Per-Set Replicate info: ");
			for (Map.Entry<String, SetStats> e : setInfo.descendingMap().entrySet()) {
				System.out.println(e.getValue());
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			rps.putL2(rc);
		}
		
		return notdup;
	}
	
	/**
	 * type@set@serverid@block@offset@length@disk
	 * @param info
	 * @return
	 */
	public static String getServerid(String info) {
		String[] ss = info.split("@");
		if (ss.length != 7)
			throw new IllegalArgumentException("Invalid info " + info);
		return ss[2];
	}
}
