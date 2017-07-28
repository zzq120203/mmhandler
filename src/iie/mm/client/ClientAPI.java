package iie.mm.client;

import iie.mm.client.PhotoClient.SocketHashEntry;
import iie.mm.common.MMConf;
import iie.mm.common.RedisPoolSelector.RedisConnection;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import net.sf.json.JSONObject;

import javax.activation.MimetypesFileTypeMap;

public class ClientAPI {
	private PhotoClient pc;
	private AtomicInteger index = new AtomicInteger(0);
	private List<String> keyList = Collections.synchronizedList(new ArrayList<String>());
	// 缓存与服务端的tcp连接,服务端名称到连接的映射
	private ConcurrentHashMap<String, SocketHashEntry> socketHash;
	private final Timer timer = new Timer("ActiveMMSRefresher");
	//
	public ClientAPI(ClientConf conf) {
		pc = new PhotoClient(conf);
	}

	public ClientAPI() {
		pc = new PhotoClient();
	}

	public enum MMType {
		TEXT, IMAGE, AUDIO, VIDEO, APPLICATION, THUMBNAIL, OTHER,
	}

	public static String getMMTypeSymbol(MMType type) {
		switch (type) {
		case TEXT:
			return "t";
		case IMAGE:
			return "i";
		case AUDIO:
			return "a";
		case VIDEO:
			return "v";
		case APPLICATION:
			return "o";
		case THUMBNAIL:
			return "s";
		case OTHER:
		default:
			return "";
		}
	}

	/**
	 * DO NOT USE this function unless if you know what are you doing.
	 *
	 * @return PhotoClient
	 */
	public PhotoClient getPc() {
		return pc;
	}

	private void updateClientConf(Jedis jedis, ClientConf conf) {
		if (conf == null || !conf.isAutoConf() || jedis == null)
			return;
		try {
			String dupMode = jedis.hget("mm.client.conf", "dupmode");
			if (dupMode != null) {
				if (dupMode.equalsIgnoreCase("dedup")) {
					conf.setMode(ClientConf.MODE.DEDUP);
				} else if (dupMode.equalsIgnoreCase("nodedup")) {
					conf.setMode(ClientConf.MODE.NODEDUP);
				} else if (dupMode.equalsIgnoreCase("onlydata")) {
					conf.setMode(ClientConf.MODE.ONLYDATA);
				}
			}

			String blen = jedis.hget("mm.client.conf", "blen");
			if (blen != null) {
				int bl = Integer.parseInt(blen);
				if (bl > 0)
					conf.setBlen(bl);
			}

			String vlen = jedis.hget("mm.client.conf", "vlen");
			if (vlen != null) {
				int vl = Integer.parseInt(vlen);
				if (vl > 0)
					conf.setVlen(vl);
			}

			String dupNum = jedis.hget("mm.client.conf", "dupnum");
			if (dupNum != null) {
				int dn = Integer.parseInt(dupNum);
				if (dn > 1)
					conf.setDupNum(dn);
			}
			String sockPerServer = jedis.hget("mm.client.conf", "sockperserver");
			if (sockPerServer != null) {
				int sps = Integer.parseInt(sockPerServer);
				if (sps >= 1)
					conf.setSockPerServer(sps);
			}
			String dupinfo = jedis.hget("mm.client.conf", "dupinfo");
			if (dupinfo != null) {
				int di = Integer.parseInt(dupinfo);
				if (di > 0)
					conf.setLogDupInfo(true);
				else
					conf.setLogDupInfo(false);
			}
			String mgetTimeout = jedis.hget("mm.client.conf", "mgetto");
			if (mgetTimeout != null) {
				int to = Integer.parseInt(mgetTimeout);
				if (to > 0)
					conf.setMgetTimeout(to * 1000);
			}

			String DataSynchronization = jedis.hget("mm.client.conf", "ds");
			if (DataSynchronization != null) {
				int ds = Integer.parseInt(DataSynchronization);
				if (ds > 0)
					conf.setDataSync(true);
				else
					conf.setDataSync(false);
			}
			Map<String, String> ftmap = jedis.hgetAll("mm.ftype");
			if (ftmap != null) {
				conf.setFileTypeMap(ftmap);
			}
			System.out.println("Auto conf client with: dupMode=" + conf.getMode() + ", dupNum=" + conf.getDupNum()
					+ ", logDupInfo=" + conf.isLogDupInfo() + ", sockPerServer=" + conf.getSockPerServer()
					+ ", mgetTimeout=" + conf.getMgetTimeout() + ", blockLen=" + conf.getBlen());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private int init_by_sentinel(ClientConf conf, String urls, String nameBase) throws Exception {
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
		pc.init();
		Jedis jedis = pc.getRpL1().getResource();
		try {
			if (jedis != null)
				updateClientConf(jedis, conf);
			String nb = jedis.hget("mm.namebase", nameBase);
			if (nb != null)
				this.nameBase = nb;
			else
				throw new Exception("init failed! namebase is error,please enter right namebase.");
		} finally {
			pc.getRpL1().putInstance(jedis);
		}

		return 0;
	}

	private int init_by_standalone(ClientConf conf, String urls, String nameBase) throws Exception {
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
				HostAndPort hap = new HostAndPort(s[0], Integer.parseInt(s[1]));
				conf.setHap(hap);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		pc.init();
		Jedis jedis = pc.getRpL1().getResource();
		try {
			if (jedis != null) {
				updateClientConf(jedis, conf);
				String nb = jedis.hget("mm.namebase", nameBase);
				if (nb != null)
					this.nameBase = nb;
				else
					throw new Exception("init failed! namebase is error,please enter right namebase.");
			}

		} finally {
			pc.getRpL1().putInstance(jedis);
		}

		return 0;
	}

	private boolean refreshActiveMMS(boolean isInit) {
		// refresh active secondary server and ckpt.ts now
		getActiveSecondaryServer();

		if (isInit) {
			return getActiveMMS();
		} else {
			try {
				List<String> active = pc.getActiveMMSByHB();
				Set<String> activeMMS = new TreeSet<String>();

				// BUG-XXX: getActiveMMSByHB() return the DNSed server name, it
				// might be different
				// with saved server name. Thus, if a server changes its ip
				// address, we can't
				// find it even we put it into socketHash.
				// Possibly, we get server_name:server_ip pair, and try to find
				// by name and ip.
				// If server_name in servers, check up
				// socketHash.get(server_name) {free it?}
				// and check up socketHash.get(server_ip), finally update
				// server_ip to servers?
				// else if server_ip in servers, it is ok.
				// else register ourself to servers?
				if (active.size() > 0) {
					for (String a : active) {
						String[] c = a.split(":");

						if (c.length == 2) {
							if (socketHash.get(a) == null) {
								// new MMS?
								Socket sock = new Socket();
								SocketHashEntry she = new SocketHashEntry(c[0], Integer.parseInt(c[1]),
										pc.getConf().getSockPerServer());
								try {
									sock.setTcpNoDelay(true);
									sock.connect(new InetSocketAddress(c[0], Integer.parseInt(c[1])));
									she.addToSockets(sock, new DataInputStream(sock.getInputStream()),
											new DataOutputStream(sock.getOutputStream()));
									if (socketHash.putIfAbsent(a, she) != null) {
										she.clear();
									}
								} catch (SocketException e) {
									e.printStackTrace();
									continue;
								} catch (NumberFormatException e) {
									e.printStackTrace();
									continue;
								} catch (IOException e) {
									e.printStackTrace();
									continue;
								}
							}
							activeMMS.add(a);
						}
					}
					synchronized (keyList) {
						keyList.clear();
						keyList.addAll(activeMMS);
						keyList.retainAll(socketHash.keySet());
					}
				} else {
					keyList.clear();
				}
				if (pc.getConf().isPrintServerRefresh())
					System.out.println("Refresh active servers: " + keyList);

			} catch (IOException ignored) {
			}
		}
		return true;
	}

	private void getActiveSecondaryServer() {
		Jedis jedis = pc.getRpL1().getResource();
		if (jedis == null)
			return;
		String ss = null, ckpt = null;
		String ls = null;

		try {
			ss = jedis.get("mm.ss.id");
			ls = jedis.get("mm.ls.id");
			ckpt = jedis.get("mm.ckpt.ts");
		} catch (Exception e) {
			System.out.println("getActiveSecondaryServer exception: " + e.getMessage());
		} finally {
			pc.getRpL1().putInstance(jedis);
		}
		try {
			if (ss != null)
				pc.setSs_id(Long.parseLong(ss));
			if (ls != null)
				pc.setLs_id(Long.parseLong(ls));
		} catch (Exception e) {
			System.out.println("Convert secondary server id '" + ss + "' to LONG failed.");
		}
		try {
			if (ckpt != null)
				pc.setCkpt_ts(Long.parseLong(ckpt));
		} catch (Exception e) {
			System.out.println("Convert checkpoint ts '" + ckpt + "' to LONG failed.");
		}
	}

	private boolean getActiveMMS() {
		Jedis jedis = pc.getRpL1().getResource();
		if (jedis == null)
			return false;

		try {
			Set<Tuple> active = jedis.zrangeWithScores("mm.active", 0, -1);
			Set<String> activeMMS = new TreeSet<String>();

			if (active != null && active.size() > 0) {
				for (Tuple t : active) {
					// translate ServerName to IP address
					String ipport = jedis.hget("mm.dns", t.getElement());

					// update server ID->Name map
					if (ipport == null) {
						pc.addToServers((long) t.getScore(), t.getElement());
						ipport = t.getElement();
					} else
						pc.addToServers((long) t.getScore(), ipport);

					String[] c = ipport.split(":");
					if (c.length == 2 && socketHash.get(ipport) == null) {
						Socket sock = new Socket();
						SocketHashEntry she = new SocketHashEntry(c[0], Integer.parseInt(c[1]),
								pc.getConf().getSockPerServer());
						activeMMS.add(ipport);
						try {
							sock.setTcpNoDelay(true);
							sock.connect(new InetSocketAddress(c[0], Integer.parseInt(c[1])));
							she.addToSockets(sock, new DataInputStream(sock.getInputStream()),
									new DataOutputStream(sock.getOutputStream()));
							if (socketHash.putIfAbsent(ipport, she) != null) {
								she.clear();
							}
						} catch (SocketException e) {
							System.out.println(
									"[WARN] Connect to MMS " + c[0] + ":" + c[1] + " failed: " + e.getMessage());
							e.printStackTrace();
							continue;
						} catch (NumberFormatException e) {
							System.out.println(
									"[FAIL] Transform string port(" + c[1] + ") to integer failed: " + e.getMessage());
							e.printStackTrace();
							continue;
						} catch (IOException e) {
							System.out.println(
									"[WARN] IO Error for MMS " + c[0] + ":" + c[1] + " failed: " + e.getMessage());
							e.printStackTrace();
							continue;
						}
					}
				}
			}
			synchronized (keyList) {
				keyList.addAll(activeMMS);
				keyList.retainAll(socketHash.keySet());
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			pc.getRpL1().putInstance(jedis);
		}

		return true;
	}

	private class MMCTimerTask extends TimerTask {
		@Override
		public void run() {
			try {
				refreshActiveMMS(false);
			} catch (Exception e) {
				System.out.println("[ERROR] refresh active MMS failed: " + e.getMessage() + ".\n" + e.getCause());
			}
		}
	}

	/**
	 * 连接服务器,进行必要初始化,并与redis服务器建立连接 如果初始化本对象时传入了conf，则使用conf中的redis地址，否则使用参数url
	 * It is not thread-safe!
	 *
	 * @param urls
	 *            MM STANDARD PROTOCOL: STL:// or STA://
	 * @return
	 * @throws Exception
	 */
	public int init(String urls, String nameBase) throws Exception {
		if (urls == null) {
			throw new Exception("The url can not be null.");
		}

		if (pc.getConf() == null) {
			pc.setConf(new ClientConf());
		}
		if (urls.startsWith("STL://")) {
			urls = urls.substring(6);
			pc.getConf().setRedisMode(ClientConf.RedisMode.SENTINEL);
		} else if (urls.startsWith("STA://")) {
			urls = urls.substring(6);
			pc.getConf().setRedisMode(ClientConf.RedisMode.STANDALONE);
		}
		switch (pc.getConf().getRedisMode()) {
		case SENTINEL:
			init_by_sentinel(pc.getConf(), urls, nameBase);
			break;
		case STANDALONE:
			init_by_standalone(pc.getConf(), urls, nameBase);
			break;
		case CLUSTER:
			System.out.println("MMS do NOT support CLUSTER mode now, " + "use STL/STA instead.");
			break;
		default:
			break;
		}

		socketHash = new ConcurrentHashMap<String, SocketHashEntry>();
		// 从redis上获取所有的服务器地址
		refreshActiveMMS(true);
		System.out.println("Got active server size=" + keyList.size());
		pc.setSocketHash(socketHash);

		timer.schedule(new MMCTimerTask(), 500, pc.getConf().getServerRefreshInterval());

		return 0;
	}

	private String __put(Set<String> targets, String[] keys, byte[] content, boolean nodedup, String fn)
			throws Exception {
		Random rand = new Random();
		String r = null;
		Set<String> saved = new TreeSet<String>();
		HashMap<String, Long> failed = new HashMap<String, Long>();
		int targetnr = targets.size();

		do {
			for (String server : targets) {
				SocketHashEntry she = socketHash.get(server);
				if (she.probSelected()) {
					// BUG-XXX: we have to check if we can recover from this
					// exception,
					// then try our best to survive.
					try {
						r = pc.syncStorePhoto(keys[0], keys[1], content, she, nodedup, content.length, 0, fn);
						if ('#' == r.charAt(1))
							r = r.substring(2);
						nodedup = r.split("#").length < pc.getConf().getDupNum();
						saved.add(server);
					} catch (Exception e) {
						// e.printStackTrace();
						if (failed.containsKey(server)) {
							failed.put(server, failed.get(server) + 1);
						} else
							failed.put(server, 1L);
						System.out.println("[PUT] " + keys[0] + "@" + keys[1] + " to " + server + " failed: ("
								+ e.getMessage() + ") for " + failed.get(server) + " times.");
					}
				} else {
					// this means target server has no current usable
					// connection, we try to use
					// another server
				}
			}
			if (saved.size() < targetnr) {
				List<String> remains = new ArrayList<String>(keyList);
				remains.removeAll(saved);
				for (Map.Entry<String, Long> e : failed.entrySet()) {
					if (e.getValue() > 2) {
						remains.remove(e.getKey());
					}
				}
				if (remains.size() == 0)
					break;
				targets.clear();
				for (int i = saved.size(); i < targetnr; i++) {
					targets.add(remains.get(rand.nextInt(remains.size())));
				}
			} else
				break;
		} while (true);

		if (saved.size() == 0) {
			throw new Exception("Error in saving Key: " + keys[0] + "@" + keys[1]);
		}
		return r;
	}

	private String __put(Set<String> targets, String[] keys, byte[] content, boolean nodedup, long fLen, long off,
			File file, String fn) throws Exception {
		Random rand = new Random();
		String r = null;
		Set<String> saved = new TreeSet<String>();
		HashMap<String, Long> failed = new HashMap<String, Long>();
		int targetnr = targets.size();

		do {
			for (String server : targets) {
				SocketHashEntry she = socketHash.get(server);
				if (she.probSelected()) {
					// BUG-XXX: we have to check if we can recover from this
					// exception,
					// then try our best to survive.
					try {
						r = pc.syncStorePhoto(keys[0], keys[1], content, she, nodedup, fLen, off, fn);
						// System.out.println(r);
						if (ClientConf.MODE.ONLYDATA.name().equals(r))
							break;
						if ('#' == r.charAt(1))
							r = r.substring(2);
						nodedup = r.split("#").length < pc.getConf().getDupNum();
						saved.add(server);
					} catch (Exception e) {
						if (failed.containsKey(server)) {
							failed.put(server, failed.get(server) + 1);
						} else
							failed.put(server, 1L);
						System.out.println("[PUT] " + keys[0] + "@" + keys[1] + " to " + server + " failed: ("
								+ e.getMessage() + ") for " + failed.get(server) + " times.");
					}
				} else {
					// this means target server has no current usable
					// connection, we try to use
					// another server
				}
			}
			if (saved.size() < targetnr) {
				List<String> remains = new ArrayList<String>(keyList);
				remains.removeAll(saved);
				for (Map.Entry<String, Long> e : failed.entrySet()) {
					if (e.getValue() > 9) {
						remains.remove(e.getKey());
						pc.delThisInfo(keys, e.getKey());
						if (remains.size() == 0)
							break;
						targets.clear();
						for (int i = saved.size(); i < targetnr; i++) {
							targets.add(remains.get(rand.nextInt(remains.size())));
						}
						// System.out.println("在新的点上 重新存");
						return putErrFile(file, off, targets, keys, fn);
					}
				}
				targets.removeAll(saved);
			} else
				break;
		} while (true);

		if (saved.size() == 0) {
			throw new Exception("Error in saving Key: " + keys[0] + "@" + keys[1]);
		}
		return r;
	}

	private String putErrFile(File file, long offset, Set<String> targets, String[] keys, String fn) {
		// TODO 获取文件上传失败部分，重写
		long fLen = file.length();
		int blen = pc.getConf().getBlen();
		RandomAccessFile randomFile = null;
		int b;
		long bs = 0;
		String info = "";
		try {
			randomFile = new RandomAccessFile(file, "r");
			randomFile.seek(0);
			byte[] bytes = new byte[blen];
			b = randomFile.read(bytes);
			bs += b;
			info = __put(targets, keys, bytes, true, fLen, randomFile.getFilePointer(), file, fn);
			while ((b = randomFile.read(bytes)) != -1 && (bs < offset)) {
				// 如果是-1或者返回的info中infos[8]不相同或者有的有这一位有的没有，
				// 证明有的副本存失败了，在redis中删除这些偏移量比其他的小的但是如果
				// 有infos[8]不存在的，那就吧存在infos[8]的info删除
				if (!info.contains("@"))
					randomFile.seek(Long.parseLong(info));
				if (b == blen) {
					info = __put(targets, keys, bytes, false, -fLen, randomFile.getFilePointer(), file, fn);
				} else {
					byte[] bytes2 = new byte[b];
					randomFile.read(bytes2);
					info = __put(targets, keys, bytes, false, -fLen, randomFile.getFilePointer(), file, fn);
				}
				bs += b;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (randomFile != null) {
				try {
					randomFile.close();
				} catch (IOException ignored) {
				}
			}
		}
		return info;
	}

	@Deprecated
	public String put(String key, byte[] content) throws Exception {
		if (pc.getConf().isDataSync())
			dataSync(key, null, UpType.set);
		return put(key, content, null);
	}

	/**
	 * 同步写,对外提供的接口 It is thread-safe!
	 *
	 * @param key
	 * @param content
	 * @return info to locate the file
	 * @throws Exception
	 */
	String put(String key, byte[] content, String fn) throws Exception {
		if (key == null || keyList.size() == 0) {
			throw new Exception("Key can not be null or no active MMServer (" + keyList.size() + ").");
		}
		String[] keys = key.split("@");
		if (keys.length != 2)
			throw new Exception("Wrong format of key: " + key);
		boolean nodedup = false;
		int dupnum = Math.min(keyList.size(), pc.getConf().getDupNum());

		// roundrobin select dupnum servers from keyList, if error in put,
		// random select in remain servers
		Set<String> targets = new TreeSet<String>();
		int idx = index.getAndIncrement();
		if (idx < 0) {
			index.compareAndSet(idx, 0);
			idx = index.get();
		}
		for (int i = 0; i < dupnum; i++) {
			targets.add(keyList.get((idx + i) % keyList.size()));
		}

		return __put(targets, keys, content, nodedup, fn);
	}

	private String put(String key, byte[] content, long fLen, long off, File file, String fn) throws Exception {
		return zput(key, content, fLen, off, file, fn);
	}

	private String getMd5(File file) throws IOException, NoSuchAlgorithmException {
		int M = 1024 * 1024;
		long lineOfBig = 20L * M;
		byte[] b2 = null;
		RandomAccessFile raf = null;
		FileInputStream fis = null;
		MessageDigest md;
		md = MessageDigest.getInstance("md5");
		String ss = file.getName() + file.length();
		String md5 = "";
		byte[] b1 = ss.getBytes();
		byte[] bs = new byte[10 * M + b1.length];
		try {
			if (file.length() > lineOfBig) {
				raf = new RandomAccessFile(file, "r");
				b2 = new byte[10 * M];
				raf.read(b2, 0, 2 * M);
				raf.seek(file.length() / 4 - 1 * M);
				raf.read(b2, 2 * M, 2 * M);
				raf.seek(file.length() / 2 - 1 * M);
				raf.read(b2, 4 * M, 2 * M);
				raf.seek(file.length() / 4 * 3 - 1 * M);
				raf.read(b2, 6 * M, 2 * M);
				raf.seek(file.length() - 2 * M);
				raf.read(b2, 8 * M, 2 * M);
				System.arraycopy(b2, 0, bs, 0, 10 * M);
				System.arraycopy(b1, 0, bs, 10 * M, ss.length());
			} else {
				fis = new FileInputStream(file);
				bs = new byte[(int) file.length()];
				fis.read(bs);
			}
			md.update(bs);
			byte[] mdbytes = md.digest();
			StringBuffer sb = new StringBuffer();
			for (int j = 0; j < mdbytes.length; j++) {
				sb.append(Integer.toString((mdbytes[j] & 0xff) + 0x100, 16).substring(1));
			}
			md5 = sb.toString();
			// System.out.println(md5);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (raf != null)
				raf.close();
			if (fis != null)
				fis.close();
		}
		return md5;
	}

	@Deprecated
	public String getKeyForFile(String localFilePath) throws Exception {
		File file = new File(localFilePath);
		// filename
		String fn = file.getName();
		String key = getMd5(file);
		String fileType = new MimetypesFileTypeMap().getContentType(file);
		char t = 'o';
		if (!fileType.startsWith("app"))
			t = fileType.charAt(0);
		long time = System.currentTimeMillis();
		key = t + (time - time % 3600000) / 1000 + "@" + key;
		return key;
	}

	/**
	 * 
	 * @param localFilePath
	 * @return key
	 * @throws Exception
	 */
	@Deprecated
	public String uploadFile(String localFilePath) throws Exception {
		File file = new File(localFilePath);
		return uploadFile(null, file);
	}

	/**
	 * 
	 * @param localFilePath
	 * @param fileType
	 * @return
	 * @throws Exception
	 */
	public String uploadFile(String localFilePath, String jsonStr) throws Exception {
		File file = new File(localFilePath);
		return uploadFile(jsonStr, file);
	}

	/**
	 * 
	 * @param file
	 * @return key
	 * @throws Exception
	 */
	private String uploadFile(String jsonStr, File file) throws Exception {
		String key = null;
		String fType = null;
		String g_id = null;
		JSONObject jsono = null;
		String info = null;
		String set = null;
		String md5 = getMd5(file);
		if (jsonStr == null) {
			fType = new MimetypesFileTypeMap().getContentType(file).charAt(0) + "";
			g_id = md5;
		} else {
			jsono = JSONObject.fromObject(jsonStr);
			if (jsono.containsKey("type")) {
				fType = jsono.get("type").toString();
				fType = pc.getConf().getFileTypeMap().get(fType);
			}
			if (jsono.containsKey("g_id"))
				g_id = jsono.get("g_id").toString();
		}
		if (g_id.contains("@")){
			key = g_id;
			String[] gid = g_id.split("@");
			set = gid[0];
			g_id = gid[1];
		} else {
			long time = System.currentTimeMillis();
			// key = nameBase + (time - time % 3600000) / 1000 + "@" + key;
			set = fType + (time - time % 3600000) / 1000;
			key = set + "@" + g_id;
		}
        RedisConnection rc = null;
        try {
            rc = pc.getRPS().getL2(set, true);
            Jedis jedis = rc.jedis;
            if (jedis != null) {
                info = jedis.hget(set, md5);
                if (info != null){
                	jedis.hset(set, g_id, info);
            		if (pc.getConf().isDataSync())
            			dataSync(key, g_id, UpType.set);
                }else {
                	info = uploadFile(file, key, g_id);
                	if (info != null && g_id != md5)
                		jedis.hset(set, md5, info);
                }
            }
        } catch (Exception e) {
        	e.printStackTrace();
		} finally {
			pc.getRPS().putL2(rc);
		}
		
		if (info == null)
			key = null;
		return key;
	}

	/**
	 * 
	 * @param file
	 * @param key
	 * @return info
	 * @throws Exception
	 */
	@Deprecated
	public String uploadFile(File file, String key) throws Exception {
		return uploadFile(file, key, null);
	}

	private String uploadFile(File file, String key, String g_id) throws Exception {
		// filename
		String fn = file.getName();

		int blen = pc.getConf().getBlen();
		RandomAccessFile randomFile = null;
		int b = -1;
		String info = null;
		try {
			randomFile = new RandomAccessFile(file, "r");
			long fileLength = randomFile.length();
			boolean bigFile = (fileLength > blen) ? true : false;
			if (bigFile) {
				randomFile.seek(0);
				byte[] bytes = new byte[blen];
				randomFile.read(bytes);
				info = put(key, bytes, fileLength, blen, file, fn);
				if (info.equals(key))
					return info;
				if (!info.contains("@"))
					randomFile.seek(Long.parseLong(info));
				while ((b = randomFile.read(bytes)) != -1) {
					if (b != blen) {
						bytes = Arrays.copyOf(bytes, b);
						info = put(key, bytes, -fileLength, randomFile.getFilePointer(), file, fn);
					} else
						info = put(key, bytes, -fileLength, randomFile.getFilePointer(), file, fn);
					if (!info.contains("@"))
						randomFile.seek(Long.parseLong(info));
				}
			} else {
				byte[] bytes = new byte[(int) fileLength];
				randomFile.seek(0);
				randomFile.read(bytes);
				info = put(key, bytes, fn);
			}
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} finally {
			if (randomFile != null) {
				try {
					randomFile.close();
				} catch (IOException e) {
					e.printStackTrace();
					return null;
				}
			}
		}
		if (pc.getConf().isDataSync())
			dataSync(key, g_id, UpType.set);

		return info;
	}

	enum UpType {
		set, del
	}

	private void dataSync(String key, String g_id, UpType upType) {
		Jedis jedis = pc.getRpL1().getResource();
		if (g_id == null)
			g_id = "-1";
		try {
			if (jedis != null)
				jedis.hset("ds." + upType.name(), key, g_id);
		} finally {
			pc.getRpL1().putInstance(jedis);
		}
	}

	private String zput(String key, byte[] content, long fLen, long off, File file, String fn) throws Exception {
		if (key == null || keyList.size() == 0) {
			throw new Exception("Key can not be null or no active MMServer (" + keyList.size() + ").");
		}
		String[] keys = key.split("@");
		if (keys.length != 2)
			throw new Exception("Wrong format of key: " + key);
		boolean nodedup = false;
		int dupnum = Math.min(keyList.size(), pc.getConf().getDupNum());
		// roundrobin select dupnum servers from keyList, if error in put,
		// random select in remain servers

		Set<String> targets = new TreeSet<String>();
		if (fLen > 0) {
			int idx = index.getAndIncrement();
			if (idx < 0) {
				index.compareAndSet(idx, 0);
				idx = index.get();
			}
			for (int i = 0; i < dupnum; i++) {
				targets.add(keyList.get((idx + i) % keyList.size()));
			}
		} else {
			// 查redis，每个info的serverId添加到targets
			for (String s : pc.getServerIds(key)) {
				targets.add(s);
			}

		}
		return __put(targets, keys, content, nodedup, fLen, off, file, fn);
	}

	/**
	 * 异步写,对外提供的接口（暂缓使用）
	 *
	 * @param key
	 * @param content
	 * @return
	 */
	public void iPut(String key, byte[] content) throws IOException, Exception {
		/*
		 * if(key == null) throw new Exception("key can not be null."); String[]
		 * keys = key.split("@"); if(keys.length != 2) throw new
		 * Exception("wrong format of key:"+key); for (int i = 0; i <
		 * pc.getConf().getDupNum(); i++) { // Socket sock =
		 * socketHash.get(keyList.get((index + i) % keyList.size())); //
		 * pc.asyncStorePhoto(keys[0], keys[1], content, sock); } index++;
		 * if(index >= socketHash.size()){ index = 0; }
		 */
	}

	/**
	 * 批量同步写,对外提供的接口（暂缓使用）
	 *
	 * @param set
	 * @param md5s
	 * @param content
	 * @return
	 */
	public String[] mPut(String set, String[] md5s, byte[][] content) throws Exception {
		if (set == null || md5s.length == 0 || content.length == 0) {
			throw new Exception("set or md5s or contents can not be null.");
		} else if (md5s.length != content.length)
			throw new Exception("arguments length mismatch.");
		String[] r = null;
		int idx = index.getAndIncrement();

		if (idx < 0) {
			index.compareAndSet(idx, 0);
			idx = index.get();
		}
		for (int i = 0; i < pc.getConf().getDupNum(); i++) {
			SocketHashEntry she = socketHash.get(keyList.get((idx + i) % keyList.size()));
			r = pc.mPut(set, md5s, content, she);
		}

		return r;
	}

	
	/**
	 * 批量异步写，对外提供的接口（暂缓使用）
	 *
	 * @param keys
	 *            redis中的键以set开头+#+md5的字符串形成key
	 * @return 图片内容, 如果图片不存在则返回长度为0的byte数组
	 */
	public void imPut(String[] keys, byte[][] contents) throws Exception {
		/*
		 * if(keys.length != contents.length){ throw new
		 * Exception("keys's length is not the same as contents'slength."); }
		 * for(int i = 0;i<keys.length;i++){ iPut(keys[i],contents[i]); }
		 */
	}

	/**
	 * 根据info从系统中读取对象内容，重新写入到MMServer上仅当info中活动的MMServer个数小于dupnum时
	 * <p>
	 * (should only be used by system tools)
	 *
	 * @param key
	 * @param info
	 * @return
	 * @throws IOException
	 * @throws Exception
	 */
	@Deprecated
	public String xput(String key, String info, String fn) throws IOException, Exception {
		if (key == null || keyList.size() == 0) {
			throw new Exception("Key can not be null or no active MMServer (" + keyList.size() + ").");
		}
		String[] keys = key.split("@");
		if (keys.length != 2)
			throw new Exception("Wrong format of key: " + key);

		boolean nodedup = true;
		int dupnum = Math.min(keyList.size(), pc.getConf().getDupNum());

		// if (content == null) {
		// throw new IOException("Try to get content of KEY: " + key + "
		// failed.");
		// }
		// round-robin select dupnum servers from keyList, if error in put,
		// random select in remain servers
		TreeSet<String> targets = new TreeSet<String>();
		Set<String> active = new TreeSet<String>();
		targets.addAll(keyList);

		String[] infos = info.split("#");
		for (String s : infos) {
			long sid = Long.parseLong(s.split("@")[2]);
			String server = this.getPc().getServers().get(sid);
			if (server != null) {
				targets.remove(server);
				active.add(server);
			}
		}
		if (active.size() >= dupnum)
			return info;
		if (targets.size() == 0)
			throw new IOException("No more copy can be made: " + key + " --> " + info);
		else if (targets.size() > (dupnum - active.size())) {
			Random rand = new Random();
			String[] tArray = targets.toArray(new String[0]);
			targets.clear();
			for (int i = 0; i < dupnum - active.size(); i++) {
				targets.add(tArray[rand.nextInt(tArray.length)]);
			}
		}
		int blen = pc.getConf().getBlen();

		long fLen = Long.parseLong(info.split("@")[5]);
		int rem, sum;
		if (fLen > blen) {
			rem = (int) (fLen % blen);
			if (rem == 0) {
				sum = (int) (fLen / blen);
				int offset = 0;
				byte[] contents = this.get(info, offset, blen);
				if (contents == null) {
					throw new IOException("Try to get content of KEY: " + key + " failed.");
				}
				info = __put(targets, keys, contents, nodedup, fLen, offset, null, fn);
				for (int i = 1; i <= (sum - 1); i++) {
					offset += blen;
					contents = this.get(info, offset, blen);
					info = __put(targets, keys, contents, nodedup, -fLen, offset, null, fn);
				}
			} else {
				sum = (int) (fLen / blen) + 1;
				int offset = 0;
				byte[] contents = this.get(info, offset, blen);
				info = __put(targets, keys, contents, nodedup, fLen, offset, null, fn);
				for (int i = 1; i <= sum; i++) {
					offset += blen;
					if (i != sum) {
						contents = this.get(info, offset, blen);
						info = __put(targets, keys, contents, nodedup, -fLen, offset, null, fn);
					} else {
						contents = new byte[rem];
						contents = this.get(info, offset, blen);
						info = __put(targets, keys, contents, nodedup, -fLen, offset, null, fn);
					}
				}
			}
		} else {
			byte[] content = this.get(info);
			info = __put(targets, keys, content, nodedup, fn);
		}
		return info;
	}

	@Deprecated
	public boolean isInMMS(String key) throws Exception {
		if (key == null)
			throw new Exception("key can not be null.");
		return pc.isInMMS(key);
		
	}
	
	/**
	 * 获取文件信息
	 *
	 * @param key
	 * @return
	 * @throws Exception
	 */
	public FileInfo getFile(String key) throws Exception {
		if (key == null)
			throw new Exception("key can not be null.");
		return pc.getFileInfo(key);
	}

	/**
	 * 从偏移offset读取长度为length的数据,若一次读取整个小文件则把len参数设为-1
	 *
	 * @param key
	 * @param off
	 * @param len
	 * @return
	 * @throws Exception
	 */
	public byte[] downloadFile(String key, long off, int len) throws Exception {
		if (len < 0) {
			return get(key);
		}
		return get(key, off, len);
	}

	private byte[] get(String key, long offset, int length) throws Exception {
		if (key == null)
			throw new Exception("key can not be null.");
		String[] keys = key.split("@|#");
		// TODO:1
		if (keys.length == 2)
			return pc.getPhoto(keys[0], keys[1], offset, length);
		// TODO:2 useless
		else if (keys.length == pc.getConf().getVlen())
			return pc.searchByInfo(key, keys, offset, length);
		// TODO:3 useless
		else if (keys.length % pc.getConf().getVlen() == 0) // 如果是拼接的元信息，分割后长度是7的倍数
			return pc.searchPhoto(key, offset, length);
		else
			throw new Exception("wrong format of key:" + key);
	}

	/**
	 * 同步取，对外提供的接口 It is thread-safe
	 *
	 * @param key
	 *            或者是set@md5,或者是文件元信息，可以是拼接后的
	 * @return 图片内容, 如果图片不存在则返回长度为0的byte数组
	 * @throws IOException
	 * @throws Exception
	 */
	@Deprecated
	public byte[] get(String key) throws IOException, Exception {
		if (key == null)
			throw new Exception("key can not be null.");
		String[] keys = key.split("@|#");
		if (keys.length == 2)
			return pc.getPhoto(keys[0], keys[1]);
		else if (keys.length == pc.getConf().getVlen())
			return pc.searchByInfo(key, keys);
		else if (keys.length % pc.getConf().getVlen() == 0) // 如果是拼接的元信息，分割后长度是8的倍数
			return pc.searchPhoto(key);
		else
			throw new Exception("wrong format of key:" + key);
	}

	/**
	 * 批量读取某个集合的所有key It is thread-safe
	 *
	 * @param type
	 * @param begin_time
	 * @return
	 * @throws IOException
	 * @throws Exception
	 */
	public List<String> getkeys(String type, long begin_time) throws IOException, Exception {
		String prefix;

		if (type == null)
			throw new Exception("type can not be null");
		if (type.equalsIgnoreCase("image")) {
			prefix = getMMTypeSymbol(MMType.IMAGE);
		} else if (type.equalsIgnoreCase("thumbnail")) {
			prefix = getMMTypeSymbol(MMType.THUMBNAIL);
		} else if (type.equalsIgnoreCase("text")) {
			prefix = getMMTypeSymbol(MMType.TEXT);
		} else if (type.equalsIgnoreCase("audio")) {
			prefix = getMMTypeSymbol(MMType.AUDIO);
		} else if (type.equalsIgnoreCase("video")) {
			prefix = getMMTypeSymbol(MMType.VIDEO);
		} else if (type.equalsIgnoreCase("application")) {
			prefix = getMMTypeSymbol(MMType.APPLICATION);
		} else if (type.equalsIgnoreCase("other")) {
			prefix = getMMTypeSymbol(MMType.OTHER);
		} else {
			throw new Exception("type '" + type + "' is invalid.");
		}

		// query on redis
		TreeSet<Long> tranges = pc.getSets(prefix);
		try {
			long setTs = tranges.tailSet(begin_time).first();
			return pc.getSetElements(prefix + setTs);
		} catch (NoSuchElementException e) {
			throw new Exception("Can not find any keys larger or equal to " + begin_time);
		}
	}

	/**
	 * 批量获取keys对应的所有多媒体对象内容 it is thread safe.
	 *
	 * @param keys
	 * @param cookies
	 * @return
	 * @throws IOException
	 * @throws Exception
	 */
	public List<byte[]> mget(List<String> keys, Map<String, String> cookies) throws IOException, Exception {
		if (keys == null || cookies == null)
			throw new Exception("keys or cookies list can not be null.");
		if (keys.size() > 0) {
			String key = keys.get(keys.size() - 1);
			if (key != null) {
				long ts = -1;
				try {
					ts = Long.parseLong(key.split("@")[0].substring(1));
				} catch (Exception ignored) {
				}
				cookies.put("ts", Long.toString(ts));
			}
		}
		return pc.mget(keys, cookies);
	}

	public ResultSet objectSearch(List<Feature> features, byte[] obj, List<String> specified_servers) throws Exception {
		if (features == null || features.size() == 0)
			throw new Exception("Invalid or empty features list.");
		return pc.objectSearch(features, obj, specified_servers);
	}

	/**
	 * 同步删，对外提供的接口 It is thread-safe
	 *
	 * @param key
	 *            set@md5
	 * @throws Exception
	 */
	public void deleteFile(String key) throws Exception {
		if (key == null)
			throw new Exception("key can not be null.");
		String[] keys = key.split("@");
		if (keys.length == 2)
			pc.deletePhoto(keys[0], keys[1]);
		else
			throw new Exception("wrong format of key:" + key);
		if (pc.getConf().isDataSync())
			dataSync(key, null, UpType.del);
	}

	/**
	 * 退出多媒体客户端，释放内部资源
	 */
	public void quit() {
		pc.close();
		timer.cancel();
	}
}
