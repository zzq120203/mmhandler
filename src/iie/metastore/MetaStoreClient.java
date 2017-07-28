package iie.metastore;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.DiskManager.DeviceInfo;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.CreateOperation;
import org.apache.hadoop.hive.metastore.api.CreatePolicy;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Device;
import org.apache.hadoop.hive.metastore.api.FOFailReason;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MSOperation;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Node;
import org.apache.hadoop.hive.metastore.api.NodeGroup;
import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.hadoop.hive.metastore.api.SFileLocation;
import org.apache.hadoop.hive.metastore.api.SplitValue;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.User;
import org.apache.hadoop.hive.metastore.api.statfs;
import org.apache.hadoop.hive.metastore.model.MetaStoreConst;
import org.apache.hadoop.hive.metastore.tools.PartitionFactory;
import org.apache.hadoop.hive.metastore.tools.PartitionFactory.PartitionInfo;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransportException;
import com.alibaba.fastjson.JSON;


import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.exceptions.JedisException;

import devmap.DevMap;
import devmap.DevMap.DevStat;

public class MetaStoreClient {
	// client should be the local datacenter;
	public Database local_db;
	public String local_alterUri;
	private String byUri = null;
	public IMetaStoreClient client;
	private String master_host = null;
	private int master_port;
	private static boolean useHA = false;
	private static long master_id = -1l;
	private static boolean needMasterChange = false;
	private ConcurrentHashMap<String, IMetaStoreClient> climap = new ConcurrentHashMap<String, IMetaStoreClient>();
	private ConcurrentHashMap<String, String> alterUriMap = new ConcurrentHashMap<String, String>();
	
	private static ConcurrentHashMap<Long, String> servers = new ConcurrentHashMap<Long, String>();
	private static final Timer timer = new Timer("ActiveMSSRefresher");
	private static RedisFactory rf = null;
	
	private class MSCTimerTask extends TimerTask {
		@Override
		public void run() {
			try {
				refreshActiveMSS(false);
			} catch (Exception e) {
				System.out.println("[ERROR] refresh active MSS failed: " + e.getMessage()
						+ ".\n" + e.getCause());
			}
		}
	}
	
	public static void quit() {
		timer.cancel();
	    if (rf != null) {
	    	rf.quit();
	    	rf = null;
	    }
	}
	
	private static void __EXIT(int err) {
		quit();
		System.out.println("__EXIT__");
		System.exit(err);
	}
	
	public static <T> T newInstance(Class<T> theClass,
			Class<?>[] parameterTypes, Object[] initargs) {
		// Perform some sanity checks on the arguments.
		if (parameterTypes.length != initargs.length) {
			throw new IllegalArgumentException(
					"Number of constructor parameter types doesn't match number of arguments");
		}
		for (int i = 0; i < parameterTypes.length; i++) {
			Class<?> clazz = parameterTypes[i];
			if (!(clazz.isInstance(initargs[i]))) {
				throw new IllegalArgumentException("Object : " + initargs[i]
						+ " is not an instance of " + clazz);
			}
		}

		try {
			Constructor<T> meth = theClass
					.getDeclaredConstructor(parameterTypes);
			meth.setAccessible(true);
			return meth.newInstance(initargs);
		} catch (Exception e) {
			throw new RuntimeException("Unable to instantiate "
					+ theClass.getName(), e);
		}
	}
	
	private boolean getActiveMSS(Jedis jedis, boolean isInit) {
		Set<Tuple> active = jedis.zrangeWithScores("ms.active", 0, -1);
		
		if (active != null && active.size() > 0) {
			for (Tuple t : active) {
				String ipport = t.getElement();
				String[] c = ipport.split(":");
				if (c.length == 2) {
					// ok, valid server address, add it to servers map
					servers.put((long)t.getScore(), ipport);
				}
			}
		}
		
		String masterId = jedis.get("ms.master");
		if (masterId == null) {
			System.out.println("[WARN] no master registered yet, wait for lucy guy.");
			master_id = -1l;
		} else {
			try {
				Long mid = Long.parseLong(masterId);
				if (mid != master_id) {
					long oldid = master_id;
					if (servers.get(mid) != null) {
						master_id = mid;
						if (!isInit)
							needMasterChange = true;
					}
					System.out.println("[WARN] need to change master from " + oldid 
							+ " to " + masterId + ":[" + servers.get(mid) + "] = " + 
							needMasterChange);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return true;
	}

	public void refreshActiveMSS(boolean isInit) {
		boolean isErr = false;
		Jedis j = null;

		if (rf == null)
			return;
		try {
			j = rf.getNewInstace();
			if (j != null)
				getActiveMSS(j, isInit);
		} catch (JedisException e) {
			isErr = true;
		} finally {
			if (isErr)
				rf.putBrokenInstace(j);
			else
				rf.putInstance(j);
		}
	}

	public static class RetryingMetaStoreClient implements InvocationHandler {

		private IMetaStoreClient base;
		private final int retryLimit;
		private final int retryDelaySeconds;

		protected RetryingMetaStoreClient(
				String msUri, int retry, int retryDelay, 
				HiveMetaHookLoader hookLoader,
				Class<? extends IMetaStoreClient> msClientClass)
				throws MetaException {
			this.retryLimit = retry;
			this.retryDelaySeconds = retryDelay;
			this.base = (IMetaStoreClient) newInstance(
					msClientClass, new Class[] { String.class, Integer.class, Integer.class, HiveMetaHookLoader.class }, 
					new Object[] { msUri, new Integer(retry), new Integer(retryDelay), hookLoader });
		}
		
		private void changeBase(String msUri,
				HiveMetaHookLoader hookLoader,
				Class<? extends IMetaStoreClient> msClientClass) {
			try {
				base.close();
			} catch (Exception e) {
			}
			base = (IMetaStoreClient) newInstance(
					msClientClass, new Class[] { String.class, Integer.class, Integer.class, HiveMetaHookLoader.class }, 
					new Object[] { msUri, new Integer(retryLimit), new Integer(retryDelaySeconds), hookLoader });
		}
		
		public static Class<?> getClass(String rawStoreClassName)
				throws MetaException {
			try {
				return Class.forName(rawStoreClassName, true,
						JavaUtils.getClassLoader());
			} catch (ClassNotFoundException e) {
				throw new MetaException(rawStoreClassName + " class not found");
			}
		}

		public static IMetaStoreClient getProxy(String msUri, int retry, int retryDelay, HiveMetaHookLoader hookLoader, String mscClassName)
				throws MetaException {

			Class<? extends IMetaStoreClient> baseClass = (Class<? extends IMetaStoreClient>) getClass(mscClassName);

			RetryingMetaStoreClient handler = new RetryingMetaStoreClient(msUri, retry, retryDelay, hookLoader, baseClass);

			return (IMetaStoreClient) Proxy.newProxyInstance(
					RetryingMetaStoreClient.class.getClassLoader(),
					baseClass.getInterfaces(), handler);
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			Object ret = null;
			int retriesMade = 0;
			TException caughtException = null;
			while (true) {
				// FIXME: try to detect master change
				if (useHA && master_id == -1l) {
					try {
						if (base != null) {
							base.close();
							base = null;
						}
					} catch (Exception e) {
					}
					throw new MetaException("No active MS master, please wait.");
				}
				if (needMasterChange || base == null) {
					String msUri = "thrift://" + servers.get(master_id);
					HiveMetaHookLoader hookLoader = new HiveMetaHookLoader() {
						public HiveMetaHook getHook(
								org.apache.hadoop.hive.metastore.api.Table tbl)
										throws MetaException {

							return null;
						}
					};
					changeBase(msUri, hookLoader, 
							(Class<? extends IMetaStoreClient>)getClass(HiveMetaStoreClient.class.getName()));
					needMasterChange = false;
				}
				try {
					ret = method.invoke(base, args);
					break;
				} catch (UndeclaredThrowableException e) {
					throw e.getCause();
				} catch (InvocationTargetException e) {
					if ((e.getCause() instanceof TApplicationException)
							|| (e.getCause() instanceof TProtocolException)
							|| (e.getCause() instanceof TTransportException)) {
						caughtException = (TException) e.getCause();
					} else if ((e.getCause() instanceof MetaException)
							&& e.getCause().getMessage()
									.matches("JDO[a-zA-Z]*Exception")) {
						caughtException = (MetaException) e.getCause();
					} else {
						throw e.getCause();
					}
				}

				if (retriesMade >= retryLimit) {
					// FIXME: try to detect new master
					if (needMasterChange) {
						String msUri = "thrift://" + servers.get(master_id);
						HiveMetaHookLoader hookLoader = new HiveMetaHookLoader() {
							public HiveMetaHook getHook(
									org.apache.hadoop.hive.metastore.api.Table tbl)
											throws MetaException {

								return null;
							}
						};
						changeBase(msUri, hookLoader, 
								(Class<? extends IMetaStoreClient>)getClass(HiveMetaStoreClient.class.getName()));
						needMasterChange = false;
					}
					throw caughtException;
				}
				retriesMade++;
				System.out.println(
						"MetaStoreClient lost connection. Attempting to reconnect." + 
						caughtException);
				Thread.sleep(retryDelaySeconds * 1000);
				base.reconnect();
			}
			return ret;
		}
	}
	
	public IMetaStoreClient createMetaStoreClient() throws MetaException {
		return createMetaStoreClient("localhost", 9083);
	}
	
	public IMetaStoreClient createMetaStoreClient(String uri) throws MetaException {
		HiveMetaHookLoader hookLoader = new HiveMetaHookLoader() {
			public HiveMetaHook getHook(
					org.apache.hadoop.hive.metastore.api.Table tbl)
					throws MetaException {

				return null;
			}
		};
		return RetryingMetaStoreClient.getProxy(uri, 5, 1, hookLoader, HiveMetaStoreClient.class.getName());
	}

	public IMetaStoreClient createMetaStoreClient(String serverName, int port) throws MetaException {
		HiveMetaHookLoader hookLoader = new HiveMetaHookLoader() {
			public HiveMetaHook getHook(
					org.apache.hadoop.hive.metastore.api.Table tbl)
					throws MetaException {

				return null;
			}
		};
		master_host = serverName;
		master_port = port;
		return RetryingMetaStoreClient.getProxy("thrift://" + serverName + ":" + port, 5, 1, hookLoader, HiveMetaStoreClient.class.getName());
	}
	
	public MetaStoreClient() throws MetaException {
		client = createMetaStoreClient();
		initmap(false);
	}
	
	public MetaStoreClient(String serverName, int port) throws MetaException {
		client = createMetaStoreClient(serverName, port);
		initmap(false);
	}

	public MetaStoreClient(String serverName, boolean preconnect) throws MetaException {
		client = createMetaStoreClient(serverName, 9083);
		initmap(preconnect);
	}
	
	public MetaStoreClient(String uris) throws MetaException {
		client = createMetaStoreClient(uris);
		byUri = uris;
		initmap(false);
	}
	
	public MetaStoreClient(String uri, String user, String passwd) throws MetaException {
		if (!useHA) {
			// get msUri from redis
			if (rf == null) {
				rf = new RedisFactory(uri, "mymaster", 30 * 1000);
			}

			refreshActiveMSS(true);
			if (master_id == -1l) {
				throw new MetaException("No service master registered, please retry later.");
			} else {
				String msUri = "thrift://" + servers.get(master_id);
				client = createMetaStoreClient(msUri);
				byUri = msUri;
				initmap(false);
			}
			timer.schedule(new MSCTimerTask(), 0, 5000);
			useHA = true;
		} else {
			refreshActiveMSS(false);
			if (master_id == -1l) {
				throw new MetaException("No service master registered, please retry later.");
			} else {
				String msUri = "thrift://" + servers.get(master_id);
				client = createMetaStoreClient(msUri);
				byUri = msUri;
				initmap(false);
			}
		}
	}
	
	private void initmap(boolean preconnect) throws MetaException {
		// get local attribution
		try {
			local_db = client.get_local_attribution();
			local_alterUri = client.get_ms_uris();
			// get alter uri, and append it to normal uri
			if (local_alterUri != null && local_alterUri.length() > 0) {
				// check if we should update our URI
				boolean needUpdate = true;
				
				if (byUri != null && byUri.startsWith("thrift://")) {
					if (byUri.substring(9).equalsIgnoreCase(local_alterUri)) {
						needUpdate = false;
					}
				}
				if (needUpdate) {
					try {
						client.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
					if (byUri != null)
						client = createMetaStoreClient(byUri + "," + local_alterUri);
					else
						client = createMetaStoreClient("thrift://" + master_host + ":" + 
								master_port + "," + local_alterUri);
				}
			}
		} catch (TException e) {
			throw new MetaException(e.toString());
		}
		climap.put(local_db.getName(), client);
		alterUriMap.put(local_db.getName(), local_alterUri);
		
		// get all attributions
		List<Database> ld;
		try {
			ld = client.get_all_attributions();
		} catch (TException e) {
			throw new MetaException(e.toString());
		}
		for (Database db : ld) {
			if (!db.getName().equals(local_db.getName())) {
				if (preconnect) {
					System.out.println("Try to connect to Attribution " + db.getName() + ", uri=" + db.getParameters().get("service.metastore.uri"));
					try { 
						IMetaStoreClient cli = createMetaStoreClient(db.getParameters().get("service.metastore.uri"));
						String uri = cli.get_ms_uris();
						climap.put(db.getName(), cli);
						alterUriMap.put(db.getName(), uri);
					} catch (Exception me) {
						System.out.println("Connect to Datacenter " + db.getName() + ", uri=" + db.getParameters().get("service.metastore.uri") + " failed!");
						me.printStackTrace();
					}
				}
			}
		}
	}
	
	public IMetaStoreClient getCli(String db_name) {
		IMetaStoreClient cli =  climap.get(db_name);
		if (cli == null) {
			// do reconnect now
			try {
				Database rdb = client.get_attribution(db_name);
				cli = createMetaStoreClient(rdb.getParameters().get("service.metastore.uri"));
				String uri = cli.get_ms_uris();
				climap.put(db_name, cli);
				alterUriMap.put(db_name, uri);
			} catch (NoSuchObjectException e) {
				System.out.println(e);
			} catch (MetaException e) {
				System.out.println(e);
			} catch (TException e) {
				System.out.println(e);
			}
		}
		return cli;
	}
	
	public IMetaStoreClient getLocalCli() {
		return client;
	}
	
	public void stop() {
		for (Map.Entry<String, IMetaStoreClient> e : climap.entrySet()) {
			try {
				e.getValue().close();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}
	
	public static String splitValueToString(List<SplitValue> values) {
		String r = "", keys = "", vals = "";
		
		if (values == null)
			return "null";
		
		for (SplitValue sv : values) {
			long value = 0;
			try {
				value = Long.parseLong(sv.getValue());
			} catch (Exception e) {
				value = -1;
			}
			String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(value * 1000));
			keys += sv.getSplitKeyName() + ",";
			if (value >= 0)
				vals += "L:" + sv.getLevel() + ":V:" + sv.getVerison() + ":" + sv.getValue() + "(" + date + "),";
			else
				vals += "L:" + sv.getLevel() + ":V:" + sv.getVerison() + ":" + sv.getValue() + ",";
		}
		r += "KEYS [" + keys + "], VALS [" + vals + "]";
		
		return r;
	}
	
	public static String toStringSFLVS(int vs) {
		switch (vs) {
		case MetaStoreConst.MFileLocationVisitStatus.OFFLINE:
			return "offline";
		case MetaStoreConst.MFileLocationVisitStatus.ONLINE:
			return "online";
		case MetaStoreConst.MFileLocationVisitStatus.SUSPECT:
			return "suspect";
		}
		return "unknown";
	}
	
	public static String toStringSFileLocation(SFileLocation sfl) {
		String r = "<";
		
		r += "fid:" + sfl.getFid() + 
				", node:" + sfl.getNode_name() + 
				", dev:" + sfl.getDevid() + 
				", location:" + sfl.getLocation() + 
				", repid:" + sfl.getRep_id() + 
				", updateTs:" + sfl.getUpdate_time() + 
				", visit_status:" + toStringSFLVS(sfl.getVisit_status()) + 
				", digest:" + sfl.getDigest();
		r += ">";
		
		return r;
	}
	
	public static String toStringSFile(SFile file) {
		if (file == null) {
			return "null";
		}
		
		String r = "<";
		r += "fid:" + file.getFid() + ", ";
		r += "db:" + file.getDbName() + ", ";
		r += "table:" + file.getTableName() + ", ";
		r += "status: " + file.getStore_status() + ", ";
		r += "repnr: " + file.getRep_nr() + ", ";
		r += "digest: " + file.getDigest() + ", ";
		r += "rec_nr: " + file.getRecord_nr() + ", ";
		r += "allrec_nr: " + file.getAll_record_nr() + ",";
		r += "len: " + file.getLength() + ",";
		r += "values: {" + splitValueToString(file.getValues()); 
		r += "}, [\n";
		if (file.getLocations() != null) {
			for (SFileLocation loc : file.getLocations()) {
				r += loc.getNode_name() + ":" + loc.getDevid() + ":"
						+ loc.getLocation() + ":" + loc.getRep_id() + ":"
						+ loc.getUpdate_time() + ":" + toStringSFLVS(loc.getVisit_status())
						+ ":" + loc.getDigest() + "\n";
			}
		} else {
			r += "NULL";
		}
		r += "]>";
		
		return r;
	}
	
	public static class FgetThread extends Thread {
		private MetaStoreClient cli;
		public String xURI;
		public String serverName;
		public int serverPort;
		public long begin, end, sum;
		public boolean getlen = true;
		public boolean failed = false;
		public TreeMap<Long, Map<String, FileStat>> fmap;
		
		public FgetThread(MetaStoreClient cli, String xURI, String serverName, int serverPort, 
				TreeMap<Long, Map<String, FileStat>> fmap, long begin, long end, 
				boolean getlen) {
			this.cli = cli;
			this.begin = begin;
			this.end = end;
			this.fmap = fmap;
			this.sum = begin;
			this.getlen = getlen;
		}
		
		public void run() {
			try {
				for (long i = begin; i < end; i += 1000) {
					while (cli == null) {
						cli = __reconnect(xURI, serverName, serverPort);
					}

					List<Long> fids = new ArrayList<Long>();
					for (long j = i; j < i + 1000; j++) {
						fids.add(new Long(j));
					}
					try {
						List<SFile> files = cli.client.get_files_by_ids(fids);
						synchronized (fmap) {
							try {
								statfs2_update_map(cli, fmap, files, getlen);
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					} catch (FileOperationException e) {
						e.printStackTrace();
					} catch (MetaException e) {
						e.printStackTrace();
						cli = null;
						continue;
					} catch (TException e) {
						e.printStackTrace();
						cli = null;
						continue;
					}
					sum = i + 1000;
				}
				cli.stop();
				System.out.println("\rDone.");
			} catch (Exception e) {
				e.printStackTrace();
				failed = true;
			}
		}
	}
	
	public static class LFDThread extends Thread {
		private MetaStoreClient cli;
		public long begin, end;
		public String digest;
		public String line = "";
		public long fnr = 0;

		public LFDThread(MetaStoreClient cli, String digest) {
			this.cli = cli;
			this.digest = digest;
		}
		
		public void run() {
			try {
				long start = System.nanoTime();
				List<Long> files = cli.client.listFilesByDigest(digest);
				long stop = System.nanoTime();
				
				fnr = files.size();
				if (fnr > 0) {
					begin = System.nanoTime();
					for (Long fid : files) {
						SFile f = cli.client.get_file_by_id(fid);
						line += "fid " + f.getFid();
					}
					end = System.nanoTime();
					System.out.println(Thread.currentThread().getId() + "--> Search by digest consumed " + (stop - start) / 1000.0 + " us.");
					System.out.println(Thread.currentThread().getId() + "--> Get " + files.size() + " files in " + (end - begin) / 1000.0 + " us, GPS is " + files.size() * 1000000000.0 / (end - begin));
				}
			} catch (MetaException e) {
				e.printStackTrace();
			} catch (TException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static class PingPongThread extends Thread {
		MetaStoreClient cli;
		public long begin, end;
		public long ppnr, pplen;
		
		public PingPongThread(MetaStoreClient cli, long ppnr, long pplen) {
			this.cli = cli;
			this.ppnr = ppnr;
			this.pplen = pplen;
		}
		
		public void run() {
			StringBuffer sb = new StringBuffer();
			
    		for (int i = 0; i < pplen; i++) {
    			sb.append(Integer.toHexString(i).charAt(0));
    		}
    		begin = System.nanoTime();
    		try {
    			for (int i = 0; i < ppnr; i++) {
    				cli.client.pingPong(sb.toString());
    			}
    		} catch (MetaException e) {
    			e.printStackTrace();
    		} catch (TException e) {
    			e.printStackTrace();
    		}
    		end = System.nanoTime();
		}
	}
	
	public static boolean runRemoteCmd(String cmd) throws IOException {
		Process p = Runtime.getRuntime().exec(new String[] {"/bin/bash", "-c", cmd});
		try {
			InputStream err = p.getErrorStream();
			InputStreamReader isr = new InputStreamReader(err);
			BufferedReader br = new BufferedReader(isr);

			String line = null;

			System.out.println("<ERROR>");

			while ((line = br.readLine()) != null)

				System.out.println(line);
			System.out.println("</ERROR>");

			int exitVal = p.waitFor();
			System.out.println(" -> exit w/ " + exitVal);
			br.close();
			isr.close();
			err.close();
			if (exitVal > 0)
				return false;
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return true;
	}
	
	public static String runRemoteCmdWithResult(String cmd) throws IOException {
		return runRemoteCmdWithResultVerbose(cmd, true);
	}
	
	public static String runRemoteCmdWithResultVerbose(String cmd, boolean verbose) throws IOException {
		Process p = Runtime.getRuntime().exec(new String[] {"/bin/bash", "-c", cmd});
		StringBuilder result = new StringBuilder();
		
		try {
			InputStream err = p.getErrorStream();
			InputStreamReader isr = new InputStreamReader(err);
			BufferedReader br = new BufferedReader(isr);

			String line = null;

			if (verbose) System.out.println("<ERROR>");

			while ((line = br.readLine()) != null) {
				if (verbose) System.out.println(line);
			}
			if (verbose) System.out.println("</ERROR>");
			br.close();
			isr.close();
			err.close();
			
			InputStream out = p.getInputStream();
			isr = new InputStreamReader(out);
			br = new BufferedReader(isr);

			if (verbose) System.out.println("<OUTPUT>");

			while ((line = br.readLine()) != null) {
				result.append(line + "\n");
				if (verbose) System.out.println(line);
			}
			if (verbose) System.out.println("</OUTPUT>");

			int exitVal = p.waitFor();
			if (verbose) System.out.println(" -> exit w/ " + exitVal);
			br.close();
			isr.close();
			out.close();
			if (exitVal > 0)
				return result.toString();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return result.toString();
	}
	
	public static long __get_file_length(MetaStoreClient cli, SFile f) throws MetaException, IOException {
		if (f.getLocationsSize() == 0)
			return 0;
		SFileLocation sfl = f.getLocations().get(0);
		String mp;
		String n = sfl.getNode_name();
		try {
			if (n.contains(";")) {
				n = sfl.getNode_name().split(";")[0];
			}
			mp = cli.client.getMP(n, sfl.getDevid());
		} catch (TException e) {
			e.printStackTrace();
			return 0;
		}
		String cmd = "ssh " + n + " du -s " + mp + "/" + sfl.getLocation();
		String result = runRemoteCmdWithResult(cmd);
		long r = 0;
		
		if (!result.equals("")) {
			String[] res = result.split("\t");
			try {
				r = Long.parseLong(res[0]);
			} catch (NumberFormatException nfe) {
				nfe.printStackTrace();
			}
		}
		
		return r * 1024;
	}
	
	public static class FreeSpace {
		Double ratio;
		Double l1ratio;
		Double l2ratio;
		Double l3ratio;
		Double l4ratio;
		long total;
	}
	
	public static boolean __large(Double cur, Double target) {
		if (cur == null || Double.compare(cur, Double.NaN) == 0) {
			cur = Double.MAX_VALUE;
		}
		return cur > target;
	}
	
	public static FreeSpace __get_free_space_ratio(MetaStoreClient cli) throws MetaException, TException, NumberFormatException, IOException {
		String dms = cli.client.getDMStatus();
		BufferedReader bufReader = new BufferedReader(new StringReader(dms));
		String line = null;
		FreeSpace fs = new FreeSpace();
		
		while ((line = bufReader.readLine()) != null) {
			if (line.startsWith("True  space")) {
				String[] ls = line.split(" ");
				fs.total = Long.parseLong(ls[3].substring(0, ls[3].length() - 2)) * 1000000;
				fs.ratio = Double.parseDouble(ls[ls.length - 1]);
			} else if (line.startsWith("L1 True  space")) {
				String[] ls = line.split(" ");
				fs.l1ratio = Double.parseDouble(ls[ls.length - 1]);
			} else if (line.startsWith("L2 True  space")) {
				String[] ls = line.split(" ");
				fs.l2ratio = Double.parseDouble(ls[ls.length - 1]);
			} else if (line.startsWith("L3 True  space")) {
				String[] ls = line.split(" ");
				fs.l3ratio = Double.parseDouble(ls[ls.length - 1]);
			} else if (line.startsWith("L4 True  space")) {
				String[] ls = line.split(" ");
				fs.l4ratio = Double.parseDouble(ls[ls.length - 1]);
			}
		}
		
		return fs; 
	}
	
	public static void statfs2_update_map(MetaStoreClient cli, TreeMap<Long, Map<String, FileStat>> fmap, List<SFile> files, boolean getlen) throws MetaException, TException, IOException {
		if (files.size() > 0) {
			for (SFile f : files) {
				if (f.getValuesSize() > 0) {
					Long btime = Long.parseLong(f.getValues().get(0).getValue());
					Map<String, FileStat> fsmap = fmap.get(btime);
					
					if (fsmap == null)
						fsmap = new TreeMap<String, FileStat>();
							
					FileStat fs = fsmap.get(f.getTableName());
					if (fs == null)
						fs = new FileStat(f.getTableName());
						
					fs.fids.add(f.getFid());
					// calculate space now
					if (f.getLength() == 0 && f.getLocationsSize() > 0) {
						if (getlen) {
							for (int i = 0; i < f.getLocationsSize(); i++) {
								SFileLocation sfl = f.getLocations().get(i);
								try {
									String n = sfl.getNode_name();
									if (n.contains(";"))
										n = sfl.getNode_name().split(";")[0];
									String mp = cli.client.getMP(n, sfl.getDevid());
									String cmd = "ssh " + n + " du -s " + mp + "/" + sfl.getLocation();
									String result = runRemoteCmdWithResult(cmd);
									if (!result.equals("")) {
										String[] res = result.split("\t");
										try {
											fs.addSpace(Long.parseLong(res[0]) * 1024 / 1000);
											fs.addRecordnr(f.getRecord_nr());
										} catch (NumberFormatException nfe) {
											nfe.printStackTrace();
											continue;
										}
									}
								} catch (MetaException mee) {
									mee.printStackTrace();
									continue;
								}
								break;
							}
						}
					} else if (f.getLength() > 0) {
						fs.addSpace(f.getLength() / 1000);
						fs.addRecordnr(f.getRecord_nr());
					}
					fsmap.put(f.getTableName(), fs);
					fmap.put(btime, fsmap);
				} else {
					// unnamed-db/unnamed-table
					Long btime = 0L;
					Map<String, FileStat> fsmap = fmap.get(btime);
					
					if (fsmap == null)
						fsmap = new TreeMap<String, FileStat>();
					
					FileStat fs = fsmap.get("UNNAMED-DB");
					if (fs == null)
						fs = new FileStat("UNNAMED-DB");
					
					fs.fids.add(f.getFid());
					// calculate space now
					if (f.getLength() == 0 && f.getLocationsSize() > 0) {
						if (getlen) {
							SFileLocation sfl = f.getLocations().get(0);
							String n = sfl.getNode_name();
							if (n.contains(";"))
								n = sfl.getNode_name().split(";")[0];
							String mp = cli.client.getMP(n, sfl.getDevid());
							String cmd = "ssh " + n + " du -s " + mp + "/" + sfl.getLocation();
							String result = runRemoteCmdWithResult(cmd);
							if (!result.equals("")) {
								String[] res = result.split("\t");
								try {
									fs.addSpace(Long.parseLong(res[0]));
								} catch (NumberFormatException nfe) {
									nfe.printStackTrace();
								}
							}
						}
					} else if (f.getLength() > 0) {
						fs.addSpace(f.getLength() / 1000);
					}
					fsmap.put("UNNAMED-DB", fs);
					fmap.put(btime, fsmap);
				}
			}
		}
	}
	
	public static MetaStoreClient __reconnect(String xURI, String serverName, int serverPort) {
		MetaStoreClient tcli = null;
		int err = 0;

		if (xURI == null) {
			if (serverName == null)
				try {
					tcli = new MetaStoreClient();
				} catch (Exception e) {
					e.printStackTrace();
					err = -1;
				}
			else
				try {
					tcli = new MetaStoreClient(serverName, serverPort);
				} catch (Exception e) {
					e.printStackTrace();
					err = -1;
				}
		} else {
			try {
				tcli = new MetaStoreClient(xURI, "user", "passwd");
			} catch (Exception e) {
				e.printStackTrace();
				MetaStoreClient.__EXIT(0);
			}
		}
		if (err == 0)
			tcli.client.setTimeout(120);
		return tcli;
	}
	
	public static boolean update_fmap(MetaStoreClient cli, int lfdc_thread, String xURI, String serverName, int serverPort,
			TreeMap<Long, Map<String, FileStat>> fmap, long from, long to, 
			boolean getlen) {
		boolean isFailed = false;
		
		List<FgetThread> fgts = new ArrayList<FgetThread>();
		for (int i = 0; i < lfdc_thread; i++) {
			MetaStoreClient tcli = null;

			if (xURI == null) {
				if (serverName == null)
					try {
						tcli = new MetaStoreClient();
					} catch (Exception e) {
						e.printStackTrace();
						MetaStoreClient.__EXIT(0);
					}
				else
					try {
						tcli = new MetaStoreClient(serverName, serverPort);
					} catch (Exception e) {
						e.printStackTrace();
						MetaStoreClient.__EXIT(0);
					}
			} else {
				try {
					tcli = new MetaStoreClient(xURI, "user", "passwd");
				} catch (Exception e) {
					e.printStackTrace();
					MetaStoreClient.__EXIT(0);
				}
			}
			tcli.client.setTimeout(120);
			fgts.add(new FgetThread(tcli, xURI, serverName, serverPort, fmap, 
					from + i * ((to - from) / lfdc_thread), 
					from + (i + 1) * ((to - from) / lfdc_thread), getlen));
		}
		for (FgetThread t : fgts) {
			t.start();
		}

		do {
			long total = 0, cur = 0;
			
			for (FgetThread t : fgts) {
				total += t.end - t.begin;
				cur += t.sum - t.begin;
				isFailed |= t.failed;
			}
			System.out.format("\rGet files %.2f %%", (double) cur / total * 100);
			if (isFailed) {
				System.out.println("\rSome Thread Failed, retry next time.");
				break;
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			if (cur >= total)
				break;
		} while (true);

		for (FgetThread t : fgts) {
			try {
				t.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		return isFailed;
	}
	
	public static List<SFile> __get_table_file_by_date(MetaStoreClient cli, Table t, long date) {
		List<SFile> rf = new ArrayList<SFile>();
		List<SplitValue> lsv = new ArrayList<SplitValue>();
		
		if (t.getFileSplitKeysSize() > 0) {
			int maxv = 0;
			List<PartitionInfo> allpis = PartitionFactory.PartitionInfo.getPartitionInfo(t.getFileSplitKeys());
			
			for (PartitionInfo pi : allpis) {
				if (maxv < pi.getP_version())
					maxv = pi.getP_version();
			}
			List<List<PartitionInfo>> vpis = new ArrayList<List<PartitionInfo>>();
			for (int i = 0; i <= maxv; i++) {
				List<PartitionInfo> lpi = new ArrayList<PartitionInfo>();
				vpis.add(lpi);
			}
			for (PartitionInfo pi : allpis) {
				vpis.get(pi.getP_version()).add(pi);
			}
			// ok, we get versioned PIs; for each version, we generate a LSV and call filterTable
			for (int i = 0; i <= maxv; i++) {
				// BUG: in our lv13 demo systems, versions leaks, so we have to ignore some nonexist versions
				if (vpis.get(i).size() <= 0) {
					System.out.println("Metadata corrupted, version " + i + " leaks for table " + 
							t.getDbName() + "." + t.getTableName() + ".");
					continue;
				}
				if (vpis.get(i).get(0).getP_type() != PartitionFactory.PartitionType.interval)
					continue;
				lsv.add(new SplitValue(vpis.get(i).get(0).getP_col(), 1, ((Long)date).toString(), vpis.get(i).get(0).getP_version()));
				lsv.add(new SplitValue(vpis.get(i).get(0).getP_col(), 1, ((Long)(date + Integer.parseInt(vpis.get(i).get(0).getArgs().get(1)) * 3600)).toString(), vpis.get(i).get(0).getP_version()));
				try {
					List<SFile> files = cli.client.filterTableFiles(t.getDbName(), t.getTableName(), lsv);
					//System.out.println("Got Table " + t.getTableName() + " LSV: " + lsv + " Hit " + files.size());
					if (files.size() > 0)
						rf.addAll(files);
				} catch (Exception e) {
					e.printStackTrace();
				}
				lsv.clear();
			}
		}
		return rf;
	}
	
	public static Map<Long, SFile> __get_files_by_external_file_4mig(String dbName,
			String tableName, String fpath) {
		HashMap<Long, SFile> targets = new HashMap<Long, SFile>();

		try {
			File mfile = new File(fpath);
			FileReader fr = new FileReader(mfile.getAbsoluteFile());
			BufferedReader br = new BufferedReader(fr);
			String line = null, xpline;

			System.out.println("#EXPECT LINE FORMAT: FID|TABNAME|L1V(,)|L2V(-)|HOST|LOC|RECNR");
			while ((line = br.readLine()) != null) {
				String[] ln = line.split("\\|");
				if (ln.length == 7) {
					Long fid = Long.parseLong(ln[0]);
					Long recordnr = Long.parseLong(ln[6]);

					xpline = "FID " + fid + " TABNAME " + ln[1] +
							" L1V " + ln[2] + " L2V " + ln[3] +
							" HOST " + ln[4] +
							" LOC " + ln[5] + " RECNR " + recordnr;
					if (!targets.containsKey(fid)) {
						System.out.println("#INSERT " + xpline);
						SFile nsf = new SFile();
						nsf.setDbName(dbName);
						nsf.setTableName(tableName);
						nsf.setFid(fid);
						nsf.setStore_status(MetaStoreConst.MFileStoreStatus.REPLICATED);
						nsf.setRep_nr(2);
						nsf.setDigest("MIGRATE_BY_EXTERNAL_FILE_IMPORT_" + fid);
						nsf.setRecord_nr(recordnr);
						// set split values
						List<SplitValue> values = new ArrayList<SplitValue>();
						SplitValue l11 = new SplitValue();
						SplitValue l12 = new SplitValue();
						SplitValue l2 = new SplitValue();
						String[] sa1 = ln[2].split("=");
						if (sa1 != null && sa1.length == 2) {
							l11.setSplitKeyName(sa1[0]);
							l12.setSplitKeyName(sa1[0]);
							l11.setLevel(1);
							l12.setLevel(1);
							String[] sa12 = sa1[1].split(",");
							if (sa12 != null && sa12.length == 2) {
								l11.setValue(sa12[0]);
								l12.setValue(sa12[1]);
							}
						}
						String[] sa2 = ln[3].split("=");
						if (sa2 != null && sa2.length == 2) {
							l2.setSplitKeyName(sa2[0]);
							l2.setLevel(2);
							String[] sa22 = sa2[1].split("-");
							if (sa22 != null && sa22.length == 2) {
								l2.setValue(sa22[1]);
							}
						}
						values.add(l11);
						values.add(l12);
						values.add(l2);
						nsf.setValues(values);
						// set SFL
						SFileLocation sfl = new SFileLocation();
						sfl.setNode_name(ln[4]);
						sfl.setFid(fid);
						sfl.setLocation(ln[5]);
						sfl.setVisit_status(MetaStoreConst.MFileLocationVisitStatus.ONLINE);
						// add this file to hash map
						nsf.addToLocations(sfl);
						targets.put(nsf.getFid(), nsf);
					} else {
						System.out.println("#IGNORE " + xpline);
					}
				}
			}
			br.close();
			fr.close();
		} catch (Exception e) {
			e.printStackTrace();
			targets.clear();
		}
		return targets;
	}

	public static Map<Long, SFile> __get_table_files_4mig(MetaStoreClient cli, 
			String dbName, String tableName,
			String bdate, String edate) {
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH");
		Date bd, ed;
		HashMap<Long, SFile> targets = new HashMap<Long, SFile>();

		// [bdate, edate)
		try {
			bd = df.parse(bdate);
			ed = df.parse(edate);
		} catch (ParseException e1) {
			e1.printStackTrace();
			return targets;
		}

		// find dates' files
		try {
			Table tab = cli.client.getTable(dbName, tableName);

			for (long i = bd.getTime() / 1000; i < ed.getTime() / 1000; i += 3600) {
				try {
					System.out.println("Handle date " + df.format(new Date(i * 1000)));
					List<SFile> files = __get_table_file_by_date(cli,
							tab, i);
					if (files != null) {
						for (SFile f : files) {
							if (f.getStore_status() != MetaStoreConst.MFileStoreStatus.RM_PHYSICAL &&
									f.getLocationsSize() > 0) {
								targets.put(f.getFid(), f);
							}
						}
					}
				} catch (Exception e) {
					System.out.println("Table " + dbName + "." + tableName + " metadata corrupted?");
					e.printStackTrace();
				}
			}
		} catch (MetaException e) {
			e.printStackTrace();
		} catch (NoSuchObjectException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
		
		return targets;
	}
	
	public static boolean isSFileUpdated(SFile f, long ts) {
		if (f.getLocationsSize() > 0) {
			for (SFileLocation sfl : f.getLocations()) {
				if (sfl.getUpdate_time() > ts)
					return true;
			}
		}
		return false;
	}
	
	public static class FileNRs {
		public SFile file;
		public long rec_nr;
		public long all_rec_nr;
		public long length;
		public long ts;
		
		public FileNRs(SFile f) {
			file = f;
			rec_nr = -1;
			all_rec_nr = -1;
			length = -1;
			ts = 0;
		}
	}
	
	public static FileNRs __compute_nrs(MetaStoreClient cli, SFile f) {
		FileNRs fnr = new FileNRs(f);
		
		if (f.getLocationsSize() > 0) {
			for (int i = 0; i < f.getLocationsSize(); i++) {
				SFileLocation sfl = f.getLocations().get(i);
				try {
					String n = sfl.getNode_name();
					if (n.contains(";"))
						n = sfl.getNode_name().split(";")[0];
					String cmd = "ssh %s 'cd sotstore/dservice ; java -cp build/devmap.jar:build/iie.jar:lib/lucene-core-4.2.1.jar -Djava.library.path=build/ iie.metastore.LuceneStat %s %s'";
					String result = runRemoteCmdWithResultVerbose(String.format(cmd, 
							n, sfl.getDevid(), sfl.getLocation()), false);
					if (!"".equals(result) && result.indexOf("$") >= 0) {
						int start = result.indexOf("$");
						int stop = result.indexOf(")");
						result = result.substring(start+2,stop);	
						String[] dres = result.split(",");
						try {
							fnr.length = Long.parseLong(dres[1]);
							fnr.rec_nr = Long.parseLong(dres[0]);
							fnr.ts = System.currentTimeMillis();
							break;
						} catch (NumberFormatException nfe) {
							nfe.printStackTrace();
							continue;
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
					continue;
				}
				break;
			}
		}
		return fnr;
	}
	
	public static class FileStat {
		public String table;
		public TreeSet<Long> fids;
		public long space;	// space total used
		public long recordnr;
		
		public FileStat(String table) {
			this.table = table;
			fids = new TreeSet<Long>();
			space = 0;
			recordnr = 0;
		}
		
		public void addSpace(long toAdd) {
			space += toAdd;
		}
		
		public void addRecordnr(long toAdd) {
			recordnr += toAdd;
		}
	}
	
	public static class ScrubRule {
		public String type;
		public int soft, hard;
		public enum ScrubAction {
			DELETE, DOWNREP,
		}
		public ScrubAction action;
		
		public String toString() {
			String r = "";
			String act = null;
			
			switch (action) {
			case DELETE:
				act = "delete";
				break;
			case DOWNREP:
				act = "downrep";
				break;
			default:
				act = "unknown";
			}
			r += "Rule -> {" + type + ", soft=" + soft + ", hard=" + hard + ", action=" + act + "}";
			return r;
		}
	}
	
	public static class ScrubnRule {
		public String type;
		public List<Integer> times = new ArrayList<Integer>();
		public List<String> rules = new ArrayList<String>();
		public enum ScrubAction {
			DELETE, DOWNREP, MIGRATE,
		}
		public ScrubAction action;
		
		public String toString() {
			String r = "Rule -> {" + type;
			for(int i = 1 ; i <= 4 ; i++){
				String act = null;
				if(rules.get(i-1).equals("del")){
					action = ScrubAction.DELETE;
				} else if(rules.get(i-1).equals("drep")){
					action = ScrubAction.DOWNREP;
				}else if(rules.get(i-1).equals("migr")){
					action = ScrubAction.MIGRATE;
				} 
				
				switch (action) {
				case DELETE:
					act = "delete";
					break;
				case DOWNREP:
					act = "downrep";
					break;
				case MIGRATE:
					act = "migrate";
					break;
				default:
					act = "unknown";
				}
				if(i == 4){
					r+= "; t" + i + "_limit=" + times.get(i-1) + ", t" + i + "_action=" + act + "}";
				}else{
					r+= "; t" + i + "_limit=" + times.get(i-1) + ", t" + i + "_action=" + act;
				}
			}
			return r;
		}
	}
	
	public static class Option {
	     String flag, opt;
	     public Option(String flag, String opt) { this.flag = flag; this.opt = opt; }
	}
	
	private static class _FSNR {
		Long s[];
		public _FSNR() {
			s = new Long[5];
			for (int i = 0; i < s.length; i++) {
				s[i] = 0L;
			}
		}
	}
	
	public static void main(String[] args) throws IOException {
		MetaStoreClient cli = null;
		String node = null;
		String serverName = null;
		int serverPort = 9083;
		List<String> ipl = new ArrayList<String>();
		int repnr = 3;
		SFile file = null, r = null;
		List<String> argsList = new ArrayList<String>();  
	    List<Option> optsList = new ArrayList<Option>();
	    List<String> doubleOptsList = new ArrayList<String>();
	    String dbName = null, tableName = null, partName = null, to_dc = null, to_db = null, to_nas_devid = null,
	    		tunnel_in = null, tunnel_out = null, tunnel_node = null, tunnel_user = null;
	    int prop = 0, pplen = 0, ppnr = 1, ppthread = 1, lfdc_thread = 1;
	    String devid = null;
	    long balanceNum = 0l;
	    String node_name = null;
	    String sap_key = null, sap_value = null;
	    String flt_l1_key = null, flt_l1_value = null, flt_l2_key = null, flt_l2_value = null;
	    int flctc_nr = 0;
	    String digest = "";
	    boolean lfd_verbose = false;
	    long begin_time = -1, end_time = -1, statfs_range = -1;
	    String ANSI_RESET = "\u001B[0m";
	    String ANSI_RED = "\u001B[31m";
	    String ANSI_GREEN = "\u001B[32m";
	    long ofl_fid = -1, srep_fid = -1, fsck_begin = -1, fsck_end = -1;
	    int srep_repnr = -1;
	    String ofl_sfl_dev = null;
	    boolean ofl_del = false;
	    int flt_version = 0;
	    String ng_name = null;
	    boolean statfs2_xj = false, statfs2_del = false, statfs2_getlen = true;
	    String statfs2_tbl = "all";	// dx_rz, ybrz, cdr
	    long statfs2_bday = -1, statfs2_days = -1;
	    String scrub_rule = null;
	    long scrub_max = -1;
	    String dfl_dev = null, dfl_location = null;
	    String dfl_file = null;
	    int fls_op = -1;
	    String fls_args = "l2";
	    int old_port = 8111, new_port = 10101;
	    int devtype = 0, devquota = 100;
	    int upnr_days = 1;
	    String bdate = null, edate = null;
	    String nethint = "69";
	    int bwlimit = 1000;
	    boolean useHost = true;
	    String remoteUri = null, ng = null;
	    String ds_fname = null;
	    String ds_ftype = null;
	    String ds_fargs = null;
	    boolean ds_del = false, ds_verbose = false;
	    String ds_df = null;
	    int mignr_max = 100;
	    // for MIG target, we prefer to migrate T2 time;
	    int mig_prio = 2;
	    String xURI = null;
	    String fimportPath = null;
	    long seed = System.currentTimeMillis();
	    
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
	                    	optsList.add(new MetaStoreClient.Option(args[i], args[i+1]));
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
		
	    for (Option o : optsList) {
	    	if (o.flag.equals("-h")) {
	    		// print help message
	    		System.out.println("-h   : print this help.");
	    		System.out.println("-r   : server name.");
	    		System.out.println("-p   : server port.");
	    		
	    		System.out.println("-uri : server URI.");
	    		
	    		System.out.println("\n[Node]");
	    		System.out.println("-n   : add current machine as a new node.");
	    		System.out.println("-nn  : add node with specified name.");
	    		System.out.println("-dn  : delete node.");
	    		System.out.println("-ln  : list existing node.");
	    		
	    		System.out.println("\n[File and FileLocation]");
	    		System.out.println("-f   : auto test file operations, from create to delete.");
	    		System.out.println("-frr : read the file object by fid.");
	    		System.out.println("-fro : reopen a file.");
	    		System.out.println("-srep: (re)set file repnr.");
	    		System.out.println("-fcr : create a new file and return the fid.");
	    		System.out.println("-fcl : close the file.");
	    		System.out.println("-fcd : delete the file.");
	    		System.out.println("-gbn : get a file by SFL keys.");
	    		System.out.println("-ofl : offline a file location.");
	    		System.out.println("-dfl : delete a file location and remove the physical data.");
	    		System.out.println("-dflf: delete a file location (read from a file).");
	    		System.out.println("-lfbd: list FID by devices.");
	    		System.out.println("-rep : replicate FID to specified type device.");
	    		System.out.println("-frpt: file report for specified day.");
	    		System.out.println("-ds  : data search based on 'lucene searcher'.");
	    		
	    		System.out.println("\n[Device]");
	    		System.out.println("-sd  : show device.");
	    		System.out.println("-md  : modify device: change prop or attached node.");
	    		System.out.println("-cd  : add new device.");
	    		System.out.println("-dd  : delete device.");
	    		System.out.println("-ld  : list existing devices.");
	    		System.out.println("-ldd : list existing devices with more infos.");
	    		System.out.println("-ond : online device.");
	    		System.out.println("-ofd : offline device.");
	    		System.out.println("-ofdp: offline device physically.");
	    		System.out.println("-ldbn: list device by node.");
	    		System.out.println("-lossrpt: generate data lost report for specified devices.");
	    		
	    		System.out.println("\n[DM Info]");
	    		System.out.println("-gni : get current active Node Info from DM.");
	    		System.out.println("-dms : get current DM status.");
	    		
	    		System.out.println("\n[DB and Table]");
	    		System.out.println("-lt  : list regular db.tables and filter with pattern.");
	    		System.out.println("-sap : set attribution parameters.");
	    		System.out.println("-lst : list table files.");
	    		System.out.println("-lfd : list files by digest.");
	    		System.out.println("-flt : filter table files.");
                System.out.println("-flc : count stats of the filter table files.");
	    		System.out.println("-tct : truncate table files.");
	    		System.out.println("-ltg : list table's nodegroups.");
	    		System.out.println("-trunc: trunc table files FAST.");
	    		
	    		System.out.println("\n[Tools]");
	    		System.out.println("-FSCK   : do system checking: check md5sum of files' locations.");
	    		System.out.println("-cvt    : convert date to timestamp.");
	    		System.out.println("-tsm    : toggle safe mode of DM.");
	    		System.out.println("-alz    : analyze the system to report file nr and space.");
	    		System.out.println("-statfs : stat the file system and report file state.");
	    		System.out.println("-statfs2: scan files to REMOVE/DELETE.");
	    		System.out.println("-statfs3: scan files to get record/size info.");
	    		System.out.println("-scrub_fast: use multi-thread to get files.");
	    		System.out.println("-avglen : get avg len by table split value.");
	    		System.out.println("-scrub  : into scrub mode, do auto clean.");
	    		System.out.println("-scrubn : into scrubn mode, do auto clean.");
	    		System.out.println("-fls    : control FLSelector watch list.");
	    		System.out.println("-statchk: do OldMS/NewMS file/filelocation status check.");
	    		System.out.println("-rchk   : NewMS redis index/content check.");
	    		System.out.println("-rfix   : NewMS redis index/content check and fix.");
	    		System.out.println("-sysi   : System monitor info.");
	    		System.out.println("-upnrs  : Update SFile nrs periodically.");
	    		
	    		System.out.println("\n[Test]");
	    		System.out.println("-pp    : ping pong latency test.");
	    		System.out.println("-lst_test: list table files' test (single thread).");
	    		System.out.println("-flctc : lots of file createtion test.");
	    		System.out.println("-lfdc  : concurrent liFst files by digest test.");
	    		System.out.println("-slsb  : set loadstatus bad.");
	    			    		
	    		System.out.println("");
	    		System.out.println("Be careful with following operations!");
	    		System.out.println("");
	    			    		
	    		System.out.println("-bdnu : need to data balance 's quantities.");
	    		System.out.println("-dabal : data balance operation.");
	    		
	    		System.out.println("");
	    		System.out.println("Examples:");
	    		System.out.println("");
	    		
	    		System.out.println("# Flush existing orders");
	    		System.out.println(" -fls -db db1 -table t_cx_rz -fls_op 4 -fls_args \";\"");
	    		System.out.println("");
	    		
	    		System.out.println("# Update orders");
	    		System.out.println(" -fls -db db1 -table t_cx_rz -fls_op 4 -fls_args \"l1;l2\"");
	    		System.out.println("");
	    		
	    		System.out.println("# Add table to FLS");
	    		System.out.println(" -fls -db db1 -table t_cx_rz -fls_op 0 -fls_args \"none/fair_nodes/ordered_alloc\"");
	    		System.out.println("");
	    		
	    		System.out.println("# Delete FLS");
	    		System.out.println(" -fls -db db1 -table t_cx_rz -fls_op 1");
	    		System.out.println("");
	    		
	    		System.out.println("# Flush FLS");
	    		System.out.println(" -fls -db db1 -table t_cx_rz -fls_op 2");
	    		System.out.println("");
	    		
	    		System.out.println("# Repnr FLS");
	    		System.out.println(" -fls -db db1 -table t_cx_rz -fls_op 3 -fls_args NR");
	    		System.out.println("");
	    		
	    		System.out.println("# ORDER FLS: ordered alloc advices, default as 'l2;l3;l4'");
	    		System.out.println(" -fls -db db1 -table t_cx_rz -fls_op 4 -fls_args \"l1;l2;l3;l4\"");
	    		System.out.println("");
	    		
	    		System.out.println("# Round FLS: r1: create_file; r2: rep_thread; r3: do_replicate, default as 'l2;l2;l2'");
	    		System.out.println(" -fls -db db1 -table t_cx_rz -fls_op 5 -fls_args \"l1;l2;l4\"");
	    		System.out.println("");
	    		
	    		System.out.println("# Set new dev type and new quota");
	    		System.out.println("-md -node NODE32 -devid nas1-part1 -devtype 1 -devquota 90");
	    		System.out.println("");
	    		
	    		System.out.println("# Manual replicate");
	    		System.out.println(" -rep FID -devtype [4,0,5,1]");
	    		System.out.println("");
	    		
	    		System.out.println("# Add all dx_rz tables to FLS as ordered_alloc tables");
	    		System.out.println("./mstool.sh -r node13 -p 10101 -lt .*t_dx_rz_[^q].* | sed -n '/--/,${/--/!p;}' | sed -e 's|\\([^.]*\\).\\(.*\\)$|./mstool.sh -r node13 -p 10101 -fls -db \\1 -table \\2 -fls_op 0 -fls_args ordered_alloc|g' | awk '{system($0);}'");
	    		System.out.println("");
	    		
	    		System.out.println("# Add all qydx tables to FLS as fair_nodes tables");
	    		System.out.println("./mstool.sh -r node13 -p 10101 -lt .*t_dx_rz_[q].* | grep qydx | sed -e 's|\\([^.]*\\).\\(.*\\)$|./mstool.sh -r node13 -p 10101 -fls -db \\1 -table \\2 -fls_op 0 -fls_args fair_nodes|g' | awk '{system($0)}'");
	    		System.out.println("");
	    		
	    		MetaStoreClient.__EXIT(0);
	    	}
	    	
	    	if(o.flag.equals("-bdnu")){
				// set balanceNum
	    		if (o.opt == null) {
	    			System.out.println("-bdna : need to data balance 's quantities.");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		balanceNum = Long.parseLong(o.opt);
			}
	    	if (o.flag.equals("-r")) {
	    		// set servername;
	    		serverName = o.opt;
	    	}
	    	if (o.flag.equals("-p")) {
	    		// set serverPort
	    		serverPort = Integer.parseInt(o.opt);
	    	}
	    	if (o.flag.equals("-uri")) {
	    		// set redisSentinelURI
	    		xURI = o.opt;
	    	}
	    	if (o.flag.equals("-prop")) {
	    		// device prop
	    		prop = Integer.parseInt(o.opt);
	    	}
	    	if (o.flag.equals("-devtype")) {
	    		// set device type
	    		System.out.println("TYPE LIST: \n L1(CACHE)=4;\n L2(GENERAL)=0;\n L3(MASS)=5;\n L4(SHARED)=1;\n");
	    		devtype = Integer.parseInt(o.opt);
	    	}
	    	if (o.flag.equals("-devquota")) {
	    		// set device quota
	    		devquota = Integer.parseInt(o.opt);
	    	}
	    	if (o.flag.equals("-devid")) {
	    		// device ID
	    		devid = o.opt;
	    	}
	    	if (o.flag.equals("-node")) {
	    		// node name for device creation
	    		node_name = o.opt;
	    	}
	    	if (o.flag.equals("-table")) {
	    		// set table name
	    		if (o.opt == null) {
	    			System.out.println("-table tableName");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		tableName = o.opt;
	    	}
	    	if (o.flag.equals("-db")) {
	    		// set db name
	    		if (o.opt == null) {
	    			System.out.println("-db dbName");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		dbName = o.opt;
	    	}
	    	if (o.flag.equals("-part")) {
	    		// set part name
	    		if (o.opt == null) {
	    			System.out.println("-part partName");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		partName = o.opt;
	    	}
	    	if (o.flag.equals("-todc")) {
	    		// set to_dc name
	    		if (o.opt == null) {
	    			System.out.println("-todc target_dc");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		to_dc = o.opt;
	    	}
	    	if (o.flag.equals("-todb")) {
	    		// set to_db name
	    		if (o.opt == null) {
	    			System.out.println("-todb target_db");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		to_db = o.opt;
	    	}
	    	if (o.flag.equals("-tonasdev")) {
	    		// set to_nas_devid name
	    		if (o.opt == null) {
	    			System.out.println("-tonasdev NAS_DEVID");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		to_nas_devid = o.opt;
	    	}
	    	if (o.flag.equals("-tunnel_in")) {
	    		// set tunnel name
	    		if (o.opt == null) {
	    			System.out.println("-tunnel_in TUNNEL_IN_PATH");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		tunnel_in = o.opt;
	    	}
	    	if (o.flag.equals("-tunnel_out")) {
	    		// set tunnel_out name
	    		if (o.opt == null) {
	    			System.out.println("-tunnel_out TUNNEL_OUT_PATH");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		tunnel_out = o.opt;
	    	}
	    	if (o.flag.equals("-tunnel_node")) {
	    		// set tunnel_node name
	    		if (o.opt == null) {
	    			System.out.println("-tunnel_node TUNNEL_NODE_NAME");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		tunnel_node = o.opt;
	    	}
	    	if (o.flag.equals("-tunnel_user")) {
	    		// set tunnel_user name
	    		if (o.opt == null) {
	    			System.out.println("-tunnel_user TUNNEL_USER_NAME");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		tunnel_user = o.opt;
	    	}
	    	if (o.flag.equals("-sap_key")) {
	    		// set sap key
	    		if (o.opt == null) {
	    			System.out.println("-sap_key ATTRIBUTION_KEY");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		sap_key = o.opt;
	    	}
	    	if (o.flag.equals("-sap_value")) {
	    		// set sap value
	    		if (o.opt == null) {
	    			System.out.println("-sap_value ATTRIBUTION_VALUE");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		sap_value = o.opt;
	    	}
	    	if (o.flag.equals("-flt_version")) {
	    		// set filter table level_1 version
	    		if (o.opt == null) {
	    			System.out.println("-flt_version version");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		flt_version = Integer.parseInt(o.opt);
	    	}
	    	if (o.flag.equals("-flt_l1_key")) {
	    		// set filter table level_1 key
	    		if (o.opt == null) {
	    			System.out.println("-flt_l1_key level 1 pkey");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		flt_l1_key = o.opt;
	    	}
	    	if (o.flag.equals("-flt_l1_value")) {
	    		// set filter table level_1 value
	    		if (o.opt == null) {
	    			System.out.println("-flt_l1_key level 1 value");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		flt_l1_value = o.opt;
	    	}
	    	if (o.flag.equals("-flt_l2_key")) {
	    		// set filter table level_2 key
	    		if (o.opt == null) {
	    			System.out.println("-flt_l2_key level 2 pkey");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		flt_l2_key = o.opt;
	    	}
	    	if (o.flag.equals("-flt_l2_value")) {
	    		// set filter table level_2 value
	    		if (o.opt == null) {
	    			System.out.println("-flt_l2_value level 2 value");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		flt_l2_value = o.opt;
	    	}
	    	if (o.flag.equals("-pplen")) {
	    		// set ping pong string length
	    		if (o.opt == null) {
	    			System.out.println("-pplen length");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		pplen = Integer.parseInt(o.opt);
	    	}
	    	if (o.flag.equals("-ppnr")) {
	    		// set ping pong number
	    		if (o.opt == null) {
	    			System.out.println("-ppnr number");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		ppnr = Integer.parseInt(o.opt);
	    	}
	    	if (o.flag.equals("-ppthread")) {
	    		// set ping pong thread number
	    		if (o.opt == null) {
	    			System.out.println("-ppthread number");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		ppthread = Integer.parseInt(o.opt);
	    	}
	    	if (o.flag.equals("-flctc_nr")) {
	    		// set lots of files number
	    		if (o.opt == null) {
	    			System.out.println("-flctc_nr number");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		flctc_nr = Integer.parseInt(o.opt);
	    	}
	    	if (o.flag.equals("-lfd_digest")) {
	    		// set digest string
	    		if (o.opt == null) {
	    			System.out.println("-lfd_digest STRING");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		digest = o.opt;
	    	}
	    	if (o.flag.equals("-lfd_verbose")) {
	    		// set LFD verbose flag
	    		if (o.opt == null) {
	    			System.out.println("-lfd_verbose");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		lfd_verbose = true;
	    	}
	    	if (o.flag.equals("-lfdc_thread")) {
	    		// set LFD thread number
	    		if (o.opt == null) {
	    			System.out.println("-lfdc_thread number");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		lfdc_thread = Integer.parseInt(o.opt);
	    	}
	    	if (o.flag.equals("-begin_time")) {
	    		// set begin_time
	    		if (o.opt == null) {
	    			System.out.println("-begin_time timestamp");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		begin_time = Long.parseLong(o.opt);
	    	}
	    	if (o.flag.equals("-end_time")) {
	    		// set end time
	    		if (o.opt == null) {
	    			System.out.println("-end_time timestamp");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		end_time = Long.parseLong(o.opt);
	    	}
	    	if (o.flag.equals("-statfs_range")) {
	    		// set statfs time range
	    		if (o.opt == null) {
	    			System.out.println("-statfs_range timelength");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		statfs_range = Long.parseLong(o.opt);
	    	}
	    	if (o.flag.equals("-scrub_rule")) {
	    		// set scrub rules
	    		if (o.opt == null) {
	    			System.out.println("-scrub_rule RULES");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		scrub_rule = o.opt;
	    	}
	    	if (o.flag.equals("-scrub_max")) {
	    		// set scrub max
	    		if (o.opt == null) {
	    			System.out.println("-scrub_max ID");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		scrub_max = Long.parseLong(o.opt);
	    	}
	    	if (o.flag.equals("-statfs2_xj")) {
	    		// set statfs time range
	    		statfs2_xj = true;
	    	}
	    	if (o.flag.equals("-statfs2_del")) {
	    		// set statfs default action to del
	    		statfs2_del = true;
	    	}
	    	if (o.flag.equals("-statfs2_getlen")) {
	    		// set statfs time range
	    		statfs2_getlen = false;
	    	}
	    	if (o.flag.equals("-statfs2_tbl")) {
	    		// set statfs table
	    		if (o.opt == null) {
	    			System.out.println("-statfs2_tbl TABLE");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		statfs2_tbl = o.opt;
	    	}
	    	if (o.flag.equals("-statfs2_bday")) {
	    		// set statfs2 begin day offset
	    		if (o.opt == null) {
	    			System.out.println("-statfs2_bday 30");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		statfs2_bday = Long.parseLong(o.opt);
	    	}
	    	if (o.flag.equals("-statfs2_days")) {
	    		// set statfs2 days
	    		if (o.opt == null) {
	    			System.out.println("-statfs2_days days");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		statfs2_days = Long.parseLong(o.opt);
	    	}
	    	if (o.flag.equals("-ofl_fid")) {
	    		// set offline file id
	    		if (o.opt == null) {
	    			System.out.println("-ofl_fid fid");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		ofl_fid = Long.parseLong(o.opt);
	    	}
	    	if (o.flag.equals("-ofl_sfl_dev")) {
	    		// set offline sfl device
	    		if (o.opt == null) {
	    			System.out.println("-ofl_sfl_dev DEV");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		ofl_sfl_dev = o.opt;
	    	}
	    	if (o.flag.equals("-ofl_del")) {
	    		// set delete flag for the local file
	    		ofl_del = true;
	    	}
	    	if (o.flag.equals("-srep_fid")) {
	    		// set rep file id
	    		if (o.opt == null) {
	    			System.out.println("-srep_fid fid");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		srep_fid = Long.parseLong(o.opt);
	    	}
	    	if (o.flag.equals("-srep_repnr")) {
	    		// set file repnr
	    		if (o.opt == null) {
	    			System.out.println("-srep_repnr NR");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		srep_repnr = Integer.parseInt(o.opt);
	    	}
	    	if (o.flag.equals("-fsck_begin")) {
	    		// set fsck max
	    		if (o.opt == null) {
	    			System.out.println("-fsck_begin NR");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		fsck_begin = Long.parseLong(o.opt);
	    	}
	    	if (o.flag.equals("-fsck_end")) {
	    		// set fsck max
	    		if (o.opt == null) {
	    			System.out.println("-fsck_end NR");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		fsck_end = Long.parseLong(o.opt);
	    	}
	    	if (o.flag.equals("-ng_name")) {
	    		// set ng name
	    		if (o.opt == null) {
	    			System.out.println("-ng_name NAME");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		ng_name = o.opt;
	    	}
	    	if (o.flag.equals("-dfl_dev")) {
	    		// device ID
	    		if (o.opt == null) {
	    			System.out.println("-dfl_dev devid");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		dfl_dev = o.opt;
	    	}
	    	if (o.flag.equals("-dfl_location")) {
	    		// location
	    		if (o.opt == null) {
	    			System.out.println("-dfl_location loc");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		dfl_location = o.opt;
	    	}
	    	if (o.flag.equals("-dfl_file")) {
	    		// dfl file
	    		if (o.opt == null) {
	    			System.out.println("-dfl_file FILEPATH");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		dfl_file = o.opt;
	    	}
	    	if (o.flag.equals("-fls_op")) {
	    		// set fls_op
	    		if (o.opt == null) {
	    			System.out.println("-fls_op 0/1/2");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		fls_op = Integer.parseInt(o.opt);
	    	}
	    	if (o.flag.equals("-fls_args")) {
	    		// set fls_order
	    		if (o.opt == null) {
	    			System.out.println("-fls_args l1;l2;l3;l4");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		fls_args = o.opt;
	    	}
	    	if (o.flag.equals("-upnr_days")) {
	    		// set upnr_days
	    		if (o.opt == null) {
	    			System.out.println("-upnr_days DAYS");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		upnr_days = Integer.parseInt(o.opt);
	    	}
	    	if (o.flag.equals("-bdate")) {
	    		// set begin date
	    		if (o.opt == null) {
	    			System.out.println("-bdate yyyy-MM-dd-HH");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		bdate = o.opt;
	    	}
	    	if (o.flag.equals("-edate")) {
	    		// set end date
	    		if (o.opt == null) {
	    			System.out.println("-edate yyyy-MM-dd-HH");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		edate = o.opt;
	    	}
	    	if (o.flag.equals("-bwlimit")) {
	    		// set bwlimit
	    		if (o.opt == null) {
	    			System.out.println("-bwlimit XXX");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		bwlimit = Integer.parseInt(o.opt);
	    	}
	    	if (o.flag.equals("-remoteUri")) {
	    		// set remoteUri
	    		if (o.opt == null) {
	    			System.out.println("-remoteUri thrift://xxxx:yyyy");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		remoteUri = o.opt;
	    	}
	    	if (o.flag.equals("-ng")) {
	    		// set node group
	    		if (o.opt == null) {
	    			System.out.println("-ng NG_NAME");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		ng = o.opt;
	    	}
	    	if (o.flag.equals("-ds_fname")) {
	    		// set ds field name
	    		if (o.opt == null) {
	    			System.out.println("-ds_fname FIELD_NAME");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		ds_fname = o.opt;
	    	}
	    	if (o.flag.equals("-ds_ftype")) {
	    		// set ds field type
	    		if (o.opt == null) {
	    			System.out.println("-ds_ftype FIELD_TYPE");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		ds_ftype = o.opt;
	    	}
	    	if (o.flag.equals("-ds_fargs")) {
	    		// set ds field args
	    		if (o.opt == null) {
	    			System.out.println("-ds_fargs FIELD_ARGS");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		ds_fargs = o.opt;
	    	}
	    	if (o.flag.equals("-ds_del")) {
	    		// set ds field DELETE
	    		ds_del = true;
	    	}
	    	if (o.flag.equals("-ds_df")) {
	    		// set display field name, active in verbose mode
	    		if (o.opt == null) {
	    			System.out.println("-ds_df DISPLAY_FIELD");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		ds_df = o.opt;
	    	}
	    	if (o.flag.equals("-ds_verbose")) {
	    		// set ds display VERBOSE mode
	    		ds_verbose = true;
	    	}
	    	if (o.flag.equals("-mignr_max")) {
	    		// set mignr_max
	    		if (o.opt == null) {
	    			System.out.println("-mignr_max NR");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		try {
	    			mignr_max = Integer.parseInt(o.opt);
	    		} catch (Exception e) {}
	    	}
	    	if (o.flag.equals("-mig_prio")) {
	    		// set migrate priority
	    		if (o.opt == null) {
	    			System.out.println("-mig_prio 1/2/3/4");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		try {
	    			mig_prio = Integer.parseInt(o.opt);
	    		} catch (Exception e) {}
	    	}
	    	if (o.flag.equals("-fimpath")) {
	    		// set file import path for migrate
	    		if (o.opt == null) {
	    			System.out.println("-fimpath PATH_TO_FILE_TO_IMPORT");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		fimportPath = o.opt;
	    	}
	    	if (o.flag.equals("-seed")) {
	    		// set seed from MBF/M1BF running
	    		if (o.opt == null) {
	    			System.out.println("-seed SEED_TO_RERUN");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		seed = Long.parseLong(o.opt);
	    	}
	    }
	    if (cli == null) {
	    	if (xURI == null) {
	    		try {
	    			if (serverName == null)
	    				cli = new MetaStoreClient();
	    			else
	    				cli = new MetaStoreClient(serverName, serverPort);
	    		} catch (Exception e) {
	    			e.printStackTrace();
	    			MetaStoreClient.__EXIT(0);
	    		}
	    	} else {
	    		try {
	    			cli = new MetaStoreClient(xURI, "user", "passwd");
	    		} catch (Exception e) {
	    			e.printStackTrace();
	    			MetaStoreClient.__EXIT(0);
	    		}
	    	}
	    }
	    try {
	    	String netPat = "10.*\\." + nethint + "\\..*";
	    	
			if (useHost)
				node = InetAddress.getLocalHost().getHostName();
			else {
				Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
				while (e.hasMoreElements()) {
					NetworkInterface n = (NetworkInterface)e.nextElement();
					Enumeration<InetAddress> ee = n.getInetAddresses();
					while (ee.hasMoreElements()) {
						InetAddress i = (InetAddress)ee.nextElement();
						if (i.getHostAddress().matches(netPat)) {
							node = i.getHostAddress();
							break;
						}
					}
					if (node != null)
						break;
				}
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		}

	    for (Option o : optsList) {
	    	if (o.flag.equals("-pp")) {
	    		// ping pong test
	    		long tppnr;
	    		
	    		tppnr = ppnr / ppthread * ppthread;
	    		List<PingPongThread> ppts = new ArrayList<PingPongThread>();
	    		for (int i = 0; i < ppthread; i++) {
	    			MetaStoreClient tcli = null;
	    			
	    			if (xURI == null) {
	    				if (serverName == null)
	    					try {
	    						tcli = new MetaStoreClient();
	    					} catch (MetaException e) {
	    						e.printStackTrace();
	    						MetaStoreClient.__EXIT(0);
	    					}
	    				else
	    					try {
	    						tcli = new MetaStoreClient(serverName, serverPort);
	    					} catch (Exception e) {
	    						e.printStackTrace();
	    						MetaStoreClient.__EXIT(0);
	    					}
	    			} else {
	    				try {
	    					tcli = new MetaStoreClient(xURI, "user", "passwd");
	    				} catch (Exception e) {
	    					e.printStackTrace();
	    					MetaStoreClient.__EXIT(0);
	    				}
	    			}
	    			ppts.add(new PingPongThread(tcli, tppnr, pplen));
	    		}
	    		long begin = System.nanoTime();
	    		for (PingPongThread t : ppts) {
	    			t.start();
	    		}
	    		for (PingPongThread t : ppts) {
	    			try {
						t.join();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
	    		}
	    		long end = System.nanoTime();
	    		long tp = 0;
	    		for (PingPongThread t : ppts) {
	    			tp += t.ppnr / ((end - begin) / 1000000000.0);
	    		}
	    		System.out.println("PingPong: thread " + ppthread + " nr " + tppnr + " len " + pplen + 
	    				" avg Latency " + (end - begin) / tppnr / 1000.0 + " us, ThroughPut " + tp + ".");
	    	}
	    	if (o.flag.equals("-authtest")) {
	    		// auth test
	    		User user = new User("macan", "111111", System.currentTimeMillis(), "root");
	    		List<MSOperation> ops = new ArrayList<MSOperation>();
	    		
	    		ops.add(MSOperation.CREATETABLE);
	    		try {
					Table tbl = cli.client.getTable("db1", "pokes");
					System.out.println("AUTH CHECK: " + cli.client.user_authority_check(user, tbl, ops));
				} catch (AlreadyExistsException e) {
					e.printStackTrace();
				} catch (InvalidObjectException e) {
					e.printStackTrace();
				} catch (MetaException e) {
					e.printStackTrace();
				} catch (NoSuchObjectException e) {
					e.printStackTrace();
				} catch (TException e) {
					e.printStackTrace();
				}

	    	}
	    	if (o.flag.equals("-m11")) {
	    		// migrate by rsync, stage 1
	    		if (dbName == null || tableName == null || 
	    				bdate == null || edate == null ||
	    				tunnel_node == null || tunnel_out == null) {
	    			System.out.println("Please set dbname,tableName,bdate,edate,tunnel_node,tunnel_out!");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		Map<Long, SFile> targets = __get_table_files_4mig(cli, dbName, 
	    				tableName, bdate, edate);
	    		
	    		// get all files, generate rsync string
	    		for (Map.Entry<Long, SFile> entry : targets.entrySet()) {
	    			System.out.print("#" + entry.getKey() + ",");
	    			
	    			SFileLocation sfl = entry.getValue().getLocations().get(0);
	    			String sourceNode = null;
	    			String sourceFile = null;
	    			try {
	    				if (sfl.getNode_name().contains(";")) {
	    					// NAS device
	    					sourceNode = sfl.getNode_name().split(";")[0];
	    					sourceFile = cli.client.getMP(sourceNode, 
	    							sfl.getDevid()) + sfl.getLocation();
	    				} else {
	    					// local device
	    					sourceNode = sfl.getNode_name();
	    					sourceFile = cli.client.getMP(sourceNode, sfl.getDevid())
	    							+ sfl.getLocation();
	    				}
	    			} catch (Exception e) {
	    				e.printStackTrace();
	    			}
	    			String targetFile = tunnel_out + sfl.getLocation();
	    			// create the target directory now
	    			File tf = new File(targetFile);
	    			System.out.println("#Create parent DIR " + "@" + tunnel_node + ": " + tf.getParent());
	    			String cmd = "ssh " + tunnel_node + " 'mkdir -p " + tf.getParent() + "; " + "chmod ugo+rw " + tf.getParent() + ";'";
	    			System.out.println(cmd);
	    			
	    			// do copy now
	    			System.out.println("#Copy by TUNNEL: " + sourceNode + ":" + sourceFile
	    					+ " -> " + 
	    					tunnel_node + ":" + targetFile);
	    			if (tunnel_user == null)
	    				tunnel_user = "metastore";
	    			cmd = "#scp -rp " + sourceNode + ":" + sourceFile + " " + tunnel_user + "@" + 
	    					tunnel_node + ":" + tf.getParent() + ";";
	    			cmd = "ssh " + sourceNode + " 'rsync --bwlimit=" + bwlimit + " -rpzP " + sourceFile + " " + tunnel_user + "@" +
	    					tunnel_node + ":" + tf.getParent() + ";'";
	    			System.out.println(cmd);
	    		}
	    		System.out.println();
	    	}
	    	if (o.flag.equals("-m1f")) {
	    		// migrate by rsync, stage 1 and 2, create metadata in remote db
	    		if (dbName == null || tableName == null || to_dc == null || devid == null ||
	    				bdate == null || edate == null ||
	    				tunnel_node == null || remoteUri == null || ng == null || ofl_fid == -1) {
	    			System.out.println("Please set dbName,tableName,to_dc,devid,tunnel_node,remoteUri,ng,ofl_fid");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		Map<Long, SFile> targets = new HashMap<Long, SFile>();
	    		HashMap<Long, SFileLocation> fileMap = new HashMap<Long, SFileLocation>();
	    		
	    		try {
	    			SFile t = cli.client.get_file_by_id(ofl_fid);
	    			targets.put(ofl_fid, t);
	    		} catch (Exception e) {
	    			e.printStackTrace();
	    			break;
	    		}
	    		// get all files, generate rsync string
	    		for (Map.Entry<Long, SFile> entry : targets.entrySet()) {
	    			System.out.print("#" + entry.getKey() + ",");
	    			
	    			if (entry.getValue().getValues() != null) {
	    				for (SplitValue sv : entry.getValue().getValues()) {
	    					sv.setVerison(flt_version);
	    				}
	    			}
	    			SFileLocation sfl = entry.getValue().getLocations().get(0);
	    			String location = sfl.getLocation().replace(dbName, to_dc);
	    			String sourceNode = null;
	    			String sourceFile = null;
	    			try {
	    				if (sfl.getNode_name().contains(";")) {
	    					// NAS device
	    					sourceNode = sfl.getNode_name().split(";")[0];
	    					sourceFile = cli.client.getMP(sourceNode, 
	    							sfl.getDevid()) + sfl.getLocation();
	    				} else {
	    					// local device
	    					sourceNode = sfl.getNode_name();
	    					sourceFile = cli.client.getMP(sourceNode, sfl.getDevid())
	    							+ sfl.getLocation();
	    				}
	    			} catch (Exception e) {
	    				e.printStackTrace();
	    			}
	    			String targetFile = tunnel_out + location;
	    			// create the target directory now
	    			File tf = new File(targetFile);
	    			System.out.println("#Create parent DIR " + "@" + tunnel_node + ": " + tf.getParent());
	    			String cmd = "ssh " + tunnel_node + " 'mkdir -p " + tf.getParent() + "; " + "chmod ugo+rw " + tf.getParent() + ";'";
	    			System.out.println(cmd);
	    			
	    			// do copy now
	    			System.out.println("#Copy by TUNNEL: " + sourceNode + ":" + sourceFile
	    					+ " -> " + 
	    					tunnel_node + ":" + targetFile);
	    			if (tunnel_user == null)
	    				tunnel_user = "metastore";
	    			cmd = "#scp -rp " + sourceNode + ":" + sourceFile + " " + tunnel_user + "@" + 
	    					tunnel_node + ":" + tf.getParent() + ";";
	    			cmd = "ssh " + sourceNode + " 'rsync --bwlimit=" + bwlimit + " -rpzP " + sourceFile + " " + tunnel_user + "@" +
	    					tunnel_node + ":" + tf.getParent() + ";'";
	    			System.out.println(cmd);
	    			
	    			// update to fileMap
	    			sfl.setNode_name(tunnel_node);
	    			sfl.setLocation(location);
	    			fileMap.put(entry.getKey(), sfl);
	    		}
	    		
	    		try {
					Table tbl = cli.client.getTable(dbName, tableName);
					short maxIndexNum = 1000;
					List<Index> idxs = cli.client.listIndexes(dbName, tableName, maxIndexNum);
					if (idxs != null && idxs.size() > 0) {
						for (Index i : idxs) {
							i.setDbName(to_dc);
						}
					}
					List<NodeGroup> ngs = new ArrayList<NodeGroup>();
					ngs.add(new NodeGroup(ng, null, 0, null));
					tbl.setNodeGroups(ngs);
					tbl.setDbName(to_dc);
					// call by remote URI
					MetaStoreClient remote = new MetaStoreClient(remoteUri);
					System.out.println("Auth to " + remoteUri + " " + 
							remote.client.authentication("root", "111111"));
					if (remote.client.migrate_in(tbl, targets, idxs, dbName, devid, fileMap))
						System.out.println("OK");
					else
						System.out.println("Failed");
				} catch (MetaException e) {
					e.printStackTrace();
				} catch (NoSuchObjectException e) {
					e.printStackTrace();
				} catch (TException e) {
					e.printStackTrace();
				}
	    	}
	    	if (o.flag.equals("-m")) {
	    		// migrate by rsync, stage 1 and 2, create metadata in remote db
	    		if (dbName == null || tableName == null || to_dc == null || devid == null ||
	    				bdate == null || edate == null ||
	    				tunnel_node == null || remoteUri == null || ng == null) {
	    			System.out.println("Please set dbName,tableName,to_dc,devid,tunnel_node,remoteUri,ng");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		Map<Long, SFile> targets = __get_table_files_4mig(cli, dbName, 
	    				tableName, bdate, edate);
	    		HashMap<Long, SFileLocation> fileMap = new HashMap<Long, SFileLocation>();
	    		
	    		// get all files, generate rsync string
	    		for (Map.Entry<Long, SFile> entry : targets.entrySet()) {
	    			System.out.print("#" + entry.getKey() + ",");
	    			
	    			if (entry.getValue().getValues() != null) {
	    				for (SplitValue sv : entry.getValue().getValues()) {
	    					sv.setVerison(flt_version);
	    				}
	    			}
	    			SFileLocation sfl = entry.getValue().getLocations().get(0);
	    			String location = sfl.getLocation().replace(dbName, to_dc);
	    			String sourceNode = null;
	    			String sourceFile = null;
	    			try {
	    				if (sfl.getNode_name().contains(";")) {
	    					// NAS device
	    					sourceNode = sfl.getNode_name().split(";")[0];
	    					sourceFile = cli.client.getMP(sourceNode, 
	    							sfl.getDevid()) + sfl.getLocation();
	    				} else {
	    					// local device
	    					sourceNode = sfl.getNode_name();
	    					sourceFile = cli.client.getMP(sourceNode, sfl.getDevid())
	    							+ sfl.getLocation();
	    				}
	    			} catch (Exception e) {
	    				e.printStackTrace();
	    			}
	    			String targetFile = tunnel_out + location;
	    			// create the target directory now
	    			File tf = new File(targetFile);
	    			System.out.println("#Create parent DIR " + "@" + tunnel_node + ": " + tf.getParent());
	    			String cmd = "ssh " + tunnel_node + " 'mkdir -p " + tf.getParent() + "; " + "chmod ugo+rw " + tf.getParent() + ";'";
	    			System.out.println(cmd);
	    			
	    			// do copy now
	    			System.out.println("#Copy by TUNNEL: " + sourceNode + ":" + sourceFile
	    					+ " -> " + 
	    					tunnel_node + ":" + targetFile);
	    			if (tunnel_user == null)
	    				tunnel_user = "metastore";
	    			cmd = "#scp -rp " + sourceNode + ":" + sourceFile + " " + tunnel_user + "@" + 
	    					tunnel_node + ":" + tf.getParent() + ";";
	    			cmd = "ssh " + sourceNode + " 'rsync --bwlimit=" + bwlimit + " -rpzP " + sourceFile + " " + tunnel_user + "@" +
	    					tunnel_node + ":" + tf.getParent() + ";'";
	    			System.out.println(cmd);
	    			
	    			// update to fileMap
	    			sfl.setNode_name(tunnel_node);
	    			sfl.setLocation(location);
	    			fileMap.put(entry.getKey(), sfl);
	    		}
	    		
	    		try {
					Table tbl = cli.client.getTable(dbName, tableName);
					short maxIndexNum = 1000;
					List<Index> idxs = cli.client.listIndexes(dbName, tableName, maxIndexNum);
					if (idxs != null && idxs.size() > 0) {
						for (Index i : idxs) {
							i.setDbName(to_dc);
						}
					}
					List<NodeGroup> ngs = new ArrayList<NodeGroup>();
					ngs.add(new NodeGroup(ng, null, 0, null));
					tbl.setNodeGroups(ngs);
					tbl.setDbName(to_dc);
					// call by remote URI
					MetaStoreClient remote = new MetaStoreClient(remoteUri);
					System.out.println("Auth to " + remoteUri + " " + 
							remote.client.authentication("root", "111111"));
					if (remote.client.migrate_in(tbl, targets, idxs, dbName, devid, fileMap))
						System.out.println("OK");
					else
						System.out.println("Failed");
				} catch (MetaException e) {
					e.printStackTrace();
				} catch (NoSuchObjectException e) {
					e.printStackTrace();
				} catch (TException e) {
					e.printStackTrace();
				}
	    	}
	    	if (o.flag.equals("-m1bf")) {
	    		Random rand = new Random(seed);
	    		System.out.println("#USE SEED: " + seed);
	    		
	    		// migrate by rsync, stage 1 and 2, create metadata in remote db
	    		if (dbName == null || tableName == null || to_dc == null || devid == null ||
	    				tunnel_node == null || remoteUri == null || ng == null || fimportPath == null) {
	    			System.out.println("Please set dbName,tableName,to_dc,devid,tunnel_node,remoteUri,ng");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		Map<Long, SFile> targets = __get_files_by_external_file_4mig(dbName, 
	    				tableName, fimportPath);
	    		
	    		// get all files, generate rsync string
	    		for (Map.Entry<Long, SFile> entry : targets.entrySet()) {
	    			System.out.print("#" + entry.getKey() + ",");
	    			
	    			if (entry.getValue().getValues() != null) {
	    				for (SplitValue sv : entry.getValue().getValues()) {
	    					sv.setVerison(flt_version);
	    				}
	    			}
	    			SFileLocation sfl = entry.getValue().getLocations().get(0);
	    			String new_location = "/data/" + to_dc + "/" + tableName + "/" + 
	    					rand.nextInt(Integer.MAX_VALUE);
	    			String sourceNode = null;
	    			String sourceFile = null;
	    			try {
	    				if (sfl.getNode_name().contains(";")) {
	    					// NAS device
	    					sourceNode = sfl.getNode_name().split(";")[0];
	    					sourceFile = sfl.getLocation();
	    				} else {
	    					// local device
	    					sourceNode = sfl.getNode_name();
	    					sourceFile = sfl.getLocation();
	    				}
	    			} catch (Exception e) {
	    				e.printStackTrace();
	    			}
	    			String targetFile = tunnel_out + new_location;
	    			// create the target directory now
	    			File tf = new File(targetFile);
	    			System.out.println("#Create DIR " + "@" + tunnel_node + ": " + tf.getParent());
	    			String cmd = "ssh " + tunnel_node + " 'mkdir -p " + tf.getAbsolutePath() + "; " + 
	    					"chmod ugo+rw " + tf.getAbsolutePath() + ";'";
	    			System.out.println(cmd);
	    			
	    			// do copy now
	    			System.out.println("#Copy by TUNNEL: " + sourceNode + ":" + sourceFile
	    					+ " -> " + 
	    					tunnel_node + ":" + targetFile);
	    			if (tunnel_user == null)
	    				tunnel_user = "metastore";
	    			cmd = "#scp -rp " + sourceNode + ":" + sourceFile + " " + tunnel_user + "@" + 
	    					tunnel_node + ":" + tf.getAbsolutePath() + ";";
	    			cmd = "ssh " + sourceNode + " 'rsync --bwlimit=" + bwlimit + " -rpzP " + sourceFile + "/* " + tunnel_user + "@" +
	    					tunnel_node + ":" + tf.getAbsolutePath() + "/;'";
	    			System.out.println(cmd);
	    		}
	    	}
	    	if (o.flag.equals("-mbf")) {
	    		Random rand = new Random(System.currentTimeMillis());
	    		System.out.println("#USE SEED: " + seed);
	    		
	    		// migrate by rsync, stage 1 and 2, create metadata in remote db
	    		if (dbName == null || tableName == null || to_dc == null || devid == null ||
	    				bdate == null || edate == null ||
	    				tunnel_node == null || remoteUri == null || ng == null || fimportPath == null) {
	    			System.out.println("Please set dbName,tableName,to_dc,devid,tunnel_node,remoteUri,ng");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		Map<Long, SFile> targets = __get_files_by_external_file_4mig(dbName, 
	    				tableName, fimportPath);
	    		HashMap<Long, SFileLocation> fileMap = new HashMap<Long, SFileLocation>();
	    		
	    		// get all files, generate rsync string
	    		for (Map.Entry<Long, SFile> entry : targets.entrySet()) {
	    			System.out.print("#" + entry.getKey() + ",");
	    			
	    			if (entry.getValue().getValues() != null) {
	    				for (SplitValue sv : entry.getValue().getValues()) {
	    					sv.setVerison(flt_version);
	    				}
	    			}
	    			SFileLocation sfl = entry.getValue().getLocations().get(0);
	    			String new_location = "/data/" + to_dc + "/" + tableName + "/" + 
	    					rand.nextInt(Integer.MAX_VALUE);
	    			String sourceNode = null;
	    			String sourceFile = null;
	    			try {
	    				if (sfl.getNode_name().contains(";")) {
	    					// NAS device
	    					sourceNode = sfl.getNode_name().split(";")[0];
	    					sourceFile = sfl.getLocation();
	    				} else {
	    					// local device
	    					sourceNode = sfl.getNode_name();
	    					sourceFile = sfl.getLocation();
	    				}
	    			} catch (Exception e) {
	    				e.printStackTrace();
	    			}
	    			String targetFile = tunnel_out + new_location;
	    			// create the target directory now
	    			File tf = new File(targetFile);
	    			System.out.println("#Create DIR " + "@" + tunnel_node + ": " + tf.getParent());
	    			String cmd = "ssh " + tunnel_node + " 'mkdir -p " + tf.getAbsolutePath() + "; " + 
	    					"chmod ugo+rw " + tf.getAbsolutePath() + ";'";
	    			System.out.println(cmd);
	    			
	    			// do copy now
	    			System.out.println("#Copy by TUNNEL: " + sourceNode + ":" + sourceFile
	    					+ " -> " + 
	    					tunnel_node + ":" + targetFile);
	    			if (tunnel_user == null)
	    				tunnel_user = "metastore";
	    			cmd = "#scp -rp " + sourceNode + ":" + sourceFile + " " + tunnel_user + "@" + 
	    					tunnel_node + ":" + tf.getAbsolutePath() + ";";
	    			cmd = "ssh " + sourceNode + " 'rsync --bwlimit=" + bwlimit + " -rpzP " + sourceFile + "/* " + tunnel_user + "@" +
	    					tunnel_node + ":" + tf.getAbsolutePath() + "/;'";
	    			System.out.println(cmd);
	    			
	    			// update to fileMap
	    			sfl.setNode_name(tunnel_node);
	    			sfl.setLocation(new_location);
	    			fileMap.put(entry.getKey(), sfl);
	    		}
	    		
	    		try {
					Table tbl = cli.client.getTable(dbName, tableName);
					short maxIndexNum = 1000;
					List<Index> idxs = cli.client.listIndexes(dbName, tableName, maxIndexNum);
					if (idxs != null && idxs.size() > 0) {
						for (Index i : idxs) {
							i.setDbName(to_dc);
						}
					}
					List<NodeGroup> ngs = new ArrayList<NodeGroup>();
					ngs.add(new NodeGroup(ng, null, 0, null));
					tbl.setNodeGroups(ngs);
					tbl.setDbName(to_dc);
					// call by remote URI
					MetaStoreClient remote = new MetaStoreClient(remoteUri);
					System.out.println("Auth to " + remoteUri + " " + 
							remote.client.authentication("root", "111111"));
					if (remote.client.migrate_in(tbl, targets, idxs, dbName, devid, fileMap))
						System.out.println("OK");
					else
						System.out.println("Failed");
				} catch (MetaException e) {
					e.printStackTrace();
				} catch (NoSuchObjectException e) {
					e.printStackTrace();
				} catch (TException e) {
					e.printStackTrace();
				}
	    	}
	    	if (o.flag.equals("-m21")) {
	    		// migrate by NAS stage 1
	    		if (dbName == null || tableName == null || partName == null || to_dc == null || to_nas_devid == null || tunnel_in == null ||
	    				tunnel_out == null || tunnel_node == null) {
	    			System.out.println("Please set dbname,tableName,partName,to_dc,tonasdev,tunnel_in,tunnel_out,tunnel_node!");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		if (tunnel_user == null) {
	    			System.out.println("#Copy using default user, this might not be your purpose.");
	    		}
	    		List<String> partNames = new ArrayList<String>();
	    		partNames.add(partName);
	    		try {
	    			List<SFileLocation> lsfl = cli.client.migrate2_stage1(dbName, tableName, partNames, to_dc);
	    			//SFileLocation t = new SFileLocation("", 90, "devid", "/data/default/swjl/1031676804", 9, 1000, 1, "digest");
	    			//lsfl.add(t);
	    			if (lsfl.size() > 0) {
	    				// copy NAS or non-NAS LOC to TUNNEL 
	    				for (SFileLocation sfl : lsfl) {
	    					if (sfl.getNode_name().equals("")) {
	    						System.out.println("#Get NAS LOC " + sfl.getDevid() + ":" + sfl.getLocation() + " : " + sfl.getDigest());
	    					} else {
	    						System.out.println("#Get non-NAS LOC " + sfl.getDevid() + ":" + sfl.getLocation());
	    					}
	    					// calculate the source path: tunnel_in + location
	    					String sourceFile = tunnel_in + sfl.getLocation();
	    					// calculate the target path
	    					String targetFile = tunnel_out + sfl.getLocation();
	    					// create the target directory now
	    					File tf = new File(targetFile);
	    					System.out.println("#Create parent DIR " + "@" + tunnel_node + ": " + tf.getParent());
	    					String cmd = "mkdir -p " + tf.getParent() + "; " + "chmod ugo+rw " + tf.getParent() + ";";
	    					System.out.println(cmd);
	    						    					
	    					// do copy now
	    					if (sfl.getNode_name().equals("")) {
	    						System.out.println("#Copy     NAS SFL by TUNNEL: " + sourceFile + " -> " + tf.getParent());
	    						cmd = "cp -r " + sourceFile + " " + tf.getParent() + "; " + "chmod -R ugo+rw " + targetFile + ";";
	    						System.out.println(cmd);
	    						cmd = "find " + targetFile + " -type f -exec md5sum {} + | awk '{print $1}' | sort | md5sum | awk '{print $1}';";
	    						System.out.println(cmd);
	    					} else {
	    						// reset sourceFile
	    						String n = sfl.getNode_name();
	    						if (n.contains(";"))
	    							n = sfl.getNode_name().split(";")[0];
	    						sourceFile = cli.client.getMP(n, sfl.getDevid()) + "/" + sfl.getLocation();
	    						System.out.println("#Copy non-NAS SFL by TUNNEL: " + sourceFile + " -> " + tf.getParent());
	    						cmd = "scp -r metastore@" + n + ":" + sourceFile + " " + tf.getParent() + "; " + "chmod -R ugo+rw " + targetFile + ";";
	    						System.out.println(cmd);
	    						cmd = "find " + targetFile + " -type f -exec md5sum {} + | awk '{print $1}' | sort | md5sum | awk '{print $1}';";
	    						System.out.println(cmd);
	    					}
	    				}
	    			} else {
	    				System.out.println("No data to migrate");
	    			}
	    		} catch (MetaException me) {
	    			me.printStackTrace();
	    			break;
	    		} catch (TException e) {
	    			e.printStackTrace();
	    			break;
	    		}
	    	}
	    	if (o.flag.equals("-m22")) {
	    		// migrate by NAS
	    		if (dbName == null || tableName == null || partName == null || to_dc == null || to_nas_devid == null || tunnel_in == null ||
	    				tunnel_out == null || tunnel_node == null) {
	    			System.out.println("Please set dbname,tableName,partName,to_dc,tonasdev,tunnel_in,tunnel_out,tunnel_node!");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		List<String> partNames = new ArrayList<String>();
	    		partNames.add(partName);
	    		try {
	    			List<SFileLocation> lsfl = cli.client.migrate2_stage1(dbName, tableName, partNames, to_dc);
	    			if (lsfl.size() > 0) {
	    				// migrate by NAS stage2
	    				if (cli.client.migrate2_stage2(dbName, tableName, partNames, to_dc, to_db, to_nas_devid)) {
	    					System.out.println("Migrate2 Stage2 Done.");
	    				} else 
	    					System.out.println("Migrate2 Stage2 Failed.");
	    			} else {
	    				System.out.println("No data to migrate");
	    			}
	    		} catch (MetaException me) {
	    			me.printStackTrace();
	    			break;
	    		} catch (TException e) {
	    			e.printStackTrace();
	    			break;
	    		}
	    	}
	    	if (o.flag.equals("-m2")) {
	    		// migrate by NAS
	    		if (dbName == null || tableName == null || partName == null || to_dc == null || to_nas_devid == null || tunnel_in == null ||
	    				tunnel_out == null || tunnel_node == null) {
	    			System.out.println("Please set dbname,tableName,partName,to_dc,tonasdev,tunnel_in,tunnel_out,tunnel_node!");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		if (tunnel_user == null) {
	    			System.out.println("Copy using default user, this might not be your purpose.");
	    		}
	    		List<String> partNames = new ArrayList<String>();
	    		partNames.add(partName);
	    		try {
	    			List<SFileLocation> lsfl = cli.client.migrate2_stage1(dbName, tableName, partNames, to_dc);
	    			//SFileLocation t = new SFileLocation("", 90, "devid", "/data/default/swjl/1031676804", 9, 1000, 1, "digest");
	    			//lsfl.add(t);
	    			if (lsfl.size() > 0) {
	    				// copy NAS or non-NAS LOC to TUNNEL 
	    				for (SFileLocation sfl : lsfl) {
	    					if (sfl.getNode_name().equals("")) {
	    						System.out.println("Get NAS LOC " + sfl.getDevid() + ":" + sfl.getLocation() + " : " + sfl.getDigest());
	    					} else {
	    						System.out.println("Get non-NAS LOC " + sfl.getDevid() + ":" + sfl.getLocation());
	    					}
	    					// calculate the source path: tunnel_in + location
	    					String sourceFile = tunnel_in + sfl.getLocation();
	    					// calculate the target path
	    					String targetFile = tunnel_out + sfl.getLocation();
	    					// create the target directory now
	    					File tf = new File(targetFile);
	    					System.out.println("Create parent DIR " + "@" + tunnel_node + ": " + tf.getParent());
	    					String cmd = "ssh " + (tunnel_user == null ? "" : tunnel_user + "@") + tunnel_node + " 'mkdir -p " + tf.getParent() + "; " + "chmod ugo+rw " + tf.getParent() + ";'";
	    					System.out.println(cmd);
	    					if (!runRemoteCmd(cmd)) {
	    						MetaStoreClient.__EXIT(1);
	    					}
	    						    					
	    					// do copy now
	    					if (sfl.getNode_name().equals("")) {
	    						System.out.println("Copy     NAS SFL by TUNNEL: " + sourceFile + " -> " + tf.getParent());
	    						cmd = "ssh " + (tunnel_user == null ? "" : tunnel_user + "@") + tunnel_node + " 'cp -r " + sourceFile + " " + tf.getParent() + "; " + "chmod -R ugo+rw " + targetFile + ";'";
	    						System.out.println(cmd);
	    						if (!runRemoteCmd(cmd)) {
	    							MetaStoreClient.__EXIT(1);
	    						}
	    						
	    						cmd = "ssh " + (tunnel_user == null ? "" : tunnel_user + "@") + tunnel_node;
	    						cmd += " find " + targetFile + " -type f -exec md5sum {} + | awk '{print $1}' | sort | md5sum | awk '{print $1}';";
	    						System.out.println(cmd);
	    						String md5 = runRemoteCmdWithResult(cmd);
	    						if (!sfl.getDigest().equalsIgnoreCase(md5) && !sfl.getDigest().equals("MIGRATE2-DIGESTED!") && !sfl.getDigest().equals("REMOTE-DIGESTED!") && !sfl.getDigest().equals("SFL_DEFAULT")) {
	    							System.out.println("MD5 mismatch: original MD5 " + sfl.getDigest() + ", target MD5 " + md5 + ".");
	    							MetaStoreClient.__EXIT(1);
	    						}
	    					} else {
	    						// reset sourceFile
	    						String n = sfl.getNode_name();
	    						if (n.contains(";"))
	    							n = sfl.getNode_name().split(";")[0];
	    						sourceFile = cli.client.getMP(n, sfl.getDevid()) + "/" + sfl.getLocation();
	    						System.out.println("Copy non-NAS SFL by TUNNEL: " + sourceFile + " -> " + tf.getParent());
	    						cmd = "ssh " + (tunnel_user == null ? "" : tunnel_user + "@") + tunnel_node + " 'scp -r metastore@" + n + ":" + sourceFile + " " + tf.getParent() + "; " + "chmod -R ugo+rw " + targetFile + ";'";
	    						System.out.println(cmd);
	    						if (!runRemoteCmd(cmd)) {
	    							MetaStoreClient.__EXIT(1);
	    						}
	    						
	    						cmd = "ssh " + (tunnel_user == null ? "" : tunnel_user + "@") + tunnel_node;
	    						cmd += " \"find " + targetFile + " -type f -exec md5sum {} + | awk '{print $1}' | sort | md5sum | awk '{print $1}';\"";
	    						System.out.println(cmd);
	    						String md5 = runRemoteCmdWithResult(cmd);
	    						if (!sfl.getDigest().equalsIgnoreCase(md5) && !sfl.getDigest().equals("MIGRATE2-DIGESTED!") && !sfl.getDigest().equals("REMOTE-DIGESTED!") && !sfl.getDigest().equals("SFL_DEFAULT")) {
	    							System.out.println("MD5 mismatch: original MD5 " + sfl.getDigest() + ", target MD5 " + md5 + ".");
	    							MetaStoreClient.__EXIT(1);
	    						}
	    					}
	    				}
	    				// begin stage2
	    				if (cli.client.migrate2_stage2(dbName, tableName, partNames, to_dc, to_db, to_nas_devid)) {
	    					System.out.println("Migrate2 Stage2 Done.");
	    				} else 
	    					System.out.println("Migrate2 Stage2 Failed.");
	    			} else {
	    				System.out.println("No data to migrate");
	    			}
	    		} catch (MetaException me) {
	    			me.printStackTrace();
	    			break;
	    		} catch (TException e) {
	    			e.printStackTrace();
	    			break;
	    		}
	    	}
	    	if (o.flag.equals("-ld")) {
	    		// list device
	    		List<Device> ds;
				try {
					ds = cli.client.listDevice();
					if (ds.size() > 0) {
		    			for (Device d : ds) {
		    				System.out.println("-node " + d.getNode_name() + " -devid " + d.getDevid() + " -prop " + d.getProp());
		    			}
					}
				} catch (MetaException e) {
					e.printStackTrace();
				} catch (TException e) {
					e.printStackTrace();
				}
	    	}
	    	if (o.flag.equals("-ldd")) {
	    		// list device
	    		List<Device> ds;
				try {
					ds = cli.client.listDevice();
					if (ds.size() > 0) {
						for (Device d : ds) {
							String sprop, status;
							switch (DeviceInfo.getType(d.getProp())) {
							case MetaStoreConst.MDeviceProp.GENERAL:
								sprop = "GENERAL";
								break;
							case MetaStoreConst.MDeviceProp.SHARED:
								sprop = "SHARED";
								break;
							case MetaStoreConst.MDeviceProp.BACKUP:
								sprop = "BACKUP";
								break;
							case MetaStoreConst.MDeviceProp.BACKUP_ALONE:
								sprop = "BACKUP_ALONE";
								break;
							case MetaStoreConst.MDeviceProp.CACHE:
								sprop = "CACHE";
								break;
							case MetaStoreConst.MDeviceProp.MASS:
								sprop = "MASS";
								break;
							default:
								sprop = "Unknown";
							}
							switch (d.getStatus()) {
							case MetaStoreConst.MDeviceStatus.ONLINE:
								status = "ONLINE";
								break;
							case MetaStoreConst.MDeviceStatus.OFFLINE:
								status = "OFFLINE";
								break;
							case MetaStoreConst.MDeviceStatus.SUSPECT:
								status = "SUSPECT";
								break;
							case MetaStoreConst.MDeviceStatus.DISABLE:
								status = "DISABLE";
								break;
							default:
								status = "Unknown";
							}
							System.out.println("Device " + d.getDevid() + " -> [" + d.getNode_name() + ":" + sprop + ":" + status + "]");
						}
					}
				} catch (MetaException e) {
					e.printStackTrace();
				} catch (TException e) {
					e.printStackTrace();
				}
	    	}
	    	if (o.flag.equals("-ldbn")) {
	    		// list device by node
	    		if (node_name == null) {
	    			System.out.println("Please set -node");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		List<String> devids;
	    		try {
	    			devids = cli.client.listDevsByNode(node_name);
	    			if (devids.size() > 0) {
	    				for (String d : devids) {
	    					System.out.println("DEVID: " + d);
	    				}
	    			}
	    		} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
	    	}
	    	if (o.flag.equals("-sd")) {
	    		// show device
	    		if (devid == null) {
	    			System.out.println("Please set -devid");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		try {
					Device d = cli.client.getDevice(devid);
					String sprop, status;
					switch (DeviceInfo.getType(d.getProp())) {
					case MetaStoreConst.MDeviceProp.GENERAL:
						sprop = "GENERAL";
						break;
					case MetaStoreConst.MDeviceProp.SHARED:
						sprop = "SHARED";
						break;
					case MetaStoreConst.MDeviceProp.BACKUP:
						sprop = "BACKUP";
						break;
					case MetaStoreConst.MDeviceProp.BACKUP_ALONE:
						sprop = "BACKUP_ALONE";
						break;
					case MetaStoreConst.MDeviceProp.CACHE:
						sprop = "CACHE";
						break;
					case MetaStoreConst.MDeviceProp.MASS:
						sprop = "MASS";
						break;
					default:
						sprop = "Unknown";
					}
					switch (d.getStatus()) {
					case MetaStoreConst.MDeviceStatus.ONLINE:
						status = "ONLINE";
						break;
					case MetaStoreConst.MDeviceStatus.OFFLINE:
						status = "OFFLINE";
						break;
					case MetaStoreConst.MDeviceStatus.SUSPECT:
						status = "SUSPECT";
						break;
					case MetaStoreConst.MDeviceStatus.DISABLE:
						status = "DISABLE";
						break;
					default:
						status = "Unknown";
					}
					System.out.println("Device -> [" + d.getNode_name() + ":" + d.getDevid() + ":" + sprop + ":" + status + "]");
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
	    	}
	    	if (o.flag.equals("-md")) {
	    		// modify Device
	    		// FIXME:
	    		if (node_name == null || devid == null) {
	    			System.out.println("Please set -node -devid (-prop or -devtype -devquota [0:100]).");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		if (devtype != 0 || devquota != 100) {
	    			if (devquota > 100)
	    				devquota = 100;
	    			System.out.println("Set device prop type=" + devtype + ", quota=" + devquota);
	    			devquota = 100 - devquota;
	    			prop = devtype | (devquota << MetaStoreConst.MDeviceProp.__QUOTA_SHIFT__);
	    		}
	    		try {
	    			Node n = cli.client.get_node(node_name);
					Device d = cli.client.getDevice(devid);
					d.setProp(prop);
					cli.client.changeDeviceLocation(d, n);
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
	    	}
	    	if (o.flag.equals("-dbd")) {
	    		// disable device, into non-rw mode
	    		if (devid == null) {
	    			System.out.println("Please set -devid.");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		try {
	    			Device d = cli.client.getDevice(devid);
	    			String nn = d.getNode_name().split(",")[0];
	    			if (nn == null) {
	    				System.out.println("Invalid node name: " + d.getNode_name());
	    				break;
	    			}
	    			Node n = cli.client.get_node(nn);
	    			d.setStatus(MetaStoreConst.MDeviceStatus.DISABLE);
	    			cli.client.changeDeviceLocation(d, n);
	    		} catch (MetaException e) {
	    			e.printStackTrace();
	    			break;
	    		} catch (TException e) {
	    			e.printStackTrace();
	    			break;
	    		}
	    	}
	    	if (o.flag.equals("-ofd")) {
	    		// offline Device, into read-only mode
	    		if (devid == null) {
	    			System.out.println("Please set -devid.");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		try {
					cli.client.offlineDevice(devid);
					System.out.println("Offline Device '" + devid + "' done.");
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
	    	}
	    	if (o.flag.equals("-ofdp")) {
	    		// physically offline a device
	    		if (devid == null) {
	    			System.out.println("Please set -devid");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		try {
	    			cli.client.setTimeout(600);
	    			cli.client.offlineDevicePhysically(devid);
	    			System.out.println("Offline Device '" + devid + "' done physically.");
	    		} catch (MetaException e) {
	    			e.printStackTrace();
	    			break;
	    		} catch (TException e) {
	    			e.printStackTrace();
	    			break;
	    		}
	    	}
	    	if (o.flag.equals("-lfbd")) {
	    		// list FID by devices
	    		if (o.opt == null) {
	    			System.out.println("Please set -lfbd <DEVID,DEVID,...>");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		List<Long> fids = null;
	    		String[] devids = o.opt.split(",");
	    		
	    		if (devids.length > 0) {
	    			try {
	    				fids = cli.client.listFilesByDevs(Arrays.asList(devids));
					} catch (MetaException e) {
						e.printStackTrace();
						break;
					} catch (TException e) {
						e.printStackTrace();
						break;
					}
	    		}
	    		if (fids != null && fids.size() > 0) {
	    			for (Long _fid : fids) {
	    				System.out.println(_fid);
	    			}
	    			System.out.println("-> Total " + fids.size() + " FIDs.");
	    		}
	    	}
	    	if (o.flag.equals("-fls")) {
	    		// control FLSelector watch list
	    		int true_op = fls_op;
	    		
	    		if (dbName == null || tableName == null || fls_op == -1) {
	    			System.out.println("Please set -db and -table");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		
	    		try {
	    			switch (fls_op & 0xff) {
	    			case 0:
	    				// parse policy to value
	    				if (fls_args.equals("none")) {
	    					fls_op = (fls_op & 0xff);
	    				} else if (fls_args.equals("fair_nodes")) {
	    					fls_op = (fls_op & 0xff) | (1 << 8);
	    				} else if (fls_args.equals("ordered_alloc")) {
	    					fls_op = (fls_op & 0xff) | (2 << 8);
	    				}
	    				break;
	    			case 1:
	    			case 2:
	    				break;
	    			case 3:
	    				// parse repnr to value
	    				int rnr = 1;
	    				try {
	    					rnr = Integer.parseInt(fls_args);
	    				} catch (Exception e) {
	    				}
	    				fls_op = (fls_op & 0xff) | (rnr << 8);
	    				break;
	    			case 4:
	    				List<Integer> orders = new ArrayList<Integer>();
	    				String[] os = fls_args.split(";");
	    				for (String to : os) {
	    					if (to.equalsIgnoreCase("l1")) {
	    						orders.add(MetaStoreConst.MDeviceProp.L1);
	    					} else if (to.equalsIgnoreCase("l2")) {
	    						orders.add(MetaStoreConst.MDeviceProp.L2);
	    					} else if (to.equalsIgnoreCase("l3")) {
	    						orders.add(MetaStoreConst.MDeviceProp.L3);
	    					} else if (to.equalsIgnoreCase("l4")) {
	    						orders.add(MetaStoreConst.MDeviceProp.L4);
	    					}
	    				}
	    				if (orders.size() > 0) {
	    					while (orders.size() < 6) {
	    						orders.add(0);
	    					}
	    					fls_op = 0;
	    					for (int i = 0; i < 6; i++) {
	    						fls_op >>>= 4;
	    					fls_op |= (orders.get(i) << 28);
	    					}
	    					fls_op |= 4;
	    				} else {
	    					fls_op = (0xffffff << 8) | 4;
	    				}
	    				System.out.println("Order=" + HMSHandler.parseOrderList(fls_op >>> 8));
	    				break;
	    			case 5:
	    				List<Integer> rounds = new ArrayList<Integer>();
	    				String[] rs = fls_args.split(";");
	    				for (String re : rs) {
	    					if (re.equalsIgnoreCase("l1")) {
	    						rounds.add(MetaStoreConst.MDeviceProp.L1);
	    					} else if (re.equalsIgnoreCase("l2")) {
	    						rounds.add(MetaStoreConst.MDeviceProp.L2);
	    					} else if (re.equalsIgnoreCase("l3")) {
	    						rounds.add(MetaStoreConst.MDeviceProp.L3);
	    					} else if (re.equalsIgnoreCase("l4")) {
	    						rounds.add(MetaStoreConst.MDeviceProp.L4);
	    					}
	    				}
	    				if (rounds.size() != 3) {
	    					System.out.println("Rounds' size have to be 3!");
	    					MetaStoreClient.__EXIT(0);
	    				}
	    				fls_op = 0;
	    				for (int x = 0; x < 3; x++) {
	    					fls_op |= rounds.get(2 - x);
	    					fls_op <<= 8;
	    				}
	    				fls_op |= 5;
	    				System.out.println("Rounds=" + rounds + ", fls_op=" + String.format("%x", fls_op));
	    				break;
	    			default:
	    				System.out.println("BAD operation code: " + (fls_op & 0xff));
	    				MetaStoreClient.__EXIT(0);
	    			}
					boolean res = cli.client.flSelectorWatch(dbName + "." + tableName, fls_op);
					System.out.println("Control FLSelector OP=" + (true_op == 0 ? "ADD" :
						(true_op == 1 ? "DEL" : 
							(true_op == 2 ? "FLUSH" : 
								(true_op == 3 ? "REPR " + (true_op >> 8) :
									(true_op == 4 ? "ORDER " + fls_args :
										"ROUND " + fls_args)))))
									+ " TABLE=" + (dbName + "." + tableName) + ", r=" + res);
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
	    	}
	    	if (o.flag.equals("-rchk")) {
	    		// NewMS redis index/content check
	    		String rhost;
	    		int rport;
	    		
	    		if (o.opt == null) {
	    			System.out.println("Usage: -rchk redis_server:redis_port");
	    			break;
	    		}
	    		String[] sp = o.opt.split(":");
	    		if (sp.length < 2) {
	    			System.out.println("Usage: -rchk redis_server:redis_port");
	    			break;
	    		}
	    		rhost = sp[0];
	    		rport = Integer.parseInt(sp[1]);
	    		try {
	    			Jedis jedis = new Jedis(rhost, rport, 120 * 1000);
	    			int i = 0, sfnr = 0, sflnr = 0;
	    			
	    			// Step 0: check list table index/content
	    			System.out.println("Checking list table index/content ...");
	    			Set<String> tables = jedis.keys("sf.ltf.*");
	    			for (String table : tables) {
	    				Set<String> fids = jedis.zrange(table, 0, -1);
	    				if (fids != null && fids.size() > 0) {
	    					System.out.println("Table " + table.substring(7) + " contains " + fids.size() + " files.");
	    					for (String fid : fids) {
	    						SFile f = cli.client.get_file_by_id(Integer.parseInt(fid));
	    						if (!table.substring(7).equalsIgnoreCase(f.getDbName() + "." + f.getTableName())) {
	    							System.out.println("FID " + f.getFid() + " table from " + table.substring(7) + 
	    									" to " + f.getDbName() + "." + f.getTableName());
	    						}
	    					}
	    				}
	    			}
	    			
	    			// Step 1: check file index/content
	    			System.out.println("Checking sfile index/content ...");
	    			for (i = 0; i < 5; i++) {
	    				Set<String> sfstat = jedis.smembers("sf.stat." + i);
	    				long nr = jedis.scard("sf.stat." + i);
	    				if (sfstat == null && nr != 0) {
	    					System.out.println("[SUSPECT] SF Index " + i + " is inconsistent [null].");
	    				} else {
	    					System.out.println("[INFO]    SF Index " + i + " smembers " + sfstat.size() + " scard " + nr);
	    				}
	    				if (sfstat != null && sfstat.size() > 0) {
	    					System.out.println("sf.stat." + i + " member size = " + sfstat.size());
	    					for (String fid : sfstat) {
	    						SFile f = null;

	    						try {
	    							f = cli.client.get_file_by_id(Long.parseLong(fid));
	    						} catch (FileOperationException foe) {
	    						}
	    						if (f == null) {
	    							System.out.println("SF  Index " + i + " fid " + fid + " not exists.");
	    							// ignore it, delete it manually.
	    						} else if (i != f.getStore_status()) {
	    							sfnr++;
	    							System.out.println("SF  Index " + i + ", SFile " + toStringSFile(f));
	    							// if sf.index == 0 && sf.content.status == 0, ok
	    							// if sf.index == 0 && sf.content.status == 1, ignore it
	    							// if sf.index == 0 && sf.content.status == 2/3/4, fix it
	    							// if sf.index == 1 && sf.content.status == 0, ignore it
	    							// if sf.index == 1 && sf.content.status == 1, ok
	    							// if sf.index == 1 && sf.content.status == 2, ignore it
	    							// if sf.index == 2 && sf.content.status == 0, ignore it
	    							// if sf.index == 2 && sf.content.status == 1, ignore it
	    							// if sf.index == 2 && sf.content.status == 2, ok
	    							// if sf.index == 4 && sf.content.status != 4, fix it
	    						}
	    					}
	    				}
	    			}
	    			System.out.println("---> SF  check failed " + sfnr);
	    			// Step 2: check SFL  index/content
	    			System.out.println("Checking sfilelocation index/content ...");
	    			for (i = 0; i < 3; i++) {
	    				Set<String> sflstat = jedis.smembers("sfl.stat." + i);
	    				long nr = jedis.scard("sfl.stat." + i);
	    				if (sflstat == null && nr != 0) {
	    					System.out.println("[SUSPECT] SFL Index " + i + " is inconsistent [null].");
	    				} else {
	    					System.out.println("[INFO]    SFL Index " + i + " smembers " + sflstat.size() + " scard " + nr);
	    				}
	    				if (sflstat != null && sflstat.size() > 0) {
	    					System.out.println("sfl.stat." + i + " member size = " + sflstat.size());
	    					for (String sfl : sflstat) {
	    						String js = jedis.hget("sfilelocation", sfl);
	    						if (js != null) {
	    							SFileLocation l = JSON.parseObject(js, SFileLocation.class);
	    							if (l != null && l.getVisit_status() != i) {
	    								System.out.println("SFL Index " + i + ", SFileLocation " + toStringSFileLocation(l));
	    								sflnr++;
	    							}
	    						}
	    					}
	    				}
	    			}
	    			System.out.println("---> SFL check failed " + sflnr);
	    			
	    			// Step 3: check DEV  index/content
	    			jedis.quit();
	    			jedis.close();
	    		} catch (Exception e) {
	    			e.printStackTrace();
	    			break;
	    		}
	    	}
	    	if (o.flag.equals("-rfixltf")) {
	    		// NewMS redis list table index/content check and fix
	    		String rhost;
	    		int rport;
	    		
	    		if (o.opt == null) {
	    			System.out.println("Usage: -rfixltf redis_server:redis_port");
	    			break;
	    		}
	    		String[] sp = o.opt.split(":");
	    		if (sp.length < 2) {
	    			System.out.println("Usage: -rfixltf redis_server:redis_port");
	    			break;
	    		}
	    		rhost = sp[0];
	    		rport = Integer.parseInt(sp[1]);
	    		try {
	    			Jedis jedis = new Jedis(rhost, rport, 120 * 1000);

	    			String script = "local score = redis.call('zscore',KEYS[1],ARGV[1]);"
	    					+ "if not score then "        //lua里只有false和nil被认为是逻辑的非
	    					+ "local size = redis.call('zcard',KEYS[1]);"
	    					+ "return redis.call('zadd',KEYS[1],size,ARGV[1]); end";
	    			String sha = jedis.scriptLoad(script);
	    			
	    			// Step 1: check list table index/content
	    			System.out.println("Checking list table index/content ...");
	    			Set<String> tables = jedis.keys("sf.ltf.*");
	    			for (String table : tables) {
	    				Set<String> fids = jedis.zrange(table, 0, -1);
	    				if (fids != null && fids.size() > 0) {
	    					for (String fid : fids) {
	    						SFile f = cli.client.get_file_by_id(Integer.parseInt(fid));
	    						if (!table.substring(7).equalsIgnoreCase(f.getDbName() + "." + f.getTableName())) {
	    							System.out.println("Update FID " + f.getFid() + " table from " + table.substring(7) + 
	    									" to " + f.getDbName() + "." + f.getTableName());
	    							jedis.evalsha(sha, 1, new String[]{"sf.ltf." + f.getDbName() + "." + f.getTableName(), fid});
	    							jedis.zrem(table, fid);
	    						}
	    					}
	    				}
	    			}
	    			// Step 3: check DEV  index/content
	    			jedis.quit();
	    			jedis.close();
	    		} catch (Exception e) {
	    			e.printStackTrace();
	    			break;
	    		}
	    	}
	    	if (o.flag.equals("-rfix")) {
	    		// NewMS redis index/content check and fix
	    		String rhost;
	    		int rport;
	    		
	    		if (o.opt == null) {
	    			System.out.println("Usage: -rchk redis_server:redis_port");
	    			break;
	    		}
	    		String[] sp = o.opt.split(":");
	    		if (sp.length < 2) {
	    			System.out.println("Usage: -rchk redis_server:redis_port");
	    			break;
	    		}
	    		rhost = sp[0];
	    		rport = Integer.parseInt(sp[1]);
	    		try {
	    			Jedis jedis = new Jedis(rhost, rport, 120 * 1000);
	    			int i = 0;
	    			
	    			// Step 1: check file index/content
	    			System.out.println("Checking sfile index/content ...");
	    			for (i = 0; i < 5; i++) {
	    				Set<String> sfstat = jedis.smembers("sf.stat." + i);
	    				long nr = jedis.scard("sf.stat." + i);
	    				if (sfstat == null && nr != 0) {
	    					System.out.println("[SUSPECT] SF Index " + i + " is inconsistent [null].");
	    				} else {
	    					System.out.println("[INFO]    SF Index " + i + " smembers " + sfstat.size() + " scard " + nr);
	    				}
	    				if (sfstat != null && sfstat.size() > 0) {
	    					System.out.println("sf.stat." + i + " member size = " + sfstat.size());
	    					for (String fid : sfstat) {
	    						SFile f = null;

	    						try {
	    							f = cli.client.get_file_by_id(Long.parseLong(fid));
	    						} catch (FileOperationException foe) {
	    						}
	    						if (f == null) {
	    							System.out.println("SF  Index " + i + " fid " + fid + " not exists.");
	    							// ignore it, delete it manually.
	    						} else if (i != f.getStore_status()) {
	    							// if sf.index == 0 && sf.content.status == 2/3/4, fix it
	    							// if sf.index == 4 && sf.content.status != 4, fix it
	    							if (i == 0 && (f.getStore_status() == 2 || f.getStore_status() == 3 || f.getStore_status() == 4)) {
	    								System.out.println("Update fid " + fid + " index state from " + i + " to " + f.getStore_status());
	    								jedis.srem("sf.stat." + i, fid);
	    								jedis.sadd("sf.stat." + f.getStore_status(), fid);
	    							}
	    							if (i == 1 && (f.getStore_status() == 2 || f.getStore_status() == 1)) {
	    								do {
	    									System.out.print("Update fid " + fid + " index state from " + i + " to " + f.getStore_status() + " Y/N?");
	    									int rd = System.in.read();
	    									if (rd == 'Y') {
	    										jedis.srem("sf.stat." + i, fid);
	    										jedis.sadd("sf.stat." + f.getStore_status(), fid);
	    										System.in.skip(System.in.available());
	    										break;
	    									} else if (rd == 'N') {
	    										System.in.skip(System.in.available());
	    										break;
	    									}
	    								} while (true);
	    							}
	    						}
	    					}
	    				}
	    			}
	    			// Step 2: check SFL  index/content
	    			System.out.println("Checking sfilelocation index/content ...");
	    			for (i = 0; i < 3; i++) {
	    				Set<String> sflstat = jedis.smembers("sfl.stat." + i);
	    				long nr = jedis.scard("sfl.stat." + i);
	    				if (sflstat == null && nr != 0) {
	    					System.out.println("[SUSPECT] SFL Index " + i + " is inconsistent [null].");
	    				} else {
	    					System.out.println("[INFO]    SFL Index " + i + " smembers " + sflstat.size() + " scard " + nr);
	    				}
	    				if (sflstat != null && sflstat.size() > 0) {
	    					System.out.println("sfl.stat." + i + " member size = " + sflstat.size());
	    					for (String sfl : sflstat) {
	    						String js = jedis.hget("sfilelocation", sfl);
	    						if (js != null) {
	    							SFileLocation l = JSON.parseObject(js, SFileLocation.class);
	    							if (l != null && l.getVisit_status() != i) {
	    								do {
	    									System.out.print("Update FID " + l.getFid() + " SFL Index " + i + " to " + l.getVisit_status() + " Y/N?");
	    									int rd = System.in.read();
	    									if (rd == 'Y') {
	    										jedis.srem("sfl.stat." + i, sfl);
	    										jedis.sadd("sfl.stat." + l.getVisit_status(), sfl);
	    										System.in.skip(System.in.available());
	    										break;
	    									} else if (rd == 'N') {
	    										System.in.skip(System.in.available());
	    										break;
	    									}
	    								} while (true);
	    							}
	    						}
	    					}
	    				}
	    			}
	    			// Step 3: check DEV  index/content
	    			jedis.quit();
	    			jedis.close();
	    		} catch (Exception e) {
	    			e.printStackTrace();
	    			break;
	    		}
	    	}
	    	if (o.flag.equals("-statchk")) {
	    		// do OldMS/NewMS file/filelocation status check
	    		if (old_port < 0 || new_port < 0) {
	    			System.out.println("Please provide -old_port # and -new_port #");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		System.out.println("OldMS port " + old_port + ", NewMS port " + new_port);
	    		MetaStoreClient oldms, newms;
	    		try {
					oldms = new MetaStoreClient(serverName, old_port);
					newms = new MetaStoreClient(serverName, new_port);
					long maxfid = 0;
					
					maxfid = newms.client.getMaxFid();
					for (long i = 0; i < maxfid; i++) {
						SFile nf = null, of = null;
						boolean isOldExist = true, isNewExist = true;
						boolean isSFLOk = false;
						
						try { 
							nf = newms.client.get_file_by_id(i);
						} catch (Exception e) {
							isNewExist = false;
						}
						try {
							of = oldms.client.get_file_by_id(i);
						} catch (Exception e) {
							isOldExist = false;
						}
						if (isOldExist != isNewExist) {
							System.out.println("Mismatch SFile " + i + " by EXIST {OLD " + 
									isOldExist + ", NEW " + isNewExist + "}");
							continue;
						}
						if (nf == null || of == null) 
							continue;
						if (nf.getStore_status() != of.getStore_status()) {
							System.out.println("Mismatch SFile " + i + " by STORE_STATUS {OLD " + 
									of.getStore_status() + ", NEW " + nf.getStore_status() + "}");
						}
						if (nf.getLocationsSize() != of.getLocationsSize()) {
							System.out.println("Mismatch SFile " + i + " by SFL SIZE {OLD " + 
									of.getLocationsSize() + ", NEW " + nf.getLocationsSize() + "}");
						}
						if (nf.getLocations() == null && of.getLocations() == null)
							isSFLOk = true;
						if (nf.getLocations() == null || of.getLocations() == null)
							continue;
						isSFLOk = true;
						for (SFileLocation nsfl : nf.getLocations()) {
							boolean isOK = false;
							for (SFileLocation osfl: of.getLocations()) {
								if (nsfl.getDevid().equals(osfl.getDevid()) && 
										nsfl.getLocation().equals(osfl.getLocation()) && 
										nsfl.getRep_id() == osfl.getRep_id() &&
										nsfl.getVisit_status() == osfl.getVisit_status()) {
									isOK = true;
									break;
								}
							}
							if (!isOK) {
								isSFLOk = false;
								break;
							}
						}
						if (!isSFLOk) {
							System.out.println("Mismatch SFile " + i + " by SFL {OLD " + 
									of.getLocations() + ", NEW " + nf.getLocations() + "}");
						}
					}
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
	    	}
	    	if (o.flag.equals("-rep")) {
	    		// replicate to specified typed device
	    		if (o.opt == null) {
	    			System.out.println("Please set replicate file id and -devtype.");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		long fid = 0;
	    		
	    		try {
	    			fid = Long.parseLong(o.opt);
	    			System.out.println("Try to replicate file " + fid + " to " + 
	    					DeviceInfo.getTypeStr(devtype) + " device.");
	    			cli.client.replicate(fid, devtype);
	    			System.out.println("You can use -frr " + fid + " to check new replica now.");
	    		} catch (Exception e) {
	    			e.printStackTrace();
	    			break;
	    		}
	    	}
	    	if (o.flag.equals("-ond")) {
	    		// online Device
	    		if (devid == null) {
	    			System.out.println("Please set -devid.");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		try {
					cli.client.onlineDevice(devid);
					System.out.println("Online Device '" + devid + "' done.");
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
	    	}
	    	if (o.flag.equals("-cd")) {
	    		// add Device
	    		if (node_name == null) {
	    			System.out.println("Please set -node -prop.");
	    			MetaStoreClient.__EXIT(0);
	    		}
	    		try {
					Device d = cli.client.createDevice(o.opt, prop, node_name);
					System.out.println("Add Device: " + d.getDevid() + ", prop " + d.getProp() + ", node " + d.getNode_name());
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
	    	}
	    	if (o.flag.equals("-dd")) {
	    		// del Device
	    		try {
					cli.client.delDevice(o.opt);
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
	    	}
			if (o.flag.equals("-n")) {
				// add Node
				try {
					ipl.add(InetAddress.getLocalHost().getHostAddress());
					System.out.println("Add Node: " + node + ", IPL: " + ipl.get(0));
					Node n = cli.client.add_node(node, ipl);
				} catch (MetaException me) {
					me.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					return;
				} catch (UnknownHostException e) {
					e.printStackTrace();
					break;
				}
			}
			if (o.flag.equals("-nn")) {
				// add Node with specified name
				if (o.opt != null) {
					try {
						ipl.add(InetAddress.getByName(o.opt).getHostAddress());
						//ipl.add(InetAddress.getLocalHost().getHostAddress());
						System.out.println("Add Node: " + o.opt + ", IPL: " + ipl.get(0));
						Node n = cli.client.add_node(o.opt, ipl);
					} catch (UnknownHostException e1) {
						e1.printStackTrace();
					} catch (MetaException e) {
						e.printStackTrace();
					} catch (TException e) {
						e.printStackTrace();
					}
				}
			}
			if (o.flag.equals("-dn")) {
				// del Node
				try {
					cli.client.del_node(o.opt);
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
			}
			if (o.flag.equals("-ln")) {
				// list all Nodes
				List<Node> lns;
				try {
					lns = cli.client.get_all_nodes();
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
				for (Node n : lns) {
					System.out.println("Node '" + n.getNode_name() + "' {" + n.getIps().toString() + "}");
				}
			}
			if (o.flag.equals("-gni")) {
				// get NodeInfo 
				String nis;
				try {
					nis = cli.client.getNodeInfo();
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
				System.out.println(nis);
			}
			if (o.flag.equals("-dms")) {
				// get DM status
				String dms;
				try {
					dms = cli.client.getDMStatus();
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
				System.out.println(dms);
			}
			if (o.flag.equals("-eda")) {
				// emergency device space average: remove file location that on this 
				// device to free some space
				if (devid == null) {
					System.out.println("Please set -devid.");
					MetaStoreClient.__EXIT(0);
				}
			}
			if (o.flag.equals("-scrub")) {
				// into scrub mode, auto clean
				// Logic: 
				// 1. get all files, calculate the file length, store into hash map;
				// 2. while (true) {
				//       get store ratio
				//       decide file set that should scrub
				//       reget new files, update hash map
				//    }
				// RULE LOGIC => type:action:soft_limit:hard_limit
				// RULE EXAMP => ratio:0.15;+hlw:del:15:10;+all:drep:30:30;
				Double target_ratio = 0.15;
				List<ScrubRule> srl = new ArrayList<ScrubRule>();
				
				cli.client.setTimeout(120);
				if (scrub_rule != null) {
					String[] rules = scrub_rule.split(";");
					for (int i = 0; i < rules.length; i++) {
						if (rules[i].startsWith("ratio")) {
							String[] r1 = rules[i].split(":");
							if (r1.length >= 2)
								target_ratio = Double.parseDouble(r1[1]);
						}
						if (rules[i].startsWith("+")) {
							String[] r2 = rules[i].split(":");
							if (r2.length >= 4) {
								ScrubRule sr = new ScrubRule();
								sr.type = r2[0].substring(1);
								sr.soft = new Double(Double.parseDouble(r2[2]) * 24.0).intValue();
								sr.hard = new Double(Double.parseDouble(r2[3]) * 24.0).intValue();
								if (r2[1].equalsIgnoreCase("del")) {
									sr.action = ScrubRule.ScrubAction.DELETE;
								} else if (r2[1].equalsIgnoreCase("drep")) {
									sr.action = ScrubRule.ScrubAction.DOWNREP;
								}
								srl.add(sr);
							}
						}
					}
				} else {
					ScrubRule sr = new ScrubRule();
					sr.type = "all";
					sr.soft = 30 * 24;
					sr.hard = 30 * 24;
					sr.action = ScrubRule.ScrubAction.DOWNREP;
					srl.add(sr);
					sr = new ScrubRule();
					sr.type = "all";
					sr.soft = 400000; // year before unix ZERO year
					sr.hard = 400000;
					sr.action = ScrubRule.ScrubAction.DELETE;
					srl.add(sr);
				}
				System.out.println("Target Ratio " + target_ratio);
				for (ScrubRule sr : srl) {
					System.out.println(sr);
				}
				
				TreeMap<Long, Map<String, FileStat>> fmap = new TreeMap<Long, Map<String, FileStat>>();
				if (scrub_max < 0) {
					try {
						scrub_max = cli.client.getMaxFid();
					} catch (MetaException e) {
						e.printStackTrace();
						break;
					} catch (TException e) {
						e.printStackTrace();
						break;
					}
				}
				System.out.println("Get MaxFid() " + scrub_max);
				/*for (int i = 0; i < scrub_max; i += 2000) {
					System.out.format("\rGet files %.2f %%", (double)i / scrub_max * 100);
					List<Long> fids = new ArrayList<Long>();
					for (int j = i; j < i + 2000; j++) {
						fids.add(new Long(j));
					}
					try {
						List<SFile> files = cli.client.get_files_by_ids(fids);
						statfs2_update_map(cli, fmap, files, statfs2_getlen);
					} catch (FileOperationException e) {
						e.printStackTrace();
					} catch (MetaException e) {
						e.printStackTrace();
					} catch (TException e) {
						e.printStackTrace();
					}
				}
				System.out.println("\rDone.");*/
				
				long sleepnr = 10;
				long last_fetch = System.currentTimeMillis();
				long last_got = 0;
				
				update_fmap(cli, 10, xURI, serverName, serverPort, fmap, 0, scrub_max, statfs2_getlen);
				last_got = scrub_max / 10 * 10;
				System.out.println("Get File Info upto FID " + last_got);
				
				while (true) {
					try {
						while (cli == null) {
							cli = __reconnect(xURI, serverName, serverPort);
						}

						Double ratio = 0.0;
						try {
							Thread.sleep(sleepnr * 1000);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						if (System.currentTimeMillis() - last_fetch >= 3600 * 1000) {
							// do fetch now
							try {
								scrub_max = cli.client.getMaxFid();
							} catch (Exception e) {
								scrub_max = last_got;
							}
							last_fetch = System.currentTimeMillis();
							if (scrub_max > last_got) {
								if (!update_fmap(cli, 1, xURI, serverName, serverPort, fmap, last_got, scrub_max, statfs2_getlen)) {
									last_got = scrub_max;
									System.out.println("Get File Info upto FID " + last_got);
								} else {
									System.out.println("Retry ten minites later ...");
									last_fetch = System.currentTimeMillis() - 3000 * 1000;
								}
							}
						}
						try {
							String dms = cli.client.getDMStatus();
							BufferedReader bufReader = new BufferedReader(new StringReader(dms));
							String line = null;
							while ((line = bufReader.readLine()) != null) {
								if (line.startsWith("True  space")) {
									String[] ls = line.split(" ");
									ratio = Double.parseDouble(ls[ls.length - 1]);
									break;
								}
							}
							System.out.println(" -> Current free ratio " + ratio + ", target ratio " + target_ratio);
							if (target_ratio < ratio) {
								sleepnr = Math.min(sleepnr * 2, 60);
								continue;
							} else {
								sleepnr = Math.max(sleepnr / 2, 10);
							}

							// sort by time
							boolean stop = false;
							long cur_hour = System.currentTimeMillis() / 1000 / 3600 * 3600;
							List<Long> fsmapToDel = new ArrayList<Long>();

							for (Long k : fmap.keySet()) {
								Map<String, FileStat> fsmap = fmap.get(k);
								long hours = (cur_hour - k) / 3600;
								long total_free = 0;

								System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(k * 1000)) + "\t" + hours + " hrs");
								// iterate on each rule
								for (ScrubRule sr : srl) {
									List<String> toDel = new ArrayList<String>();
									
									if (hours > sr.soft || hours < -10000) {
										// act on each table
										for (Map.Entry<String, FileStat> e : fsmap.entrySet()) {
											if (sr.type.equalsIgnoreCase("hlw")) {
												if (e.getKey().contains("t_gkrz") || 
														e.getKey().contains("t_gzrz") ||
														e.getKey().contains("t_jcrz") ||
														e.getKey().contains("t_ybrz")) {
													// ok
												} else
													continue;
											} else if (sr.type.equalsIgnoreCase(e.getKey())) {
												// ok
											} else if (sr.type.equalsIgnoreCase("all")) {
												// ok
											} else {
												continue;
											}

											Set<Long> idToDel = new TreeSet<Long>();
											System.out.print(sr + " => on " + e.getKey() + " " + e.getValue().fids.size() + " files [");
											for (Long fid : e.getValue().fids) {
												try {
													SFile f = cli.client.get_file_by_id(fid);

													switch (sr.action) {
													case DELETE:
														cli.client.rm_file_physical(f);
														total_free += e.getValue().space * f.getRep_nr();
														toDel.add(e.getKey());
														System.out.print(f.getFid() + ",");
														break;
													case DOWNREP:
														//total_free += e.getValue().space;
														if (f.getRep_nr() > 1) {
															cli.client.set_file_repnr(f.getFid(), f.getRep_nr() - 1);
															System.out.print(f.getFid() + ",");
														}
														break;
													}
												} catch (FileOperationException foe) {
													idToDel.add(fid);
												} catch (Exception foe) {
												}
											}
											System.out.println("]");
											if (idToDel.size() > 0) {
												for (Long fid : idToDel) {
													e.getValue().fids.remove(fid);
												}
											}
										}
										
										FreeSpace fs = __get_free_space_ratio(cli);
										if (((double)total_free / fs.total) + fs.ratio >= target_ratio) {
											stop = true;
											break;
										}
									}
									for (String s : toDel) {
										fsmap.remove(s);
									}
								}
								if (fsmap.size() == 0)
									fsmapToDel.add(k);
								if (stop)
									break;
							}
							if (fsmapToDel.size() > 0) {
								for (Long k : fsmapToDel) {
									fmap.remove(k);
								}
							}
						} catch (MetaException e1) {
							e1.printStackTrace();
							if (e1.getCause() instanceof ConnectException) {
								try {
									cli.stop();
								} catch (Exception se) {
								}
								cli = null;
							}
						} catch (TException e1) {
							e1.printStackTrace();
							try {
								cli.stop();
							} catch (Exception se) {
							}
							cli = null;
						}
					} catch (Exception e) {
						e.printStackTrace();
						try {
							cli.stop();
						} catch (Exception se) {
						}
						cli = null;
					}
				}
			}
			if (o.flag.equals("-scrubn")) {
				// into scrubn mode, auto clean
				// n levels data life period management
				// Logic: 
				// 1. get all files, calculate the file length, store into hash map;
				// 2. while (true) {
				//       get store ratio
				//       decide file set that should scrub
				//       reget new files, update hash map
				//    }
				// RULE LOGIC => type:action:t1_limit:t2_limit:t3_limit:t4_limit
				// RULE EXAMP => ratio:0.15:0.15:0.15:0.15:0.15;+hlw:drep#730:drep#730:del#730:migr#730;+all:drep#30:drep#30:drep#30:drep#30;
				Double target_ratio = 0.15;
				Double ctarget_ratio = 0.15;
				Double gtarget_ratio = 0.15;
				Double mtarget_ratio = 0.15;
				Double starget_ratio = 0.15;
				
				List<ScrubnRule> srl = new ArrayList<ScrubnRule>();
				
				cli.client.setTimeout(120);
				if (scrub_rule != null) {
					String[] rules = scrub_rule.split(";");
					for (int i = 0; i < rules.length; i++) {
						if (rules[i].startsWith("ratio")) {
							String[] r1 = rules[i].split(":");
							if (r1.length >= 6) {
								target_ratio = Double.parseDouble(r1[1]);
								ctarget_ratio = Double.parseDouble(r1[2]);
								gtarget_ratio = Double.parseDouble(r1[3]);
								mtarget_ratio = Double.parseDouble(r1[4]);
								starget_ratio = Double.parseDouble(r1[5]);
							}
						}
						if (rules[i].startsWith("+")) {
							String[] r2 = rules[i].split(":");
							if (r2.length >= 5) {
								ScrubnRule sr = new ScrubnRule();
								sr.type = r2[0].substring(1);
								for(int j = 0 ;j < 4 ; j++){
									String [] r3 = r2[j+1].split("#");
									if (r3[0].equalsIgnoreCase("del")) {
										sr.rules.add(r3[0]);
									} else if (r3[0].equalsIgnoreCase("drep")) {
										sr.rules.add(r3[0]);
									} else if (r3[0].equalsIgnoreCase("migr")) {
										sr.rules.add(r3[0]);
									}
									sr.times.add(new Double(Double.parseDouble(r3[1]) * 24.0).intValue());
								}
								srl.add(sr);
							}
						}
					}
				} else {
					ScrubnRule sr = new ScrubnRule();
					sr.type = "all";
					for(int j = 0;j<4;j++){
						sr.times.add(30 * 24);
						sr.rules.add("drep");
					}
					srl.add(sr);
					
					sr = new ScrubnRule();
					sr.type = "all";
					for(int j = 0;j<4;j++){
						sr.times.add(400000);
						sr.rules.add("drel");
					}
					srl.add(sr);
					
					sr = new ScrubnRule();
					sr.type = "all";
					for(int j = 0;j<4;j++){
						sr.times.add(30 * 24);
						sr.rules.add("migr");
					}
					srl.add(sr);
				}
				System.out.println("Target Ratio " + target_ratio + " L1 Target Ratio " + ctarget_ratio
						 + " L2 Target Ratio " + gtarget_ratio + " L3 Target Ratio " + mtarget_ratio + " L4 Target Ratio" + starget_ratio);
				for (ScrubnRule sr : srl) {
					System.out.println(sr);
				}
				
				TreeMap<Long, Map<String, FileStat>> fmap = new TreeMap<Long, Map<String, FileStat>>();
				if (scrub_max < 0) {
					try {
						scrub_max = cli.client.getMaxFid();
					} catch (MetaException e) {
						e.printStackTrace();
						break;
					} catch (TException e) {
						e.printStackTrace();
						break;
					}
				}
				System.out.println("Get MaxFid() " + scrub_max);
				long sleepnr = 10;
				long last_fetch = System.currentTimeMillis();
				long last_got = 0;
				
				//update the fmap
				update_fmap(cli, 10, xURI, serverName, serverPort, fmap, 0, scrub_max, statfs2_getlen);
				last_got = scrub_max / 10 * 10;
				System.out.println("Get File Info upto FID " + last_got);
				
				while (true) {
					try {
						while (cli == null) {
							cli = __reconnect(xURI, serverName, serverPort);
						}

						Double ratio = 0.0;
						Double l1ratio = 0.0;
						Double l2ratio = 0.0;
						Double l3ratio = 0.0;
						Double l4ratio = 0.0;
						try {
							Thread.sleep(sleepnr * 1000);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						if (System.currentTimeMillis() - last_fetch >= 3600 * 1000) {
							// do fetch now
							try {
								scrub_max = cli.client.getMaxFid();
							} catch (Exception e) {
								scrub_max = last_got;
							}
							last_fetch = System.currentTimeMillis();
							if (scrub_max > last_got) {
								if (!update_fmap(cli, 1, xURI, serverName, serverPort, fmap, last_got, scrub_max, statfs2_getlen)) {
									last_got = scrub_max;
									System.out.println("Get File Info upto FID " + last_got);
								} else {
									System.out.println("Retry ten minites later ...");
									last_fetch = System.currentTimeMillis() - 3000 * 1000;
								}
							}
						}
						//judge the ratio to decide to scrub the data or not
						try {
							String dms = cli.client.getDMStatus();
							BufferedReader bufReader = new BufferedReader(new StringReader(dms));
							String line = null;
							while ((line = bufReader.readLine()) != null) {
								if (line.startsWith("True  space")) {
									String[] ls = line.split(" ");
									ratio = Double.parseDouble(ls[ls.length - 1]);
								} else if (line.startsWith("L1 True  space")){
									String[] ls = line.split(" ");
									l1ratio = Double.parseDouble(ls[ls.length - 1]);
								} else if (line.startsWith("L2 True  space")){
									String[] ls = line.split(" ");
									l2ratio = Double.parseDouble(ls[ls.length - 1]);
								} else if (line.startsWith("L3 True  space")){
									String[] ls = line.split(" ");
									l3ratio = Double.parseDouble(ls[ls.length - 1]);
								} else if (line.startsWith("L4 True  space")){
									String[] ls = line.split(" ");
									l4ratio = Double.parseDouble(ls[ls.length - 1]);
								}
							}
							System.out.println(" -> Current free ratio " + ratio + ", target ratio " + target_ratio
									+ ", free L1 ratio " + l1ratio + ", L1 taget ratio " + ctarget_ratio
									+ ", free L2 ratio " + l2ratio + ", L2 taget ratio " + gtarget_ratio
									+ ", free L3 ratio " + l3ratio + ", L3 taget ratio " + mtarget_ratio
									+ ", free L4 ratio " + l4ratio + ", L4 taget ratio " + starget_ratio);
							
							if (target_ratio < ratio && ctarget_ratio < l1ratio && gtarget_ratio < l2ratio
									&& mtarget_ratio < l3ratio && starget_ratio < l4ratio) {
								sleepnr = Math.min(sleepnr * 2, 60);
								continue;
							} else {
								sleepnr = Math.max(sleepnr / 2, 10);
							}

							// sort by time
							boolean stop = false;
							long cur_hour = System.currentTimeMillis() / 1000 / 3600 * 3600;
							List<Long> fsmapToDel = new ArrayList<Long>();
							int mignr = 0;

							for (Long k : fmap.keySet()) {
								Map<String, FileStat> fsmap = fmap.get(k);
								long hours = (cur_hour - k) / 3600;
								System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(
										new Date(k * 1000)) + "\t" + hours + " hrs");
								// iterate on each rule
								for (ScrubnRule sr : srl) {
									List<String> toDel = new ArrayList<String>();
									//judge the table name is right or not
									for (Map.Entry<String, FileStat> e : fsmap.entrySet()) {
										if (sr.type.equalsIgnoreCase("hlw")) {
											if (e.getKey().contains("t_gkrz") || 
													e.getKey().contains("t_gzrz") ||
													e.getKey().contains("t_jcrz") ||
													e.getKey().contains("t_ybrz")) {
												// ok
											} else
												continue;
										} else if (sr.type.equalsIgnoreCase(e.getKey())) {
											// ok
										} else if (sr.type.equalsIgnoreCase("all")) {
											// ok
										} else {
											continue;
										}
										
										if (hours > sr.times.get(0) && hours <= sr.times.get(1)) {
											Set<Long> idToDel = new TreeSet<Long>();
											System.out.print("T1 " + sr + " => on " + e.getKey() + " " + e.getValue().fids.size() + " files [");
											for (Long fid : e.getValue().fids) {
												try {
													SFile f = cli.client.get_file_by_id(fid);
													if (sr.rules.get(0).equalsIgnoreCase("del")) {
														sr.action = ScrubnRule.ScrubAction.DELETE;
													} else if (sr.rules.get(0).equalsIgnoreCase("drep")) {
														sr.action = ScrubnRule.ScrubAction.DOWNREP;
													} else if (sr.rules.get(0).equalsIgnoreCase("migr")) {
														sr.action = ScrubnRule.ScrubAction.MIGRATE;
													}

													switch (sr.action) {
													case DELETE:
														cli.client.rm_file_physical(f);
														toDel.add(e.getKey());
														System.out.print("DEL:" + f.getFid() + ",");
														break;
													case DOWNREP:
														if (f.getRep_nr() > 1) {
															cli.client.set_file_repnr(f.getFid(), 1);
															System.out.print("DRP:" + f.getFid() + ",");
														}
														break;
													case MIGRATE:
														if (f.getStore_status() == MetaStoreConst.MFileStoreStatus.INCREATE ||
															f.getStore_status() == MetaStoreConst.MFileStoreStatus.RM_PHYSICAL || 
															mignr > mignr_max)
															break;
														List<SFileLocation> sfl = f.getLocations();
														List<SFileLocation> sfl1 = new ArrayList<SFileLocation>();//SSD
														List<SFileLocation> sfl2 = new ArrayList<SFileLocation>();//GENERAL
														for (SFileLocation s :sfl) {
															Device dev = cli.client.getDevice(s.getDevid());
															if (DeviceInfo.getType(dev.getProp()) == MetaStoreConst.MDeviceProp.CACHE) {
																sfl1.add(s);
															} else if (DeviceInfo.getType(dev.getProp()) == MetaStoreConst.MDeviceProp.GENERAL) {
																sfl2.add(s);
															}
														}
														if (sfl1.size() != 0) {
															if (f.getRep_nr() >= 1) {
																if (sfl2.size() == 0) {
																	boolean do_migr = true;
																	// BUG-XXX: if we detect a recent running SFL replicate, we do not trigger a new replicate
																	for (SFileLocation s : sfl) {
																		if (s.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.OFFLINE &&
																				s.getUpdate_time() + 1.1 * 3600 * 1000 > System.currentTimeMillis()) {
																			// do NOT trigger new migrate
																			do_migr = false;
																			break;
																		}
																	}
																	if (do_migr) {
																		cli.client.set_file_repnr(f.getFid(), 1);
																		cli.client.replicate(f.getFid(), MetaStoreConst.MDeviceProp.GENERAL);
																		mignr++;
																		System.out.print("MIG:" + f.getFid() + ",");
																	}
																} else {
																	SFile sf = cli.client.get_file_by_id(fid);
																	List<SFileLocation> sfls = sf.getLocations();
																	for (SFileLocation sfi : sfls) {
																		Device de = cli.client.getDevice(sfi.getDevid());
																		if (sfi.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE
																				 && DeviceInfo.getType(de.getProp())  == MetaStoreConst.MDeviceProp.GENERAL) {
																			cli.client.set_file_repnr(f.getFid(), 1);
																			for (SFileLocation s : sfl1) {
																				cli.client.del_filelocation(s.getDevid(), s.getLocation());
																			}
																			System.out.print("DFL:" + f.getFid() + ",");
																			break;
																		}
																	}
																}
															}
														}
														break;
													}
												} catch (FileOperationException foe) {
													idToDel.add(fid);
												} catch (Exception foe) {
												}
											}
											System.out.println("]");
											if (idToDel.size() > 0) {
												for (Long fid : idToDel) {
													e.getValue().fids.remove(fid);
												}
											}
										} else if (hours > sr.times.get(1) && hours <= sr.times.get(2)) {
											Set<Long> idToDel = new TreeSet<Long>();
											System.out.print("T2 " + sr + " => on " + e.getKey() + " " + e.getValue().fids.size() + " files [");
											for (Long fid : e.getValue().fids) {
												try {
													SFile f = cli.client.get_file_by_id(fid);
													if (sr.rules.get(1).equalsIgnoreCase("del")) {
														sr.action = ScrubnRule.ScrubAction.DELETE;
													} else if (sr.rules.get(1).equalsIgnoreCase("drep")) {
														sr.action = ScrubnRule.ScrubAction.DOWNREP;
													} else if (sr.rules.get(1).equalsIgnoreCase("migr")) {
														sr.action = ScrubnRule.ScrubAction.MIGRATE;
													}
													switch (sr.action) {
													case DELETE:
														cli.client.rm_file_physical(f);
														toDel.add(e.getKey());
														System.out.print("DEL:" + f.getFid() + ",");
														break;
													case DOWNREP:
														if (f.getRep_nr() > 1) {
															cli.client.set_file_repnr(f.getFid(), 1);
															System.out.print("DRP:" + f.getFid() + ",");
														}
														break;
													case MIGRATE:
														//System.out.println("fid:" + f.getFid() + ",status:" + f.getStore_status() + ",mignr=" + mignr);
														if (f.getStore_status() == MetaStoreConst.MFileStoreStatus.INCREATE ||
															f.getStore_status() == MetaStoreConst.MFileStoreStatus.RM_PHYSICAL ||
																mignr > mignr_max) 
															break;
														List<SFileLocation> sfl = f.getLocations();
														List<SFileLocation> sfl1 = new ArrayList<SFileLocation>();//GENERAL
														List<SFileLocation> sfl2 = new ArrayList<SFileLocation>();//MASS
														for (SFileLocation s : sfl) {
															Device dev = cli.client.getDevice(s.getDevid());
															if (DeviceInfo.getType(dev.getProp()) == MetaStoreConst.MDeviceProp.GENERAL) {
																sfl1.add(s);
															} else if(DeviceInfo.getType(dev.getProp()) == MetaStoreConst.MDeviceProp.MASS) {
																sfl2.add(s);
															}
														}
														if (sfl1.size() != 0) {
															if (f.getRep_nr() >= 1) {
																if (sfl2.size() == 0) {
																	boolean do_migr = true;
																	// BUG-XXX: if we detect a recent running SFL replicate, we do not trigger a new replicate
																	for (SFileLocation s : sfl) {
																		if (s.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.OFFLINE &&
																				s.getUpdate_time() + 1.1 * 3600 * 1000 > System.currentTimeMillis()) {
																			// do NOT trigger new migrate
																			do_migr = false;
																			break;
																		}
																	}
																	if (do_migr) {
																		cli.client.set_file_repnr(f.getFid(), 1);
																		cli.client.replicate(f.getFid(), MetaStoreConst.MDeviceProp.MASS);
																		mignr++;
																		System.out.print("MIG:" + f.getFid() + ",");
																	}
																} else {
																	SFile sf = cli.client.get_file_by_id(fid);
																	List<SFileLocation> sfls = sf.getLocations();
																	for (SFileLocation sfi : sfls) {
																		Device de = cli.client.getDevice(sfi.getDevid());
																		if (sfi.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE
																				 && DeviceInfo.getType(de.getProp())  == MetaStoreConst.MDeviceProp.MASS) {
																			cli.client.set_file_repnr(f.getFid(), 1);
																			for (SFileLocation s : sfl1) {
																				Device dev = cli.client.getDevice(s.getDevid());
																				cli.client.del_filelocation(dev.getDevid(), s.getLocation());
																			}
																			System.out.print("DFL:" + f.getFid() + ",");
																			break;
																		}
																	}
																}
															}
														}
														break;
													}
												} catch (FileOperationException foe) {
													idToDel.add(fid);
												} catch (Exception foe) {
												}
											}
											System.out.println("]");
											if (idToDel.size() > 0) {
												for (Long fid : idToDel) {
													e.getValue().fids.remove(fid);
												}
											}
										} else if (hours > sr.times.get(2) && hours <= sr.times.get(3)) {
											Set<Long> idToDel = new TreeSet<Long>();
											System.out.print("T3 " + sr + " => on " + e.getKey() + " " + e.getValue().fids.size() + " files [");
											for (Long fid : e.getValue().fids) {
												try {
													SFile f = cli.client.get_file_by_id(fid);
													if (sr.rules.get(2).equalsIgnoreCase("del")) {
														sr.action = ScrubnRule.ScrubAction.DELETE;
													} else if (sr.rules.get(2).equalsIgnoreCase("drep")) {
														sr.action = ScrubnRule.ScrubAction.DOWNREP;
													} else if (sr.rules.get(2).equalsIgnoreCase("migr")) {
														sr.action = ScrubnRule.ScrubAction.MIGRATE;
													}
													switch (sr.action) {
													case DELETE:
														cli.client.rm_file_physical(f);
														toDel.add(e.getKey());
														System.out.print("DEL:" + f.getFid() + ",");
														break;
													case DOWNREP:
														if (f.getRep_nr() > 1) {
															cli.client.set_file_repnr(f.getFid(), 1);
															System.out.print("DRP:" + f.getFid() + ",");
														}
														break;
													case MIGRATE:
														if (f.getStore_status() == MetaStoreConst.MFileStoreStatus.INCREATE ||
															f.getStore_status() == MetaStoreConst.MFileStoreStatus.RM_PHYSICAL || 
															mignr > mignr_max)
															break;
														List<SFileLocation> sfl = f.getLocations();
														List<SFileLocation> sfl1 = new ArrayList<SFileLocation>();//MASS
														List<SFileLocation> sfl2 = new ArrayList<SFileLocation>();//NAS
														for (SFileLocation s :sfl) {
															Device dev = cli.client.getDevice(s.getDevid());
															if (DeviceInfo.getType(dev.getProp()) == MetaStoreConst.MDeviceProp.MASS) {
																sfl1.add(s);
															} else if (DeviceInfo.getType(dev.getProp()) == MetaStoreConst.MDeviceProp.SHARED) {
																sfl2.add(s);
															}
														}
														if (sfl1.size() != 0) {
															if (f.getRep_nr() >= 1) {
																if (sfl2.size() == 0) {
																	boolean do_migr = true;
																	// BUG-XXX: if we detect a recent running SFL replicate, we do not trigger a new replicate
																	for (SFileLocation s : sfl) {
																		if (s.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.OFFLINE &&
																				s.getUpdate_time() + 1.1 * 3600 * 1000 > System.currentTimeMillis()) {
																			// do NOT trigger new migrate
																			do_migr = false;
																			break;
																		}
																	}
																	if (do_migr) {
																		cli.client.set_file_repnr(f.getFid(), 1);
																		cli.client.replicate(f.getFid(), MetaStoreConst.MDeviceProp.SHARED);
																		mignr++;
																		System.out.print("MIG:" + f.getFid() + ",");
																	}
																} else {
																	SFile sf = cli.client.get_file_by_id(fid);
																	List<SFileLocation> sfls = sf.getLocations();
																	for (SFileLocation sfi : sfls) {
																		Device de = cli.client.getDevice(sfi.getDevid());
																		if (sfi.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE
																				 && DeviceInfo.getType(de.getProp())  == MetaStoreConst.MDeviceProp.SHARED) {
																			cli.client.set_file_repnr(f.getFid(), 1);
																			for (SFileLocation s : sfl1) {
																				Device dev = cli.client.getDevice(s.getDevid());
																				cli.client.del_filelocation(dev.getDevid(), s.getLocation());
																			}
																			System.out.print("DFL:" + f.getFid() + ",");
																			break;
																		}
																	}
																}
															}
															break;
														}
													}
												} catch (FileOperationException foe) {
													idToDel.add(fid);
												} catch (Exception foe) {
												}
											}
											System.out.println("]");
											if (idToDel.size() > 0) {
												for (Long fid : idToDel) {
													e.getValue().fids.remove(fid);
												}
											}
										} else if (hours > sr.times.get(3) || hours < -10000) {
											Set<Long> idToDel = new TreeSet<Long>();
											System.out.print("T4 " + sr + " => on " + e.getKey() + " " + e.getValue().fids.size() + " files [");
											for (Long fid : e.getValue().fids) {
												try {
													SFile f = cli.client.get_file_by_id(fid);
													if (sr.rules.get(3).equalsIgnoreCase("del")) {
														sr.action = ScrubnRule.ScrubAction.DELETE;
													} else if(sr.rules.get(3).equalsIgnoreCase("drep")) {
														sr.action = ScrubnRule.ScrubAction.DOWNREP;
													} else if(sr.rules.get(3).equalsIgnoreCase("migr")) {
														sr.action = ScrubnRule.ScrubAction.MIGRATE;
													}
													switch (sr.action) {
													case DELETE:
														cli.client.rm_file_physical(f);
														toDel.add(e.getKey());
														System.out.print("DEL:" + f.getFid() + ",");
														break;
													case DOWNREP:
														if (f.getRep_nr() > 1) {
															cli.client.set_file_repnr(f.getFid(), 1);
															System.out.print("DRP:" + f.getFid() + ",");
														}
														break;
													case MIGRATE:
														System.out.println("[ERROR] Can not do migrate on L4 device for fid: " + fid);
														break;
													}
												} catch (FileOperationException foe) {
													idToDel.add(fid);
												} catch (Exception foe) {
												}
											}
											System.out.println("]");
											if (idToDel.size() > 0) {
												for (Long fid : idToDel) {
													e.getValue().fids.remove(fid);
												}
											}
										}
										FreeSpace fs = __get_free_space_ratio(cli);
										if (__large(fs.ratio, target_ratio) &&
												__large(fs.l1ratio, ctarget_ratio) &&
												__large(fs.l2ratio, gtarget_ratio) &&
												__large(fs.l3ratio, mtarget_ratio) &&
												__large(fs.l4ratio, starget_ratio)) {
											stop = true;
											break;
										}
									}
									for (String s : toDel) {
										fsmap.remove(s);
									}
								}
								if (fsmap.size() == 0)
									fsmapToDel.add(k);
								if (stop)
									break;
							}
							if (fsmapToDel.size() > 0) {
								for (Long k : fsmapToDel) {
									fmap.remove(k);
								}
							}
						} catch (MetaException e1) {
							e1.printStackTrace();
							if (e1.getCause() instanceof ConnectException) {
								try {
									cli.stop();
								} catch (Exception se) {
								}
								cli = null;
							}
						} catch (TException e1) {
							e1.printStackTrace();
							try {
								cli.stop();
							} catch (Exception se) {
							}
							cli = null;
						}
					} catch (Exception e) { 
						e.printStackTrace();
						try {
							cli.stop();
						} catch (Exception se) {
						}
						cli = null;
					}
				}
			}
			if (o.flag.equals("-avglen")) {
				// get avg length and record number of each table in some time range
				long end = 0;
				if ((dbName == null) ||
						((begin_time < 0 && end_time < 0) &&
						(statfs_range <= 0) &&
						(statfs2_bday <= 0 && statfs2_days <= 0))) {
					System.out.println("Please set -statfs_range or (-statfs2_bday and -statfs2_days) and -db.");
					MetaStoreClient.__EXIT(0);
				}
				
				if (statfs2_bday >= 0 && statfs2_days >= 0) {
					end_time = System.currentTimeMillis() / 1000;
					end = end_time / 3600 * 3600;
					end = end - statfs2_bday * 86400;
					begin_time = end - statfs2_days * 86400;
				} else if (statfs_range > 0) {
					end_time = System.currentTimeMillis() / 1000;
					// find a valid Hour start time
					end = end_time / 3600 * 3600;
					begin_time = end_time - statfs_range;
				} else {
					end = end_time / 3600 * 3600;
					begin_time = begin_time / 3600 * 3600;
				}
				List<String> tables;
				try {
					TreeMap<Long, Map<String, FileStat>> fmap = new TreeMap<Long, Map<String, FileStat>>();
					tables = cli.client.getAllTables(dbName);
					for (; end >= begin_time; end -= 3600) {
						List<SplitValue> lsv = new ArrayList<SplitValue>();
						System.out.println("Handling data begin @ " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(end * 1000)));
						
						for (String tbl : tables) {
							lsv.clear();
							Table t = cli.client.getTable(dbName, tbl);
							if (t.getFileSplitKeysSize() > 0) {
								int maxv = 0;
								List<PartitionInfo> allpis = PartitionFactory.PartitionInfo.getPartitionInfo(t.getFileSplitKeys());

								for (PartitionInfo pi : allpis) {
									if (maxv < pi.getP_version())
										maxv = pi.getP_version();
								}
								List<List<PartitionInfo>> vpis = new ArrayList<List<PartitionInfo>>();
								for (int i = 0; i <= maxv; i++) {
									List<PartitionInfo> lpi = new ArrayList<PartitionInfo>();
									vpis.add(lpi);
								}
								for (PartitionInfo pi : allpis) {
									vpis.get(pi.getP_version()).add(pi);
								}
								// ok, we get versioned PIs; for each version, we generate a LSV and call filterTable
								for (int i = 0; i <= maxv; i++) {
									// BUG: in our lv13 demo systems, versions leaks, so we have to ignore some nonexist versions
									if (vpis.get(i).size() <= 0) {
										System.out.println("Metadata corrupted, version " + i + " leaks for table " + tbl + ".");
										continue;
									}
									if (vpis.get(i).get(0).getP_type() != PartitionFactory.PartitionType.interval)
										continue;
									lsv.add(new SplitValue(vpis.get(i).get(0).getP_col(), 1, ((Long)end).toString(), vpis.get(i).get(0).getP_version()));
									lsv.add(new SplitValue(vpis.get(i).get(0).getP_col(), 1, ((Long)(end + Integer.parseInt(vpis.get(i).get(0).getArgs().get(1)) * 3600)).toString(), vpis.get(i).get(0).getP_version()));
									// call update map
									List<SFile> files = cli.client.filterTableFiles(dbName, tbl, lsv);
									System.out.println("Got Table " + tbl + " LSV: " + lsv + " Hit " + files.size());
									lsv.clear();
									statfs2_update_map(cli, fmap, files, statfs2_getlen);
									
									if (statfs2_xj) {
										lsv.add(new SplitValue(vpis.get(i).get(0).getP_col(), 1, ((Long)end).toString(), vpis.get(i).get(0).getP_version()));
										lsv.add(new SplitValue(vpis.get(i).get(0).getP_col(), 1, ((Long)(end + Integer.parseInt(vpis.get(i).get(0).getArgs().get(1)) * 3600 - 1)).toString(), vpis.get(i).get(0).getP_version()));
										// call update map
										files = cli.client.filterTableFiles(dbName, tbl, lsv);
										System.out.println("Got Table " + tbl + " LSV: " + lsv + " Hit " + files.size());
										lsv.clear();
										statfs2_update_map(cli, fmap, files, statfs2_getlen);
									}
								}
							}
						}
					}
					Long total_size = 0L;
					Map<String, Long> sizeMap = new TreeMap<String, Long>();
					Map<String, Long> fnrMap = new TreeMap<String, Long>();
					for (Long k : fmap.keySet()) {
						Map<String, FileStat> fsmap = fmap.get(k);
						System.out.print(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(k * 1000)) + "\t");
						for (Map.Entry<String, FileStat> e : fsmap.entrySet()) {
							System.out.print(e.getKey() + ":" + e.getValue().fids.size() + 
									":" + e.getValue().space + "; ");
							total_size += e.getValue().space;
							if (sizeMap.get(e.getKey()) == null) {
								sizeMap.put(e.getKey(), e.getValue().space);
								fnrMap.put(e.getKey(), (long)e.getValue().fids.size());
							} else {
								sizeMap.put(e.getKey(), sizeMap.get(e.getKey()) + e.getValue().space);
								fnrMap.put(e.getKey(), fnrMap.get(e.getKey()) + e.getValue().fids.size());
							}
						}
						System.out.println();
					}
					for (Map.Entry<String, Long> e : fnrMap.entrySet()) {
						if (e.getValue() > 0)
							System.out.println("Table " + e.getKey() + " -> Total " + sizeMap.get(e.getKey()) + " KB, avg " + 
									((double)sizeMap.get(e.getKey()) / e.getValue()) + " KB.");
					}
				} catch (MetaException e) {
					e.printStackTrace();
				} catch (UnknownDBException e) {
					e.printStackTrace();
				} catch (TException e) {
					e.printStackTrace();
				}
			}
			if (o.flag.equals("-scrub_fast")) {
				// scrub in fast mode
				TreeMap<Long, Map<String, FileStat>> fmap = new TreeMap<Long, Map<String, FileStat>>();
				long last_got = 0;
				
				if (scrub_max < 0) {
					try {
						scrub_max = cli.client.getMaxFid();
					} catch (MetaException e) {
						e.printStackTrace();
						break;
					} catch (TException e) {
						e.printStackTrace();
						break;
					}
				}
				System.out.println("Get Max FID " + scrub_max);
				
				update_fmap(cli, lfdc_thread, xURI, serverName, serverPort, fmap, 0, scrub_max,
						statfs2_getlen);
				last_got = scrub_max / lfdc_thread * lfdc_thread;
				System.out.println("Get File Info upto FID " + last_got);
				
				Long total_size = 0L;
				Map<String, Long> sizeMap = new TreeMap<String, Long>();
				Map<String, Long> fnrMap = new TreeMap<String, Long>();
				for (Long k : fmap.keySet()) {
					Map<String, FileStat> fsmap = fmap.get(k);
					System.out.print(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(k * 1000)) + "\t");
					for (Map.Entry<String, FileStat> e : fsmap.entrySet()) {
						System.out.print(e.getKey() + ":" + e.getValue().fids.size() + 
								":" + e.getValue().space + "; ");
						total_size += e.getValue().space;
						if (sizeMap.get(e.getKey()) == null) {
							sizeMap.put(e.getKey(), e.getValue().space);
							fnrMap.put(e.getKey(), (long)e.getValue().fids.size());
						} else {
							sizeMap.put(e.getKey(), sizeMap.get(e.getKey()) + e.getValue().space);
							fnrMap.put(e.getKey(), fnrMap.get(e.getKey()) + e.getValue().fids.size());
						}
					}
					System.out.println();
				}
				for (Map.Entry<String, Long> e : fnrMap.entrySet()) {
					if (e.getValue() > 0)
						System.out.println("Table " + e.getKey() + " -> Total " + sizeMap.get(e.getKey()) + " KB, avg " + 
								((double)sizeMap.get(e.getKey()) / e.getValue()) + " KB.");
				}
			}
			
			if (o.flag.equals("-statfs2")) {
				// stat the file system by SplitValue
				long end = 0;
				boolean isEmergency = false;

				if ((dbName == null) ||
					((begin_time < 0 && end_time < 0) &&
					(statfs_range <= 0) &&
					(statfs2_bday <= 0 && statfs2_days <= 0))) {
					System.out.println("Please set -statfs_range or (-statfs2_bday and -statfs2_days) and -db.");
					MetaStoreClient.__EXIT(0);
				}

				if (statfs2_bday >= 0 && statfs2_days >= 0) {
					end_time = System.currentTimeMillis() / 1000;
					end = end_time / 3600 * 3600;
					end = end - statfs2_bday * 86400;
					begin_time = end - statfs2_days * 86400;
				} else if (statfs_range > 0) {
					end_time = System.currentTimeMillis() / 1000;
					// find a valid Hour start time
					end = end_time / 3600 * 3600;
					begin_time = end_time - statfs_range;
				} else {
					end = end_time / 3600 * 3600;
					begin_time = begin_time / 3600 * 3600;
				}
				try {
					String dms = cli.client.getDMStatus();
					BufferedReader bufReader = new BufferedReader(new StringReader(dms));
					String line = null;
					while ((line = bufReader.readLine()) != null) {
						if (line.startsWith("True  space")) {
							String[] ls = line.split(" ");
							if (Double.parseDouble(ls[ls.length - 1]) <= 0.05) {
								// emergency mode, automatically delete
								isEmergency = true;
							} else if (Double.parseDouble(ls[ls.length - 1]) <= 0.1) {
								// alert mode, do NOT
							}
						}
					}
				} catch (MetaException e1) {
					e1.printStackTrace();
					break;
				} catch (TException e1) {
					e1.printStackTrace();
					break;
				}
				// find oldest files by SplitValue?
				System.out.println("Note: statfs2 only count SplitValue which is one hour range.");
				List<String> tables;
				try {
					TreeMap<Long, Map<String, FileStat>> fmap = new TreeMap<Long, Map<String, FileStat>>();
					tables = cli.client.getAllTables(dbName);
					if (statfs2_tbl.equalsIgnoreCase("all")) {
						// do nothing
					} else if (statfs2_tbl.equalsIgnoreCase("dx_rz")) {
						List<String> new_tables = new ArrayList<String>();
						for (String t : tables) {
							if (t.contains("dx_rz")) {
								new_tables.add(t);
							}
						}
						tables = new_tables;
					} else if (statfs2_tbl.equalsIgnoreCase("cdr")) {
						List<String> new_tables = new ArrayList<String>();
						for (String t : tables) {
							if (t.contains("cdr")) {
								new_tables.add(t);
							}
						}
						tables = new_tables;
					} else if (statfs2_tbl.equalsIgnoreCase("HLW")) {
						List<String> new_tables = new ArrayList<String>();
						for (String t : tables) {
							if (t.contains("t_gkrz") ||
									t.contains("t_gzrz") ||
									t.contains("t_jcrz") ||
									t.contains("t_ybrz")) {
								new_tables.add(t);
							}
						}
						tables = new_tables;
					} else if (statfs2_tbl.equalsIgnoreCase("gkrz")) {
						List<String> new_tables = new ArrayList<String>();
						for (String t : tables) {
							if (t.contains("gkrz")) {
								new_tables.add(t);
							}
						}
						tables = new_tables;
					} else if (statfs2_tbl.equalsIgnoreCase("gzrz")) {
						List<String> new_tables = new ArrayList<String>();
						for (String t : tables) {
							if (t.contains("gzrz")) {
								new_tables.add(t);
							}
						}
						tables = new_tables;
					} else if (statfs2_tbl.equalsIgnoreCase("jcrz")) {
						List<String> new_tables = new ArrayList<String>();
						for (String t : tables) {
							if (t.contains("jcrz")) {
								new_tables.add(t);
							}
						}
						tables = new_tables;
					} else if (statfs2_tbl.equalsIgnoreCase("ybrz")) {
						List<String> new_tables = new ArrayList<String>();
						for (String t : tables) {
							if (t.contains("ybrz")) {
								new_tables.add(t);
							}
						}
						tables = new_tables;
					}  else {
						tables = new ArrayList<String>();
						tables.add(statfs2_tbl);
					}
					for (; end >= begin_time; end -= 3600) {
						List<SplitValue> lsv = new ArrayList<SplitValue>();
						System.out.println("Handling data begin @ " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(end * 1000)));
						
						for (String tbl : tables) {
							lsv.clear();
							Table t = cli.client.getTable(dbName, tbl);
							if (t.getFileSplitKeysSize() > 0) {
								int maxv = 0;
								List<PartitionInfo> allpis = PartitionFactory.PartitionInfo.getPartitionInfo(t.getFileSplitKeys());

								for (PartitionInfo pi : allpis) {
									if (maxv < pi.getP_version())
										maxv = pi.getP_version();
								}
								List<List<PartitionInfo>> vpis = new ArrayList<List<PartitionInfo>>();
								for (int i = 0; i <= maxv; i++) {
									List<PartitionInfo> lpi = new ArrayList<PartitionInfo>();
									vpis.add(lpi);
								}
								for (PartitionInfo pi : allpis) {
									vpis.get(pi.getP_version()).add(pi);
								}
								// ok, we get versioned PIs; for each version, we generate a LSV and call filterTable
								for (int i = 0; i <= maxv; i++) {
									// BUG: in our lv13 demo systems, versions leaks, so we have to ignore some nonexist versions
									if (vpis.get(i).size() <= 0) {
										System.out.println("Metadata corrupted, version " + i + " leaks for table " + tbl + ".");
										continue;
									}
									if (vpis.get(i).get(0).getP_type() != PartitionFactory.PartitionType.interval)
										continue;
									lsv.add(new SplitValue(vpis.get(i).get(0).getP_col(), 1, ((Long)end).toString(), vpis.get(i).get(0).getP_version()));
									lsv.add(new SplitValue(vpis.get(i).get(0).getP_col(), 1, ((Long)(end + Integer.parseInt(vpis.get(i).get(0).getArgs().get(1)) * 3600)).toString(), vpis.get(i).get(0).getP_version()));
									// call update map
									List<SFile> files = cli.client.filterTableFiles(dbName, tbl, lsv);
									System.out.println("Got Table " + tbl + " LSV: " + lsv + " Hit " + files.size());
									lsv.clear();
									statfs2_update_map(cli, fmap, files, statfs2_getlen);
									
									if (statfs2_xj) {
										lsv.add(new SplitValue(vpis.get(i).get(0).getP_col(), 1, ((Long)end).toString(), vpis.get(i).get(0).getP_version()));
										lsv.add(new SplitValue(vpis.get(i).get(0).getP_col(), 1, ((Long)(end + Integer.parseInt(vpis.get(i).get(0).getArgs().get(1)) * 3600 - 1)).toString(), vpis.get(i).get(0).getP_version()));
										// call update map
										files = cli.client.filterTableFiles(dbName, tbl, lsv);
										System.out.println("Got Table " + tbl + " LSV: " + lsv + " Hit " + files.size());
										lsv.clear();
										statfs2_update_map(cli, fmap, files, statfs2_getlen);
									}
								}
							}
						}
					}
					Long total_size = 0L;
					Map<String, Long> sizeMap = new TreeMap<String, Long>();
					Map<String, Long> fnrMap = new TreeMap<String, Long>();
					for (Long k : fmap.keySet()) {
						Map<String, FileStat> fsmap = fmap.get(k);
						System.out.print(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(k * 1000)) + "\t");
						for (Map.Entry<String, FileStat> e : fsmap.entrySet()) {
							System.out.print(e.getKey() + ":" + e.getValue().fids.size() + 
									":" + e.getValue().space + "; ");
							total_size += e.getValue().space;
							if (sizeMap.get(e.getKey()) == null) {
								sizeMap.put(e.getKey(), e.getValue().space);
								fnrMap.put(e.getKey(), (long)e.getValue().fids.size());
							} else {
								sizeMap.put(e.getKey(), sizeMap.get(e.getKey()) + e.getValue().space);
								fnrMap.put(e.getKey(), fnrMap.get(e.getKey()) + e.getValue().fids.size());
							}
						}
						System.out.println();
					}
					for (Map.Entry<String, Long> e : sizeMap.entrySet()) {
						System.out.println("Table " + e.getKey() + " -> " + fnrMap.get(e.getKey()) + " " + e.getValue() + " KB");
					}
					System.out.println("Total Size " + total_size + " KB");
					if (statfs2_del)
						System.err.print("Do you really want to DELETE these files? (Y or N) ");
					else
						System.err.print("Do you really want to DOWN-REP these files? (Y or N) ");
					
					if ((System.in.read() == 'Y') || isEmergency) {
						for (Long k : fmap.keySet()) {
							Map<String, FileStat> fsmap = fmap.get(k);
							System.out.print(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(k * 1000)) + "\t");
							for (Map.Entry<String, FileStat> e : fsmap.entrySet()) {
								System.out.print(e.getKey() + ":" + e.getValue().fids + "; ");
								for (Long fid : e.getValue().fids) {
									try {
										SFile f = cli.client.get_file_by_id(fid);
										if (statfs2_del)
											cli.client.rm_file_physical(f);
										else
											cli.client.set_file_repnr(f.getFid(), f.getRep_nr() > 1 ? f.getRep_nr() - 1 : 1);
									} catch (FileOperationException foe) {
										// ignore it
									}
								}
							}
							System.out.println();
						}
					} else {
						System.err.println("Delete aborted.");
					}
				} catch (MetaException e) {
					e.printStackTrace();
				} catch (UnknownDBException e) {
					e.printStackTrace();
				} catch (TException e) {
					e.printStackTrace();
				}
			}
			if (o.flag.equals("-statfs")) {
				// stat the file system
				if ((begin_time < 0 || end_time < 0) && statfs_range <= 0) {
					System.out.println("Please set (-begin_time and -end_time) or -statfs_range");
					MetaStoreClient.__EXIT(0);
				}
				if (statfs_range > 0) {
					end_time = System.currentTimeMillis() / 1000;
					begin_time = end_time - statfs_range;
				}
				try {
					cli.client.setTimeout(120);
					statfs s = cli.client.statFileSystem(begin_time, end_time);
					System.out.println("Query on time range [" + begin_time + "," + end_time + ") {" +
							new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(begin_time * 1000)) + "," + 
							new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(end_time * 1000)) + 
							"} -> ");
					System.out.println(" INCREATE     " + s.getIncreate());
					System.out.println(" CLOSE        " + s.getClose());
					System.out.println(" REPLICATED   " + s.getReplicated());
					System.out.println(" RM_LOGICAL   " + s.getRm_logical());
					System.out.println(" RM_PHYSICAL  " + s.getRm_physical());
					System.out.println("");
					System.out.println(" OVERREP      " + s.getOverrep());
					System.out.println(" UNDERREP     " + s.getUnderrep());
					System.out.println(" LINGER       " + s.getLinger());
					System.out.println(" SUSPECT      " + s.getSuspect());
					System.out.println("");
					System.out.println(" INC OFFLINE  " + (s.getIncreate() - s.getInc_ons() - s.getInc_ons2()));
					System.out.println(" INC ONLINE   " + s.getInc_ons());
					System.out.println(" INC ONLINE2+ " + ANSI_RED + s.getInc_ons2() + ANSI_RESET);
					System.out.println(" CLS OFFLINE  " + ANSI_RED + s.getCls_offs() + ANSI_RESET);
					System.out.println("");
					System.out.println(" COLS         " + s.getClos());
					System.out.println(" INCS O2ERR   " + s.getIncs());
					System.out.println("");
					System.out.println(" File in Tab  " + s.getFnrs());
					System.out.println("");
					System.out.println(" Total Rec #  " + s.getRecordnr());
					System.out.println(" Total Length " + (s.getLength() / 1000000000.0) + " GB");
					if (s.getIncsSize() > 0) {
						System.out.println(ANSI_RED + "BAD STATE in Our Store! Please notify <macan@iie.ac.cn>" + ANSI_RESET);
					} else {
						System.out.println(ANSI_GREEN + "GOOD STATE ;)" + ANSI_RESET);
					}
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
			}
			if (o.flag.equals("-dfl")) {
				// delete a file location, and remove the physical data
				if (dfl_dev == null || dfl_location == null) {
					System.out.println("Please set -dfl_dev and -dfl_location");
					MetaStoreClient.__EXIT(0);
				}

				try {
					cli.client.del_filelocation(dfl_dev, dfl_location);
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
			}
			if (o.flag.equals("-dflf")) {
				// delete a file location (read from a file), and remove the physical data
				if (dfl_file == null) {
					System.out.println("Please set -dfl_file");
					MetaStoreClient.__EXIT(0);
				}

				try {
					File tf = new File(dfl_file);
					FileReader fr = new FileReader(tf.getAbsoluteFile());
					BufferedReader br = new BufferedReader(fr);
					String line = null;
					
					while ((line = br.readLine()) != null) {
						String[] ln = line.split(",");
						if (ln.length == 2) {
							System.out.println("Got DEVID " + ln[0] + " LOC " + ln[1]);
							cli.client.del_filelocation(ln[0], ln[1]);
						}
					}
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
			}
			if (o.flag.equals("-gbn")) {
				// get a file by sfl keys
				if (dfl_dev == null || dfl_location == null) {
					System.out.println("Please set -dfl_dev and -dfl_location.");
					MetaStoreClient.__EXIT(0);
				}
				SFile f;
				try {
					f = cli.client.get_file_by_name("", dfl_dev, dfl_location);
					if (f != null) {
						System.out.println("Read file: " + toStringSFile(f));
						// iterator on file locations
						if (f.getLocationsSize() > 0) {
							for (SFileLocation sfl : f.getLocations()) {
								String n = sfl.getNode_name();
								if (sfl.getNode_name().contains(";")) {
									n = sfl.getNode_name().split(";")[0];
								}
								String mp = cli.client.getMP(n, sfl.getDevid());
								System.out.println("ssh " + n + " ls -l " + mp + "/" + sfl.getLocation());
							}
						}
					}
				} catch (FileOperationException e) {
					e.printStackTrace();
				} catch (MetaException e) {
					e.printStackTrace();
				} catch (TException e) {
					e.printStackTrace();
				}
			}
			if (o.flag.equals("-ofl")) {
				// offline a file location
				if (ofl_fid < 0 || ofl_sfl_dev == null) {
					System.out.println("Please set -ofl_fid and -ofl_sfl_dev.");
					MetaStoreClient.__EXIT(0);
				}
				SFile f;
				try {
					f = cli.client.get_file_by_id(ofl_fid);
					SFileLocation sfl = null;
				
					if (f.getLocationsSize() > 0) {
						for (SFileLocation fl : f.getLocations()) {
							if (fl.getDevid().equalsIgnoreCase(ofl_sfl_dev)) {
								sfl = fl;
								break;
							}
						}
					}
					if (sfl != null)
						ofl_del = ofl_del && cli.client.offline_filelocation(sfl);
					if (ofl_del) {
						String mp, cmd = null;
						String n = sfl.getNode_name();
						if (n.contains(";"))
							n = sfl.getNode_name().split(";")[0];
						mp = cli.client.getMP(n, sfl.getDevid());
						if (mp != null)
							cmd = "ssh " + n + " rm -rf " + mp + "/" + sfl.getLocation();
						System.out.println("CMD: {" + cmd + "}");
						// runRemoteCmd(cmd);
					}
				} catch (FileOperationException e) {
					e.printStackTrace();
				} catch (MetaException e) {
					e.printStackTrace();
				} catch (TException e) {
					e.printStackTrace();
				}
			}
			if (o.flag.equals("-tsm")) {
				// toggle safe mode, do NOT use it unless you know what are you doing
				try {
					System.out.println("Toggle Safe Mode: " + cli.client.toggle_safemode());
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
			}
			if (o.flag.equals("-sap")) {
				// set attribution kv parameter
				if (sap_key == null || sap_value == null) {
					System.out.println("Please set sap_key and sap_value.");
					MetaStoreClient.__EXIT(0);
				}
				Database db;
				try {
					db = cli.client.get_local_attribution();
					Map<String, String> nmap = db.getParameters();
					nmap.put(sap_key, sap_value);
					db.setParameters(nmap);
					cli.client.alterDatabase(db.getName(), db);
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
			}
			if (o.flag.equals("-statfs3")){
				long end = 0;
				
				if ((dbName == null) ||
					((begin_time < 0 && end_time < 0) &&
					(statfs_range <= 0) &&
					(statfs2_bday <= 0 && statfs2_days <= 0))) {
					System.out.println("Please set -statfs_range or (-statfs2_bday and -statfs2_days) and -db.");
					MetaStoreClient.__EXIT(0);
				}
				
				if (statfs2_bday >= 0 && statfs2_days >= 0) {
					end_time = System.currentTimeMillis() / 1000;
					end = end_time / 3600 * 3600;
					end = end - statfs2_bday * 86400;
					begin_time = end - statfs2_days * 86400;
				} else if (statfs_range > 0) {
					end_time = System.currentTimeMillis() / 1000;
					// find a valid Hour start time
					end = end_time / 3600 * 3600;
					begin_time = end_time - statfs_range;
				} else {
					end = end_time / 3600 * 3600;
					begin_time = begin_time / 3600 * 3600;
				}
				
				try {
					List<String> tables;
					TreeMap<Long, Map<String, FileStat>> fmap = new TreeMap<Long, Map<String, FileStat>>();
					
					if (tableName == null) 
						tables = cli.client.getAllTables(dbName);
					else {
						tables = new ArrayList<String>();
						tables.add(tableName);
					}

					for (; end >= begin_time; end -= 3600) {
						List<SplitValue> lsv = new ArrayList<SplitValue>();
						System.out.println("Handling data begin @ " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(end * 1000)));
						
						for (String tbl : tables) {
							lsv.clear();
							Table t = cli.client.getTable(dbName, tbl);
							if (t.getFileSplitKeysSize() > 0) {
								int maxv = 0;
								List<PartitionInfo> allpis = PartitionFactory.PartitionInfo.getPartitionInfo(t.getFileSplitKeys());

								for (PartitionInfo pi : allpis) {
									if (maxv < pi.getP_version())
										maxv = pi.getP_version();
								}
								List<List<PartitionInfo>> vpis = new ArrayList<List<PartitionInfo>>();
								for (int i = 0; i <= maxv; i++) {
									List<PartitionInfo> lpi = new ArrayList<PartitionInfo>();
									vpis.add(lpi);
								}
								for (PartitionInfo pi : allpis) {
									vpis.get(pi.getP_version()).add(pi);
								}
								// ok, we get versioned PIs; for each version, we generate a LSV and call filterTable
								for (int i = 0; i <= maxv; i++) {
									// BUG: in our lv13 demo systems, versions leaks, so we have to ignore some nonexist versions
									if (vpis.get(i).size() <= 0) {
										System.out.println("Metadata corrupted, version " + i + " leaks for table " + tbl + ".");
										continue;
									}
									if (vpis.get(i).get(0).getP_type() != PartitionFactory.PartitionType.interval)
										continue;
									lsv.add(new SplitValue(vpis.get(i).get(0).getP_col(), 1, ((Long)end).toString(), vpis.get(i).get(0).getP_version()));
									lsv.add(new SplitValue(vpis.get(i).get(0).getP_col(), 1, ((Long)(end + Integer.parseInt(vpis.get(i).get(0).getArgs().get(1)) * 3600)).toString(), vpis.get(i).get(0).getP_version()));
									// call update map
									List<SFile> files = cli.client.filterTableFiles(dbName, tbl, lsv);
									System.out.println("Got Table " + tbl + " LSV: " + lsv + " Hit " + files.size());
									lsv.clear();
									statfs2_update_map(cli, fmap, files, statfs2_getlen);
									
									if (statfs2_xj) {
										lsv.add(new SplitValue(vpis.get(i).get(0).getP_col(), 1, ((Long)end).toString(), vpis.get(i).get(0).getP_version()));
										lsv.add(new SplitValue(vpis.get(i).get(0).getP_col(), 1, ((Long)(end + Integer.parseInt(vpis.get(i).get(0).getArgs().get(1)) * 3600 - 1)).toString(), vpis.get(i).get(0).getP_version()));
										// call update map
										files = cli.client.filterTableFiles(dbName, tbl, lsv);
										System.out.println("Got Table " + tbl + " LSV: " + lsv + " Hit " + files.size());
										lsv.clear();
										statfs2_update_map(cli, fmap, files, statfs2_getlen);
									}
								}
							}
						}
					}
					
					Long total_size = 0L;
					Map<String, Long> sizeMap = new TreeMap<String, Long>();
					Map<String, Long> fnrMap = new TreeMap<String, Long>();
					Map<String, List<Long>> fidMap = new TreeMap<String, List<Long>>();
					for (Long k : fmap.keySet()) {
						Map<String, FileStat> fsmap = fmap.get(k);
						System.out.print(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(k * 1000)) + "\t");
						for (Map.Entry<String, FileStat> e : fsmap.entrySet()) {
							System.out.print(e.getKey() + ":" + e.getValue().fids.size() + 
									":" + e.getValue().space + "; ");
							total_size += e.getValue().space;
							if (sizeMap.get(e.getKey()) == null) {
								sizeMap.put(e.getKey(), e.getValue().space);
								fnrMap.put(e.getKey(), (long)e.getValue().fids.size());
								List<Long> fids = new ArrayList<Long>();
								fids.addAll(e.getValue().fids);
								fidMap.put(e.getKey(), fids);
							} else {
								sizeMap.put(e.getKey(), sizeMap.get(e.getKey()) + e.getValue().space);
								fnrMap.put(e.getKey(), fnrMap.get(e.getKey()) + e.getValue().fids.size());
								fidMap.get(e.getKey()).addAll(e.getValue().fids);
							}
						}
						System.out.println();
					}
					for (Map.Entry<String, Long> e : sizeMap.entrySet()) {
						System.out.println("Table " + e.getKey() + " -> " + fnrMap.get(e.getKey()) + " " + e.getValue() + " KB");
					}
					System.out.println("Total Size " + total_size + " KB");
					
					String command = "ssh %s 'cd sotstore/dservice ; java -cp build/devmap.jar:build/iie.jar:lib/lucene-core-4.2.1.jar -Djava.library.path=build/ iie.metastore.LuceneStat %s %s'";
					TreeSet<Long> fs = new TreeSet<Long>();
					for (Map.Entry<String, List<Long>> e : fidMap.entrySet()) {
						long totalRecord = 0;
						long totalSize = 0;
						long fnrs = 0, freps = 0, ignore = 0;
						List<Long> dsizelist = new LinkedList<Long>();
						
						fs.clear();
						for (Long fid : e.getValue()) {
							SFile f = cli.client.get_file_by_id(fid);
							String result = "";
							boolean isCalc = false;
							
							freps = 0;
							fnrs++;
							fs.add(fid);
							List<Long> tr = new ArrayList<Long>(f.getLocationsSize());
							List<Long> ts = new ArrayList<Long>(f.getLocationsSize());
							if (f.getLocationsSize() > 0) {
								for (SFileLocation loc : f.getLocations()) {
									if (loc.getVisit_status() != MetaStoreConst.MFileLocationVisitStatus.ONLINE) {
										ignore++;
										continue;
									}
									freps++;
									result = runRemoteCmdWithResultVerbose(String.format(command, 
											loc.getNode_name(), loc.getDevid(), loc.getLocation()), false);

									if (!"".equals(result) && result.indexOf("$") >= 0){
										int start = result.indexOf("$");
										int stop = result.indexOf(")");
										result = result.substring(start+2,stop);
										String[] dres = result.split(",");
										long drecord = Long.parseLong(dres[0]);
										long dsize = Long.parseLong(dres[1]);
										if (!isCalc) {
											isCalc = true;
											totalRecord += drecord;
											totalSize += dsize;
										}
										//System.out.printf("Name:%d Records:%d Size:%.2f MB\n",fid,drecord,dsize);
										dsizelist.add(dsize);
										tr.add(drecord);
										ts.add(dsize);
									} else {
										dsizelist.add(0L);
										tr.add(0L);
										ts.add(0L);
									}
								}
							}
							long xtr = -1;
							for (int i = 0; i < freps; i++) {
								if (xtr < 0)
									xtr = tr.get(i);
								if (xtr != tr.get(i)) 
									System.out.println("Bad File fid=" + fid + " -> " + xtr + " vs " + tr.get(i));
							}
							System.out.format("\r%.2f %%", ((double)fnrs / e.getValue().size() * 100));
						}
						Collections.sort(dsizelist);
						double stdev = 0.0;
						double avg = totalSize / fnrs;
						for (Long l : dsizelist) {
							stdev += (l - avg) * (l - avg);
						}
						stdev /= (fnrs * dsizelist.size());
						stdev = Math.sqrt(stdev);
						System.out.println("Table " + e.getKey() + " -> FNR: " + fnrs + " FRep: " + freps + " Ignore: " + ignore +
								" TotalRecords: " + totalRecord + " TotalSize: " + (totalSize / 1024) + 
								" FMAX: " + (dsizelist.size() <= 0 ? 0 : (dsizelist.get(dsizelist.size() - 1) / 1024)) + 
								" FMIN: " + (dsizelist.size() <= 0 ? 0 : dsizelist.get(0) / 1024) + 
								" FAVG: " + (totalSize / 1024 / fnrs) +
								" STDEV: " + stdev + " KB.");
						System.out.println(fs);
					}
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
			}
			
			if (o.flag.equals("-flt")) {
				// filter table files
				if (dbName == null || tableName == null) {
					System.out.println("please set -db and -table");
					MetaStoreClient.__EXIT(0);
				}
				System.out.println("Version " + flt_version);
				List<SplitValue> values = new ArrayList<SplitValue>();
				if (flt_l1_key != null && flt_l1_value != null) {
					// split value into many sub values
					String[] l1vs = flt_l1_value.split(";");
					for (String vs : l1vs) {
						values.add(new SplitValue(flt_l1_key, 1, vs, flt_version));
					}
					if (flt_l2_key != null && flt_l2_value != null) {
						String[] l2vs = flt_l2_value.split(";");
						for (String vs : l2vs) {
							values.add(new SplitValue(flt_l2_key, 2, vs, flt_version));
						}
					}
				}
				try {
					long recordnr = 0, length = 0;
					List<SFile> files = cli.client.filterTableFiles(dbName, tableName, values);
					for (SFile f : files) {
						System.out.println("fid " + f.getFid() + " -> " + toStringSFile(f));
						recordnr += f.getRecord_nr();
						length += f.getLength();
					}
					System.out.println("Total " + files.size() + " file(s) listed, record # " + recordnr + ", length " + (length / 1000000.0) + "MB.");
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
			}
			if (o.flag.equals("-tct")) {
				// trunc table files
				if (dbName == null || tableName == null) {
					System.out.println("please set -db and -table.");
					MetaStoreClient.__EXIT(0);
				}
				try {
					cli.client.truncTableFiles(dbName, tableName);
					System.out.println("Begin backgroud table truncate now, please wait!");
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
			}
			if (o.flag.equals("-trunc")) {
				// trunc table files FAST
				if (dbName == null || tableName == null) {
					System.out.println("please set -db and -table");
					MetaStoreClient.__EXIT(0);
				}
				try {
					long size = 0, recordnr = 0, length = 0;
					boolean isWrapped = false, isNone = true;
					for (int i = 0; i < Integer.MAX_VALUE; i += 1000) {
						List<Long> files = cli.client.listTableFiles(dbName, tableName, i, i + 1000);
						if (files.size() > 0) {
							for (Long fid : files) {
								try {
									SFile f = cli.client.get_file_by_id(fid);
									if (f.getStore_status() != MetaStoreConst.MFileStoreStatus.RM_PHYSICAL) {
										recordnr += f.getRecord_nr();
										if (f.getLength() > 0)
											length += f.getLength();
										System.out.println("DEL fid " + fid);
										cli.client.rm_file_physical(f);
										isNone = false;
									} else {
										System.out.println("IGN fid " + fid);
									}
								} catch (FileOperationException foe) {
									// ignore it
								}
							}
						}
						size += files.size();
						if (files.size() == 0) {
							if (i != 0) {
								if (isWrapped && isNone)
									break;
								System.out.println("Wrap " + i + "," + isWrapped + "," + isNone);
								i = -1000;
								isWrapped = true;
								isNone = true;
								continue;
							} else {
								break;
							}
						}
					}
					System.out.println("Total " + size + " file(s) listed, record # (~=) " + recordnr + ", length (~=) " + (length / 1000000.0) + "MB.");
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
			}
			if (o.flag.equals("-lfd")) {
				// list files by digest
				System.out.println("Please use -lfd_digest to change digest string; -lfd_verbose to dump more file info.");
				try {
					long start = System.nanoTime();
					List<Long> files = cli.client.listFilesByDigest(digest);
					long stop = System.nanoTime();
					if (files.size() > 0) {
						long begin = System.nanoTime();
						for (Long fid : files) {
							SFile f = cli.client.get_file_by_id(fid);
							String line = "fid " + fid;
							if (lfd_verbose) {
								line += " -> " + toStringSFile(f);
							}
							System.out.println(line);
						}
						long end = System.nanoTime();
						System.out.println("--> Search by digest consumed " + (stop - start) / 1000.0 + " us.");
						System.out.println("--> Get " + files.size() + " files in " + (end - begin) / 1000.0 + " us, GPS is " + files.size() * 1000000000.0 / (end - begin));
					}
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
			}
			if (o.flag.equals("-lfdc")) {
				// list files by digest with concurrent mgets
				System.out.println("ONLY USED for TEST. Set lfdc_thread to thread number.");
				List<LFDThread> lfdts = new ArrayList<LFDThread>();
				for (int i = 0; i < lfdc_thread; i++) {
					MetaStoreClient tcli = null;
	    			
					if (xURI == null) {
						if (serverName == null)
							try {
								tcli = new MetaStoreClient();
							} catch (Exception e) {
								e.printStackTrace();
								MetaStoreClient.__EXIT(0);
							}
						else
							try {
								tcli = new MetaStoreClient(serverName, serverPort);
							} catch (Exception e) {
								e.printStackTrace();
								MetaStoreClient.__EXIT(0);
							}
					} else {
						try {
							tcli = new MetaStoreClient(xURI, "user", "passwd");
						} catch (Exception e) {
							e.printStackTrace();
							MetaStoreClient.__EXIT(0);
						}
					}
	    			lfdts.add(new LFDThread(tcli, digest));
				}
				for (LFDThread t : lfdts) {
					t.start();
				}
				for (LFDThread t : lfdts) {
					try {
						t.join();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				long tfnr = 0, tgps = 0;
				for (LFDThread t : lfdts) {
					tfnr += t.fnr;
					tgps += t.fnr / ((t.end - t.begin) / 1000000000.0);
				}
				System.out.println("LFDCON: thread " + lfdc_thread + " total got " + tfnr + 
						" files, total GPS " + tgps);
			}
			if (o.flag.equals("-ltg")) {
				// list table groups
				if (dbName == null || tableName == null) {
					System.out.println("please set -db and -table.");
					MetaStoreClient.__EXIT(0);
				}
				try {
					List<NodeGroup> ngs = cli.client.getTableNodeGroups(dbName, tableName);
					for (NodeGroup mng : ngs) {
						System.out.println("NG: " + mng.getNode_group_name() + " -> {");
						if (mng.getNodesSize() > 0) {
							for (Node n : mng.getNodes()) {
								System.out.println(" Node " + n.getNode_name());
							}
						}
					}
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
			}
			if (o.flag.equals("-alz")) {
				// analyze the system to find a set of files that should be physically removed
				if (dbName == null) {
					System.out.println("Please set -db.");
					MetaStoreClient.__EXIT(0);
				}
				TreeMap<Long, Map<String, FileStat>> fmap = new TreeMap<Long, Map<String, FileStat>>();
				try {
					List<String> tables = cli.client.getAllTables(dbName);
					for (String tbl : tables) {
						System.out.println("Handle table '" + tbl + "'");
						for (int i = 0; i < Integer.MAX_VALUE; i += 1000) {
							List<Long> files = cli.client.listTableFiles(dbName, tbl, i, i + 1000);
							if (files.size() > 0) {
								// insert it into hash table
								List<SFile> lf = cli.client.get_files_by_ids(files);
								statfs2_update_map(cli, fmap, lf, statfs2_getlen);
							} else {
								break;
							}
						}
					}
					// ok, dump the file stats
					for (Long k : fmap.keySet()) {
						Map<String, FileStat> fsmap = fmap.get(k);
						System.out.print(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(k * 1000)) + "\t");
						for (Map.Entry<String, FileStat> e : fsmap.entrySet()) {
							System.out.print(e.getKey() + ":" + e.getValue().fids.size() + 
									":" + e.getValue().space + "; ");
						}
						System.out.println();
					}
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (UnknownDBException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
			}
			if (o.flag.equals("-lst_test")) {
				// list table files
				if (dbName == null || tableName == null) {
					System.out.println("please set -db and -table.");
					MetaStoreClient.__EXIT(0);
				}
				try {
					Set<Long> fids = new TreeSet<Long>();
					boolean isWrapped = false, isNone = true;
					for (int i = 0; i < Integer.MAX_VALUE; i += 1000) {
						List<Long> files = cli.client.listTableFiles(dbName, tableName, i, i + 1000);
						if (files.size() > 0) {
							for (Long fid : files) {
								if (fids.add(fid))
									isNone = false;
							}
						}
						System.out.println("Got " + i + " -> " + files.size() + ": " + fids.size() + "," + isNone);
						if (files.size() == 0) {
							if (i != 0) {
								if (isWrapped && isNone)
									break;
								i = -1000;
								isWrapped = true;
								isNone = true;
								continue;
							} else
								break;
						}
					}
					System.out.println("Total Table FID SET size " + fids.size());
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
			}
			if (o.flag.equals("-lst")) {
				// list table files
				if (dbName == null || tableName == null) {
					System.out.println("please set -db and -table.");
					MetaStoreClient.__EXIT(0);
				}
				try {
					long size = 0, recordnr = 0, length = 0;
					for (int i = 0; i < Integer.MAX_VALUE; i += 1000) {
					List<Long> files = cli.client.listTableFiles(dbName, tableName, i, i + 1000);
						if (files.size() > 0) {
							for (Long fid : files) {
								SFile f = cli.client.get_file_by_id(fid);
								recordnr += f.getRecord_nr();
								if (f.getLength() > 0)
									length += f.getLength();
								else
									length += __get_file_length(cli, f);
								System.out.println("fid " + fid + " -> " + toStringSFile(f));
							}
						}
						size += files.size();
						if (files.size() == 0)
							break;
					}
					System.out.println("Total " + size + " file(s) listed, record # " + recordnr + ", length " + (length / 1000000.0) + "MB.");
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
			}
			if (o.flag.equals("-dabal")) {
				// data balance
				if (devid == null || balanceNum == 0l) {
					System.out.println("please set -db and -bdnu.");
					MetaStoreClient.__EXIT(0);
				}
				try {
					List<SFileLocation> locatsDel = new ArrayList<SFileLocation>();
					boolean isBreak = false;
					long maxFid = cli.client.getMaxFid();
					for (int i = 0; i < maxFid; i += 1000) {
						List<Long> ids = new ArrayList<Long>();
						for (long j = i; j < j + 1000; j++){
							ids.add(j);
						}
						List<SFile> files = cli.client.get_files_by_ids(ids);
						if (files.size() > 0) {
							for(SFile sf : files){
								List<SFileLocation> locations = sf.getLocations();
								if(locations.size()<2){
									continue;
								}else{								
									for(SFileLocation sl : locations) {
										if(sl.getDevid().equalsIgnoreCase(devid) && sl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE){
											balanceNum -= sf.getLength();
											locatsDel.add(sl);
											if(balanceNum <= 0){
												isBreak = true;
												break;
											}
										}else{
											continue;
										}
									}
								}
								if(isBreak){												
									break;
								}
							}
						}else{
							break;
						}					
					}
					for (SFileLocation sln : locatsDel){
//						cli.client.del_fileLocation(sln);
					}
					System.out.println("Delete  " + balanceNum + " M files on device " + devid);
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
			}
			if (o.flag.equals("-flctc")) {
				// create lots of files, touch it, close it to do pressure-test
				System.out.println("Please use -flctc_nr to set file numbers, default is " + flctc_nr + ".");
				List<SplitValue> values = new ArrayList<SplitValue>();
				DevMap dm = new DevMap();
				
				try {
					long begin = System.nanoTime();
					for (int i = 0; i < flctc_nr; i++) {
						file = cli.client.create_file(node, repnr, null, null, values);
						System.out.print("Create file: " + file.getFid());
						file.setDigest("MSTOOL_LARGE_SCALE_FILE_TEST");
						file.getLocations().get(0).setVisit_status(MetaStoreConst.MFileLocationVisitStatus.ONLINE); 
						String path = dm.getPath(file.getLocations().get(0).getDevid(), file.getLocations().get(0).getLocation());
						File nf = new File(path);
						nf.mkdirs();
						System.out.println(", write to location " + path + ", and close it");
						cli.client.close_file(file);
					}
					long end = System.nanoTime();
					System.out.println("--> Create " + flctc_nr + " files in " + (end - begin) / 1000 + " us, CPS is " + flctc_nr * 1000000000.0 / (end - begin));
				} catch (FileOperationException e) {
					e.printStackTrace();
				} catch (TException e) {
					e.printStackTrace();
				}
			}
			if (o.flag.equals("-fcrp")) {
				// create a new file by policy
				CreatePolicy cp = new CreatePolicy();
				cp.setOperation(CreateOperation.CREATE_NEW_RANDOM);
				
				try {
					List<SplitValue> values = new ArrayList<SplitValue>();
					Date d = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss").parse(o.opt);
					values.add(new SplitValue("c_fssj", 1, d.getTime() /1000 +"", 2));
					values.add(new SplitValue("c_fssj", 1, d.getTime() /1000 +3600 + "", 2));
					values.add(new SplitValue("c_ydz", 2, "8-7", 2));

					file = cli.client.create_file_by_policy(cp, 2, "db1", "t_dx_rz_qydx", values);
					System.out.println("Create file: " + toStringSFile(file));
				} catch (ParseException e) {
					e.printStackTrace();
					break;
				} catch (FileOperationException e) {
					e.printStackTrace();
				} catch (TException e) {
					e.printStackTrace();
				}
			}
			if (o.flag.equals("-cvt")) {
				// convert date to timestamp
				try {
					Date d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(o.opt);
					System.out.println(d.getTime() / 1000);
				} catch (ParseException e) {
					e.printStackTrace();
					break;
				}
			}
			if (o.flag.equals("-srep")) {
				// set file repnr
				if (srep_fid < 0 || srep_repnr <= 0) {
					System.out.println("Please set -srep_fid and -srep_repnr");
					MetaStoreClient.__EXIT(0);
				}
				try {
					cli.client.set_file_repnr(srep_fid, srep_repnr);
				} catch (FileOperationException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
			}
			if (o.flag.equals("-FSCK")) {
				// do file system checking
				if (fsck_begin < 0 || fsck_end < 0 || fsck_end < fsck_begin) {
					System.out.println("Please set -fsck_begin NR -fsck_end NR");
					MetaStoreClient.__EXIT(0);
				}
				List<Long> badfiles = new ArrayList<Long>();
				
				for (long i = fsck_begin; i < fsck_end; i++) {
					try {
						SFile f = cli.client.get_file_by_id(i);
						List<NodeGroup> ngs;
						String backupNodeName = null;
						
						if (f.getTableName() != null && !f.getTableName().equals("")) {
							ngs = cli.client.getTableNodeGroups(f.getDbName(), f.getTableName());
						} else {
							ngs = cli.client.listNodeGroups();
						}
						
						/* select a backup node */
						if (ngs.size() > 0) {
							for (Node n : ngs.get(0).getNodes()) {
								backupNodeName = n.getNode_name();
								break;
							}
						}
						
						if (f.getLocationsSize() > 0) {
							String[] md5s = new String[f.getLocationsSize()];
							int j = 0;
							
							for (SFileLocation sfl : f.getLocations()) {
								String n = sfl.getNode_name();
								if (n.contains(";"))
									n = sfl.getNode_name().split(";")[0];
								String cmd = "ssh " + (n.equals("") ? backupNodeName : n);
								String mp = cli.client.getMP(n, sfl.getDevid());
								cmd += " \"cd " + mp + "/" + sfl.getLocation() + "; find . -type f -exec md5sum {} + | awk '{print $1}' | sort | md5sum | awk '{print $1}';\"";
								//System.out.println(cmd);
								md5s[j] = runRemoteCmdWithResult(cmd);
								j++;
							}
							String lastmd5 = md5s[0];
							boolean isConsistent = true;
							for (j = 1; j < f.getLocationsSize(); j++) {
								if (f.getLocations().get(j).getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE) {
									if (!lastmd5.equalsIgnoreCase(md5s[j])) {
										isConsistent = false;
										break;
									}
								}
							}
							if (!isConsistent) {
								// dump all md5s
								j = 0;
								System.out.println("FID " + f.getFid());
								for (SFileLocation sfl : f.getLocations()) {
									System.out.println("SFL: " + sfl.getNode_name() + ":" + sfl.getDevid() + ":" + sfl.getLocation() + " -> SAVED{" + sfl.getDigest() + "} COMPUTED{" + md5s[j] + "}");
									j++;
								}
								badfiles.add(f.getFid());
							}
						}
					} catch (FileOperationException e) {
						// it is ok
					} catch (MetaException e) {
						// it is ok
					} catch (TException e) {
						e.printStackTrace();
						break;
					}
				}
				System.out.println("Total Scaned Files:" + (fsck_end - fsck_begin) + ", Bad Files: " + badfiles);
			}
			if (o.flag.equals("-fro")) {
				// reopen a file
				boolean ok = false;
				long fid = 0;
				
				try {
					fid = Long.parseLong(o.opt);
					ok = cli.client.reopen_file(fid);
					if (ok) {
						file = cli.client.get_file_by_id(fid);
						System.out.println("Reopen file: " + toStringSFile(file));
					} else {
						System.out.println("Reopen file failed.");
					}
				} catch (NumberFormatException e) {
					e.printStackTrace();
				} catch (FileOperationException e) {
					e.printStackTrace();
				} catch (MetaException e) {
					e.printStackTrace();
				} catch (TException e) {
					e.printStackTrace();
				}
			}
			if (o.flag.equals("-fcr")) {
				// create a new file and return the fid
				try {
					List<SplitValue> values = new ArrayList<SplitValue>();
					//values.add(new SplitValue("COOL_KEY_1", 1, "COOL_KEY_V1", 0));
					//values.add(new SplitValue("COOL_KEY_2", 2, "COOL_KEY_V2", 0));
					file = cli.client.create_file(node, repnr, null, null, values);
					System.out.println("Create file: " + toStringSFile(file));
				} catch (FileOperationException e) {
					e.printStackTrace();
				} catch (TException e) {
					e.printStackTrace();
				}
			}
			if (o.flag.equals("-frr")) {
				// read the file object
				try {
					file = cli.client.get_file_by_id(Long.parseLong(o.opt));
					if (file.getLocations() != null && file.getLocationsSize() > 0) {
						for (SFileLocation sfl : file.getLocations()) {
							System.out.println("SFL: node " + sfl.getNode_name() + ", dev " + sfl.getDevid() + ", loc " + sfl.getLocation());
						}
					}
					System.out.println("Read file: " + toStringSFile(file));
					// iterator on file locations
					if (file.getLocationsSize() > 0) {
						for (SFileLocation sfl : file.getLocations()) {
							String n = sfl.getNode_name();
							if (sfl.getNode_name().contains(";")) {
								n = sfl.getNode_name().split(";")[0];
							}
							String mp = cli.client.getMP(n, sfl.getDevid());
							System.out.println("ssh " + n + " ls -l " + mp + "/" + sfl.getLocation());
						}
					}
				} catch (NumberFormatException e) {
					e.printStackTrace();
				} catch (FileOperationException e) {
					e.printStackTrace();
				} catch (MetaException e) {
					e.printStackTrace();
				} catch (TException e) {
					e.printStackTrace();
				}
			}
			if (o.flag.equals("-fcl")) {
				// close the file
				try {
					file = cli.client.get_file_by_id(Long.parseLong(o.opt));
					file.setDigest("MSTOOL Digested!");
					file.getLocations().get(0).setVisit_status(MetaStoreConst.MFileLocationVisitStatus.ONLINE);
					cli.client.close_file(file);
					System.out.println("Close file: " + toStringSFile(file));
					DevMap dm = new DevMap();
					String path = dm.getPath(file.getLocations().get(0).getDevid(), file.getLocations().get(0).getLocation());
					System.out.println("File local location is : " + path);
					File nf = new File(path);
					nf.mkdirs();
				} catch (NumberFormatException e) {
					e.printStackTrace();
				} catch (FileOperationException e) {
					e.printStackTrace();
				} catch (MetaException e) {
					e.printStackTrace();
				} catch (TException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (o.flag.equals("-fcd")) {
				// delete the file
				try {
					file = cli.client.get_file_by_id(Long.parseLong(o.opt));
					cli.client.rm_file_physical(file);
				} catch (FileOperationException e) {
					e.printStackTrace();
				} catch (MetaException e) {
					e.printStackTrace();
				} catch (TException e) {
					e.printStackTrace();
				}
			}
			if (o.flag.equals("-f")) {
				// test file
				try {
					List<SplitValue> values = new ArrayList<SplitValue>();
					//values.add(new SplitValue("COOL_KEY_NAME", 1, "COOL_KEY_VALUE", 0));
					file = cli.client.create_file(node, repnr, null, null, values);
					System.out.println("Create file: " + toStringSFile(file));
					// write something here
					String filepath;
					DevMap dm = new DevMap();
					dm.refreshDevMap();
					DevStat ds;
					do {
						ds = dm.findDev(file.getLocations().get(0).getDevid());
						if (ds == null || ds.mount_point == null) {
							dm.refreshDevMap();
						} else 
							break;
					} while (true);
					filepath = ds.mount_point + "/" + file.getLocations().get(0).getLocation();
					System.out.println("Trying to write to file location: " + filepath);
					File newfile = new File(filepath + "/test_file");
					try {
						newfile.getParentFile().mkdirs();
						newfile.createNewFile();
						FileOutputStream out = new FileOutputStream(filepath + "/test_file");
						out.close();
					} catch (IOException e) {
						e.printStackTrace();
						cli.client.rm_file_physical(file);
						break;
					}
					file.setDigest("DIGESTED!");
					file.getLocations().get(0).setVisit_status(MetaStoreConst.MFileLocationVisitStatus.ONLINE);
					cli.client.online_filelocation(file);
					cli.client.close_file(file);
					System.out.println("Closed file: " + toStringSFile(file));
					r = cli.client.get_file_by_id(file.getFid());
					while (r.getStore_status() != MetaStoreConst.MFileStoreStatus.REPLICATED) {
						try {
							Thread.sleep(10000);
							r = cli.client.get_file_by_id(file.getFid());
						} catch (InterruptedException ex) {
							Thread.currentThread().interrupt();
						}
					}
					System.out.println("Read   file: " + toStringSFile(r));
					cli.client.rm_file_physical(r);
					System.out.println("DEL    file: " + r.getFid());
				} catch (FileOperationException e) {
					e.printStackTrace();
				} catch (TException e) {
					e.printStackTrace();
				}
			}
			if (o.flag.equals("-lt")) {
				// list all tables that match a pattern
				List<Database> dbs;
				String regex;
				
				if (o.opt == null)
					regex = ".*";
				else
					regex = o.opt;
				
				System.out.println("--List tables that match pattern: " + regex);
				
				try {
					dbs = cli.client.get_all_attributions();
					for (Database db : dbs) {
						List<String> tbnames = cli.client.getAllTables(db.getName());

						for (String tbname : tbnames) {
							String reg_dbname = db.getName() + "." + tbname;

							if (reg_dbname.matches(regex)) {
								System.out.println(reg_dbname);
							}
						}
					}
				} catch (MetaException e) {
					e.printStackTrace();
				} catch (TException e) {
					e.printStackTrace();
				}
			}
			if (o.flag.equals("-upnrs")) {
				// update SFile nrs
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
				DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				Date d;
				long od = -1, last_increate_update_ts = -1;
				boolean do_flush = false, do_increate_update = false;
				HashMap<Long, FileNRs> oldmap = null;
				HashMap<Long, FileNRs> workmap = new HashMap<Long, FileNRs>();

				while (true) {
					d = new Date(System.currentTimeMillis());
					if (d.getTime() > od) {
						od = d.getTime();
						do_flush = true;
						oldmap = workmap;
						workmap = new HashMap<Long, FileNRs>();
					}
					if (last_increate_update_ts < 0) 
						last_increate_update_ts = d.getTime();
					if (d.getTime() >= last_increate_update_ts + 60 * 1000) {
						do_increate_update = true;
					} else {
						do_increate_update = false;
					}
					System.out.println("Current TS is " + df2.format(d) + ", do_flush " + do_flush + 
							", do_increate_update " + do_increate_update);
					try {
						d = df.parse(df.format(d));
					} catch (ParseException e1) {
						e1.printStackTrace();
						break;
					}

					// Step 1: find latest 7 days' files
					try {
						// find all the databases
						List<Database> dbs = cli.client.get_all_attributions();

						for (Database db : dbs) {
							List<String> tbnames = cli.client.getAllTables(db.getName());

							for (String tbname : tbnames) {
								Table tab = cli.client.getTable(db.getName(), tbname);

								// find files that create in specified date
								for (int j = 0; j < upnr_days; j++) {
									for (int i = 0; i < 24; i++) {
										try {
											List<SFile> files = __get_table_file_by_date(cli, 
													tab, d.getTime() / 1000 + i * 3600 - (j * 86400));
											if (files != null) {
												for (SFile f : files) {
													FileNRs fnr = new FileNRs(f);

													if (f.getStore_status() == MetaStoreConst.MFileStoreStatus.REPLICATED ||
															f.getStore_status() == MetaStoreConst.MFileStoreStatus.CLOSED) {
														if (f.getLocationsSize() > 0) {
															for (SFileLocation sfl : f.getLocations()) {
																if (sfl.getUpdate_time() > fnr.ts)
																	fnr.ts = sfl.getUpdate_time();
															}
														}
													}
													workmap.put(f.getFid(), fnr);
												}
											}
										} catch (Exception e) {
											System.out.println("Table " + db.getName() + "." + tbname + " metadata corrupted?");
											e.printStackTrace();
										}
									}
								}
							}
						}
					} catch (MetaException e) {
						e.printStackTrace();
					} catch (TException e) {
						e.printStackTrace();
					}
					System.out.println("oldmap size=" + oldmap.size() + ", workmap size=" + workmap.size());
					if (do_flush) {
						for (Map.Entry<Long, FileNRs> entry : oldmap.entrySet()) {
							FileNRs fnr = workmap.get(entry.getKey());
							if (fnr != null) {
								// update it
								fnr.rec_nr = entry.getValue().rec_nr;
								fnr.all_rec_nr  = entry.getValue().all_rec_nr;
								fnr.length = entry.getValue().length;
								fnr.ts = entry.getValue().ts;
							}
						}
						do_flush = false;
					}
					// Step 2: compute the nrs, update it and record the update TS
					// Step 3: update these files' metadata, detect new modifications;
					for (Map.Entry<Long, FileNRs> entry : workmap.entrySet()) {
						if (isSFileUpdated(entry.getValue().file, entry.getValue().ts)) {
							// check if the file is valid
							if (entry.getValue().file.getStore_status() == MetaStoreConst.MFileStoreStatus.CLOSED ||
									entry.getValue().file.getStore_status() == MetaStoreConst.MFileStoreStatus.REPLICATED ||
									(entry.getValue().file.getStore_status() == MetaStoreConst.MFileStoreStatus.INCREATE &&
									do_increate_update)) {
								// ok to update FileNRs now
								FileNRs fnr = __compute_nrs(cli, entry.getValue().file);
								if (fnr.rec_nr >= 0)
									entry.getValue().rec_nr = fnr.rec_nr;
								if (fnr.all_rec_nr >= 0)
									entry.getValue().all_rec_nr = fnr.all_rec_nr;
								if (fnr.length >= 0)
									entry.getValue().length = fnr.length;
								entry.getValue().ts = fnr.ts;
								try {
									cli.client.update_sfile_nrs(entry.getValue().file.getFid(), 
											fnr.rec_nr, fnr.all_rec_nr, fnr.length);
								} catch (MetaException e) {
									e.printStackTrace();
								} catch (FileOperationException e) {
									e.printStackTrace();
								} catch (TException e) {
									e.printStackTrace();
								}
								System.out.println("Update FID " + entry.getKey() + ": rec_nr " + fnr.rec_nr + 
										" length " + fnr.length + 
										", OLD(" + entry.getValue().rec_nr + "," + entry.getValue().length + ")");
							}
						}
					}
					if (do_increate_update)
						last_increate_update_ts = d.getTime();
					// Step 4: mark the files that should be recompute, then goto step 2;
					try {
						Thread.sleep(60 * 1000);
					} catch (InterruptedException e2) {
						e2.printStackTrace();
					}
				}
			}
			if (o.flag.equals("-wait")) {
				// wait for migrating one device's content to other devices
				if (devid == null) {
					System.out.println("Please set -devid [-devtype 4/0/5/1]");
					MetaStoreClient.__EXIT(0);
				}
				// Step 1: offline the device to disable new files
				try {
					boolean br = cli.client.offlineDevice(devid);
					System.out.println("Offline Devicd '" + devid + "' done => " + br + ".");
					if (!br)
						MetaStoreClient.__EXIT(0);
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
				// Step 2: detect files on this device. classify by file status 
				//         {0: close it; 1: wait it; 2: check repnr and rereplicate it;}
				List<Long> fids = null;
				String[] devids = devid.split(",");
				try {
					fids = cli.client.listFilesByDevs(Arrays.asList(devids));
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
				if (fids != null && fids.size() > 0) {
					for (Long _fid : fids) {
						SFile f = null;
						try {
							f = cli.client.get_file_by_id(_fid);
							if (f != null) {
								switch (f.getStore_status()) {
								case MetaStoreConst.MFileStoreStatus.INCREATE:
									f.setDigest("DISK_FAILURE_HANDLE_DIGESTED");
									if (f.getLocationsSize() > 0)
										f.getLocations().get(0).setVisit_status(MetaStoreConst.MFileLocationVisitStatus.ONLINE);
									cli.client.close_file(f);
									break;
								case MetaStoreConst.MFileStoreStatus.CLOSED:
									break;
								case MetaStoreConst.MFileStoreStatus.REPLICATED:
									if (f.getRep_nr() == 1) {
										cli.client.set_file_repnr(_fid, 2);
									}
									if (f.getLocationsSize() == 1) {
										cli.client.replicate(_fid, devtype);
									}
									break;
								default:
									;
								}
							}
						} catch (FileOperationException e) {
							e.printStackTrace();
							break;
						} catch (MetaException e) {
							e.printStackTrace();
							break;
						} catch (TException e) {
							e.printStackTrace();
							break;
						}
						
					}
				}
				// Step 3: make sure all files are replicated to other devices
				if (fids != null && fids.size() > 0) {
					int round = 0;
					while (true) {
						try {
							Thread.sleep(10 * 1000);
						} catch (InterruptedException e1) {
						}
						int donenr = 0;
						System.out.println("-- Round " + (++round) + " --");
						for (Long _fid : fids) {
							try {
								SFile f = cli.client.get_file_by_id(_fid);
								switch (f.getStore_status()) {
								case MetaStoreConst.MFileStoreStatus.INCREATE:
									System.out.println("Close FID " + _fid + " failed?");
									break;
								case MetaStoreConst.MFileStoreStatus.CLOSED:
									System.out.println("Wait  FID " + _fid + " CLOSED.");
									break;
								case MetaStoreConst.MFileStoreStatus.REPLICATED:
									boolean isok = false;
									if (f.getLocations() != null) {
										for (SFileLocation sfl : f.getLocations()) {
											if (sfl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE &&
													!sfl.getDevid().equalsIgnoreCase(devid)) {
												donenr++;
												isok = true;
												break;
											}
										}
									}
									System.out.println("Repl  FID " + _fid + " " + isok);
									break;
								}
							} catch (FileOperationException e) {
								e.printStackTrace();
								break;
							} catch (MetaException e) {
								e.printStackTrace();
								break;
							} catch (TException e) {
								e.printStackTrace();
								break;
							}
						}
						if (fids.size() == donenr) {
							System.out.println("-" + fids.size() + "-> " + donenr + ", OK to do final check.");
							break;
						} else {
							System.out.println("-" + fids.size() + "-> " + donenr + ", wait other unreplicated files.");
						}
					}
				}
				// Step 3.X: final check for fid changes
				List<Long> fids2 = null;
				try {
					fids2 = cli.client.listFilesByDevs(Arrays.asList(devids));
					if (fids2 != null && fids != null) {
						if (fids.size() != fids2.size()) {
							// it is BAD
							System.out.println("New files might come in: old=" + 
									fids.size() + ", new=" + fids2.size());
							break;
						}
					}
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
				// Step 4: offline the device physically
				try {
					cli.client.setTimeout(600);
					cli.client.offlineDevicePhysically(devid);
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
				System.out.println("Offline Device '" + devid + "' done physically.");
				// Step 5: report done
				System.out.println("--> Successfully migrate and offline device " + devid);
				if (fids2 != null) {
					System.out.println("--> For your reference, related fids display as bellow:");
					System.out.println(fids2);
				}
			}
			if (o.flag.equals("-dfs")) {
				// device-file-stat
				HashMap<String, Set<Long>> dfs = new HashMap<String, Set<Long>>();
				
				try {
					List<Device> devs = cli.client.listDevice();
					if (devs != null) {
						for (Device n : devs) {
							List<String> nl = new ArrayList<String>();
							nl.add(n.getDevid());
							List<Long> fl = cli.client.listFilesByDevs(nl);
							if (fl != null) {
								for (Long fid : fl) {
									SFile f = cli.client.get_file_by_id(fid);
									if (f.getLocationsSize() > f.getRep_nr()) {
										Set<Long> s = dfs.get(
												DeviceInfo.getTypeStr(n.getProp()) + "." + n.getDevid());
										if (s == null) 
											s = new HashSet<Long>();
										s.add(fid);
										dfs.put(DeviceInfo.getTypeStr(n.getProp()) + "." +n.getDevid(), s);
									}
								}
							}
						}
					}
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
				for (Map.Entry<String, Set<Long>> e : dfs.entrySet()) {
					System.out.println("Device " + e.getKey() + "{");
					for (Long i : e.getValue()) {
						System.out.print(i + ",");
					}
					System.out.println("}");
				}
			}
			if (o.flag.equals("-dstat")) {
				// get disk status from GNI info, includes node activity, disk space usage,
				// system load, mount path, write and read err rates, 
			}
			if (o.flag.equals("-ftrace")) {
				// open file tracing: report how many files are openned, written, reopenned. 
				// report how they distributed.
			}
			if (o.flag.equals("-sysi")) {
				// system monitor info:
				// nodes, devs, on/off
				try {
					System.out.println(cli.client.getSysInfo());
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
			}
			if (o.flag.equals("-slsb")) {
				// TEST USE ONLY, set loadstatus bad
				if (o.opt == null) {
					System.out.println("-slsb FID");
					MetaStoreClient.__EXIT(0);
				}
				long fid;
				try {
					fid = Long.parseLong(o.opt);
					cli.client.set_loadstatus_bad(fid);
					System.out.println("Set LSB for FID " + fid + " done.");
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (Exception e) {
					e.printStackTrace();
					break;
				}
			}
			if (o.flag.equals("-lossrpt")) {
				// generate data lost report for specified devices
				if (o.opt == null) {
					System.out.println("Please set -lossrpt <DEVID,DEVID,...>");
					MetaStoreClient.__EXIT(0);
				}
				List<Long> fids = null;
				String[] devids = o.opt.split(",");
				
				if (devids.length > 0) {
					try {
						fids = cli.client.listFilesByDevs(Arrays.asList(devids));
					} catch (MetaException e) {
						e.printStackTrace();
						break;
					} catch (TException e) {
						e.printStackTrace();
						break;
					}
				}
				if (fids != null && fids.size() > 0) {
					Long[] fstat = new Long[MetaStoreConst.MFileStoreStatus.__MAX__];
					for (int i = 0; i < fstat.length; i++)
						fstat[i] = new Long(0);
					Set<String> devs = new HashSet<String>();
					devs.addAll(Arrays.asList(devids));
					List<Long> lostfids = new ArrayList<Long>();
					HashMap<String, Long> fnr = new HashMap<String, Long>();
					HashMap<String, Long> space = new HashMap<String, Long>();
					HashMap<String, Long> record = new HashMap<String, Long>();
					long lostspace = 0;
					long lostrec = 0;
					
					for (Long _fid : fids) {
						boolean islost = true;
						try {
							SFile f = cli.client.get_file_by_id(_fid);
							if (f == null)
								continue;
							fstat[f.getStore_status()]++;
							if (f.getStore_status() == MetaStoreConst.MFileStoreStatus.RM_PHYSICAL)
								continue;
							String key = f.getDbName() + "." + f.getTableName();
							if (f.getLocations() != null) {
								for (SFileLocation sfl : f.getLocations()) {
									if (!devs.contains(sfl.getDevid()) && 
											sfl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE) {
										// not lost
										islost = false;
										break;
									}
								}
							}
							if (islost) {
								lostfids.add(_fid);
								lostspace += f.getLength();
								lostrec += f.getRecord_nr();
								if (key != null) {
									Long l = fnr.get(key);
									if (l == null) {
										l = new Long(0);
									}
									l++;
									fnr.put(key, l);
									l = space.get(key);
									if (l == null) {
										l = new Long(0);
									}
									l += f.getLength();
									space.put(key, l);
									l = record.get(key);
									if (l == null) {
										l = new Long(0);
									}
									l += f.getRecord_nr();
									record.put(key, l);
								}
							}
						} catch (FileOperationException e) {
							e.printStackTrace();
							break;
						} catch (MetaException e) {
							e.printStackTrace();
							break;
						} catch (TException e) {
							e.printStackTrace();
							break;
						}
						
					}
					System.out.println("Lost Report for Devices: " + o.opt);
					System.out.println(" File Status Stats: ");
					for (int i = 0; i < fstat.length; i++) {
						System.out.println(" \t" + i + "\t" + fstat[i]);
					}
					System.out.println(" Total Lost Space  : " + lostspace + " B.");
					System.out.println(" Total Lost Records: " + lostrec);
					System.out.println(" Lost File by Tables: (fnr, space, record)");
					for (Map.Entry<String, Long> e : fnr.entrySet()) {
						System.out.println(" \t" + e.getKey() + "\t" + e.getValue() + "\t" + 
								space.get(e.getKey()) + "\t" + record.get(e.getKey()));
					}
					System.out.println(" Lost File IDs: ");
					System.out.println(lostfids);
				}
			}
			if (o.flag.equals("-tfil")) {
				// get time-file-info-list
				
			}
			if (o.flag.equals("-frpt")) {
				// file report
				if (o.opt == null) {
					System.out.println("Please provide -frpt DATA_STR(yyyy-MM-dd)");
					MetaStoreClient.__EXIT(0);
				}
				HashMap<String, List<SFile>> map = new HashMap<String, List<SFile>>();
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
				Date d;
				
				try {
					d = df.parse(o.opt);
				} catch (ParseException e1) {
					e1.printStackTrace();
					break;
				}
				try {
					// find all the databases;
					List<Database> dbs = cli.client.get_all_attributions();
					for (Database db : dbs) {
						List<String> tbnames = cli.client.getAllTables(db.getName());
						
						for (String tbname : tbnames) {
							Table tab = cli.client.getTable(db.getName(), tbname);
							List<SFile> tfiles = new ArrayList<SFile>();
							Long nhours = 24L, nhour = 0L;
							
							for (int i = 0; i < nhours; i++) {
								List<SFile> files = __get_table_file_by_date(cli, tab, d.getTime() / 1000 + 
										(i + nhour) * 3600);
								if (files != null)
									tfiles.addAll(files);
							}
							map.put(db.getName() + "." + tab.getTableName(), tfiles);
						}
					}
				} catch (MetaException e) {
					e.printStackTrace();
				} catch (TException e) {
					e.printStackTrace();
				}
				String result = "#db.table,file_nr,empty_file_nr,fs0,fs1,fs2,fs3,fs4,rec_nr,space,AVG_LEN,AVG_SPACE,AVG_SPACE_NE\n";
				HashMap<String, _FSNR> nrmap = new HashMap<String, _FSNR>();
				_FSNR _nr;
				
				for (Map.Entry<String, List<SFile>> e : map.entrySet()) {
					if (e.getValue() != null) {
						for (SFile f : e.getValue()) {
							_nr = nrmap.get(f.getDbName() + "." + f.getTableName());
							if (_nr == null)
								_nr = new _FSNR();
							_nr.s[f.getStore_status()]++;
							nrmap.put(f.getDbName() + "." + f.getTableName(), _nr);
						}
					}
				}
				// ok, with map, we can generate full report
				for (Map.Entry<String, List<SFile>> e : map.entrySet()) {
					if (e.getValue() != null) {
						long empty_file_nr = 0;
						long rec_nr = 0;
						long space = 0;
						double AVG_LEN = 0.0;
						double AVG_SPACE = 0.0, AVG_SPACE_NE = 0.0;
						
						for (SFile f : e.getValue()) {
							if (f.getRecord_nr() == 0)
								empty_file_nr++;
							rec_nr += f.getRecord_nr();
							space += f.getLength();
						}
						AVG_LEN = (double)space / rec_nr;
						AVG_SPACE = (double)space / e.getValue().size();
						AVG_SPACE_NE = (double)space / (e.getValue().size() - empty_file_nr);
						result += e.getKey() + "," +
									e.getValue().size() + "," +
									empty_file_nr + ",";
						_FSNR n = nrmap.get(e.getKey());
						for (int i = 0; i < 5; i++) {
							if (n != null)
								result += n.s[i] + ",";
							else
								result += "0,";
						}
						result += rec_nr + "," + 
									space + "," + 
									AVG_LEN + "," +
									AVG_SPACE + "," + 
									AVG_SPACE_NE + "\n";
					} else {
						// null list, all fields are zero
						result += e.getKey() + ",0,0,0,0,0,0,0,0,0,0,0,0\n";
					}
				}
				System.out.println(result);
			}
			if (o.flag.equals("-ds")) {
				// data search based on 'lucene searcher'
				// Args: -ds_field field_name
				//       -ds_field_type number or string
				//       -ds_field_args [number{num_range, all included} | string{lucene query string}]
				//       -ds_del truely delete
				//       -db -table -bdate -edate
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH");
				Date bd, ed;
				HashMap<Long, SFile> targets = new HashMap<Long, SFile>();
				
				if (dbName == null || tableName == null || bdate == null || 
						edate == null) {
					System.out.println("");
					MetaStoreClient.__EXIT(0);
				}
				// [bdate, edate)
				try {
					bd = df.parse(bdate);
					ed = df.parse(edate);
				} catch (ParseException e1) {
					e1.printStackTrace();
					break;
				}
				
				// find dates' files
				try {
					Table tab = cli.client.getTable(dbName, tableName);
					
					for (long i = bd.getTime() / 1000; i < ed.getTime() / 1000; i += 3600) {
						try {
							List<SFile> files = __get_table_file_by_date(cli,
									tab, i);
							if (files != null) {
								for (SFile f : files) {
									if (f.getStore_status() != MetaStoreConst.MFileStoreStatus.RM_PHYSICAL &&
											f.getLocationsSize() > 0) {
										targets.put(f.getFid(), f);
									}
								}
								System.out.println("Handle date " + df.format(new Date(i * 1000)) + " ... " + files.size());
							} else 
								System.out.println("Handle date " + df.format(new Date(i * 1000)) + " ... 0");
						} catch (Exception e) {
							System.out.println("Table " + dbName + "." + tableName + " metadata corrupted?");
							e.printStackTrace();
						}
					}
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (NoSuchObjectException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
				
				long thits = 0, tall = 0;
				// for each file, iterate on each SFL, construct ssh command, and execute them
				for (Map.Entry<Long, SFile> entry : targets.entrySet()) {
					if (entry.getValue().getLocationsSize() > 0) {
						boolean isCounted = false;
						
						for (SFileLocation sfl : entry.getValue().getLocations()) {
							String command = "", result = "";
							String n = sfl.getNode_name();
							
							if (n.contains(";"))
								n = sfl.getNode_name().split(";")[0];
							
							if (sfl.getVisit_status() != MetaStoreConst.MFileLocationVisitStatus.ONLINE)
								continue;
							try {
								String mp = cli.client.getMP(n, sfl.getDevid());

								command += "ssh %s 'cd sotstore/dservice ; " +
										"java -cp build/devmap.jar:build/iie.jar:" + 
										"lib/lucene-core-4.2.1.jar:" +
										"lib/lucene-queries-4.2.1.jar:" + 
										"lib/lucene-queryparser-4.2.1.jar:" + 
										"lib/lucene-analyzers-common-4.2.1.jar:" + 
										"lib/lucene-sandbox-4.2.1.jar " + 
										"-Djava.library.path=build/ " + 
										"iie.metastore.LuceneFileFilter %s -f %s -t %s -n %s -args \"%s\" %s %s'";
								
								if (ds_fargs == null && ds_ftype.equals("number")) {
									// auto generate ds timestamp
									ds_fargs = bd.getTime() / 1000 + "," + (ed.getTime() / 1000 - 1);
								}
								result = runRemoteCmdWithResultVerbose(String.format(command, 
										sfl.getNode_name(),
										(ds_del ? "-d" : ""),
										mp + sfl.getLocation(),
										ds_ftype,
										ds_fname,
										ds_fargs,
										ds_verbose ? "-v" : "",
										ds_df == null ? "" : ("-df " + ds_df)
										), false);
								
								if (!ds_verbose) {
									long hits = 0, total = 0;
									if (!"".equals(result) && result.startsWith("Query ")) {
										String[] dres = result.split(" ");
										if (dres.length > 5) {
											try {
												hits = Long.parseLong(dres[dres.length - 5]);
												total = Long.parseLong(dres[dres.length - 2]);
												if (!isCounted) {
													thits += hits;
													tall += total;
													isCounted = true;
												}
												System.out.println("-> hit " + hits + " records in " + total + " docs.");
											} catch (Exception e) {
											}
										}
									}
									
								} else 
									System.out.println(result);
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
				}
				System.out.println("->>> Total hit " + thits + " records in " + tall + " docs.");
			}
			if (o.flag.equals("-keep_frr")) {
				// keep reading the file object, ONLY for test
				for (int i = 0; i < Integer.MAX_VALUE; i++) {
					System.out.println("----> Round " + i);
					try {
						Thread.sleep(500);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
					try {
						file = cli.client.get_file_by_id(Long.parseLong(o.opt));
						if (file.getLocations() != null && file.getLocationsSize() > 0) {
							for (SFileLocation sfl : file.getLocations()) {
								System.out.println("SFL: node " + sfl.getNode_name() + ", dev " + sfl.getDevid() + ", loc " + sfl.getLocation());
							}
						}
						System.out.println("Read file: " + toStringSFile(file));
						// iterator on file locations
						if (file.getLocationsSize() > 0) {
							for (SFileLocation sfl : file.getLocations()) {
								String n = sfl.getNode_name();
								if (sfl.getNode_name().contains(";")) {
									n = sfl.getNode_name().split(";")[0];
								}
								String mp = cli.client.getMP(n, sfl.getDevid());
								System.out.println("ssh " + n + " ls -l " + mp + "/" + sfl.getLocation());
							}
						}
					} catch (NumberFormatException e) {
						e.printStackTrace();
					} catch (FileOperationException e) {
						e.printStackTrace();
					} catch (MetaException e) {
						e.printStackTrace();
					} catch (TException e) {
						e.printStackTrace();
					}
				}
			}
			if (o.flag.equals("-r2o")) {
				// sync redis file info to oracle
				ObjectStore ob = new ObjectStore();
				ob.setConf(new HiveConf());;
				
				if (o.opt == null) {
					System.out.println("-r2o FID_LIST{id1,id2,id3,.... OR [id1:idN]}");
					MetaStoreClient.__EXIT(0);
				}
				long bid = -1, eid = -1;
				ArrayList<Long> ids = new ArrayList<Long>();
				if (o.opt.contains(":")) {
					String[] x = o.opt.split(":");
					if (x.length >= 2) {
						bid = Long.parseLong(x[0]);
						eid = Long.parseLong(x[1]);
					}
					if (eid >= bid && bid >= 0) {
						for (long j = bid; j <= eid; j++) {
							ids.add(j);
						}
					}
				} else if (o.opt.contains(",")) {
					String[] y = o.opt.split(",");
					for (int j = 0; j < y.length; j++) {
						ids.add(Long.parseLong(y[j]));
					}
				}
				if (ids.size() > 0) {
					for (Long fid : ids) {
						try {
							SFile f = cli.client.get_file_by_id(fid);
							if (f == null) {
								System.out.println("Ignore FID " + fid + " for null");
								continue;
							}
							try {
								ob.updateSFile(f);
								List<SFileLocation> oldsfls = ob.getSFileLocations(f.getFid());
								if (oldsfls != null) {
									for (SFileLocation sfl : oldsfls) {
										ob.delSFileLocation(sfl.getDevid(), sfl.getLocation());
									}
								}
								for (SFileLocation sfl : f.getLocations()) {
									ob.createFileLocation(sfl);
								}
							} catch (NullPointerException npe) {
								ob.persistFile(f);
								for (SFileLocation sfl : f.getLocations()) {
									ob.createFileLocaiton(sfl);
								}
							}
						} catch (FileOperationException foe) {
							if (foe.getReason() == FOFailReason.INVALID_FILE) {
								// file not found, just clean oracle file (if exists)
								try {
									ob.delSFile(fid); // location leaks
								} catch (MetaException e) {
									e.printStackTrace();
								}
							}
						} catch (TException te) {
							te.printStackTrace();
						}
					}
				}
				ob.shutdown();
			}
	    }
	    if (cli != null)
	    	cli.stop();
	    quit();
	}
}
