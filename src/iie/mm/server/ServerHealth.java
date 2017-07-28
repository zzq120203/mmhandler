package iie.mm.server;

import iie.mm.common.RedisPool;
import iie.mm.common.RedisPoolSelector.RedisConnection;
import org.fusesource.lmdbjni.LMDBException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.exceptions.JedisException;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

/**
 * ServerHealth contains the following module:
 *
 * 0. monitor info memory used_memory to trigger auto SS migration 1. check and
 * clean mm.dedup.info; 2. check and fix under/over replicated objects; 3. auto
 * clean block file if there are on reference on it;
 *
 * @author macan
 *
 */
public class ServerHealth extends TimerTask {
	private ServerConf conf;
	private long lastFetch = System.currentTimeMillis();
	private long lastScrub = System.currentTimeMillis();
	private long used_memory = 0;
	private boolean isMigrating = false;
	private boolean isCleaningDI = false;
	private boolean isFixingObj = false;
	private boolean doFetch = false;
	private int nhours = 1;
	private static long CLEAN_ITER_BASE = 50000;
	private long cleanIter = CLEAN_ITER_BASE;

	public static class SetInfo {
		long usedBlocks;
		long totalBlocks;
		long usedLength;
		long totalLength;
		int siteNode;

		public SetInfo() {
			usedBlocks = 0;
			totalBlocks = 0;
			usedLength = 0;
			totalLength = 0;
			siteNode = 0;
		}
	}

	public static Map<String, SetInfo> setInfos = new HashMap<String, SetInfo>();

	public ServerHealth(ServerConf conf) {
		super();
		this.conf = conf;
	}

	private void __do_migrate(String s) {
		if (!isMigrating && conf.isEnableSSMig()) {
			isMigrating = true;
			int err = SSMigrate(s + " [SSMigrate]");
			if (err < 0) {
				System.out.println(s + " migrate to SS failed w/ " + err);
			} else {
				System.out.println(s + " migrate to SS " + err + " entries.");
			}
			isMigrating = false;
		}
	}

	private void __do_clean(String s) throws Exception {
		if (!isCleaningDI) {
			isCleaningDI = true;
			// NOTE: user can set cleanDedupInfo arg(iter) here
			int err = cleanDedupInfo(s + " [cleanDedupInfo]", cleanIter);
			if (err < 0) {
				System.out.println(s + " clean dedupinfo failed w/ " + err);
			} else if (err == 0) {
				cleanIter += CLEAN_ITER_BASE;
				if (cleanIter > CLEAN_ITER_BASE * 20)
					cleanIter -= CLEAN_ITER_BASE;
				System.out.println(s + " clean dedupinfo ZERO, adjust iter to " + cleanIter);
			} else {
				System.out.println(s + " clean dedupinfo " + err + " entries.");
			}
			isCleaningDI = false;
		}
	}

	@Override
	public void run() {
		try {
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String s = df.format(new Date());

			// fetch used_memory every run
			long cur = System.currentTimeMillis();

			if (cur - lastFetch >= conf.getMemCheckInterval()) {
				Jedis jedis = null;
				try {
					jedis = StorePhoto.getRpL1(conf).getResource();
					if (jedis == null)
						System.out.println(s + " get redis connection failed.");
					else {
						String info = jedis.info("memory");

						if (info != null) {
							String lines[] = info.split("\r\n");

							if (lines.length >= 9) {
								String used_mem[] = lines[1].split(":");

								if (used_mem.length == 2 && used_mem[0].equalsIgnoreCase("used_memory")) {
									used_memory = Long.parseLong(used_mem[1]);
								}
							}
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					StorePhoto.getRpL1(conf).putInstance(jedis);
				}
				lastFetch = cur;
				doFetch = true;
			}
			if (conf.isSSMaster() && used_memory > conf.getMemorySize() * conf.getMemFullRatio()) {
				System.out.println(s + " detect L1 pool used_memory=" + used_memory + " > " + conf.getMemFullRatio()
						+ "*" + conf.getMemorySize());
				updateHeat(true);
			}
			if (conf.isSSMaster() && doFetch) {
				doFetch = false;
				for (Map.Entry<String, RedisPool> entry : StorePhoto.getRPS(conf).getRpL2().entrySet()) {
					long umem = 0;

					// get info memory
					Jedis jedis = null;
					try {
						jedis = entry.getValue().getResource();
						if (jedis != null) {
							String info = jedis.info("memory");

							if (info != null) {
								String lines[] = info.split("\r\n");

								if (lines.length >= 9) {
									String used_mem[] = lines[1].split(":");

									if (used_mem.length == 2 && used_mem[0].equalsIgnoreCase("used_memory")) {
										umem = Long.parseLong(used_mem[1]);
									}
								}
							}
						}
//					} finally {
//						entry.getValue().putInstance(jedis);
//					}
						// check ration and call migrate/clean
						if (umem > conf.getMemorySize() * conf.getMemFullRatio()) {
							System.out.println(s + " detect L2 pool " + entry.getKey() + " used_memory=" + umem + " > "
									+ conf.getMemFullRatio() + "*" + conf.getMemorySize());
							// __do_migrate(s);
							// __do_clean(s);
							__swapOut(jedis);
							if (!isFixingObj) {
								isFixingObj = true;
								isFixingObj = false;
							}
						}
					} finally {
						entry.getValue().putInstance(jedis);
					}
				}
			}
			//
			if (cur - lastScrub >= conf.getSpaceOperationInterval()) {
				scrubSets();
				gatherSpaceInfo(conf);
				lastScrub = cur;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private Set<String> updateHeat(boolean doUp) {
		// TODO Auto-generated method stub
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String s = df.format(new Date());
		Jedis jedis = null;
		Set<String> temp = new HashSet<String>();
		try {
			jedis = StorePhoto.getRpL1(conf).getResource();
			if (jedis == null) {
				System.out.println(s + " get redis connection failed.");
			} else {
				int heat = Integer.parseInt(jedis.hget("mm.client.conf", "heat"));
				Set<String> hData = jedis.keys("h.*");
				if(!doUp) {
					for (String setmd5 : hData) {
						temp.add(setmd5.substring(2));
					}
					return temp;
				}
				for (String setmd5 : hData) {
					long t = jedis.ttl(setmd5);
					if (t < (heat / 2)){
						jedis.del(setmd5);
					} else
						jedis.expire(setmd5,(int) t / 2);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			StorePhoto.getRpL1(conf).putInstance(jedis);
		}
		return null;
	}

	private int getRocksid(String set) {
		Jedis jedis = null;
		try {
			jedis = StorePhoto.getRpL1(conf).getResource();
			String rocksid = jedis.hget("rocksid", set);
			if (rocksid != null) {
				int dn = Integer.parseInt(rocksid);
				return dn;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			StorePhoto.getRpL1(conf).putInstance(jedis);
		}
		return 1;
	}

	private void __swapOut(Jedis jedis) {
		if (!isMigrating && conf.isEnableSSMig()) {
			isMigrating = true;

			int total = 0;
			// TODO
			try {
				Set<String> keys = jedis.keys("*.blk.*");
				Set<String> temp = new HashSet<String>();
				Random random = new Random();
				for (String ss : keys) {
					temp.add(ss.split("\\.")[0]);
				}
				Set<String> heat = updateHeat(false);
				for (String set : temp) {
					if (SwapOut.getSS().containsKey(set))
						continue;
					for (Map.Entry<String, String> metadata : jedis.hgetAll(set).entrySet()) {
						String key = metadata.getKey();
						String info = metadata.getValue();
                        if (heat != null && heat.contains(set + "@" + key)) continue;
                        if ('#'==info.charAt(1))
							if ("1".equals(info.split("#")[0])) {
								if (random.nextInt(100) < conf.getMigWeight()) {
									jedis.hdel(set, key);
									total++;
								}
							} else if ("2".equals(info.split("#")[0])) {
								int rocksid = getRocksid(set);
								RocksDBInterface.getRocks(rocksid).delete(key);
								jedis.hdel(set, key);
							}
					}

				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			isMigrating = false;
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String s = df.format(new Date());
			System.out.println(s +" set migrate total: " + total + " DEL : " + conf.getMigWeight() + "%");
		}

	}

	/**
	 * do set migrate, return NR of this set
	 *
	 * @param set
	 * @return
	 * @throws Exception
	 */
	private int migrateSet(String set) throws Exception {
		Jedis jedis = StorePhoto.getRpL1(conf).getResource();
		int nr = 0;

		if (jedis == null) {
			throw new Exception("Could not get avaliable Jedis instance.");
		}

		try {
			// Save all the server info for this set
			Iterator<String> ir = jedis.smembers(set + ".srvs").iterator();
			String info;
			int idx = 0;

			while (ir.hasNext()) {
				info = ir.next();
				LMDBInterface.getLmdb().write("S#" + set + ".srvs$", info);
				idx++;
			}
			if (idx > 0)
				LMDBInterface.getLmdb().write("S#" + set + ".srvs~size", "" + idx);
			jedis.del(set + ".srvs");

			// Save all the block info for this set
			Iterator<String> ikeys1 = jedis.keys(set + ".blk.*").iterator();

			while (ikeys1.hasNext()) {
				String key1 = ikeys1.next();
				try {
					String value = jedis.get(key1);
					LMDBInterface.getLmdb().write("B#" + key1, value);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			ikeys1 = jedis.keys(set + ".blk.*").iterator();
			Pipeline pipeline1 = jedis.pipelined();

			while (ikeys1.hasNext()) {
				String key1 = ikeys1.next();
				pipeline1.del(key1);
			}
			pipeline1.sync();

			// Save the entries of this set
			Map<String, String> hall = jedis.hgetAll(set);
			nr = hall.size();
			for (Map.Entry<String, String> entry : hall.entrySet()) {
				LMDBInterface.getLmdb().write("H#" + set + "." + entry.getKey(), entry.getValue());
			}
			jedis.del(set);
		} catch (LMDBException le) {
			System.out.println("LMDBException: " + le.getLocalizedMessage());
		} catch (JedisException je) {
			je.printStackTrace();
		} finally {
			StorePhoto.getRpL1(conf).putInstance(jedis);
		}

		return nr;
	}

	/**
	 * recycleSet() cleans < ckpt_ts sets (if existed)
	 */
	private int recycleSet(int up2xhour) throws Exception {
		DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Jedis jedis = null;
		long ckpt_ts = ServerConf.getCkpt_ts();
		int nr = 0;

		// get all sets
		TreeSet<String> temp = new TreeSet<String>();
		for (Map.Entry<String, RedisPool> entry : StorePhoto.getRPS(conf).getRpL2().entrySet()) {
			jedis = entry.getValue().getResource();
			if (jedis != null) {
				try {
					Set<String> keys = jedis.keys("*.blk.*");

					if (keys != null && keys.size() > 0) {
						for (String k : keys) {
							String set = k.split("\\.")[0];

							if (Character.isDigit(set.charAt(0))) {
								temp.add(set);
							} else {
								temp.add(set.substring(1));
							}
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					entry.getValue().putInstance(jedis);
				}
			}
		}

		jedis = StorePhoto.getRpL1(conf).getResource();
		if (jedis == null) {
			System.out.println("get L1 redis pool failed");
		} else {
			try {
				String[] prefixs = new String[] { "", "i", "t", "a", "v", "o", "s" };

				for (String set : temp) {
					try {
						long thistime = Long.parseLong(set);

						if (ckpt_ts < 0)
							ckpt_ts = thistime;

						if (ckpt_ts + up2xhour * 3600 >= thistime) {
							// ok, delete it
							System.out.println("Migrate Set:\t" + set + "\t" + df2.format(new Date(thistime * 1000)));

							for (String prefix : prefixs) {
								try {
									System.out.println("\t" + prefix + set + "\t" + migrateSet(prefix + set));

								} catch (Exception e) {
									System.out.println("MEE: on set " + prefix + set + "," + e.getMessage());
								}
							}
							// ok, update ckpt timestamp: if > ts, check redis,
							// else
							// check lmdb.
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
							if (this_ts > saved_ts) {
								jedis.set("mm.ckpt.ts", set);
								ServerConf.setCkpt_ts(this_ts);
							}
							System.out.println("saved=" + saved_ts + ", this=" + this_ts);
							nr++;
						}
					} catch (NumberFormatException nfe) {
						System.out.println("NFE: on set " + set);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				temp.clear();
			} finally {
				StorePhoto.getRpL1(conf).putInstance(jedis);
			}
		}

		return nr;
	}

	private int SSMigrate(String lh) {
		String line = lh;
		int nr, err = 0;

		if (ServerConf.getCkpt_ts() >= 0)
			line += " try to migrate sets that <= " + (nhours * 3600 + ServerConf.getCkpt_ts());
		else
			line += " try to migrate sets from ZERO";

		try {
			nr = recycleSet(nhours);
			if (nr <= 0)
				nhours += 2;
			else
				nhours = 1;
			line += ", mignr = " + nr + " sets.";
		} catch (Exception e) {
			e.printStackTrace();
			err = -1;
		}

		System.out.println(line);

		return err;
	}

	private int cleanDedupInfo(String lh, long xiter) throws Exception {
		int kdays = conf.getDi_keep_days();
		int deleted = 0;
		long cday = System.currentTimeMillis() / 86400000 * 86400;
		long iter = 300000, j = 0, bTs;
		Jedis jedis = null;

		if (xiter > 0)
			iter = xiter;
		bTs = cday - (kdays * 86400);
		System.out.println(
				lh + " keep day time is " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(bTs * 1000)));

		for (Map.Entry<String, RedisPool> rp : StorePhoto.getRPS(conf).getRpL2().entrySet()) {
			jedis = rp.getValue().getResource();
			try {
				if (jedis == null) {
					System.out.println(lh + " get jedis connection failed.");
					deleted--;
				} else {
					Map<String, String> infos = new HashMap<String, String>();
					ScanParams sp = new ScanParams();
					sp.match("*");
					boolean isDone = false;
					String cursor = ScanParams.SCAN_POINTER_START;

					while (!isDone) {
						ScanResult<Entry<String, String>> r = jedis.hscan("mm.dedup.info", cursor, sp);
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
									if (conf.isVerbose(1))
										System.out.println("Ignore set '" + k[0] + "'.");
								}
							}
							if (ts >= 0 && ts < bTs) {
								jedis.hdel("mm.dedup.info", entry.getKey());
								deleted++;
							}
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				rp.getValue().putInstance(jedis);
			}
		}

		return deleted;
	}

	public int gatherSpaceInfo(ServerConf conf) throws Exception {
		Jedis jedis = null;
		int err = 0;

		try {
			jedis = StorePhoto.getRpL1(conf).getResource();
			if (jedis == null)
				return -1;
			Pipeline p = jedis.pipelined();

			for (String d : conf.getStoreArray()) {
				File f = new File(d);
				if (err == 0) {
					p.hset("mm.space", ServerConf.serverId + "|" + d + "|T", "" + f.getTotalSpace());
					p.hset("mm.space", ServerConf.serverId + "|" + d + "|F", "" + f.getUsableSpace());
				}
			}
			p.sync();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			StorePhoto.getRpL1(conf).putInstance(jedis);
		}

		return err;
	}

	private int scrubSets() throws Exception {
		TreeSet<String> sets = new TreeSet<String>();
		int err = 0;

		// get all sets
		for (Map.Entry<String, RedisPool> entry : StorePhoto.getRPS(conf).getRpL2().entrySet()) {
			Jedis jedis = entry.getValue().getResource();
			if (jedis != null) {
				try {
					Set<String> keys = jedis.keys("*.blk.*");

					if (keys != null && keys.size() > 0) {
						for (String k : keys) {
							String set = k.split("\\.")[0];

							sets.add(set);
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
					sets.clear();
				} finally {
					entry.getValue().putInstance(jedis);
				}
			}
		}

		// scrub for each set
		for (String set : sets) {
			err = scrubSetData(set);
		}

		return err;
	}

	private class BlockRef {
		public long ref;
		public long len;

		public BlockRef() {
			ref = 0;
			len = 0;
		}

		public void updateRef(long refDelta, long lenDelta) {
			ref += refDelta;
			len += lenDelta;
		}
	}

	private int scrubSetData(String set) throws Exception {
		RedisConnection rc = null;
		int err = 0;

		try {
			rc = StorePhoto.getRPS(conf).getL2(set, false);
			if (rc.rp == null || rc.jedis == null) {
				System.out.println("get L2 pool " + rc.id + " : conn failed for " + set + " scrub.");
			} else {
				Jedis jedis = rc.jedis;
				// Map: disk.block -> refcount
				Map<String, BlockRef> br = new HashMap<String, BlockRef>();
				Map<String, Long> blockId = new HashMap<String, Long>();
				Set<String> disks = new TreeSet<String>();
				ScanParams sp = new ScanParams();
				boolean isDone = false;
				String cursor = ScanParams.SCAN_POINTER_START;

				// BUG-XXX: if we get _blk max after hscan, then we might delete
				// a block
				// file false positive.
				for (String d : conf.getStoreArray()) {
					String _blk = jedis.get(set + ".blk." + ServerConf.serverId + "." + d);
					if (_blk != null)
						blockId.put(d, Long.parseLong(_blk));
				}

				sp.match("*");
				while (!isDone) {
					ScanResult<Entry<String, String>> r = jedis.hscan(set, cursor, sp);

					for (Entry<String, String> entry : r.getResult()) {
						// parse value into hashmap
						String[] infos = entry.getValue().split("#");

						if (infos != null) {
							for (int i = 0; i < infos.length; i++) {
								String[] vf = infos[i].split("@");
								String disk = null, block = null, serverId = null;
								BlockRef ref = null;
								long len = 0;

								if (vf != null) {
									for (int j = 0; j < vf.length; j++) {
										switch (j) {
										case 1:
											break;
										case 2:
											// serverId
											serverId = vf[j];
											break;
										case 3:
											// blockId
											block = vf[j];
											break;
										case 5:
											// length
											try {
												len = Long.parseLong(vf[j]);
											} catch (Exception nfe) {
											}
											break;
										case 6:
											// diskId
											disk = vf[j];
											break;
										}
									}
									try {
										if (serverId != null && disk != null && block != null
												&& Long.parseLong(serverId) == ServerConf.serverId) {
											ref = br.get(disk + "." + block);
											if (ref == null) {
												ref = new BlockRef();
											}
											ref.updateRef(1, len);
											br.put(disk + "." + block, ref);
											if (!disks.contains(disk))
												disks.add(disk);
										}
									} catch (Exception nfe) {
									}
								}
							}
						}
					}
					cursor = r.getStringCursor();
					if (cursor.equalsIgnoreCase("0")) {
						isDone = true;
					}
				}

				if (!conf.getStoreArray().containsAll(disks)) {
					System.out.println("Got disks set isn't filled in configed store array (" + disks + " vs "
							+ conf.getStoreArray() + "), retain it!");
					disks.retainAll(conf.getStoreArray());
				}
				// if disks is empty set, we know that all refs are freed, thus
				// we can
				// put StoreArray to disks to free block files
				if (disks.isEmpty())
					disks.addAll(conf.getStoreArray());
				for (String d : disks) {
					long blockMax = 0;
					long usedBlocks = 0;
					long usedLength = 0, totalLength = 0;

					// find the max block id for this disk
					if (blockId.containsKey(d))
						blockMax = blockId.get(d);
					for (long i = 0; i <= blockMax; i++) {
						if (br.get(d + "." + i) == null) {
							if (fexist(d + "/" + conf.destRoot + set + "/b" + i)) {
								if (i < blockMax) {
									if (conf.isVerbose(2))
										System.out.println("Clean block file b" + i + " in disk [" + d + "] set [" + set
												+ "] ...");
									delFile(new File(d + "/" + conf.destRoot + set + "/b" + i));
								} else {
									totalLength += statFile(new File(d + "/" + conf.destRoot + set + "/b" + i));
								}
							}
						} else {
							usedBlocks++;
							if (fexist(d + "/" + conf.destRoot + set + "/b" + i)) {
								totalLength += statFile(new File(d + "/" + conf.destRoot + set + "/b" + i));
								usedLength += br.get(d + "." + i).len;
								if (conf.isVerbose(3))
									System.out.println("Used  block file b" + i + " in disk [" + d + "] set [" + set
											+ "], ref=" + br.get(d + "." + i).ref);
							} else {
								System.out.println("Set [" + String.format("%8s", set) + "] in disk [" + d
										+ "] used but not exist(blk=" + i + ",ref=" + br.get(d + "." + i).ref + ").");
							}
						}
					}
					SetInfo si = setInfos.get(set);
					if (si == null) {
						si = new SetInfo();
					}
					si.usedBlocks = usedBlocks;
					si.totalBlocks = blockMax + 1;
					si.usedLength = usedLength;
					si.totalLength = totalLength;
					si.siteNode = Integer.parseInt(rc.id);
					setInfos.put(set, si);
					if (conf.isVerbose(1))
						System.out.println("Set [" + String.format("%8s", set) + "] in disk [" + d + "] u/t="
								+ usedBlocks + "/" + (blockMax + 1) + " B.UR="
								+ String.format("%.4f", (blockMax == 0 ? 0 : (double) usedBlocks / blockMax)) + " P.UR="
								+ String.format("%.4f", (totalLength == 0 ? 0 : (double) usedLength / totalLength)));
				}
			}
		} catch (JedisException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			StorePhoto.getRPS(conf).putL2(rc);
		}

		return err;
	}

	private boolean fexist(String fpath) {
		return new File(fpath).exists();
	}

	private long statFile(File f) {
		if (!f.exists())
			return 0;
		if (f.isFile())
			return f.length();

		return 0;
	}

	private void delFile(File f) {
		if (!f.exists())
			return;
		if (f.isFile())
			f.delete();
		else {
			for (File a : f.listFiles())
				if (a.isFile())
					a.delete();
				else
					delFile(a);
			f.delete();
		}
	}

}
