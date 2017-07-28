package iie.mm.server;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.exceptions.JedisException;

import java.io.*;
import java.net.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;
import java.util.TimerTask;

public class ProfileTimerTask extends TimerTask {
    private ServerConf conf;
    public int period;
    private double lastWn = 0;
    private double lastRn = 0;
    private long lastDl = 0;
    private long lastDnr = 0;
    private long lastTs = System.currentTimeMillis();
    private long lastRecycleTs = System.currentTimeMillis();
    private String profileDir = "log/";
    private String hbkey;
    private DatagramSocket client = null;
    private PubSubThread pst = null;

    public class PubSubThread extends Thread {
        private MMSPubSub mps;

        public PubSubThread(MMSPubSub mps) {
            this.mps = mps;
        }

        public void run() {
            Jedis jedis = StorePhoto.getRpL1(conf).getResource();
            if (jedis != null) {
                try {
                    jedis.psubscribe(mps, "mm.info.*");
                } finally {
                    StorePhoto.getRpL1(conf).putInstance(jedis);
                }
            }
        }
    }

    public ProfileTimerTask(ServerConf conf, int period) throws JedisException {
        super();
        this.conf = conf;
        File dir = new File(profileDir);
        if (!dir.exists())
            dir.mkdirs();

        // 向redis的数据库1中插入心跳信息
        Jedis jedis = StorePhoto.getRpL1(conf).getResource();
        if (jedis == null)
            throw new JedisException("Get default jedis instance failed.");

        try {
            hbkey = "mm.hb." + conf.getNodeName() + ":" + conf.getServerPort();
            Pipeline pi = jedis.pipelined();
            pi.set(hbkey, "1");
            pi.expire(hbkey, period + 5);
            pi.sync();

            // update mm.dns for IP info
            if (conf.getOutsideIP() != null) {
                jedis.hset("mm.dns", conf.getNodeName() + ":" + conf.getServerPort(),
                        conf.getOutsideIP() + ":" + conf.getServerPort());
                // BUG-XXX: add HTTP port dns service
                jedis.hset("mm.dns", conf.getNodeName() + ":" + conf.getHttpPort(),
                        conf.getOutsideIP() + ":" + conf.getHttpPort());
                System.out.println("Update mm.dns for " + conf.getNodeName() + " -> " +
                        conf.getOutsideIP());
            }

            // determine the ID of ourself, register ourself
            String self = conf.getNodeName() + ":" + conf.getServerPort();
            Long sid;
            if (jedis.zrank("mm.active", self) == null) {
                sid = jedis.incr("mm.next.serverid");
                // FIXME: if two server start with the same port, fail!
                jedis.zadd("mm.active", sid, self);
            }
            // reget the sid
            sid = jedis.zscore("mm.active", self).longValue();
            ServerConf.serverId = sid;
            System.out.println("Got ServerID " + sid + " for Server " + self);


            // use the same serverID to register in mm.active.http
            self = conf.getNodeName() + ":" + conf.getHttpPort();
            jedis.zadd("mm.active.http", sid, self);
            System.out.println("Register HTTP server " + self + " done.");

            Set<Tuple> active = jedis.zrangeWithScores("mm.active.http", 0, -1);
            if (active != null && active.size() > 0) {
                for (Tuple t : active) {
                    ServerConf.servers.put((long) t.getScore(), t.getElement());
                    System.out.println("Got HTTP Server " + (long) t.getScore() + " " +
                            t.getElement());
                }
            }

            // set SS_ID
            if (conf.isSSMaster()) {
                jedis.set("mm.ss.id", "" + ServerConf.serverId);
                System.out.println("Register SS ID to " + ServerConf.serverId);

                // setup pub/sub channel
                pst = new PubSubThread(new MMSPubSub(conf));
                pst.start();
                System.out.println("Setup MM stat info to channel mm.s.info");
            }

            if (conf.isLeSlave()) {
                jedis.set("mm.ls.id", "" + ServerConf.serverId);
                System.out.println("Register LevelDB slave ID to " + ServerConf.serverId);
//
//				// setup pub/sub channel
//				pst = new PubSubThread(new MMSPubSub(conf));
//				pst.start();
//				System.out.println("Setup MM stat info to channel mm.s.info");
            }


        } finally {
            StorePhoto.getRpL1(conf).putInstance(jedis);
        }
        this.period = period;
        if (conf.getSysInfoServerName() != null && conf.getSysInfoServerPort() != -1) {
            try {
                client = new DatagramSocket();
            } catch (SocketException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void run() {
        try {
            long cur = System.currentTimeMillis();
            double wn = ServerProfile.writtenBytes.longValue() / 1024.0;
            double rn = ServerProfile.readBytes.longValue() / 1024.0;
            double wbw = (wn - lastWn) / ((cur - lastTs) / 1000.0);
            double rbw = (rn - lastRn) / ((cur - lastTs) / 1000.0);
            long dnr = ServerProfile.readN.longValue();
            long dl = ServerProfile.readDelay.longValue();

            DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String s = df.format(new Date());
            String info = s + " avg write bandwidth " + (String.format("%.4f", wbw)) + " KB/s";
            String line = (System.currentTimeMillis() / 1000) + "," + wbw + "," + rbw + ",";

            if ((dl - lastDl) == 0) {
                info += ", no read requests";
                line += "0,";
            } else {
                info += ", avg read latency " +
                        (String.format("%.4f", (double) (dl - lastDl) / (dnr - lastDnr))) + " ms";
                line += (double) (dl - lastDl) / (dnr - lastDnr) + ",";
            }
            if (cur - lastRecycleTs >= 5 * 60 * 1000) {
                info += ", recycle Write " + StorePhoto.recycleContextHash() +
                        ", Read " + StorePhoto.recycleRafHash();
                lastRecycleTs = cur;
            }
            System.out.println(info);

            // append profiles to log file. Total format is:
            // TS, wbw, rbw, latency,
            // writtenBytes, readBytes, readDelay, readN, readErr, writeN, writeErr,
            line += ServerProfile.writtenBytes.get() + ",";
            line += ServerProfile.readBytes.get() + ",";
            line += ServerProfile.readDelay.get() + ",";
            line += ServerProfile.readN.get() + ",";
            line += ServerProfile.readErr.get() + ",";
            line += ServerProfile.writeN.get() + ",";
            line += ServerProfile.writeErr.get();
            line += "\n";

            lastWn = wn;
            lastRn = rn;
            lastDnr = dnr;
            lastDl = dl;
            lastTs = cur;

            // server的心跳信息
            Jedis jedis = null;
            try {
                jedis = StorePhoto.getRpL1(conf).getResource();
                if (jedis == null)
                    info += ", redis down?";
                else {
                    Pipeline pi = jedis.pipelined();
                    pi.set(hbkey, "1");
                    pi.expire(hbkey, period + 5);
                    pi.sync();

                    // update server list
                    Set<Tuple> active = jedis.zrangeWithScores("mm.active.http", 0, -1);
                    if (active != null && active.size() > 0) {
                        for (Tuple t : active) {
                            ServerConf.servers.put((long) t.getScore(), t.getElement());
                        }
                    }
                    // update ss master
                    String ss = jedis.get("mm.ss.id");
                    String ls = jedis.get("mm.ls.id");
                    try {
                        if (ss != null)
                            ServerConf.setSs_id(Long.parseLong(ss));
                        if (ls != null)
                            ServerConf.setLsId(Long.parseLong(ls));
                    } catch (Exception e) {
                    }
                    String ckpt = jedis.get("mm.ckpt.ts");
                    try {
                        if (ckpt != null)
                            ServerConf.setCkpt_ts(Long.parseLong(ckpt));
                    } catch (Exception e) {
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                StorePhoto.getRpL1(conf).putInstance(jedis);
            }

            // Report current state to SysInfoStat Server
            if (line.length() > 0 && client != null) {
                String toSend = conf.getNodeName() + "," + conf.getServerPort() + "," + line;
                byte[] sendBuf = toSend.getBytes();
                DatagramPacket sendPacket;

                try {
                    sendPacket = new DatagramPacket(sendBuf, sendBuf.length,
                            InetAddress.getByName(conf.getSysInfoServerName()),
                            conf.getSysInfoServerPort());
                    client.send(sendPacket);
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            // 把统计信息写入文件,每一天的信息放在一个文件里
            String profileName = conf.getNodeName() + "." + conf.getServerPort() + "." +
                    s.substring(0, 10) + ".log";
            FileWriter fw = null;
            try {
                // 追加到文件尾
                fw = new FileWriter(new File(profileDir + profileName), true);
                BufferedWriter w = new BufferedWriter(fw);
                w.write(line);
                w.close();
                fw.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
