package iie.mm.server;

import iie.mm.client.Feature;
import iie.mm.client.Feature.FeatureLIREType;
import iie.mm.client.Feature.FeatureType;
import iie.mm.client.ImagePHash;
import iie.mm.client.ResultSet;
import iie.mm.common.RedisPool;
import iie.mm.common.RedisPoolSelector;
import iie.mm.common.RedisPoolSelector.RException;
import iie.mm.common.RedisPoolSelector.RedisConnection;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

import java.awt.image.BufferedImage;
import java.io.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class StorePhoto {
    private ServerConf conf;
    private String localHostName;
    // 本机监听的端口,在这里的作用就是构造存图片时返回值
    private int serverport;
    private Set<String> storeArray = new HashSet<String>();
    // 磁盘的数组
    private String[] diskArray;
    // 文件块的大小，单位是B
    private long blocksize;
    private static long writeTo = -1;
    private static long readTo = -1;    
    
    // 一级的hash,集合 + 磁盘->上下文
    // 不能放在构造函数里初始化,不然会每次创建一个storephoto类时,它都被初始化一遍
    private static ConcurrentHashMap<String, StoreSetContext> writeContextHash = new ConcurrentHashMap<String, StoreSetContext>();
    // 读文件时的随机访问流，用哈希来缓存
    private static ConcurrentHashMap<String, ReadContext> readRafHash = new ConcurrentHashMap<String, ReadContext>();

    private static ConcurrentHashMap<String, String> nilSetHash = new ConcurrentHashMap<String, String>();
    private static ConcurrentHashMap<String, String> l1SetHash = new ConcurrentHashMap<String, String>();
    private static String sha = null;
    private static String sha2 = null;
    private static String sha3 = null;

    private TimeLimitedCacheMap lookupCache = new TimeLimitedCacheMap(10, 60, 300, TimeUnit.SECONDS);

    private static RedisPool rpL1 = null;
    private static RedisPoolSelector rps = null;

    public RedisPool getRpL1() {
        return rpL1;
    }

    public static RedisPool getRpL1(ServerConf conf) {
        if (rpL1 == null) {
            rpL1 = new RedisPool(conf, "l1.master");
            try {
                rps = new RedisPoolSelector(conf, rpL1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return rpL1;
    }

    public static RedisPoolSelector getRPS(ServerConf conf) throws Exception {
        if (rps == null) {
            rps = new RedisPoolSelector(conf, rpL1);
        }
        return rps;
    }

    public static class RedirectException extends Exception {
        /**
         * serialVersionUID
         */
        private static final long serialVersionUID = 3092261885247728350L;
        public long serverId;
        public long offset;
        public int length;
        public String info;

        public RedirectException(long serverId, String info, long offset, int length) {
            this.serverId = serverId;
            this.info = info;
            this.offset = offset;
            this.length = length;
        }
    }

    public static class ObjectContent {
        byte[] content;
        int length;

        public ObjectContent() {
            content = null;
            length = 0;
        }

    }

    public static int recycleContextHash() {
        List<String> toDel = new ArrayList<String>();
        int nr = 0;

        for (Map.Entry<String, StoreSetContext> entry : writeContextHash.entrySet()) {
            if (entry.getValue().openTs > 0 && System.currentTimeMillis() - entry.getValue().openTs > writeTo) {
                toDel.add(entry.getKey());
            }
        }
        for (String key : toDel) {
            StoreSetContext ssc = writeContextHash.get(key);
            if (ssc != null) {
                synchronized (ssc) {
                    if (ssc.raf != null)
                        try {
                            ssc.raf.close();
                            ssc.raf = null;
                            nr++;
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                }
            }
        }

        return nr;
    }

    public static int recycleRafHash() {
        List<String> toDel = new ArrayList<String>();
        int nr = 0;

        for (Map.Entry<String, ReadContext> entry : readRafHash.entrySet()) {
            if (entry.getValue().accessTs > 0 && System.currentTimeMillis() - entry.getValue().accessTs > readTo) {
                toDel.add(entry.getKey());
            }
        }
        for (String key : toDel) {
            ReadContext rc = readRafHash.get(key);
            if (rc != null) {
                try {
                    nr += rc.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return nr;
    }

    public class ReadContext {
        public long accessTs = -1;
        public RandomAccessFile raf = null;
        public AtomicInteger ref = new AtomicInteger(0);
        public String name, mode;

        public ReadContext(String name, String mode) throws FileNotFoundException {
            raf = new RandomAccessFile(name, mode);
            this.name = name;
            this.mode = mode;
        }

        public void updateAccessTs() {
            accessTs = System.currentTimeMillis();
        }

        public int close() throws IOException {
            synchronized (this) {
                if (raf != null) {
                    raf.close();
                    raf = null;
                    return 1;
                }
            }
            return 0;
        }

        public void reopen() throws IOException {
            synchronized (this) {
                if (raf == null) {
                    raf = new RandomAccessFile(name, mode);
                }
            }
        }
    }

    public class StoreSetContext {
        public String key;
        public String disk;

        public AtomicLong bref = new AtomicLong(0);
        public long openTs = -1;
        // 当前可写的块
        private long curBlock = -1;
        private long offset = 0;
        private long off = 0;
        // 代表要写的块的文件
        private File newf = null;
        // 写当前的块的随机访问流
        private RandomAccessFile raf = null;

        private String path = null;

        public StoreSetContext(String set, String disk) {
            // 根据set和md5构造存储的路径
            StringBuffer sb = new StringBuffer();
            sb.append(disk);
            sb.append("/");
            sb.append(conf.destRoot);
            sb.append(set);
            sb.append("/");
            path = sb.toString();
            // 存储文件的文件夹的相对路径，不包含文件名
            File dir = new File(path);
            if (!dir.exists())
                dir.mkdirs();

            this.key = set + ":" + disk;
            this.disk = disk;
        }
    }

    public StorePhoto(ServerConf conf) throws Exception {
        this.conf = conf;
        serverport = conf.getServerPort();
        blocksize = conf.getBlockSize();
        storeArray = conf.getStoreArray();
        if (storeArray.size() == 0) {
            storeArray.add(".");
        }
        diskArray = storeArray.toArray(new String[0]);
        localHostName = conf.getNodeName();
        writeTo = conf.getWrite_fd_recycle_to();
        readTo = conf.getRead_fd_recycle_to();
        if (rpL1 == null) {
            rpL1 = new RedisPool(conf, "l1.master");
            rpL1.setPid("0");
        }
        if (rps == null)
            rps = new RedisPoolSelector(conf, rpL1);
    }

    private void loadScripts() {
        String script = "local temp = redis.call('hget', KEYS[1], ARGV[1]);"
                + "if temp then "
                + "if string.sub(temp,2,2) == '#' then "
                + "temp = string.sub(temp,3,-1);"
                + "end "
                + "temp = temp..'#'..ARGV[2];"
                + "redis.call('hset',KEYS[1],ARGV[1],'0#'..temp);"
                + "return temp;"
                + "else "
                + "redis.call('hset',KEYS[1],ARGV[1],'0#'..ARGV[2]);"
                + "return ARGV[2] end";

        // String script = "local temp = redis.call('hget', KEYS[1], ARGV[1]);"
        // + "if temp then "
        // + "temp = temp..\"#\"..ARGV[2] ;"
        // + "redis.call('hset',KEYS[1],ARGV[1],temp);"
        // + "return temp;"
        // + "else "
        // + "redis.call('hset',KEYS[1],ARGV[1],ARGV[2]);"
        // + "return ARGV[2] end";

        // try to load scripts to each L2 pool
        for (Map.Entry<String, RedisPool> entry : rps.getRpL2().entrySet()) {
            Jedis jedis = entry.getValue().getResource();
            if (jedis != null) {
                try {
                    sha = jedis.scriptLoad(script);
                    System.out.println("Load script as " + sha + " in L2 pool " + entry.getKey());
                } finally {
                    entry.getValue().putInstance(jedis);
                }
            }
        }
    }

    private void loadScripts2() {
        String script = "local temp = redis.call('hget', KEYS[1], ARGV[1]);"
                + "if temp then "
                + "return temp;"
                + "else "
                + "redis.call('hset', KEYS[1], ARGV[1], ARGV[2]);"
                + "return ARGV[2] end";

        // try to load scripts to each L2 pool
        for (Map.Entry<String, RedisPool> entry : rps.getRpL2().entrySet()) {
            Jedis jedis = entry.getValue().getResource();
            if (jedis != null) {
                try {
                    sha2 = jedis.scriptLoad(script);
                    System.out.println("Load script as " + sha2 + " in L2 pool " + entry.getKey());
                } finally {
                    entry.getValue().putInstance(jedis);
                }
            }
        }
    }

    private void loadScripts3() {
        String script = "redis.call('hset', KEYS[1], ARGV[1], ARGV[2]);"
                + "return ARGV[2];";

        // try to load scripts to each L2 pool
        for (Map.Entry<String, RedisPool> entry : rps.getRpL2().entrySet()) {
            Jedis jedis = entry.getValue().getResource();
            if (jedis != null) {
                try {
                    sha3 = jedis.scriptLoad(script);
                    System.out.println("Load script as " + sha3 + " in L2 pool " + entry.getKey());
                } finally {
                    entry.getValue().putInstance(jedis);
                }
            }
        }
    }


    /**
     * 把content代表的图片内容,存储起来,把小图片合并成一个块,块大小由配置文件中blocksize指定.
     * 文件存储在destRoot下，然后按照set分第一层子目录
     *
     * @param set     集合名
     * @param md5     文件的md5
     * @param content 文件内容
     * @return type@set@serverid@block@offset@length@disk,这几个信息通过redis存储,分别表示元信息类型
     * ,该图片所属集合,所在节点, 节点的端口号,所在相对路径（包括完整文件名）,位于所在块的偏移的字节数，该图片的字节数,磁盘
     */
    public String storePhoto(String set, String md5, byte[] content, int clen, int coff, long flen, String fn) {
        if (flen > 0)
            return _storePhoto(set, md5, content, clen, coff, flen, fn);
        else if (flen < 0)
            return _storePhoto(set, md5, content, clen, coff);
        else
            return "";
    }

    private String _storePhoto(String set, String md5, byte[] content, int clen, int coff) {

        String info = null;
        RedisConnection rc = null;
        String returnStr = "#FAIL: unknown error.";
        int err = 0;
        StoreSetContext ssc = null;
        try {
            rc = rps.getL2(set, false);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (rc.rp == null || rc.jedis == null) {
            return "#FAIL: L2 pool " + rc.id + " can not be reached.";
        }

        if (sha3 == null) {
            loadScripts3();
        }

        Jedis jedis = rc.jedis;

        try {
            if (jedis != null)
                info = jedis.hget(set, md5);
            if ('#' == info.charAt(1))
                info = info.substring(2);
            String[] infos = info.split("#");
            String[] thInfos = null;
            String thInfo = null;
            for (String string : infos) {
                if (conf.serverId == Long.parseLong(string.split("@")[2])) {
                    thInfos = string.split("@");
                }
            }
            ssc = new StoreSetContext(set, thInfos[6]);
//            ssc.path = thInfos[6] + "/" + conf.destRoot + thInfos[1] + "/b" + thInfos[3];
            synchronized (ssc) {
                ssc.newf = new File(ssc.path + "b" + thInfos[3]);
                ssc.offset = ssc.newf.length();

                ssc.raf = new RandomAccessFile(ssc.newf, "rw");
                ssc.openTs = System.currentTimeMillis();
                ssc.raf.seek(ssc.offset);

                ssc.raf.write(content, coff, clen);
                ssc.offset += clen;
                ssc.bref.incrementAndGet();
                // 统计写入的字节数
                ServerProfile.addWrite(clen);

                if (ssc.raf != null)
                    ssc.raf.close();
            }
            int vlen = conf.getVlen();
            if(conf.isIsvname()){

                thInfos[vlen] = Long.parseLong(thInfos[vlen]) + clen + "";

                if ((Long.parseLong(thInfos[vlen])) != Long.parseLong(thInfos[5])) {
                    for (int i = 0; i <= vlen; i++){
                        thInfo += thInfos[i] +"@";
                    }
                    thInfo.substring(0, thInfo.length() - 1);
                    //thInfo = thInfos[0] + "@" + thInfos[1] + "@" + thInfos[2] + "@" + thInfos[3] + "@" + thInfos[4] + "@" + thInfos[5] + "@" + thInfos[6] + "@" + thInfos[7] + "@" + thInfos[8] + "@" + thInfos[9];
                } else {
                    for (int i = 0; i < vlen; i++){
                        thInfo += thInfos[i] +"@";
                    }
                    thInfo.substring(0, thInfo.length() - 1);
                    //thInfo = thInfos[0] + "@" + thInfos[1] + "@" + thInfos[2] + "@" + thInfos[3] + "@" + thInfos[4] + "@" + thInfos[5] + "@" + thInfos[6] + "@" + thInfos[7] + "@" + thInfos[8];
                }
            }else{
                thInfos[vlen] = Long.parseLong(thInfos[vlen]) + clen + "";
                if ((Long.parseLong(thInfos[vlen])) != Long.parseLong(thInfos[5])) {
                    for (int i = 0; i <= vlen; i++){
                        thInfo += thInfos[i] +"@";
                    }
                    thInfo.substring(0, thInfo.length() - 1);
                    //thInfo = thInfos[0] + "@" + thInfos[1] + "@" + thInfos[2] + "@" + thInfos[3] + "@" + thInfos[4] + "@" + thInfos[5] + "@" + thInfos[6] + "@" + thInfos[7];
                } else {
                    for (int i = 0; i < vlen; i++){
                        thInfo += thInfos[i] +"@";
                    }
                    thInfo.substring(0, thInfo.length() - 1);
                    //thInfo = thInfos[0] + "@" + thInfos[1] + "@" + thInfos[2] + "@" + thInfos[3] + "@" + thInfos[4] + "@" + thInfos[5] + "@" + thInfos[6];
                }
            }
            //替换本节点对应的info，
            for (int i = 0; i < infos.length; i++) {
                if (conf.serverId == Long.parseLong(infos[i].split("@")[2])) {
                    infos[i] = thInfo;
                }
                if (i == 0)
                    info = "0#" + infos[i];
                else
                    info += ("#" + infos[i]);
            }
        } catch (JedisConnectionException e) {
            System.out.println(set + "@" + md5 + ": Jedis connection broken in storeObject.");
            e.printStackTrace();
            returnStr = "#FAIL:" + e.getMessage();
            err = -1;
        } catch (JedisException e) {
            System.out.println(set + "@" + md5 + ": Jedis exception: " + e.getMessage());
            e.printStackTrace();
            returnStr = "#FAIL:" + e.getMessage();
            err = -1;
        } catch (Exception e) {
            System.out.println(set + "@" + md5 + ": Exception: " + e.getMessage());
            e.printStackTrace();
            returnStr = "#FAIL:" + e.getMessage();
            err = -1;
        } finally {
            if (err < 0) {
                rps.putL2(rc);
                ServerProfile.writeErr.incrementAndGet();
                ssc.bref.decrementAndGet();
                return returnStr;
            }
        }

        try {
            returnStr = jedis.evalsha(sha3, 1, set, md5, info).toString();
        } catch (JedisConnectionException e) {
            System.out.println(set + "@" + md5 + ": Jedis connection broken in storeObject.");
            e.printStackTrace();
            err = -1;
            returnStr = "#FAIL:" + e.getMessage();
        } catch (JedisException e) {
            System.out.println(set + "@" + md5 + ": Jedis exception: " + e.getMessage());
            e.printStackTrace();
            err = -1;
            if (e.getMessage().startsWith("NOSCRIPT"))
                sha = null;
            returnStr = "#FAIL:" + e.getMessage();
        } catch (Exception e) {
            System.out.println(set + "@" + md5 + ": Exception: " + e.getMessage());
            e.printStackTrace();
            err = -1;
            returnStr = "#FAIL:" + e.getMessage();
        } finally {
            ssc.bref.decrementAndGet();
            rps.putL2(rc);
            if (err < 0) {
                ServerProfile.writeErr.incrementAndGet();
            }
        }

        return returnStr;
    }

    /**
     * 把content代表的图片内容,存储起来,把小图片合并成一个块,块大小由配置文件中blocksize指定.
     * 文件存储在destRoot下，然后按照set分第一层子目录
     *
     * @param set     集合名
     * @param md5     文件的md5
     * @param content 文件内容
     * @return type@set@serverid@block@offset@length@disk,这几个信息通过redis存储,分别表示元信息类型
     * ,该图片所属集合,所在节点, 节点的端口号,所在相对路径（包括完整文件名）,位于所在块的偏移的字节数，该图片的字节数,磁盘
     */
    private String _storePhoto(String set, String md5, byte[] content, int clen, int coff, long flen, String fn) {
        String time = setL1Seq(set);
        String returnStr = "#FAIL: unknown error.";
        RedisConnection rc = null;
        try {
            rc = rps.getL2(set, false);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (rc.rp == null || rc.jedis == null) {
            return "#FAIL: L2 pool " + rc.id + " can not be reached.";
        }

        if (sha == null) {
            loadScripts();
        }
        Jedis jedis = rc.jedis;

        // jedis.set(set + ".seq", time);

        StringBuffer rVal = new StringBuffer(128);
        int err = 0;

        // 随机选一个磁盘
        int diskid = new Random().nextInt(diskArray.length);
        StoreSetContext ssc = null;
        do {
            ssc = writeContextHash.get(set + ":" + diskArray[diskid]);
            if (ssc != null)
                break;
            ssc = new StoreSetContext(set, diskArray[diskid]);
            ssc = writeContextHash.putIfAbsent(ssc.key, ssc);
        } while (ssc == null);

        synchronized (ssc) {
            // 找到当前可写的文件块,如果当前不够大,或不存在,则新创建一个,命名block＿id(b[id]),
            // id递增,redis中只存储id
            // 用curBlock缓存当前可写的块，减少查询jedis的次数

            try {
                if (ssc.curBlock < 0) {
                    // 需要通过节点名字来标示不同节点上相同名字的集合
                    String reply = jedis.get(set + ".blk." + ServerConf.serverId + "." + ssc.disk);
                    if (reply != null) {
                        ssc.curBlock = Long.parseLong(reply);
                        ssc.newf = new File(ssc.path + "b" + ssc.curBlock);
                        ssc.offset = ssc.newf.length();
                    } else {
                        ssc.curBlock = 0;
                        ssc.newf = new File(ssc.path + "b" + ssc.curBlock);
                        // 把集合和它所在节点记录在redis的set里,方便删除,
                        // set.srvs表示set所在的服务器的位置
                        jedis.sadd(set + ".srvs", localHostName + ":" + serverport);
                        jedis.set(set + ".blk." + ServerConf.serverId + "." + ssc.disk, "" + ssc.curBlock);
                        ssc.offset = 0;
                    }
                    ssc.raf = new RandomAccessFile(ssc.newf, "rw");
                    ssc.openTs = System.currentTimeMillis();
                    ssc.raf.seek(ssc.offset);
                }
                if (ssc.offset + clen > blocksize) {
                    ssc.curBlock++;
                    ssc.newf = new File(ssc.path + "b" + ssc.curBlock);
                    // 如果换了一个新块,则先把之前的关掉
                    if (ssc.raf != null)
                        ssc.raf.close();
                    // 当前可写的块号加一
                    do {
                        if (ssc.bref.get() == 0) {
                            jedis.incr(set + ".blk." + ServerConf.serverId + "." + ssc.disk);
                            break;
                        }
                        Thread.yield();
                    } while (true);
                    ssc.offset = 0;
                    ssc.raf = new RandomAccessFile(ssc.newf, "rw");
                    ssc.openTs = System.currentTimeMillis();
                }

                // 在每个文件前面写入它的md5和offset length，从而恢复元数据
                // md5 32个字节，offset:length分配20个字节
                // ssc.offset += 52;

                // 构造返回值
                rVal.append("1@"); // type
                rVal.append(set);
                rVal.append("@");
                rVal.append(ServerConf.serverId);
                rVal.append("@");
                rVal.append(ssc.curBlock);
                rVal.append("@");
                rVal.append(ssc.offset);
                rVal.append("@");
                rVal.append(flen);
                rVal.append("@");
                // 磁盘,现在存的是磁盘的名字,读取的时候直接拿来构造路径
                rVal.append(diskArray[diskid]);
                if(conf.isIsvname()) {
                    rVal.append("@");
                    rVal.append(System.currentTimeMillis() / 1000);

                    rVal.append("@");
                    rVal.append(fn);

                }
                if (flen > clen) {
                    rVal.append("@");
                    rVal.append(clen);
                }
                if (ssc.raf == null) {
                    ssc.raf = new RandomAccessFile(ssc.newf, "rw");
                    ssc.openTs = System.currentTimeMillis();
                    ssc.raf.seek(ssc.offset);
                }
                ssc.raf.write(content, coff, clen);
                ssc.offset += clen;
                ssc.bref.incrementAndGet();
                // 统计写入的字节数
                ServerProfile.addWrite(clen);
            } catch (JedisConnectionException e) {
                System.out.println(set + "@" + md5 + ": Jedis connection broken in storeObject.");
                e.printStackTrace();
                returnStr = "#FAIL:" + e.getMessage();
                err = -1;
            } catch (JedisException e) {
                System.out.println(set + "@" + md5 + ": Jedis exception: " + e.getMessage());
                e.printStackTrace();
                returnStr = "#FAIL:" + e.getMessage();
                err = -1;
            } catch (Exception e) {
                System.out.println(set + "@" + md5 + ": Exception: " + e.getMessage());
                e.printStackTrace();
                returnStr = "#FAIL:" + e.getMessage();
                err = -1;
            } finally {
                if (err < 0) {
                    rps.putL2(rc);
                    ServerProfile.writeErr.incrementAndGet();
                    ssc.bref.decrementAndGet();
                    return returnStr;
                }
            }
        }

        try {
            returnStr = rVal.toString();
            returnStr = jedis.evalsha(sha, 1, set, md5, returnStr).toString();
        } catch (JedisConnectionException e) {
            System.out.println(set + "@" + md5 + ": Jedis connection broken in storeObject.");
            e.printStackTrace();
            err = -1;
            returnStr = "#FAIL:" + e.getMessage();
        } catch (JedisException e) {
            System.out.println(set + "@" + md5 + ": Jedis exception: " + e.getMessage());
            e.printStackTrace();
            err = -1;
            if (e.getMessage().startsWith("NOSCRIPT"))
                sha = null;
            returnStr = "#FAIL:" + e.getMessage();
        } catch (Exception e) {
            System.out.println(set + "@" + md5 + ": Exception: " + e.getMessage());
            e.printStackTrace();
            err = -1;
            returnStr = "#FAIL:" + e.getMessage();
        } finally {
            ssc.bref.decrementAndGet();
            rps.putL2(rc);
            if (err < 0) {
                ServerProfile.writeErr.incrementAndGet();
            }
        }
        try {
            FeatureSearch.add(conf, new FeatureSearch.ImgKeyEntry(conf.getFeatures(), content, 0, clen, set, md5));
        } catch (Exception e) {
            e.printStackTrace();
        }

        return returnStr;
    }

    /**
     * @param set
     * @param md5
     * @param content
     * @return 出现任何错误返回null，出现错误的话不知道哪些存储成功，哪些不成功
     */
    public String[] mstorePhoto(String set, String[] md5, byte[][] content) {

        String time = setL1Seq(set);
        int err = 0;

        if (!(md5.length == content.length && md5.length == content.length)) {
            System.out.println("Array lengths in arguments mismatch.");
            return null;
        }
        if (sha == null) {
            loadScripts();
        }

        RedisConnection rc = null;

        try {
            rc = rps.getL2(set, false);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (rc.rp == null || rc.jedis == null) {
            System.out.println("mstorePhoto " + set + " fail: L2 pool " + rc.id + " can't be reached.");
            return null;
        }
        Jedis jedis = rc.jedis;

        // jedis.set(set + ".seq", time);

        String[] returnVal = new String[content.length];
        int diskid = new Random().nextInt(diskArray.length);
        StoreSetContext ssc = null;

        do {
            ssc = writeContextHash.get(set + ":" + diskArray[diskid]);
            if (ssc != null)
                break;
            ssc = new StoreSetContext(set, diskArray[diskid]);
            ssc = writeContextHash.putIfAbsent(ssc.key, ssc);
        } while (ssc == null);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        long init_offset = 0;

        synchronized (ssc) {
            for (int i = 0; i < content.length; i++) {
                StringBuffer rVal = new StringBuffer(128);

                try {
                    if (ssc.curBlock < 0) {
                        // 需要通过节点名字来标示不同节点上相同名字的集合
                        String reply = jedis.get(set + ".blk." + ServerConf.serverId + "." + ssc.disk);
                        if (reply != null) {
                            ssc.curBlock = Long.parseLong(reply);
                            ssc.newf = new File(ssc.path + "b" + ssc.curBlock);
                            ssc.offset = ssc.newf.length();
                        } else {
                            ssc.curBlock = 0;
                            ssc.newf = new File(ssc.path + "b" + ssc.curBlock);
                            // 把集合和它所在节点记录在redis的set里,方便删除,set.srvs表示set所在的服务器的位置
                            jedis.sadd(set + ".srvs", localHostName + ":" + serverport);
                            jedis.set(set + ".blk." + ServerConf.serverId + "." + ssc.disk, "" + ssc.curBlock);
                            ssc.offset = 0;
                        }
                        ssc.raf = new RandomAccessFile(ssc.newf, "rw");
                        ssc.openTs = System.currentTimeMillis();
                        ssc.raf.seek(ssc.offset);
                    }

                    if (ssc.offset + content[i].length > blocksize) {
                        ssc.curBlock++;
                        ssc.newf = new File(ssc.path + "b" + ssc.curBlock);
                        // 如果换了一个新块,先把之前的写进去，然后再关闭流
                        if (ssc.raf != null) {
                            ssc.raf.write(baos.toByteArray());
                            ssc.raf.close();
                            baos.reset();
                        }
                        // 当前可写的块号加一
                        jedis.incr(set + ".blk." + ServerConf.serverId + "." + ssc.disk);
                        ssc.offset = 0;
                        ssc.raf = new RandomAccessFile(ssc.newf, "rw");
                        ssc.openTs = System.currentTimeMillis();
                    }

                    // 在每个文件前面写入它的md5和offset length，从而恢复元数据
                    // md5 32个字节，offset:length分配20个字节
                    // ssc.offset += 52;
                    // 统计写入的字节数
                    ServerProfile.addWrite(content[i].length);
                    // 构造返回值
                    rVal.append("1@"); // type
                    rVal.append(set);
                    rVal.append("@");
                    rVal.append(ServerConf.serverId);
                    rVal.append("@");
                    rVal.append(ssc.curBlock);
                    rVal.append("@");
                    rVal.append(ssc.offset);
                    rVal.append("@");
                    rVal.append(content[i].length);
                    rVal.append("@");
                    // 磁盘,现在存的是磁盘的名字,读取的时候直接拿来构造路径
                    rVal.append(diskArray[diskid]);

                    baos.write(content[i]);
                    returnVal[i] = rVal.toString();
                    if (i == 0)
                        init_offset = ssc.offset;
                    ssc.offset += content[i].length;
                } catch (JedisConnectionException e) {
                    System.out.println(set + ": Jedis connection broken in mstoreObject.");
                    e.printStackTrace();
                    err = -1;
                } catch (JedisException e) {
                    System.out.println(set + ": Jedis exception: " + e.getMessage());
                    e.printStackTrace();
                    err = -1;
                } catch (Exception e) {
                    System.out.println(set + ": Exception: " + e.getMessage());
                    e.printStackTrace();
                    err = -1;
                } finally {
                    if (err < 0) {
                        rps.putL2(rc);
                        ServerProfile.writeErr.incrementAndGet();
                        return null;
                    }
                }
            }
            // do write now
            try {
                if (ssc.raf == null) {
                    ssc.raf = new RandomAccessFile(ssc.newf, "rw");
                    ssc.openTs = System.currentTimeMillis();
                    ssc.raf.seek(ssc.offset);
                }
                ssc.raf.write(baos.toByteArray());
            } catch (IOException e) {
                e.printStackTrace();
                // Rollback SSC offset
                ssc.offset = init_offset;
                rps.putL2(rc);
                ServerProfile.writeErr.incrementAndGet();
                return null;
            }
        }

        try {
            for (int i = 0; i < content.length; i++) {
                returnVal[i] = jedis.evalsha(sha, 1, set, md5[i], returnVal[i]).toString();
                FeatureSearch.add(conf, new FeatureSearch.ImgKeyEntry(conf.getFeatures(), content[i], 0,
                        content[i].length, set, md5[i]));
            }
        } catch (JedisConnectionException e) {
            System.out.println(set + ": Jedis connection broken in mstoreObject.");
            e.printStackTrace();
            err = -1;
            returnVal = null;
        } catch (JedisException e) {
            System.out.println(set + ": Jedis exception: " + e.getMessage());
            e.printStackTrace();
            err = -1;
            if (e.getMessage().startsWith("NOSCRIPT"))
                sha = null;
            returnVal = null;
        } catch (Exception e) {
            System.out.println(set + ": Exception: " + e.getMessage());
            e.printStackTrace();
            err = -1;
            returnVal = null;
        } finally {
            if (err < 0) {
                ServerProfile.writeErr.incrementAndGet();
            }
            rps.putL2(rc);
        }
        return returnVal;
    }

    private boolean isInRedis(String this_set) {
        long this_ts = Long.MAX_VALUE;

        try {
            if (!Character.isDigit(this_set.charAt(0))) {
                this_set = this_set.substring(1);
            }
            this_ts = Long.parseLong(this_set);
        } catch (Exception e) {
        }
        if (this_ts > ServerConf.getCkpt_ts())
            return true;
        else
            return false;
    }

    // if ss_id is ourself, do search; otherwise redirect the request to correct
    // master server.
//	private ObjectContent getPhotoFromSS(String set, String md5) throws RedirectException {
//
//		if (ServerConf.getSs_id() == ServerConf.serverId) {
//			// ok, do search
//			String info = LMDBInterface.getLmdb().read("H#" + set + "." + md5);
//
//			if (info != null) {
//				// split if it is complex uri
//				String savedInfo = null;
//				Long savedId = -1L;
//				for (String i : info.split("#")) {
//					String[] is = i.split("@");
//
//					if (Long.parseLong(is[2]) == ServerConf.serverId)
//						return searchPhoto(i, is, null);
//					else {
//						savedInfo = i;
//						savedId = Long.parseLong(is[2]);
//					}
//				}
//				throw new RedirectException(savedId, savedInfo);
//			}
//		} else {
//			throw new RedirectException(ServerConf.getSs_id(), set + "@" + md5);
//		}
//		return null;
//	}

    /**
     * 获得md5值所代表的图片的内容
     *
     * @param md5 与storePhoto中的参数md5相对应
     * @return 该图片的内容, 与storePhoto中的参数content对应
     * @throws RedirectException
     */
    public ObjectContent getPhoto(String set, String md5, long offset, int length) throws RedirectException {
        if (nilSetHash.containsKey(set)) {
            System.out.println("MM getPhoto: md5: " + md5 + " doesn't exist in set: " + set);
            return null;
        }

        addHeat(set, md5);

        RedisConnection rc = null;
        String info = null;
        int err = 0;

        try {
            rc = rps.getL2(set, false);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        if (rc.rp == null || rc.jedis == null) {
            System.out.println("MM getPhoto: " + set + "@" + md5 + ": get L2 pool " + rc.id + " failed.");
            return null;
        }
        Jedis jedis = rc.jedis;

        // Step 1: check the local lookup cache
        info = (String) lookupCache.get(set + "." + md5);
	try {
            if (info == null)
                info = jedis.hget(set, md5);
            if (info == null)
                return swapIn(set, md5, offset, length);
        } catch (JedisConnectionException e) {
                err = -1;
                return null;
        } catch (JedisException e) {
                err = -1;
                return null;
        } finally {
                if (err < 0) {
                    ServerProfile.readErr.incrementAndGet();
                }
                rps.putL2(rc);
        }

        lookupCache.put(set + "." + md5, info);
        if ('2' == info.charAt(0)) {
                System.out.println(set + "." + md5 + "is deleted");
                return null;
        }


        if ('#' == info.charAt(1))
            info = info.substring(2);

        // split if it is complex uri
        String savedInfo = null;
        Long savedId = -1L;
        for (String i : info.split("#")) {
            String[] is = i.split("@");

            if (Long.parseLong(is[2]) == ServerConf.serverId) {
                if (length != -1)
                    return searchPhoto(i, is, null, offset, length);
                else
                    return searchPhoto(i, is, null);
            } else {
                savedInfo = i;
                savedId = Long.parseLong(is[2]);
            }
        }

        throw new RedirectException(savedId, savedInfo, offset, length);
    }


    /**
     * 获得图片内容
     *
     * @param info 对应storePhoto的type@set@serverid@block@offset@length@disk格式的返回值
     * @return 图片内容content
     */
    public ObjectContent searchPhoto(String info, String[] infos, byte[] ibuf) throws RedirectException {
        ObjectContent oc = new ObjectContent();
        long start = System.currentTimeMillis();
        int rlen = 0;
        if (infos == null)
            infos = info.split("@");

        if (infos.length != conf.getVlen()) {
            System.out.println("Invalid INFO string: " + info);
            return oc;
        }
        if (Long.parseLong(infos[2]) != ServerConf.serverId) {
            // this request should be send to another server
            throw new RedirectException(Long.parseLong(infos[2]), info, 0, ibuf.length);
        }
        String path = infos[6] + "/" + conf.destRoot + infos[1] + "/b" + infos[3];
        ReadContext readr = null;
        byte[] content = null;

        rlen = Integer.parseInt(infos[5]);
        if (ibuf != null && ibuf.length >= rlen)
            content = ibuf;
        else
            content = new byte[rlen];

        try {
            // 用哈希缓存打开的文件随机访问流
            do {
                readr = readRafHash.get(path);
                if (readr != null)
                    break;
                // 构造路径时加上磁盘
                ReadContext nreadr = new ReadContext(path, "r");
                readr = readRafHash.putIfAbsent(path, nreadr);
                if (readr != null) {
                    nreadr.close();
                }
            } while (readr == null);

            synchronized (readr) {
                if (readr.raf == null)
                    readr.reopen();
                readr.raf.seek(Long.parseLong(infos[4]));
                readr.raf.read(content, 0, rlen);
                readr.updateAccessTs();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return oc;
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return oc;
        } catch (IOException e) {
            e.printStackTrace();
            readRafHash.remove(path);
            return oc;
        }
        oc.content = content;
        oc.length = rlen;
        ServerProfile.updateRead(oc.length, System.currentTimeMillis() - start);
        return oc;
    }

    /**
     * 获得图片内容
     *
     * @param info 对应storePhoto的type@set@serverid@block@offset@length@disk格式的返回值
     * @return 图片内容content
     */
    public ObjectContent searchPhoto(String info, String[] infos, byte[] ibuf, long offset, int length) throws RedirectException {
        ObjectContent oc = new ObjectContent();
        long start = System.currentTimeMillis();
        if (infos == null)
            infos = info.split("@");

        if (infos.length != conf.getVlen()) {
            System.out.println("Invalid INFO string: " + info);
            return oc;
        }
        if (Long.parseLong(infos[2]) != ServerConf.serverId) {
            // this request should be send to another server
            throw new RedirectException(Long.parseLong(infos[2]), info, offset, length);
        }
        String path = infos[6] + "/" + conf.destRoot + infos[1] + "/b" + infos[3];
        ReadContext readr = null;
        byte[] content = null;
        if (ibuf != null && ibuf.length >= length)
            content = ibuf;
        else
            content = new byte[length];
        try {
            // 用哈希缓存打开的文件随机访问流
            do {
                readr = readRafHash.get(path);
                if (readr != null)
                    break;
                // 构造路径时加上磁盘
                ReadContext nreadr = new ReadContext(path, "r");
                readr = readRafHash.putIfAbsent(path, nreadr);
                if (readr != null) {
                    nreadr.close();
                }
            } while (readr == null);

            synchronized (readr) {
                if (readr.raf == null)
                    readr.reopen();
                readr.raf.seek(Long.parseLong(infos[4]) + offset);
                readr.raf.read(content, 0, length);
                readr.updateAccessTs();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return oc;
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return oc;
        } catch (IOException e) {
            e.printStackTrace();
            readRafHash.remove(path);
            return oc;
        }
        oc.content = content;
        oc.length = length;
        ServerProfile.updateRead(oc.length, System.currentTimeMillis() - start);
        return oc;
    }

    /**
     * 只是把元数据的对应的info置为“del”;
     *
     * @param set
     * @param md5
     */
    public void deletePhoto(String set, String md5) {
        RedisConnection rc = null;
        String info = null;
        int err = 0;
        String time = setL1Seq(set);
        try {
            rc = rps.getL2(set, false);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (rc.rp == null || rc.jedis == null) {
            System.out.println("MM deletePhoto: " + set + "@" + md5 + ": get L2 pool " + rc.id + " failed.");
        }
        Jedis jedis = rc.jedis;
        int rocksid = getRocksid(set);
        // jedis.set(set + ".seq", time);

        // Step 1: check the local lookup cache
        info = (String) lookupCache.get(set + "." + md5);
        if (info != null)
            lookupCache.remove(set + "." + md5);
        Long i = -1L;
        try {
            // if (isInRedis(set)) {
            // info = jedis.hget(set, md5);
            // i = jedis.hset(set, md5, "del");
            // }
            info = jedis.hget(set, md5);
            i = jedis.hset(set, md5, "2#" + info);

        } catch (JedisConnectionException e) {
            err = -1;
        } catch (JedisException e) {
            err = -1;
        } finally {
            if (err < 0) {
                ServerProfile.readErr.incrementAndGet();
            }
            rps.putL2(rc);
        }
        if (i == 0)
            lookupCache.put(set + "." + md5, "2#" + info);
        if (ServerConf.getSs_id() == ServerConf.serverId) {
            RocksDBInterface.getRocks(rocksid).delete(set + "@" + md5);
        }
    }

    public void delSet(String set) {
        // 删除每个磁盘上的该集合
        for (String d : diskArray)
            delFile(new File(d + "/" + conf.destRoot + set));
        // 删除一个集合后,同时删除关于该集合的全局的上下文
        for (String d : diskArray) {
            StoreSetContext ssc = writeContextHash.get(set + ":" + d);
            if (ssc != null) {
                synchronized (ssc) {
                    if (ssc.raf != null) {
                        try {
                            ssc.raf.close();
                            ssc.raf = null;
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    writeContextHash.remove(set + ":" + d);
                    // clean up the RPS cached set->pid entry
                    if (rps != null)
                        rps.__cleanup_cached(set);
                }
            }
        }
    }

    /**
     * 删除文件. 如果是一个文件,直接删除,如果是文件夹,递归删除子文件夹和文件
     *
     * @param f
     */
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

    public Set<String> getSetElements(String set) {
        Set<String> r = null;
        RedisConnection rc = null;

        try {
            rc = rps.getL2(set, false);
            Jedis jedis = rc.jedis;
            if (jedis != null)
                r = jedis.hkeys(set);
        } catch (JedisConnectionException e) {
            System.out.println(set + ": Jedis connection broken in getSetElements");
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println(set + ": Exception: " + e.getMessage());
            e.printStackTrace();
        } finally {
            rps.putL2(rc);
        }
        return r;
    }

    public static class SetStats {
        public long rnr; // record nr
        public long fnr; // file nr

        public SetStats(long rnr, long fnr) {
            this.rnr = rnr;
            this.fnr = fnr;
        }
    }

    /**
     * 获得redis中每个set的块数，存在hash表里，键是[集合名，该集合内的文件数]，值是块数
     *
     * @return
     */
    public TreeMap<String, SetStats> getSetBlks() {
        TreeMap<String, SetStats> temp = new TreeMap<String, SetStats>();

        for (Map.Entry<String, RedisPool> entry : rps.getRpL2().entrySet()) {
            Jedis jedis = entry.getValue().getResource();
            try {
                if (jedis != null) {
                    Set<String> keys = jedis.keys("*.blk.*");

                    if (keys != null && keys.size() > 0) {
                        String[] keya = keys.toArray(new String[0]);
                        List<String> vals = jedis.mget(keya);

                        for (int i = 0; i < keya.length; i++) {
                            String set = keya[i].split("\\.")[0];
                            temp.put(set,
                                    new SetStats(jedis.hlen(set),
                                            temp.containsKey(set)
                                                    ? temp.get(set).fnr + Integer.parseInt(vals.get(i)) + 1
                                                    : Integer.parseInt(vals.get(i)) + 1));
                        }
                    }
                }
            } catch (JedisConnectionException e) {
                System.out.println("getSetBlks: Jedis connection exception: " + e.getMessage());
                temp = null;
                break;
            } catch (Exception e) {
                e.printStackTrace();
                temp = null;
                break;
            } finally {
                entry.getValue().putInstance(jedis);
            }
        }
        return temp;
    }

    public Map<String, String> getDedupInfo() {
        for (Map.Entry<String, RedisPool> entry : rps.getRpL2().entrySet()) {
            Jedis jedis = entry.getValue().getResource();
            if (jedis != null) {
                try {
                    Map<String, String> di = new HashMap<String, String>();
                    ScanParams sp = new ScanParams();
                    sp.match("*");
                    boolean isDone = false;
                    String cursor = ScanParams.SCAN_POINTER_START;

                    while (!isDone) {
                        ScanResult<Entry<String, String>> r = jedis.hscan("mm.dedup.info", cursor, sp);
                        for (Entry<String, String> entry2 : r.getResult()) {
                            di.put(entry2.getKey(), entry2.getValue());
                        }
                        cursor = r.getStringCursor();
                        if (cursor.equalsIgnoreCase("0")) {
                            isDone = true;
                        }
                    }
                    return di;
                } catch (JedisConnectionException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    entry.getValue().putInstance(jedis);
                }
            }
        }

        return null;
    }

    public String getClientConfig(String field) {
        Jedis jedis = getRpL1().getResource();

        if (jedis != null) {
            try {
                String di = jedis.hget("mm.client.conf", field);
                return di;
            } catch (JedisConnectionException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                getRpL1().putInstance(jedis);
            }
        }

        return null;
    }

    // 关闭jedis连接,关闭文件访问流?
    public static void quit() {
        try {
            if (rps != null)
                rps.quit();
            if (rpL1 != null)
                rpL1.quit();
        } finally {
        }
    }

    // 释放单实例所使用的资源
    public void close() {
    }

    /**
     * Image Search based BufferedImage
     *
     * @param bitDiff
     * @param d
     * @return
     * @throws IOException
     */
    public ResultSet imageSearch(BufferedImage bi, int d, int bitDiff) throws IOException {
        ResultSet rs = new ResultSet(ResultSet.ScoreMode.PROD);

        for (FeatureType feature : conf.getFeatures()) {
            switch (feature) {
                case IMAGE_PHASH_ES:
                    String hc = new ImagePHash().getHash(bi);
                    rs.addAll(FeatureIndex.getObject(hc, ServerConf.getFeatureTypeString(feature), d, bitDiff));
                    break;
            }
        }

        return rs;
    }

    /**
     * Search by features
     *
     * @param features
     * @return
     * @throws IOException
     */
    public ResultSet featureSearch(BufferedImage bi, List<Feature> features) throws IOException {
        ResultSet rs = new ResultSet(ResultSet.ScoreMode.PROD);

        for (Feature feature : features) {
            switch (feature.type) {
                case IMAGE_PHASH_ES: {
                    int maxEdits = 4, bitDiffInBlock = 0;

                    if (feature.args != null && feature.args.size() >= 2) {
                        maxEdits = Integer.parseInt(feature.args.get(0));
                        bitDiffInBlock = Integer.parseInt(feature.args.get(1));
                    }
                    rs.addAll(FeatureIndex.getObject(feature.value, ServerConf.getFeatureTypeString(feature.type), maxEdits,
                            bitDiffInBlock));
                    break;
                }
                case IMAGE_LIRE: {
                    int maxHits = 100;
                    FeatureLIREType sType = FeatureLIREType.CEDD;
                    FeatureLIREType fType = FeatureLIREType.NONE;

                    if (feature.args != null) {
                        if (feature.args.size() >= 3) {
                            maxHits = Integer.parseInt(feature.args.get(0));
                            sType = Feature.getFeatureLIREType(feature.args.get(1));
                            fType = Feature.getFeatureLIREType(feature.args.get(2));
                        } else if (feature.args.size() >= 2) {
                            maxHits = Integer.parseInt(feature.args.get(0));
                            sType = Feature.getFeatureLIREType(feature.args.get(1));
                        } else if (feature.args.size() >= 1) {
                            maxHits = Integer.parseInt(feature.args.get(0));
                        }
                    }
                    if (bi != null) {
                        rs.addAll(FeatureIndex.getObjectLIRE(sType, fType, bi, maxHits));
                    }
                    break;
                }
                case IMAGE_FACES: {
                    int maxHits = 100;
                    FeatureLIREType sType = FeatureLIREType.CEDD;
                    FeatureLIREType fType = FeatureLIREType.NONE;

                    if (feature.args != null) {
                        if (feature.args.size() >= 3) {
                            maxHits = Integer.parseInt(feature.args.get(0));
                            sType = Feature.getFeatureLIREType(feature.args.get(1));
                            fType = Feature.getFeatureLIREType(feature.args.get(2));
                        } else if (feature.args.size() >= 2) {
                            maxHits = Integer.parseInt(feature.args.get(0));
                            sType = Feature.getFeatureLIREType(feature.args.get(1));
                        } else if (feature.args.size() >= 1) {
                            maxHits = Integer.parseInt(feature.args.get(0));
                        }
                    }
                    if (bi != null) {
                        rs.addAll(FeatureIndex.getObjectFaces(sType, fType, bi, maxHits));
                    }
                    break;
                }
            }
        }

        return rs;
    }

    public boolean swapOut(String set) {
        RedisConnection rc = null;

        try {
            rc = rps.getL2(set, false);
            if (rc.rp == null || rc.jedis == null) {
                return false;
            }
            if (sha3 == null) {
                loadScripts3();
            }
            Jedis jedis = rc.jedis;

            int rocksid = getRocksid(set);
            Set<Entry<String, String>> mi = jedis.hgetAll(set).entrySet();

            int dupNum = getDupNum();

            for (Entry<String, String> e : mi) {
                String md5 = e.getKey(), info = e.getValue();
                int lmaster = 0;
                if ('#' == info.charAt(1)) {
                    String setS = String.valueOf(info.charAt(0));
                    info = info.substring(2);
                    if (info.split("@|#").length % conf.getVlen() != 0)
                        continue;
                    if (info.split("#").length < dupNum)
                        continue;
                    if ("1".equals(setS))
                        continue;
                    if ("2".equals(setS)) {
                        System.out.println("Purge: " + set + "@" + md5 + " is deleted, doing clear");
                        RocksDBInterface.getRocks(rocksid).delete(set + "@" + md5);
                        jedis.hdel(set, md5);
                        continue;
                    }

                    if ("3".equals(setS))
                        lmaster = 3;
                    if ("4".equals(setS))
                        lmaster = 4;
                }

                if (ServerConf.getSs_id() == ServerConf.serverId && lmaster != 3) {
                    int i = 3;if (lmaster == 4) i = 1;
                    info = i + "#" + info;
                } else if (ServerConf.getLsId() == ServerConf.serverId && lmaster != 4) {
                    int i = 4;if (lmaster == 3) i = 1;
                    info = i + "#" + info;
                }
                RocksDBInterface.getRocks(rocksid).write(set + "@" + md5, info);
                jedis.evalsha(sha3, 1, set, md5, info);
            }
        } catch (RException e) {
            e.printStackTrace();
        } finally {
            rps.putL2(rc);
        }
        return true;
    }

    private ObjectContent swapIn(String set, String md5, long offset, int length) throws RedirectException {

        RedisConnection rc = null;

        try {
            rc = rps.getL2(set, false);
            if (rc.rp == null || rc.jedis == null) {
                return null;
            }

            if (sha2 == null) {
                loadScripts2();
            }

            Jedis jedis = rc.jedis;
            if (ServerConf.getSs_id() == ServerConf.serverId || ServerConf.getLsId() == ServerConf.serverId) {
                // ok, do search
                int rocksid = getRocksid(set);
                String info = RocksDBInterface.getRocks(rocksid).read(set + "@" + md5);

                if (info == null)
                    nilSetHash.put(set + "." + md5, "2#");

                if (info != null) {
                    // 写到redis
                    jedis.evalsha(sha2, 1, set, md5, info);
                    lookupCache.put(set + "@" + md5, info);
                    if ('#' == info.charAt(1))
                        info = info.substring(2);
                    // split if it is complex uri
                    String savedInfo = null;
                    Long savedId = -1L;
                    for (String i : info.split("#")) {
                        String[] is = i.split("@");
                        if (Long.parseLong(is[2]) == ServerConf.serverId) {
                            if (length != -1)
                                return searchPhoto(i, is, null, offset, length);
                            else
                                return searchPhoto(i, is, null);
                        } else {
                            savedInfo = i;
                            savedId = Long.parseLong(is[2]);
                        }
                    }
                    throw new RedirectException(savedId, savedInfo, offset, length);
                }
            } else {
                throw new RedirectException(ServerConf.getSs_id(), set + "@" + md5, offset, length);
            }
        } catch (RException e) {
            e.printStackTrace();
        } finally {
            rps.putL2(rc);
        }
        return null;
    }

    private String setL1Seq(String set) {
        Jedis jedis = null;
        String time = System.currentTimeMillis() + "";
        try {
            jedis = StorePhoto.getRpL1(conf).getResource();
            jedis.set(set + ".seq", time);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            StorePhoto.getRpL1(conf).putInstance(jedis);
        }
        return time;
    }

    private void addHeat(String set, String md5) {
        Jedis jedis = null;
        try {
            jedis = StorePhoto.getRpL1(conf).getResource();
            int period = Integer.parseInt(jedis.hget("mm.client.conf", "heat"));
            String key = "h." + set + "@" + md5;
            Pipeline pi = jedis.pipelined();
            pi.set(key, "1");
            pi.expire(key, period);
            pi.sync();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            StorePhoto.getRpL1(conf).putInstance(jedis);
        }
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
        return 0;
    }

    private int getDupNum() {
        Jedis jedis = null;
        try {
            jedis = StorePhoto.getRpL1(conf).getResource();
            String dupNum = jedis.hget("mm.client.conf", "dupnum");
            if (dupNum != null) {
                int dn = Integer.parseInt(dupNum);
                return dn;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            StorePhoto.getRpL1(conf).putInstance(jedis);
        }
        return 1;
    }

    public byte[] getFileInfo(String set, String md5) throws RedirectException{
        if (nilSetHash.containsKey(set)) {
            System.out.println("MM getFileInfo: md5: " + md5 + " doesn't exist in set: " + set);
            return null;
        }
        addHeat(set, md5);

        RedisConnection rc = null;
        String info = null;
        int err = 0;
        try {
            rc = rps.getL2(set, false);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        if (rc.rp == null || rc.jedis == null) {
            System.out.println("MM getFileInfo: " + set + "@" + md5 + ": get L2 pool " + rc.id + " failed.");
            return null;
        }
        Jedis jedis = rc.jedis;

        // Step 1: check the local lookup cache
        info = (String) lookupCache.get(set + "." + md5);
        try {
            if (info == null) {
                info = jedis.hget(set, md5);
                if (info == null) {
		    if (!(ServerConf.getSs_id() == ServerConf.serverId || ServerConf.getLsId() == ServerConf.serverId))
			throw new RedirectException(ServerConf.getSs_id(), set + "@" + md5, 0, 0);
                    int rocksid = getRocksid(set);
                    info = RocksDBInterface.getRocks(rocksid).read(set + "@" + md5);
                }
		if (info != null)
                	lookupCache.put(set + "." + md5, info);
		else
		return null;
            }
        } catch (JedisConnectionException e) {
            err = -1;
            return null;
        } catch (JedisException e) {
            err = -1;
            return null;
        } finally {
            if (err < 0) {
                ServerProfile.readErr.incrementAndGet();
            }
            rps.putL2(rc);
        }

        if ('2' == info.charAt(0)) {
            System.out.println(set + "." + md5 + "is deleted");
            return null;
        }

        if ('#' == info.charAt(1))
            info = info.substring(2);

        return info.getBytes();
    }
}
