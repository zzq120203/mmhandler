package iie.mm.server;

import redis.clients.jedis.Jedis;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 需要修改 set写操作完成后创建swapoutThread
 *
 * @author zzq
 */
public class SwapOut implements Runnable {

    private StorePhoto sp;
    private long lastFetch = System.currentTimeMillis();
    private ServerConf conf;
    private static ConcurrentHashMap<String, BlockingQueue<SwapOutTask>> ss = new ConcurrentHashMap<String, BlockingQueue<SwapOutTask>>();

    public static ConcurrentHashMap<String, BlockingQueue<SwapOutTask>> getSS() {
        return ss;
    }

    ;

    public SwapOut(ServerConf conf) {
        this.conf = conf;
    }

    @Override
    public void run() {
        while (true) {
            long cur = System.currentTimeMillis();
            Jedis jedis = StorePhoto.getRpL1(conf).getResource();
            try {
                if (cur - lastFetch >= conf.getMemCheckInterval()) {
                    lastFetch = cur;
                    if (sp == null)
                        sp = new StorePhoto(conf);
                    Set<String> seqs = jedis.keys("*.seq");
                    for (String seq : seqs) {
                        String s = jedis.get(seq);
                        String set = seq.split("\\.")[0];
                        String rocks = jedis.hget("rocksid", set);
                        int rocksid = 0;
                        if (rocks != null)
                            rocksid = Integer.parseInt(rocks);
                        if(s.equals(RocksDBInterface.getRocks(rocksid).read(seq)))
                            continue;

                        ConcurrentHashMap<String, BlockingQueue<WriteTask>> sqw = Handler.getWriteSq();
                        if (sqw != null) {
                            BlockingQueue<WriteTask> bqw = sqw.get(set);
                            if (bqw != null)
                                continue;
                        }

                        SwapOutTask t = new SwapOutTask(set, seq, s);

                        BlockingQueue<SwapOutTask> bq = ss.get(set);

                        if (bq != null) {
                            // 存在这个键,表明该线程已经存在,直接把任务加到任务队列里即可
                            bq.add(t);
                        } else {
                            // 如果不存在这个键,则需要新开启一个线程
                            BlockingQueue<SwapOutTask> tasks = new LinkedBlockingQueue<SwapOutTask>();
                            tasks.add(t);
                            ss.put(set, tasks);
                            SwapOutThread st = new SwapOutThread(conf, set, ss, rocksid);
                            new Thread(st).start();
                        }

                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                StorePhoto.getRpL1(conf).putInstance(jedis);
            }
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
