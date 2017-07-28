package iie.mm.server;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author zzq
 */
public class SwapOutThread implements Runnable {

    private String set;
    private ServerConf conf;
    private ConcurrentHashMap<String, BlockingQueue<SwapOutTask>> ss;
    private BlockingQueue<SwapOutTask> tasks;
    private int rocksid = 0;

    public SwapOutThread(ServerConf conf, String set, ConcurrentHashMap<String, BlockingQueue<SwapOutTask>> ss, int rocksid) {
        this.set = set;
        this.ss = ss;
        this.tasks = ss.get(set);
        this.conf = conf;
        this.rocksid = rocksid;
    }

    @Override
    public void run() {
        try {
            StorePhoto sp = new StorePhoto(conf);
            while (true) {
                //该线程退出条件，60秒内没有新的任务，或者在删除该集合时，手动插入一个md5为空的任务
                SwapOutTask t = tasks.poll(60l, TimeUnit.SECONDS);

                if (t == null || t.getSet() == null) {
                    ss.remove(set);
                    break;
                }
                boolean b = sp.swapOut(t.getSet());
                if (b)
                    RocksDBInterface.getRocks(rocksid).write(t.getSeq(), t.getS());
            }
            sp.close();
            System.out.println(Thread.currentThread() + "set: " + set + " SwapOutTask 结束");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
