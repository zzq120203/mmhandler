package iie.mm.server;

import org.fusesource.lmdbjni.*;

import java.io.File;

import static org.fusesource.lmdbjni.Constants.*;

public class LMDBInterface {
    public static class LMDB {
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

        public long count() {
            return db.stat().ms_entries;
        }

        public String first() {
            Transaction tx = env.createTransaction(true);
            try {
                Cursor cursor = db.openCursor(tx);
                try {
                    Entry entry = cursor.get(FIRST);
                    if (entry != null)
                        return string(entry.getKey());
                    else
                        return null;
                } finally {
                    // Make sure you close the cursor to avoid leaking reasources.
                    cursor.close();
                }
            } finally {
                // Make sure you commit the transaction to avoid resource leaks.
                tx.commit();
            }
        }

        public long firstTS() {
            String f = first();
            String r = null;
            long ts = -1;

            try {
                if (f != null && !f.equals("")) {
                    String[] fa = f.split("\\.");
                    if (fa.length > 0 && fa[0].length() > 2) {
                        r = fa[0].substring(2);
                    }
                }
                if (r != null) {
                    try {
                        if (Character.isDigit(r.charAt(0))) {
                            ts = Long.parseLong(r);
                        } else {
                            ts = Long.parseLong(r.substring(1));
                        }
                    } catch (Exception e) {
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            return ts;
        }

        public void close() {
            try {
                if (db != null)
                    db.close();
            } finally {
                env.close();
            }
        }
    }

    private static LMDB lmdb = new LMDB();
    private static boolean isOpenned = false;

    public static LMDB getLmdb() {
        return lmdb;
    }

    public LMDBInterface(ServerConf conf) {
        if (!isOpenned)
            isOpenned = lmdb.open(conf.getLmdb_prefix());
    }

    public void LMDBClose() {
        if (isOpenned)
            lmdb.close();
    }
}
