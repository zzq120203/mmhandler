package iie.mm.datasync;

import iie.mm.dao.GlobalConfig;

public class RunDS {
	private static GlobalConfig gconf = new GlobalConfig();

	public static void main(String[] args) {
		new LoadConfig(args, gconf);
		
		DataSourceKey dsk = new DataSourceKey(gconf);
		Thread dskThread = new Thread(dsk);
		dskThread.start();
		
		DataSyncPut dsp = new DataSyncPut(gconf, dsk.getPutQueue());
                dsp.runThrow();

		DataSyncPutdown dspd = new DataSyncPutdown(gconf, dsp.getBigArrSrcQueue());
		dspd.runThrow();
		DataSyncPutup dspu = new DataSyncPutup(gconf);
		dspu.runThrow();

		DataSyncDel dsd = new DataSyncDel(gconf, dsk.getDelQueue());
		dsd.runThrow();
	}

}
