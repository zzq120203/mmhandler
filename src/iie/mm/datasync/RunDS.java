package iie.mm.datasync;

import org.eclipse.jetty.server.Server;

import iie.mm.asr.ASR;
import iie.mm.asr.AsrHTTP;
import iie.mm.asr.AsrHealth;
import iie.mm.dao.GlobalConfig;

public class RunDS {
	private static GlobalConfig gconf = GlobalConfig.getConfig();

	public static void main(String[] args) {
		new LoadConfig(args, gconf);
		
		Server server = new Server(gconf.httpprot);
		server.setHandler(new AsrHTTP());
		try {
			server.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		AsrHealth ah = new AsrHealth(gconf);
		Thread ahThread = new Thread(ah);
		ahThread.start();
		
		DataSourceKey dsk = new DataSourceKey(gconf);
		Thread dskThread = new Thread(dsk);
		dskThread.start();
		
		DataSyncPut dsp = DataSyncPut.getDataSyncPut();
		dsp.init(gconf, dsk.getPutQueue());
		
		ASR asr = new ASR(gconf, dsp.getAsrQueueArr());
		asr.runThrow();

		DataSyncPutdown dspd = new DataSyncPutdown(gconf, dsp.getBigArrSrcQueue());
		dspd.runThrow();
		DataSyncPutup dspu = new DataSyncPutup(gconf);
		dspu.runThrow();

		DataSyncDel dsd = new DataSyncDel(gconf, dsk.getDelQueue());
		dsd.runThrow();
	}

}
