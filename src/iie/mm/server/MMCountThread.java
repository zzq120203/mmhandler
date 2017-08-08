package iie.mm.server;

import iie.mm.server.PhotoServer;
import iie.mm.server.ServerConf;

public class MMCountThread implements Runnable{

	private ServerConf conf;
	
	public MMCountThread(ServerConf conf) {
		super();
		this.conf = conf;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		while (true) {
			PhotoServer.mmCount(conf);
			try {
				Thread.sleep(5*60*1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
	}

}
