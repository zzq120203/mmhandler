package iie.monitor;

import org.eclipse.jetty.server.Server;

public class HMonitorServer {
	public static void main(String[] args) {
		int port = 50505;
		
		if (args.length == 1) {
			try {
				port = Integer.parseInt(args[0]);
			} catch (NumberFormatException nfe) {
				System.out.println("Wrong format of port: " + args[0]);
				System.exit(0);
			}
		}
		
		Server s = new Server(port);
		s.setHandler(new HMonitorHandler());
		try {
			s.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
