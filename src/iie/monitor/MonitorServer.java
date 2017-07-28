package iie.monitor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.eclipse.jetty.server.Server;

public class MonitorServer {
	public static void main(String[] args) {
		HashMap<String, String> addrMap = new HashMap<String, String>();
		HashMap<String, String> mmAddrMap = new HashMap<String, String>();
		String targetPath = "sotstore/sotstore/reports";
		String mmPath = "sotstore/dservice/log";
		String redisPath = "sotstore/dservice/log";
		File f;
		int port = 33333;
		
		if (args.length == 0) {
			System.out.println("use default port: " + port);
			f = new File("conf/monitor.conf");
		} else if (args.length == 1) {
			try {
				port = Integer.parseInt(args[0]);
			} catch (NumberFormatException e) {
				System.out.println("wrong format of port:"+args[0]);
				System.exit(0);
			}
			f = new File("conf/monitor.conf");
			System.out.println("use port: " + port);
		} else {
			f = new File(args[1]);
		}
		
		if (args.length == 3) {
			// this is target path
			targetPath = args[2];
		}
		
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(f));
			String line = br.readLine();
		
			while (line != null) {
				String[] pair = line.split("=");
				if (pair.length == 2) {
					if (pair[0].startsWith("mm.")) {
						mmAddrMap.put(pair[0].substring(3), pair[1]);
					} else {
						addrMap.put(pair[0], pair[1]);
					}
				}
				line = br.readLine();
			}
			System.out.println(addrMap);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(0);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		Server s = new Server(port);
		s.setHandler(new MonitorHandler(addrMap, mmAddrMap, targetPath, mmPath, redisPath));
		try {
			s.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
