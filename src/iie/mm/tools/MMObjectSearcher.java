package iie.mm.tools;

import iie.mm.client.Feature.FeatureTypeString;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.jetty.server.Server;

public class MMObjectSearcher {
	public static SearcherConf conf;
	
	public static class Option {
	     String flag, opt;
	     public Option(String flag, String opt) { this.flag = flag; this.opt = opt; }
	}

	public static void main(String[] args) {
		List<String> argsList = new ArrayList<String>();  
	    List<Option> optsList = new ArrayList<Option>();
	    List<String> doubleOptsList = new ArrayList<String>();
	    
	    // parse the args
	    for (int i = 0; i < args.length; i++) {
			System.out.println("Args " + i + ", " + args[i]);
			switch (args[i].charAt(0)) {
	        case '-':
	            if (args[i].length() < 2)
	                throw new IllegalArgumentException("Not a valid argument: "+args[i]);
	            if (args[i].charAt(1) == '-') {
	                if (args[i].length() < 3)
	                    throw new IllegalArgumentException("Not a valid argument: "+args[i]);
	                doubleOptsList.add(args[i].substring(2, args[i].length()));
	            } else {
	                if (args.length-1 > i)
	                    if (args[i + 1].charAt(0) == '-') {
	                    	optsList.add(new Option(args[i], null));
	                    } else {
	                    	optsList.add(new Option(args[i], args[i+1]));
	                    	i++;
	                    }
	                else {
	                	optsList.add(new Option(args[i], null));
	                }
	            }
	            break;
	        default:
	            // arg
	            argsList.add(args[i]);
	            break;
	        }
		}
	    
	    String uri = null;
	    int port = 40001;
	    
	    for (Option o : optsList) {
	    	if (o.flag.equals("-h")) {
	    		// print help message
	    		System.out.println("-h    : print this help.");
	    		System.out.println();
	    		System.exit(0);
	    	}
	    	if (o.flag.equals("-uri")) {
	    		// set uri
	    		if (o.opt == null) {
	    			System.out.println("-uri SERVER_URI");
	    			System.exit(0);
	    		}
	    		uri = o.opt;
	    	}
	    	if (o.flag.equals("-p")) {
	    		// set http port
	    		if (o.opt == null) {
	    			System.out.println("-p port");
	    			System.exit(0);
	    		}
	    		port = Integer.parseInt(o.opt);
	    	}
	    }
		try {
			conf = new SearcherConf(uri, port);
			conf.addToFeatures(FeatureTypeString.IMAGE_PHASH_ES);
			conf.addToFeatures(FeatureTypeString.IMAGE_LIRE);
			Server server = new Server(conf.port);
			server.setHandler(new MMObjectSearcherHandler(conf));
			server.start();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
}
