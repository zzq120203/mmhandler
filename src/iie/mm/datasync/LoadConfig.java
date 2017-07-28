/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package iie.mm.datasync;

import java.util.ArrayList;
import java.util.List;

import iie.mm.dao.GlobalConfig;

public class LoadConfig {

	private final GlobalConfig gconf;

	public static class Option {
		String flag, opt;

		public Option(String flag, String opt) {
			this.flag = flag;
			this.opt = opt;
		}
	}

	public LoadConfig(String[] args, GlobalConfig gc) {
		gconf = gc;
		loadConfigFile(args);
	}

	public final void loadConfigFile(String[] args) {
		List<String> argsList = new ArrayList<String>();
		List<Option> optsList = new ArrayList<Option>();
		List<String> doubleOptsList = new ArrayList<String>();

		for (int i = 0; i < args.length; i++) {
			System.out.println("Args " + i + ", " + args[i]);
			switch (args[i].charAt(0)) {
			case '-':
				if (args[i].length() < 2)
					throw new IllegalArgumentException("Not a valid argument: " + args[i]);
				if (args[i].charAt(1) == '-') {
					if (args[i].length() < 3)
						throw new IllegalArgumentException("Not a valid argument: " + args[i]);
					doubleOptsList.add(args[i].substring(2, args[i].length()));
				} else {
					if (args.length - 1 > i)
						if (args[i + 1].charAt(0) == '-') {
							optsList.add(new Option(args[i], null));
						} else {
							optsList.add(new Option(args[i], args[i + 1]));
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

		for (Option o : optsList) {
			if (o.flag.equals("-h")) {
				// print help message
				System.out.println("-h          : print this help.");
				System.out.println("-surl       : source address");
				System.out.println("-durl       : destination address");
				System.out.println("-path       : data temp path");
				System.out.println("-mbq        : maxBlockQueue number , 10");
				System.out.println("-mlptc      : maxLogicProcThreadCount number , 10");
				System.out.println("-mttc       : maxTransmitThreadCount number , 10");
				System.out.println("-mbqc       : maxBlockingQueueCapacity number , 100");
				System.out.println("-synct      : sync Time (S) , 60");

				System.exit(0);
			}
			
			if (o.flag.equals("-stmpp")) {
                if (o.opt == null) {
                        System.out.println("-stmpp start update MPP true/false");
                        System.exit(0);
                }
                gconf.startUpdateMpp = Boolean.parseBoolean(o.opt);
			}
			if (o.flag.equals("-mppd")) {
                                if (o.opt == null) {
                                        System.out.println("-mppd MPP Driver");
                                        System.exit(0);
                                }
                                gconf.mppDriver = o.opt;
                        }
			if (o.flag.equals("-mppurl")) {
                                if (o.opt == null) {
                                        System.out.println("-mppurl MPP URL");
                                        System.exit(0);
                                }
                                gconf.mppUrl = o.opt;
                        }
			if (o.flag.equals("-mppu")) {
                                if (o.opt == null) {
                                        System.out.println("-mppu MPP User");
                                        System.exit(0);
                                }
                                gconf.mppUser = o.opt;
                        }
			if (o.flag.equals("-mppw")) {
                                if (o.opt == null) {
                                        System.out.println("-mppw mpp Pass word");
                                        System.exit(0);
                                }
                                gconf.mppPwd = o.opt;
                        }

			if (o.flag.equals("-mbq")) {
				if (o.opt == null) {
					System.out.println("-mbq maxBlockQueue number");
					System.exit(0);
				}
				gconf.maxBlockQueue = Integer.parseInt(o.opt);
			}
			if (o.flag.equals("-mlptc")) {
				if (o.opt == null) {
					System.out.println("-mlptc maxLogicProcThreadCount number");
					System.exit(0);
				}
				gconf.maxLogicProcThreadCount = Integer.parseInt(o.opt);
			}
			if (o.flag.equals("-mttc")) {
				if (o.opt == null) {
					System.out.println("-mttc maxTransmitThreadCount number");
					System.exit(0);
				}
				gconf.maxTransmitThreadCount = Integer.parseInt(o.opt);
			}
			if (o.flag.equals("-mbqc")) {
				if (o.opt == null) {
					System.out.println("-mbqc maxBlockingQueueCapacity number");
					System.exit(0);
				}
				gconf.maxBlockingQueueCapacity = Integer.parseInt(o.opt);
			}
			if (o.flag.equals("-synct")) {
				if (o.opt == null) {
					System.out.println("-synct sync Time (S)");
					System.exit(0);
				}
				gconf.syncTime = Integer.parseInt(o.opt);
			}

			if (o.flag.equals("-surl")) {
				if (o.opt == null) {
					System.out.println("-surl ");
					System.exit(0);
				}
				gconf.sRedisURL = o.opt;
				
				try {
					gconf.scp.init(o.opt, "DS");
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			if (o.flag.equals("-durl")) {
				if (o.opt == null) {
					System.out.println("-durl ");
					System.exit(0);
				}
				gconf.dRedisURL = o.opt;
				try {
					gconf.dcp.init(o.opt, "DS");
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
			if (o.flag.equals("-path")) {
				if (o.opt == null) {
					System.out.println("-path ");
					System.exit(0);
				}
				gconf.setPath = o.opt;
			}

		}
		System.out.println(gconf.toString());
	}

}
