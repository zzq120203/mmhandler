package iie.metastore;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.metastore.msg.MSGFactory;
import org.apache.hadoop.hive.metastore.msg.MSGFactory.DDLMsg;
import org.apache.hadoop.hive.metastore.msg.MSGType;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.client.producer.SendResult;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;

public class SendMsgTool {
	static Producer producer = null;
	
	public static class Producer {
		private static Producer instance = null;
		private final MetaClientConfig metaClientConfig = new MetaClientConfig();
		private final ZKConfig zkConfig = new ZKConfig();
		private MessageSessionFactory sessionFactory = null;
		// create producer,强烈建议使用单例
		private MessageProducer producer = null;
		// publish topic
		private static String topic = "oldms";

		private Producer(String zkAddr, String itopic) {
			zkConfig.zkConnect = zkAddr;
			if (itopic != null) topic = itopic;
		}

		public void connect(String zkAddr, String itopic) {
			//设置zookeeper地址
			if (itopic != null)
				topic = itopic;
			zkConfig.zkConnect = zkAddr;
			metaClientConfig.setZkConfig(zkConfig);
			try {
				sessionFactory = new MetaMessageSessionFactory(metaClientConfig);
				producer = sessionFactory.createProducer();
				producer.publish(topic);
			} catch (MetaClientException e){
				e.printStackTrace();
			}
			System.out.println("Topic '" + topic + "' has been published.");
		}

		public static Producer getInstance(String zkAddr, String itopic) throws MetaClientException {
			if (instance == null) {
				instance = new Producer(zkAddr, itopic);
				instance.connect(zkAddr, itopic);
			}
			return instance;
		}

		boolean sendMsg(String msg) throws MetaClientException, InterruptedException{
			if (producer == null) {
				connect(zkConfig.zkConnect, topic);
				if (producer == null) {
					return false;
				}
			}
			SendResult sendResult = producer.sendMessage(
					new Message(topic, msg.getBytes()));

			boolean success = sendResult.isSuccess();
			if (!success) {
				System.out.println("Send message failed, error message: " + sendResult.getErrorMessage());
			} else {
				System.out.println("Send message successfully, sent to " + sendResult.getPartition());
			}
			return success;
		}
	}
	
	public static class Option {
	     String flag, opt;
	     public Option(String flag, String opt) { this.flag = flag; this.opt = opt; }
	}
	
	private static DDLMsg fillParams(int eventid, String opargs) {
		HashMap<String, Object> params = new HashMap<String, Object>();
		String[] oa = opargs.split(",");

		for (int i = 0; i < oa.length; i++) {
			String[] kv = oa[i].split("=");

			params.put(kv[0], kv[1]);
		}
		long now = new Date().getTime()/1000;
		return new MSGFactory.DDLMsg(eventid, -1l, null, null, 0, -1l, -1l, 
				now, null, params);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		List<String> argsList = new ArrayList<String>();  
	    List<Option> optsList = new ArrayList<Option>();
	    List<String> doubleOptsList = new ArrayList<String>();
	    String zkAddr = null, itopic = null;
		String op = null, opargs = null;
		boolean manual = false;
		
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
	    
	    for (Option o : optsList) {
	    	if (o.flag.equals("-h")) {
	    		// print help message
	    		System.out.println("-h   : print this help.");
	    	}
	    	if (o.flag.equals("-zk")) {
	    		// get zk address
	    		if (o.opt == null) {
	    			System.out.println("-zk ZKADDR");
	    			System.exit(0);
	    		}
	    		zkAddr = o.opt;
	    	}
	    	if (o.flag.equals("-t")) {
	    		// get topic
	    		if (o.opt == null) {
	    			System.out.println("-t TOPIC");
	    			System.exit(0);
	    		}
	    		itopic = o.opt;
	    	}
	    	if (o.flag.equals("-op")) {
	    		// get operation
	    		if (o.opt == null) {
	    			System.out.println("-op OPERATION");
	    			System.exit(0);
	    		}
	    		op = o.opt;
	    	}
	    	if (o.flag.equals("-args")) {
	    		// get operation args: key1=value1,key2=value2,....
	    		if (o.opt == null) {
	    			System.out.println("-args OPERATION_ARGS");
	    			System.exit(0);
	    		}
	    		opargs = o.opt;
	    	}
	    	if (o.flag.equals("-m")) {
	    		// display manual for op args
	    		manual = true;
	    	}
	    }
	    
	    // construct the message
	    DDLMsg msg = null;
	    
	    if (op == null) {
	    	System.out.println("You have to specify -op OPERATION.");
	    	System.exit(0);
	    }
	    if (op.equalsIgnoreCase("create schema")) {
	    	if (manual) {
	    		System.out.println("[CREATE SCHEMA]: schema_name");
	    	} else {
	    		msg = fillParams(MSGType.MSG_CREATE_SCHEMA, opargs);
	    	}
	    } else if (op.equalsIgnoreCase("modify schema name")) {
	    	if (manual) {
	    		System.out.println("[MODIFY SCHEMA NAME]: schema_name, old_schema_name");
	    	} else {
	    		msg = fillParams(MSGType.MSG_MODIFY_SCHEMA_NAME, opargs);
	    	}
	    } else if (op.equalsIgnoreCase("del schema")) {
	    	if (manual) {
	    		System.out.println("[DEL SCHEMA]: schema_name");
	    	} else {
	    		msg = fillParams(MSGType.MSG_DEL_SCHEMA, opargs);
	    	}
	    } else if (op.equalsIgnoreCase("alter nodegroup")) {
	    	if (manual) {
	    		System.out.println("[ALTER NODEGROUP]: nodegroup_name");
	    	} else {
	    		msg = fillParams(MSGType.MSG_ALTER_NODEGROUP, opargs);
	    	}
	    } else if (op.equalsIgnoreCase("del nodegroup")) {
	    	if (manual) {
	    		System.out.println("[DEL NODEGROUP]: nodegroup_name");
	    	} else {
	    		msg = fillParams(MSGType.MSG_DEL_NODEGROUP, opargs);
	    	}
	    } else if (op.equalsIgnoreCase("CREATE DEVICE")) {
	    	if (manual) {
	    		System.out.println("[CREATE DEVICE]: devid, node, status");
	    	} else {
	    		msg = fillParams(MSGType.MSG_CREATE_DEVICE, opargs);
	    	}
	    } else if (op.equalsIgnoreCase("del device")) {
	    	if (manual) {
	    		System.out.println("[DEL DEVICE]: devid");
	    	} else {
	    		msg = fillParams(MSGType.MSG_DEL_DEVICE, opargs);
	    	}
	    } else if (op.equalsIgnoreCase("new node")) {
	    	if (manual) {
	    		System.out.println("[NEW NODE]: node_name");
	    	} else {
	    		msg = fillParams(MSGType.MSG_NEW_NODE, opargs);
	    	}
	    }
	    
	    try {
			Producer p = Producer.getInstance(zkAddr, itopic);
			String jsonMsg = MSGFactory.getMsgData2(msg);
			System.out.println(jsonMsg);
			//p.sendMsg(jsonMsg);
		} catch (MetaClientException e) {
			e.printStackTrace();
		}
	    
	}

}
