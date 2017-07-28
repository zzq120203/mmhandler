package iie.metastore;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.MetaException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.exceptions.JedisException;

public class RedisFactory {
	private String uri;
	private Set<String> sentinels;
	private String serviceTag;
	private JedisSentinelPool jsp = null;
	private int timeout; // in miliseconds
	
	public RedisFactory(String uri, String serviceTag, int timeout) throws MetaException {
		this.uri = uri;
		this.sentinels = new HashSet<String>();
		this.serviceTag = serviceTag;
		this.timeout = timeout;
		
		if (uri == null) {
			throw new MetaException("Invalid URI or sentinels. (null)");
		}
		if (uri.startsWith("STL://")) {
			String [] s = uri.substring(6).split(";");
			for (int i = 0; i < s.length; i++) {
				sentinels.add(s[i]);
			}
		} else {
			throw new MetaException("Invalid URI or sentinels. (" + uri + ")");
		}
		// FIXME: do we need auto config?
	}
	
	public void quit() {
		if (jsp != null)
			jsp.destroy();
		jsp = null;
	}
	
	public static Jedis getRawInstace(String host, int port) {
		Jedis jedis = new Jedis(host, port);
		return jedis;
	}
	
	public static void putRawInstance(Jedis jedis) {
		jedis.quit();
	}
	
	public Jedis getNewInstace() throws JedisException {
		if (uri == null) 
			return null;
		JedisPoolConfig c = new JedisPoolConfig();
		c.setMaxIdle(5);
		c.setMinIdle(2);
		
		if (jsp != null)
			return jsp.getResource();
		else {
			jsp = new JedisSentinelPool(serviceTag, sentinels, c, timeout);
			return jsp.getResource();
		}
	}
	
	public Jedis putInstance(Jedis j) {
		try {
			if (j == null)
				return null;
			jsp.returnResource(j);
		} catch (Exception e) {
			jsp.destroy();
			jsp = null;
		}
		return null;
	}
	
	public Jedis putBrokenInstace(Jedis j) {
		try {
			if (j == null)
				return null;
			jsp.returnBrokenResource(j);
		} catch (Exception e) {
			jsp.destroy();
			jsp = null;
		}
		return null;
	}
}
