package iie.databak;

import org.apache.commons.pool.impl.GenericObjectPool;

import iie.databak.DatabakConf.RedisInstance;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.JedisPool;

public class RedisFactory {
	private static DatabakConf conf;
	private static JedisSentinelPool jsp = null;
	private static JedisPool jp = null;
	private static RedisInstance ri = null;
	private JedisPoolConfig config = null;
	public RedisFactory(DatabakConf conf) {
		RedisFactory.conf = conf;
		config = new JedisPoolConfig();
		
		config.setMaxActive(1000);
		config.setMaxIdle(50);
		config.setTestOnBorrow(false);
		config.setTestOnReturn(false);
		config.setWhenExhaustedAction(GenericObjectPool.WHEN_EXHAUSTED_BLOCK);
	}
	
	// 从配置文件中读取redis的地址和端口,以此创建jedis对象
	public synchronized Jedis getDefaultInstance() {
		switch (conf.getRedisMode()) {
		case STANDALONE:
			
			if(ri == null)
			{
				ri = conf.getRedisInstance();
				jp = new JedisPool(config,ri.hostname,ri.port);
			}
			return jp.getResource();
		case SENTINEL:
		{
			Jedis r;
			if (jsp != null)
				r = jsp.getResource();
			else {
				
				jsp = new JedisSentinelPool("mymaster", conf.getSentinels(), config);
				r = jsp.getResource();
			}
			return r;
		}
		}
		return null;
	}
	
	public synchronized static Jedis putInstance(Jedis j) {
		if (j == null)
			return null;
		switch (conf.getRedisMode()) {
		case STANDALONE:
			jp.returnResource(j);
			break;
		case SENTINEL:
			jsp.returnResource(j);
			break;
		}
		return null;
	}
	
	public synchronized static Jedis putBrokenInstance(Jedis j) {
		if (j == null)
			return null;
		switch (conf.getRedisMode()) {
		case STANDALONE:
			jp.returnBrokenResource(j);
			break;
		case SENTINEL:
			jsp.returnBrokenResource(j);
		}
		return null;
	}
}
