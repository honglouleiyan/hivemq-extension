package com.hivemq.extensions.heartbeat.plug;

import redis.clients.jedis.Jedis;

/**
 * @created with IntelliJ IDEA.
 * @author: heaven
 * @date: 2019/11/5
 * @time: 14:47
 * @description:
 **/
public class RedisUtils {
    public static Jedis getJedis() {
        Jedis jedis = new Jedis("localhost",6379);
        return jedis;
    }
}
