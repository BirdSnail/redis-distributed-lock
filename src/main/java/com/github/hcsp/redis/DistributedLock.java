package com.github.hcsp.redis;


import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.SetParams;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.Callable;

public class DistributedLock {
    /**
     * The lock name. A lock with same name might be shared in multiple JVMs.
     */
    private String name;

    /**
     * 一个redis链接池, 可以从池中获取一个redis链接
     */
    private static JedisPool pool = new JedisPool();

    /**
     * 获取锁的超时时间
     */
    private static final long DEFAULT_ACQUIRE_TIMEOUT_MILLIS = 20 * 1000L;

//    public static final String CHANNEL = "walk_up";

    public DistributedLock(String name) {
        this.name = name;
    }

    /**
     * Run a given action under lock.
     *
     * @param callable the action to be executed
     * @param <T>      return type
     * @return the result
     */
    public <T> T runUnderLock(Callable<T> callable) throws Exception {
        T result;
        Jedis jedis = pool.getResource();
        String uuid = UUID.randomUUID().toString();

        try {
            lock(jedis, uuid, 10, DEFAULT_ACQUIRE_TIMEOUT_MILLIS);
            result = callable.call();
        } finally {
            unlock(jedis, uuid);
            jedis.close();
        }
        return result;
    }

    /**
     * 进行上锁操作
     *
     * @param jedis         jedis
     * @param value         value
     * @param secondsExpiry 过期时间
     * @param timeout       超时时间(毫秒)
     * @throws InterruptedException
     */
    public void lock(Jedis jedis, String value, int secondsExpiry, long timeout) throws InterruptedException {
        SetParams params = SetParams.setParams();
        // nx: 不存在该key才设置值
        // ex: 过期时间
        params.nx().ex(secondsExpiry);

        long endTime = System.currentTimeMillis() + timeout;

        while (System.currentTimeMillis() <= endTime) {
            if ("OK".equals(jedis.set(name, value, params))) {
                return;
            }
            Thread.sleep(1000);
        }

        // 获取锁超时
        throw new RuntimeException("get lock time out");
    }

    /**
     * 原子性的释放锁，保证只释放自己上的锁
     */
    public void unlock(Jedis jedis, String value) {
        String luaScript = "if redis.call('get',KEYS[1]) == ARGV[1] then return redis.call('del',KEYS[1]) else return 0 end";
        jedis.eval(luaScript, Collections.singletonList(name), Collections.singletonList(value));
    }
}
