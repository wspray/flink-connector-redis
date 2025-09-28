package org.apache.flink.streaming.connectors.redis.container;

@FunctionalInterface
public interface RedisFunction<T> {

    T apply(RedisCommandsContainer container) throws Exception;

}
