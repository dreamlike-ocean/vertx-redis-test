package com.example.demo

import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import io.lettuce.core.codec.StringCodec
import io.lettuce.core.resource.DefaultClientResources
import io.lettuce.core.resource.EventLoopGroupProvider
import io.netty.channel.DefaultEventLoopGroup
import io.netty.channel.EventLoop
import io.netty.channel.EventLoopGroup
import io.netty.util.concurrent.DefaultPromise
import io.netty.util.concurrent.EventExecutorGroup
import io.netty.util.concurrent.Future
import io.netty.util.concurrent.GlobalEventExecutor
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.impl.ContextInternal
import io.vertx.ext.web.Router
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import io.vertx.redis.client.Redis
import io.vertx.redis.client.RedisAPI
import io.vertx.redis.client.RedisOptions
import io.vertx.sqlclient.Pool
import io.vertx.sqlclient.PoolOptions
import io.vertx.sqlclient.SqlConnectOptions
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import java.util.concurrent.TimeUnit


suspend fun main(args : Array<String>) {
    val vertx = Vertx.vertx()
    val port = if (args.isEmpty()) {
        8080
    }else {
        args[0].toIntOrNull()?:8080
    }
    val pool = Pool.pool(
        vertx,
        SqlConnectOptions().setDatabase("petclinic")
            .setHost("localhost")
            .setPassword("123456789")
            .setUser("root")
            .setPort(3306),
        PoolOptions().setMaxSize(10)
    )

    val config = DefaultClientResources.builder()
        .eventLoopGroupProvider(ContextEventLoopGroupProvider(vertx))
        .eventExecutorGroup(vertx.nettyEventLoopGroup())
        .build()
    val redis = RedisClient.create(config, "redis://localhost/0")

    vertx.deployVerticle(
        { HttpVerticle(port, pool,redis) },
        DeploymentOptions().setInstances(Runtime.getRuntime().availableProcessors())
    ).await()
    println("startup end")
}
class HttpVerticle(val port :Int = 8080,val pool: Pool,val redis: RedisClient) : CoroutineVerticle() {

    override suspend fun start() {
        val client = redis.connectAsync(StringCodec(), RedisURI.create("redis://localhost/0")).await().async()
        val vertxClient = RedisAPI.api(Redis.createClient(vertx, RedisOptions().setMaxPoolSize(1).setMaxPoolWaiting(-1).setConnectionString("redis://localhost/0")))
        val router = Router.router(vertx)
        router
            .get("/hello")
            .handler { it ->
                val context = vertx.orCreateContext
                CoroutineScope(context.dispatcher()).launch {
                  client.get("123")
                        .whenComplete { t, u ->
                            println("${Thread.currentThread()}")
                        }
                        .await()
                    delay(5)
                    val list = pool.query("select * from owners where id = 1")
                        .mapping { it.toJson() }
                        .execute()
                        .await().toList()
                    it.json(list)
                }
            }
        router
            .get("/empty")
            .handler {
                it.end("hello")
            }
        //wrk -c100 -t4 -d60s --latency http://localhost:8080/vertx-redis
        //Running 1m test @ http://localhost:8080/vertx-redis
        //  4 threads and 100 connections
        //  Thread Stats   Avg      Stdev     Max   +/- Stdev
        //    Latency    97.18ms  132.86ms 695.98ms   82.77%
        //    Req/Sec     1.96k     1.37k   12.88k    82.92%
        //  Latency Distribution
        //     50%    4.02ms
        //     75%  172.37ms
        //     90%  305.53ms
        //     99%  494.51ms
        //  405032 requests in 1.00m, 32.13MB read
        //Requests/sec:   6739.91
        //Transfer/sec:    547.43KB
        router.get("/vertx-redis")
            .handler {
                val context = vertx.orCreateContext
                CoroutineScope(context.dispatcher()).launch {
                    for (index in 1..10) {
                        vertxClient.get("123").await().toString()
                    }
                }
                it.end(Thread.currentThread().toString())
            }
        //wrk -c100 -t4 -d60s --latency http://localhost:8080/lettuce
        //Running 1m test @ http://localhost:8080/lettuce
        //  4 threads and 100 connections
        //  Thread Stats   Avg      Stdev     Max   +/- Stdev
        //    Latency    20.94ms   14.09ms 112.35ms   65.77%
        //    Req/Sec     1.21k   161.10     1.63k    76.10%
        //  Latency Distribution
        //     50%   10.57ms
        //     75%   35.84ms
        //     90%   38.43ms
        //     99%   45.81ms
        //  287999 requests in 1.00m, 22.84MB read
        //Requests/sec:   4795.91
        //Transfer/sec:    389.49KB
        router
            .get("/lettuce")
            .handler {
                val context = vertx.orCreateContext
                CoroutineScope(context.dispatcher()).launch {
                    for (index in 1..10) {
                        client.get("123").await()
                    }
                    it.end(Thread.currentThread().toString())
                }
            }
        router
            .get("/gc")
            .handler {
                System.gc()
                it.end("gc end")
            }



        val server = vertx.createHttpServer()
            .requestHandler(router)
            .listen(port).await()
        println("listen on $port")
    }
}


class ContextEventLoopGroupProvider(val vertx: Vertx) : EventLoopGroupProvider{
    override fun <T : EventLoopGroup?> allocate(type: Class<T>?) : T {
        return ContextEventLoopGroup(vertx) as T
    }

    override fun threadPoolSize() = 1

    override fun release(
        eventLoopGroup: EventExecutorGroup?,
        quietPeriod: Long,
        timeout: Long,
        unit: TimeUnit?
    ): Future<Boolean> {
        val promise = DefaultPromise<Boolean>(GlobalEventExecutor.INSTANCE)
        promise.setSuccess(true)
        return promise
    }

    override fun shutdown(quietPeriod: Long, timeout: Long, timeUnit: TimeUnit?): Future<Boolean> {
        val promise = DefaultPromise<Boolean>(GlobalEventExecutor.INSTANCE)
        promise.setSuccess(true)
        return promise
    }
}

class ContextEventLoopGroup(val vertx: Vertx) : DefaultEventLoopGroup() {
    override fun next(): EventLoop {
        val contextInternal = vertx.orCreateContext as ContextInternal
        return contextInternal.nettyEventLoop()
    }
}
