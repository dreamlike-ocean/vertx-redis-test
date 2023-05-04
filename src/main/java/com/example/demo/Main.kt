package com.example.demo

import io.vertx.core.AbstractVerticle
import io.vertx.core.DeploymentOptions
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.ext.web.Router
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch


suspend fun main(args : Array<String>) {
    val vertx = Vertx.vertx()
    val port = if (args.size == 0) {
        8080
    }else {
        args[0].toIntOrNull()?:8080
    }
    vertx.deployVerticle(
        { HttpVerticle(port) },
        DeploymentOptions().setInstances(Runtime.getRuntime().availableProcessors())
    ).await()
    println("startup end")
}


class HttpVerticle(val port :Int = 8080) : AbstractVerticle() {
    override fun start(startPromise: Promise<Void>) {
        val router = Router.router(vertx)
        router
            .get("/hello")
            .handler {
                it.end("hello, vertx ${Thread.currentThread()}")
            }
        CoroutineScope(context.dispatcher()).launch {
            val server = vertx.createHttpServer()
                .requestHandler(router)
                .listen(port).await()
            startPromise.complete()
            println("listen on $port")
        }
    }
}