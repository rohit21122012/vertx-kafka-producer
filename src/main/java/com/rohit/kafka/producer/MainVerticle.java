package com.rohit.kafka.producer;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class MainVerticle extends AbstractVerticle {
    private KafkaProducer<String, byte[]> producer;


    @Override
    public void start() throws Exception {
        HttpServer server = vertx.createHttpServer();
        Router router = Router.router(vertx);
        router.post("/topics/:topicName/messages")
            .produces("application/json")
            .handler(this::handleProduceMessages)
            .failureHandler(this::handleFailures);
        server.requestHandler(router::accept).listen(config().getInteger("http.port"));
        producer = new KafkaProducer<>(config().getJsonObject("kafka.producer").getMap());
    }

    private void handleFailures(RoutingContext routingContext) {
        routingContext.response().setStatusCode(500).end(routingContext.failure().getMessage());
    }

    private void handleProduceMessages(RoutingContext routingContext) {
        routingContext.request().bodyHandler(buffer -> handleRequest(routingContext, buffer));
    }

    private void handleRequest(RoutingContext routingContext, Buffer buffer) {
        byte[] message = buffer.getBytes();
        ProducerRecord<String, byte[]> record =
            new ProducerRecord<>(routingContext.request().getParam("topicName"),
                routingContext.request().getHeader("msg-id"),
                message);
        producer.send(record, (metadata, exception) -> {
            JsonObject responseBody = new JsonObject();
            responseBody.put("serializedValueSize", metadata.serializedValueSize());
            responseBody.put("partition", metadata.partition());
            responseBody.put("exception", exception);
            routingContext.response().end(responseBody.encode());
        });
    }
}
