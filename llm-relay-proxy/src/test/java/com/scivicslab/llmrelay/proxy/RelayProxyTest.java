package com.scivicslab.llmrelay.proxy;

import com.scivicslab.llmrelay.RelayOptions;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.Router;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The reverse proxy for a live request, on vertx-http-proxy (quarkus-chat-ui's tasks/todo.md, step 4a): interceptors
 * refuse or rewrite a request, and the answer streams back unbroken in both request shapes. Stub upstreams and dummy
 * keys only.
 */
class RelayProxyTest {

    private Vertx vertx;
    private HttpServer upstream;
    private HttpServer front;
    private final Map<String, String> seen = new ConcurrentHashMap<>();
    private final HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();

    @BeforeEach
    void start() {
        vertx = Vertx.vertx();
    }

    @AfterEach
    void stop() throws Exception {
        for (HttpServer s : new HttpServer[] {upstream, front}) {
            if (s != null) s.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
        vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    private int upstream(Consumer<HttpServerRequest> answer) throws Exception {
        upstream = vertx.createHttpServer().requestHandler(answer::accept).listen(0, "127.0.0.1")
                .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        return upstream.actualPort();
    }

    /** The front server: a Router with the proxy mounted under /llm/, as the kernel mounts it on Quarkus's router. */
    private String front(int upstreamPort, List<RelayInterceptor> interceptors) throws Exception {
        Router router = Router.router(vertx);
        new RelayProxy(vertx, RelayOptions.DEFAULTS, "127.0.0.1", upstreamPort, interceptors).mount(router, "/llm/");
        front = vertx.createHttpServer().requestHandler(router).listen(0, "127.0.0.1")
                .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        return "http://127.0.0.1:" + front.actualPort();
    }

    private HttpResponse<InputStream> post(String url, Map<String, String> headers, String body) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url)).POST(HttpRequest.BodyPublishers.ofString(body))
                .header("Content-Type", "application/json");
        headers.forEach(b::header);
        return client.send(b.build(), HttpResponse.BodyHandlers.ofInputStream());
    }

    @Test
    void anInterceptorsRefusalReachesTheClientAndTheUpstreamIsNotCalled() throws Exception {
        int port = upstream(req -> {
            seen.put("called", "yes");
            req.response().end("should not be reached");
        });
        String base = front(port, List.of(request -> new RelayInterceptor.Refusal(401, "application/json",
                "{\"error\":\"the request does not carry this scope's key\"}")));
        HttpResponse<InputStream> r = post(base + "/llm/s1/v1/chat/completions", Map.of(), "{}");
        assertEquals(401, r.statusCode());
        assertEquals("{\"error\":\"the request does not carry this scope's key\"}",
                new String(r.body().readAllBytes(), StandardCharsets.UTF_8));
        assertNull(seen.get("called"));
    }

    @Test
    void anInterceptorRewritesThePathAndTheHeadersBeforeTheRequestGoesUp() throws Exception {
        int port = upstream(req -> {
            seen.put("uri", req.uri());
            seen.put("authorization", String.valueOf(req.getHeader("Authorization")));
            req.response().end("ok");
        });
        String base = front(port, List.of(request -> {
            request.setURI(request.getURI().substring("/llm/s1".length()));
            request.headers().set("Authorization", "Bearer dummy-real-key");
            return null;
        }));
        HttpResponse<InputStream> r = post(base + "/llm/s1/v1/chat/completions?x=1",
                Map.of("Authorization", "Bearer dummy-throw-away-key"), "{}");
        assertEquals(200, r.statusCode());
        assertEquals("/v1/chat/completions?x=1", seen.get("uri"));
        assertEquals("Bearer dummy-real-key", seen.get("authorization"));
    }

    @Test
    void anOpenAiStreamPassesThroughUnbrokenAndUnbuffered() throws Exception {
        streamsUnbroken("/llm/s1/v1/chat/completions", List.of(
                "data: {\"choices\":[{\"delta\":{\"content\":\"one\"}}]}",
                "data: {\"choices\":[{\"delta\":{\"content\":\"two\"}}]}",
                "data: [DONE]"));
    }

    @Test
    void anAnthropicStreamPassesThroughUnbrokenAndUnbuffered() throws Exception {
        streamsUnbroken("/llm/s1/v1/messages", List.of(
                "event: message_start", "data: {\"type\":\"message_start\"}",
                "event: content_block_delta", "data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"one\"}}",
                "event: message_stop", "data: {\"type\":\"message_stop\"}"));
    }

    /**
     * The upstream writes the first line and waits; the client must have that line before the upstream goes on, so the
     * proxy holds nothing back. Then the rest arrives in order.
     */
    private void streamsUnbroken(String path, List<String> lines) throws Exception {
        CompletableFuture<Void> release = new CompletableFuture<>();
        int port = upstream(req -> {
            seen.put("path", req.path());
            req.response().setChunked(true).putHeader("Content-Type", "text/event-stream");
            req.response().write(lines.get(0) + "\n\n");
            release.thenRun(() -> vertx.runOnContext(v -> {
                for (String line : lines.subList(1, lines.size())) req.response().write(line + "\n\n");
                req.response().end();
            }));
        });
        String base = front(port, List.of(request -> {
            request.setURI(request.getURI().substring("/llm/s1".length()));
            return null;
        }));
        HttpResponse<InputStream> r = post(base + path, Map.of(), "{\"stream\":true}");
        assertEquals(200, r.statusCode());
        assertTrue(r.headers().firstValue("content-type").orElse("").startsWith("text/event-stream"));
        BufferedReader reader = new BufferedReader(new InputStreamReader(r.body(), StandardCharsets.UTF_8));
        assertEquals(lines.get(0), reader.readLine(), "the first line arrives while the upstream still waits");
        assertFalse(release.isDone());
        release.complete(null);
        StringBuilder rest = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) if (!line.isEmpty()) rest.append(line).append('\n');
        assertEquals(String.join("\n", lines.subList(1, lines.size())) + "\n", rest.toString());
        assertEquals(path.substring("/llm/s1".length()), seen.get("path"));
    }

    @Test
    void anUpstreamThatIsDownIsAnsweredWith502() throws Exception {
        int free;
        try (ServerSocket s = new ServerSocket(0)) {
            free = s.getLocalPort();
        }
        String base = front(free, List.of(request -> null));
        HttpResponse<InputStream> r = post(base + "/llm/s1/v1/chat/completions", Map.of(), "{}");
        assertEquals(502, r.statusCode());
    }
}
