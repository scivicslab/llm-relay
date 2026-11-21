package com.scivicslab.llmrelay;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpVersion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The upstream call keeps every behaviour of the GPU broker's HttpAiServiceClient (B1-B14 in quarkus-chat-ui's
 * tasks/todo.md, step 4a), with F1 fixed: a failure after the answer has started is not retryable. A stub upstream on
 * the loopback address only; no real model.
 */
class RelayCallTest {

    private Vertx vertx;
    private HttpServer stub;
    private final Map<String, Object> seen = new ConcurrentHashMap<>();

    @BeforeEach
    void start() {
        vertx = Vertx.vertx();
    }

    @AfterEach
    void stop() throws Exception {
        if (stub != null) stub.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    private int stub(Consumer<HttpServerRequest> answer) throws Exception {
        stub = vertx.createHttpServer().requestHandler(answer::accept).listen(0, "127.0.0.1")
                .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        return stub.actualPort();
    }

    private RelayCall.Result send(int port, byte[] body, String contentType, RelaySink sink, RelayOptions options)
            throws Exception {
        return new RelayCall(vertx, options).send(new UpstreamRequest("127.0.0.1", port, "/v1/chat/completions", body,
                contentType), sink).toCompletionStage().toCompletableFuture().get(20, TimeUnit.SECONDS);
    }

    private RelayCall.Result send(int port, RelaySink sink) throws Exception {
        return send(port, "{}".getBytes(StandardCharsets.UTF_8), "application/json", sink, RelayOptions.DEFAULTS);
    }

    /** B1, B2: the body and Content-Type go verbatim, over HTTP/1.1 with no h2c upgrade. */
    @Test
    void theBodyAndContentTypeGoVerbatimOverHttp11() throws Exception {
        byte[] body = "--b\r\nContent-Disposition: form-data; name=\"file\"\r\n\r\nX\r\n--b--\r\n".getBytes(StandardCharsets.UTF_8);
        int port = stub(req -> req.body().onSuccess(b -> {
            seen.put("version", req.version());
            seen.put("method", req.method().name());
            seen.put("path", req.path());
            seen.put("type", String.valueOf(req.getHeader("Content-Type")));
            seen.put("upgrade", String.valueOf(req.getHeader("Upgrade")));
            seen.put("body", b.getBytes());
            req.response().putHeader("Content-Type", "application/json").end("{\"ok\":true}");
        }));
        RecordingSink sink = new RecordingSink();
        RelayCall.Result result = send(port, body, "multipart/form-data; boundary=b", sink, RelayOptions.DEFAULTS);
        assertEquals(RelayCall.Outcome.COMPLETED, result.outcome());
        assertEquals(HttpVersion.HTTP_1_1, seen.get("version"));
        assertEquals("null", seen.get("upgrade"), "no h2c upgrade");
        assertEquals("POST", seen.get("method"));
        assertEquals("/v1/chat/completions", seen.get("path"));
        assertEquals("multipart/form-data; boundary=b", seen.get("type"));
        assertArrayEquals(body, (byte[]) seen.get("body"));
        assertEquals("{\"ok\":true}", sink.text());
    }

    /** B3: a 5xx is retryable, and the sink is not started. */
    @Test
    void aServerErrorIsRetryableAndNothingStarts() throws Exception {
        int port = stub(req -> req.response().setStatusCode(503).end("busy"));
        RecordingSink sink = new RecordingSink();
        RelayCall.Result result = send(port, sink);
        assertEquals(RelayCall.Outcome.RETRYABLE, result.outcome());
        assertEquals(503, result.status());
        assertEquals(List.of(), sink.calls);
    }

    /** B4: a refused connection is retryable, and the sink is not started. */
    @Test
    void aRefusedConnectionIsRetryableAndNothingStarts() throws Exception {
        int free;
        try (ServerSocket s = new ServerSocket(0)) {
            free = s.getLocalPort();
        }
        RecordingSink sink = new RecordingSink();
        RelayCall.Result result = send(free, sink);
        assertEquals(RelayCall.Outcome.RETRYABLE, result.outcome());
        assertTrue(result.cause() != null, "the cause is kept");
        assertEquals(List.of(), sink.calls);
    }

    /** B6: a 4xx is the server's own refusal and reaches the sink with its status. */
    @Test
    void aRefusalIsRelayedWithItsStatus() throws Exception {
        int port = stub(req -> req.response().setStatusCode(400).putHeader("Content-Type", "application/json")
                .end("{\"error\":\"bad\"}"));
        RecordingSink sink = new RecordingSink();
        RelayCall.Result result = send(port, sink);
        assertEquals(RelayCall.Outcome.COMPLETED, result.outcome());
        assertEquals(400, sink.status);
        assertEquals("{\"error\":\"bad\"}", sink.text());
    }

    /** B7, B10: start comes once, before any emit, with the upstream's content type or application/octet-stream. */
    @Test
    void startComesOnceBeforeEmitAndCompleteComesLast() throws Exception {
        int port = stub(req -> req.response().setChunked(true).write("a").onSuccess(v -> req.response().end("b")));
        RecordingSink sink = new RecordingSink();
        send(port, sink);
        assertEquals("start 200", sink.calls.get(0));
        assertEquals("complete", sink.calls.get(sink.calls.size() - 1));
        assertEquals(1, sink.calls.stream().filter(c -> c.startsWith("start")).count());
        assertEquals("application/octet-stream", sink.contentType);
    }

    /** B8: chunks arrive in order as the upstream writes them, at most 8 KB each, the first before the upstream ends. */
    @Test
    void chunksArriveInOrderAsTheUpstreamWritesThem() throws Exception {
        CompletableFuture<Void> release = new CompletableFuture<>();
        int port = stub(req -> {
            req.response().setChunked(true).putHeader("Content-Type", "text/event-stream");
            req.response().write("data: one\n\n");
            release.thenRun(() -> vertx.runOnContext(v -> {
                req.response().write("data: two\n\n");
                req.response().write("x".repeat(20_000));
                req.response().end("data: [DONE]\n\n");
            }));
        });
        RecordingSink sink = new RecordingSink();
        CompletableFuture<RelayCall.Result> done = new RelayCall(vertx, RelayOptions.DEFAULTS)
                .send(new UpstreamRequest("127.0.0.1", port, "/v1/chat/completions", "{}".getBytes(), "application/json"),
                        sink).toCompletionStage().toCompletableFuture();
        sink.firstChunk.get(10, TimeUnit.SECONDS);
        assertFalse(done.isDone(), "the first chunk is relayed while the upstream is still answering");
        assertTrue(sink.text().startsWith("data: one"));
        release.complete(null);
        assertEquals(RelayCall.Outcome.COMPLETED, done.get(10, TimeUnit.SECONDS).outcome());
        String text = sink.text();
        assertTrue(text.indexOf("data: one") < text.indexOf("data: two") && text.endsWith("data: [DONE]\n\n"), text);
        assertTrue(sink.chunks.stream().allMatch(c -> c.length <= 8192), "no chunk over 8 KB");
    }

    /** B9: when the sink asks to stop, the upstream connection is closed and the call counts as complete. */
    @Test
    void stopRequestedClosesTheUpstreamAndCompletes() throws Exception {
        CompletableFuture<Void> closed = new CompletableFuture<>();
        int port = stub(req -> {
            req.response().setChunked(true);
            req.connection().closeHandler(v -> closed.complete(null));
            long timer = vertx.setPeriodic(20, id -> req.response().write("data: again\n\n"));
            req.response().closeHandler(v -> vertx.cancelTimer(timer));
            req.connection().closeHandler(v -> {
                vertx.cancelTimer(timer);
                closed.complete(null);
            });
        });
        RecordingSink sink = new RecordingSink(n -> n >= 2);
        RelayCall.Result result = send(port, sink);
        assertEquals(RelayCall.Outcome.COMPLETED, result.outcome());
        assertEquals("complete", sink.calls.get(sink.calls.size() - 1));
        closed.get(10, TimeUnit.SECONDS);
    }

    /** F1 fixed: a failure after the answer has started is not retryable, and the sink is not completed. */
    @Test
    void aFailureAfterTheStartIsNotRetryable() throws Exception {
        int port = stub(req -> {
            req.response().setChunked(true).write("data: one\n\n");
            vertx.setTimer(100, id -> req.connection().close());
        });
        RecordingSink sink = new RecordingSink();
        RelayCall.Result result = send(port, sink);
        assertEquals(RelayCall.Outcome.FAILED_AFTER_START, result.outcome());
        assertFalse(sink.calls.contains("complete"), sink.calls.toString());
        assertTrue(sink.calls.get(0).startsWith("start"));
    }

    /** B11, B12: the blocking form waits on the caller's thread and, when interrupted, keeps the interrupt flag. */
    @Test
    void theBlockingFormKeepsTheInterruptFlag() throws Exception {
        int port = stub(req -> { /* never answers */ });
        CompletableFuture<Object[]> out = new CompletableFuture<>();
        Thread caller = Thread.ofVirtual().start(() -> {
            RelayCall.Result r = new RelayCall(vertx, RelayOptions.DEFAULTS).sendAndWait(
                    new UpstreamRequest("127.0.0.1", port, "/v1/chat/completions", "{}".getBytes(), "application/json"),
                    new RecordingSink());
            out.complete(new Object[] {r, Thread.currentThread().isInterrupted()});
        });
        Thread.sleep(300);
        caller.interrupt();
        Object[] r = out.get(10, TimeUnit.SECONDS);
        assertEquals(RelayCall.Outcome.RETRYABLE, ((RelayCall.Result) r[0]).outcome(), "interrupted before the start");
        assertTrue((Boolean) r[1], "the interrupt flag is kept");
    }

    /** B14, F2: no timeout by default; an idle timeout can be set, and one that fires before the start is retryable. */
    @Test
    void noTimeoutByDefaultAndAnIdleTimeoutCanBeSet() throws Exception {
        assertNull(RelayOptions.DEFAULTS.connectTimeout());
        assertNull(RelayOptions.DEFAULTS.idleTimeout());
        assertEquals(0, RelayOptions.DEFAULTS.httpClientOptions().getConnectTimeout(), "0: no connect timeout");
        assertEquals(0, RelayOptions.DEFAULTS.httpClientOptions().getIdleTimeout(), "0: no idle timeout");
        int port = stub(req -> { /* never answers */ });
        RecordingSink sink = new RecordingSink();
        long t0 = System.nanoTime();
        RelayCall.Result result = send(port, "{}".getBytes(), "application/json", sink,
                new RelayOptions(null, Duration.ofSeconds(1)));
        assertEquals(RelayCall.Outcome.RETRYABLE, result.outcome());
        assertTrue(System.nanoTime() - t0 < TimeUnit.SECONDS.toNanos(10));
        assertEquals(List.of(), sink.calls);
    }
}
