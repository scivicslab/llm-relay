package com.scivicslab.llmrelay;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.RequestOptions;

import java.util.concurrent.ExecutionException;

/**
 * The streaming upstream call: POSTs a request's bytes to an upstream LLM server and hands its answer to a
 * {@link RelaySink} as it arrives (quarkus-chat-ui's tasks/todo.md, step 4a). Built on the Vert.x HTTP client; the
 * caller passes its {@link Vertx}, so in a Quarkus application it is Quarkus's own.
 *
 * <p>The outcome tells the caller what it may do next:
 * <ul>
 * <li>{@link Outcome#RETRYABLE}: the connection failed, or the upstream answered 5xx, before anything reached the
 * sink. Another upstream may be tried with the same request.</li>
 * <li>{@link Outcome#FAILED_AFTER_START}: the answer had started and then broke. The sink already has part of it, so the
 * request must not be sent again.</li>
 * <li>{@link Outcome#COMPLETED}: the answer ended, or the sink asked to stop. A status below 500 is the upstream's own
 * answer and reaches the sink as it is.</li>
 * </ul>
 * Choosing another upstream and counting attempts is the caller's business.</p>
 */
public final class RelayCall {

    /** What a call ended as. */
    public enum Outcome { COMPLETED, RETRYABLE, FAILED_AFTER_START }

    /**
     * @param outcome what the call ended as
     * @param status  the upstream's status, or 0 when none came
     * @param cause   why it failed, or {@code null}
     */
    public record Result(Outcome outcome, int status, Throwable cause) {
    }

    private final HttpClient client;

    /** @param vertx the Vert.x instance the client belongs to */
    public RelayCall(Vertx vertx, RelayOptions options) {
        this.client = vertx.createHttpClient(options.httpClientOptions());
    }

    /** @return the outcome, once the call has ended */
    public Future<Result> send(UpstreamRequest request, RelaySink sink) {
        return new Call(request, sink).promise.future();
    }

    /**
     * The blocking form, for a caller on a virtual thread: waits for the outcome. When the waiting thread is
     * interrupted, the upstream connection is closed, the interrupt flag is kept, and the outcome says whether the
     * answer had started.
     */
    public Result sendAndWait(UpstreamRequest request, RelaySink sink) {
        Call call = new Call(request, sink);
        try {
            return call.promise.future().toCompletionStage().toCompletableFuture().get();
        } catch (InterruptedException e) {
            call.cancel();
            Thread.currentThread().interrupt();
            return new Result(call.started ? Outcome.FAILED_AFTER_START : Outcome.RETRYABLE, call.status, e);
        } catch (ExecutionException e) {
            return new Result(call.started ? Outcome.FAILED_AFTER_START : Outcome.RETRYABLE, call.status, e.getCause());
        }
    }

    /** One call in flight. */
    private final class Call {
        final Promise<Result> promise = Promise.promise();
        final RelaySink sink;
        volatile HttpClientRequest request;
        volatile boolean started;
        volatile boolean stopped;
        volatile int status;

        Call(UpstreamRequest upstream, RelaySink sink) {
            this.sink = sink;
            RequestOptions options = new RequestOptions().setMethod(HttpMethod.POST).setHost(upstream.host())
                    .setPort(upstream.port()).setURI(upstream.path());
            if (upstream.contentType() != null) options.putHeader("Content-Type", upstream.contentType());
            client.request(options)
                    .onFailure(this::failed)
                    .onSuccess(req -> {
                        request = req;
                        if (stopped) {
                            req.reset();
                            return;
                        }
                        req.send(Buffer.buffer(upstream.body())).onSuccess(this::answer).onFailure(this::failed);
                    });
        }

        private void answer(HttpClientResponse response) {
            status = response.statusCode();
            if (status >= 500) {
                stopped = true;
                request.reset();
                promise.tryComplete(new Result(Outcome.RETRYABLE, status, null));
                return;
            }
            response.exceptionHandler(this::failed);
            response.endHandler(v -> {
                if (stopped) return;
                stopped = true;
                sink.complete();
                promise.tryComplete(new Result(Outcome.COMPLETED, status, null));
            });
            response.handler(chunk -> {
                if (stopped) return;
                try {
                    sink.emit(chunk.getBytes());
                    if (sink.stopRequested()) {
                        stopped = true;
                        request.reset();
                        sink.complete();
                        promise.tryComplete(new Result(Outcome.COMPLETED, status, null));
                    }
                } catch (RuntimeException e) {
                    stopped = true;
                    request.reset();
                    promise.tryComplete(new Result(Outcome.FAILED_AFTER_START, status, e));
                }
            });
            String type = response.getHeader("Content-Type");
            started = true;
            sink.start(type == null ? "application/octet-stream" : type, status);
        }

        private void failed(Throwable cause) {
            if (stopped) return;
            stopped = true;
            promise.tryComplete(new Result(started ? Outcome.FAILED_AFTER_START : Outcome.RETRYABLE, status, cause));
        }

        void cancel() {
            stopped = true;
            HttpClientRequest r = request;
            if (r != null) r.reset();
        }
    }
}
