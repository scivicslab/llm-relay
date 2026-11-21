package com.scivicslab.llmrelay;

import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpVersion;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * How the relay's HTTP client talks to an upstream LLM server.
 *
 * <ul>
 * <li>HTTP/1.1 always, with no h2c upgrade: the upgrade corrupts multipart/form-data bodies sent to uvicorn/FastAPI
 * backends (the GPU broker's YomiToku, Marker and embedding queues).</li>
 * <li>Answer bodies are relayed in chunks of at most 8 KB.</li>
 * <li>No timeout unless one is set: {@code null} means none, as with the JDK client the GPU broker used before.</li>
 * </ul>
 *
 * @param connectTimeout how long to wait for the connection, or {@code null} for no limit
 * @param idleTimeout    how long the connection may stay silent, or {@code null} for no limit
 */
public record RelayOptions(Duration connectTimeout, Duration idleTimeout) {

    /** HTTP/1.1, no timeout. */
    public static final RelayOptions DEFAULTS = new RelayOptions(null, null);

    /** The largest chunk handed on at once. */
    public static final int MAX_CHUNK = 8192;

    /** @return the Vert.x HTTP client options these stand for */
    public HttpClientOptions httpClientOptions() {
        return new HttpClientOptions()
                .setProtocolVersion(HttpVersion.HTTP_1_1)
                .setHttp2ClearTextUpgrade(false)
                .setKeepAlive(true)
                .setMaxChunkSize(MAX_CHUNK)
                // 0 is "no limit" for both (Netty does not arm a connect timer for 0).
                .setConnectTimeout(connectTimeout == null ? 0 : (int) connectTimeout.toMillis())
                .setIdleTimeoutUnit(TimeUnit.MILLISECONDS)
                .setIdleTimeout(idleTimeout == null ? 0 : (int) idleTimeout.toMillis());
    }
}
