package com.scivicslab.llmrelay.proxy;

import io.vertx.httpproxy.ProxyRequest;

/**
 * One consumer's rule for a proxied request, run before the request goes upstream (quarkus-chat-ui's tasks/todo.md,
 * step 4a). It may rewrite the request -- its URI ({@link ProxyRequest#setURI}) and headers
 * ({@link ProxyRequest#headers}) -- or refuse it. agent-connector-kernel's rule checks a harness's throw-away key and
 * puts the real credential in its place.
 */
@FunctionalInterface
public interface RelayInterceptor {

    /**
     * An answer for the client in place of going upstream.
     *
     * @param status      the HTTP status
     * @param contentType the body's Content-Type, or {@code null} for none
     * @param body        the body, or {@code null} for none
     */
    record Refusal(int status, String contentType, String body) {
    }

    /** @return {@code null} to let the request go on, possibly rewritten; or a refusal */
    Refusal intercept(ProxyRequest request);
}
