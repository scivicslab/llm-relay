package com.scivicslab.llmrelay.proxy;

import com.scivicslab.llmrelay.RelayOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.proxy.handler.ProxyHandler;
import io.vertx.httpproxy.Body;
import io.vertx.httpproxy.HttpProxy;
import io.vertx.httpproxy.ProxyContext;
import io.vertx.httpproxy.ProxyInterceptor;
import io.vertx.httpproxy.ProxyRequest;
import io.vertx.httpproxy.ProxyResponse;

import java.util.List;

/**
 * A reverse proxy for a live request to one upstream LLM server, on vertx-http-proxy (quarkus-chat-ui's
 * tasks/todo.md, step 4a). The request and the answer stream through as they arrive, both shapes alike
 * ({@code /v1/chat/completions} and {@code /v1/messages}): the proxy parses no body. The upstream client is the one
 * {@link RelayOptions} describes (HTTP/1.1, timeouts). Interceptors run in order before the request goes upstream; the
 * first refusal answers the client instead. An upstream that cannot be reached is answered with 502 by vertx-http-proxy.
 */
public final class RelayProxy {

    private final HttpProxy proxy;

    /**
     * @param vertx        the Vert.x instance; in a Quarkus application, Quarkus's own
     * @param host         the upstream's host
     * @param port         the upstream's port
     * @param interceptors the rules, in order
     */
    public RelayProxy(Vertx vertx, RelayOptions options, String host, int port, List<RelayInterceptor> interceptors) {
        this.proxy = HttpProxy.reverseProxy(vertx.createHttpClient(options.httpClientOptions())).origin(port, host);
        List<RelayInterceptor> rules = List.copyOf(interceptors);
        proxy.addInterceptor(new ProxyInterceptor() {
            @Override
            public Future<ProxyResponse> handleProxyRequest(ProxyContext context) {
                ProxyRequest request = context.request();
                for (RelayInterceptor rule : rules) {
                    RelayInterceptor.Refusal refusal = rule.intercept(request);
                    if (refusal != null) {
                        request.release();
                        ProxyResponse answer = request.response().setStatusCode(refusal.status());
                        if (refusal.contentType() != null) answer.putHeader("Content-Type", refusal.contentType());
                        answer.setBody(Body.body(Buffer.buffer(refusal.body() == null ? "" : refusal.body())));
                        return Future.succeededFuture(answer);
                    }
                }
                return context.sendRequest();
            }
        });
    }

    /** Mounts the proxy on {@code router} for every path under {@code prefix}, e.g. {@code /llm/}. */
    public void mount(Router router, String prefix) {
        router.route(prefix + "*").handler(ProxyHandler.create(proxy));
    }

    /** @return the vertx-http-proxy proxy itself, for a server that is not a Router */
    public HttpProxy httpProxy() {
        return proxy;
    }
}
