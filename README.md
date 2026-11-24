# llm-relay

One streaming relay to an OpenAI-compatible or Anthropic LLM server, built on Vert.x. It is shared by
[quarkus-chat-ui](https://github.com/scivicslab/quarkus-chat-ui)'s `agent-connector-kernel` (its LLM proxy) and
quarkus-gpu-broker (its upstream call).

| Module | What it is |
|---|---|
| `llm-relay-core` | The streaming upstream call (`RelayCall`): POSTs a request to an upstream LLM server and hands the answer to a `RelaySink` as it arrives. The outcome says whether another upstream may be tried (`RETRYABLE`), the answer broke after it started (`FAILED_AFTER_START`), or it ended (`COMPLETED`). The caller passes its own `Vertx`. |
| `llm-relay-proxy` | A reverse proxy for a live request (`RelayProxy`) on `vertx-http-proxy`. Request and answer stream through unparsed, for `/v1/chat/completions` and `/v1/messages` alike. Interceptors (`RelayInterceptor`) run before the request goes upstream, for example to inject a credential. |

Vert.x is a `provided` dependency: the application brings its own (in Quarkus, the versions Quarkus's BOM pins).

## Build

Java 25 and Maven:

```
mvn install
```

## License

Apache License 2.0
