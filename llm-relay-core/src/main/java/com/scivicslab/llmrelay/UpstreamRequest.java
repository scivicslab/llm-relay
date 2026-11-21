package com.scivicslab.llmrelay;

/**
 * One POST to an upstream LLM server. The body goes as it is: the relay never parses it.
 *
 * @param host        the upstream's host
 * @param port        the upstream's port
 * @param path        the path, with its query, e.g. {@code /v1/chat/completions}
 * @param body        the request body
 * @param contentType the client's Content-Type, or {@code null} for none
 */
public record UpstreamRequest(String host, int port, String path, byte[] body, String contentType) {
}
