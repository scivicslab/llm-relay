package com.scivicslab.llmrelay;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.IntPredicate;

/** A {@link RelaySink} for tests: records every call in order. */
final class RecordingSink implements RelaySink {

    final List<String> calls = new CopyOnWriteArrayList<>();
    final List<byte[]> chunks = new CopyOnWriteArrayList<>();
    final CompletableFuture<Void> firstChunk = new CompletableFuture<>();
    volatile String contentType;
    volatile int status;
    private final IntPredicate stopAfter;

    RecordingSink() {
        this(n -> false);
    }

    /** @param stopAfter asked with the number of chunks emitted so far; true asks the relay to stop */
    RecordingSink(IntPredicate stopAfter) {
        this.stopAfter = stopAfter;
    }

    @Override
    public void start(String contentType, int status) {
        this.contentType = contentType;
        this.status = status;
        calls.add("start " + status);
    }

    @Override
    public void emit(byte[] chunk) {
        chunks.add(chunk);
        calls.add("emit " + chunk.length);
        firstChunk.complete(null);
    }

    @Override
    public boolean stopRequested() {
        return stopAfter.test(chunks.size());
    }

    @Override
    public void complete() {
        calls.add("complete");
    }

    String text() {
        StringBuilder b = new StringBuilder();
        for (byte[] c : chunks) b.append(new String(c, java.nio.charset.StandardCharsets.UTF_8));
        return b.toString();
    }
}
