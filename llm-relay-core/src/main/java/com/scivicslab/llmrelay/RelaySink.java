package com.scivicslab.llmrelay;

/**
 * Where the relay writes the upstream's answer. The order is always {@code start} once, then any number of
 * {@code emit}, then {@code complete} -- or none of them, when the call is {@link RelayCall.Outcome#RETRYABLE}, and no
 * {@code complete} when it is {@link RelayCall.Outcome#FAILED_AFTER_START}. The calls come on the Vert.x event loop, so
 * a sink must not block.
 */
public interface RelaySink {

    /** The upstream's Content-Type (or {@code application/octet-stream}) and its status, once, before any emit. */
    void start(String contentType, int status);

    /** One chunk of the answer, as it arrived, at most {@link RelayOptions#MAX_CHUNK} bytes. */
    void emit(byte[] chunk);

    /**
     * Asked after every emit. True closes the upstream connection, which ends the generation, and the call then counts
     * as complete.
     */
    default boolean stopRequested() {
        return false;
    }

    /** The answer ended, or the sink asked to stop. */
    void complete();
}
