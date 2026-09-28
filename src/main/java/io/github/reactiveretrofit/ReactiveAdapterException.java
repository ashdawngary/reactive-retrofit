package io.github.reactiveretrofit;

/** Indicates that a response could not be adapted into the declared Reactor type. */
public final class ReactiveAdapterException extends RuntimeException {
    /**
     * Creates an exception for an unsuccessful response-body adaptation.
     *
     * @param message explanation of the adaptation failure
     * @param cause underlying I/O or conversion failure
     */
    public ReactiveAdapterException(String message, Throwable cause) {
        super(message, cause);
    }
}
