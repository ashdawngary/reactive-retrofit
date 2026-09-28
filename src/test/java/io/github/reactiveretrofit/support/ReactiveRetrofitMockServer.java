package io.github.reactiveretrofit.support;

import okhttp3.HttpUrl;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Reusable MockWebServer fixture for exercising Reactive Retrofit behavior in integration tests.
 */
public final class ReactiveRetrofitMockServer implements Closeable {
    private static final String JSON = "application/json";
    private final MockWebServer server = new MockWebServer();
    private final long streamPeriodMillis;

    /** Creates a server which spaces streaming chunks far enough apart to observe manually. */
    public ReactiveRetrofitMockServer() {
        this(250);
    }

    /** Set {@code streamPeriodMillis} to zero for fast automated tests. */
    public ReactiveRetrofitMockServer(long streamPeriodMillis) {
        if (streamPeriodMillis < 0) {
            throw new IllegalArgumentException("streamPeriodMillis must not be negative");
        }
        this.streamPeriodMillis = streamPeriodMillis;
        server.setDispatcher(new ApiDispatcher());
    }

    public void start() throws IOException {
        server.start();
    }

    public void start(int port) throws IOException {
        server.start(port);
    }

    public HttpUrl baseUrl() {
        return server.url("/");
    }

    @Override
    public void close() throws IOException {
        server.shutdown();
    }

    private final class ApiDispatcher extends Dispatcher {
        @Override
        public MockResponse dispatch(RecordedRequest request) {
            String path = Objects.requireNonNull(request.getRequestUrl()).encodedPath();
            String method = request.getMethod();

            if (path.equals("/health")) {
                return json(200, "{\"status\":\"ok\"}");
            }
            if (path.equals("/users/42") && method.equals("GET")) {
                return json(200, "{\"id\":42,\"name\":\"Ada\"}");
            }
            if (path.equals("/users") && method.equals("GET")) {
                return json(200, "[{\"id\":1,\"name\":\"Ada\"},{\"id\":2,\"name\":\"Grace\"}]");
            }
            if (path.equals("/users") && method.equals("POST")) {
                return json(201, "{\"id\":7,\"name\":\"Created\"}");
            }
            if (path.equals("/ndjson")) {
                return streaming("application/x-ndjson",
                        "{\"id\":1,\"name\":\"first\"}\n"
                                + "{\"id\":2,\"name\":\"second\"}\n"
                                + "{\"id\":3,\"name\":\"third\"}\n");
            }
            if (path.equals("/sse")) {
                return streaming("text/event-stream",
                        "id: 1\nevent: user\ndata: {\"id\":1,\"name\":\"first\"}\n\n"
                                + "id: 2\nevent: user\ndata: {\"id\":2,\"name\":\"second\"}\n\n"
                                + "id: 3\nevent: user\ndata: {\"id\":3,\"name\":\"third\"}\n\n")
                        .setHeader("Cache-Control", "no-cache");
            }
            if (path.equals("/empty")) {
                return new MockResponse().setResponseCode(204);
            }
            if (path.equals("/slow")) {
                return json(200, "{\"id\":99,\"name\":\"slow\"}")
                        .setBodyDelay(2, TimeUnit.SECONDS);
            }
            if (path.equals("/error")) {
                return json(422, "{\"code\":\"invalid_request\",\"message\":\"Example failure\"}");
            }
            return json(404, "{\"code\":\"not_found\",\"path\":\"" + path + "\"}");
        }
    }

    private MockResponse streaming(String contentType, String body) {
        MockResponse response = new MockResponse()
                .setResponseCode(200)
                .setHeader("Content-Type", contentType)
                .setChunkedBody(body, 32);
        return streamPeriodMillis == 0
                ? response
                : response.throttleBody(32, streamPeriodMillis, TimeUnit.MILLISECONDS);
    }

    private static MockResponse json(int status, String body) {
        return new MockResponse()
                .setResponseCode(status)
                .setHeader("Content-Type", JSON)
                .setBody(body);
    }

}
