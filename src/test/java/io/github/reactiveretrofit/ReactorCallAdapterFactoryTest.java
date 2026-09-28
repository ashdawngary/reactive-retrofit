package io.github.reactiveretrofit;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import retrofit2.HttpException;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;
import retrofit2.http.Body;
import retrofit2.http.GET;
import retrofit2.http.Header;
import retrofit2.http.POST;
import retrofit2.http.Path;
import retrofit2.http.Query;
import retrofit2.http.Streaming;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReactorCallAdapterFactoryTest {
    private MockWebServer server;
    private Api api;

    @BeforeEach
    void startServer() throws IOException {
        server = new MockWebServer();
        server.start();
        api = retrofitBuilder().build().create(Api.class);
    }

    @AfterEach
    void stopServer() throws IOException {
        server.shutdown();
    }

    @Test
    void monoIsLazyBindsRetrofitArgumentsAndCanBeResubscribed() throws InterruptedException {
        server.enqueue(json(200, "{\"id\":42,\"name\":\"Ada\"}"));
        server.enqueue(json(200, "{\"id\":42,\"name\":\"Ada\"}"));

        Mono<User> call = api.user(42, "teams", "token");
        assertNull(server.takeRequest(100, TimeUnit.MILLISECONDS));

        StepVerifier.create(call).expectNext(new User(42, "Ada")).verifyComplete();
        StepVerifier.create(call).expectNext(new User(42, "Ada")).verifyComplete();

        RecordedRequest first = server.takeRequest();
        assertEquals("/users/42?include=teams", first.getPath());
        assertEquals("token", first.getHeader("Authorization"));
        assertEquals("/users/42?include=teams", server.takeRequest().getPath());
    }

    @Test
    void usesRetrofitConvertersForRequestAndResponseBodies() throws InterruptedException {
        server.enqueue(json(201, "{\"id\":7,\"name\":\"Grace\"}"));

        StepVerifier.create(api.create(new NewUser("Grace")))
                .expectNext(new User(7, "Grace"))
                .verifyComplete();

        RecordedRequest request = server.takeRequest();
        assertEquals("POST", request.getMethod());
        assertEquals("{\"name\":\"Grace\"}", request.getBody().readUtf8());
        assertEquals("application/json; charset=UTF-8", request.getHeader("Content-Type"));
    }

    @Test
    void fluxDecodesJsonArraysAndSingleJsonValues() {
        server.enqueue(json(200, "[{\"id\":1,\"name\":\"Ada\"},{\"id\":2,\"name\":\"Grace\"}]"));
        server.enqueue(json(200, "{\"id\":3,\"name\":\"Linus\"}"));

        StepVerifier.create(api.users())
                .expectNext(new User(1, "Ada"), new User(2, "Grace"))
                .verifyComplete();
        StepVerifier.create(api.users())
                .expectNext(new User(3, "Linus"))
                .verifyComplete();
    }

    @Test
    void fluxDecodesStreamingNdjsonAndServerSentEvents() {
        server.enqueue(new MockResponse()
                .setHeader("Content-Type", "application/x-ndjson")
                .setChunkedBody("{\"id\":1,\"name\":\"first\"}\n{\"id\":2,\"name\":\"second\"}\n", 9));
        server.enqueue(new MockResponse()
                .setHeader("Content-Type", "text/event-stream")
                .setChunkedBody("event: user\ndata: {\"id\":3,\"name\":\"third\"}\n\n", 7));

        StepVerifier.create(api.events())
                .expectNext(new User(1, "first"), new User(2, "second"))
                .verifyComplete();
        StepVerifier.create(api.sse())
                .expectNext(new User(3, "third"))
                .verifyComplete();
    }

    @Test
    void bodyTypesFailOnHttpErrorsWhileResponseTypesExposeThem() {
        server.enqueue(json(404, "{\"message\":\"gone\"}"));
        server.enqueue(json(404, "{\"message\":\"gone\"}"));

        StepVerifier.create(api.missing())
                .expectErrorSatisfies(error -> {
                    HttpException http = (HttpException) error;
                    assertEquals(404, http.code());
                    try {
                        assertEquals("{\"message\":\"gone\"}", http.response().errorBody().string());
                    } catch (IOException ioException) {
                        throw new AssertionError(ioException);
                    }
                })
                .verify();

        StepVerifier.create(api.missingResponse())
                .assertNext(response -> {
                    assertFalse(response.isSuccessful());
                    assertEquals(404, response.code());
                })
                .verifyComplete();
    }

    @Test
    void rejectsRawReactorReturnTypesDuringEagerValidation() {
        Retrofit retrofit = retrofitBuilder().validateEagerly(true).build();
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> retrofit.create(InvalidApi.class));
        assertFalse(error.getMessage().isBlank());
    }

    private Retrofit.Builder retrofitBuilder() {
        return new Retrofit.Builder()
                .baseUrl(server.url("/"))
                .addConverterFactory(JacksonConverterFactory.create())
                .addCallAdapterFactory(ReactorCallAdapterFactory.create());
    }

    private static MockResponse json(int status, String body) {
        return new MockResponse()
                .setResponseCode(status)
                .setHeader("Content-Type", "application/json")
                .setBody(body);
    }

    interface Api {
        @GET("users/{id}")
        Mono<User> user(@Path("id") long id, @Query("include") String include,
                        @Header("Authorization") String authorization);

        @POST("users")
        Mono<User> create(@Body NewUser user);

        @GET("users")
        Flux<User> users();

        @Streaming
        @GET("events")
        Flux<User> events();

        @Streaming
        @GET("sse")
        Flux<User> sse();

        @GET("missing")
        Mono<User> missing();

        @GET("missing")
        Mono<Response<User>> missingResponse();
    }

    interface InvalidApi {
        @GET("users")
        @SuppressWarnings("rawtypes")
        Mono users();
    }

    record User(long id, String name) {}
    record NewUser(String name) {}
}
