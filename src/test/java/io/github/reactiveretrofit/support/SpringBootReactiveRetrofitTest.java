package io.github.reactiveretrofit.support;

import io.github.reactiveretrofit.ReactorCallAdapterFactory;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import retrofit2.HttpException;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;
import retrofit2.http.Body;
import retrofit2.http.GET;
import retrofit2.http.POST;
import retrofit2.http.Streaming;

import java.io.IOException;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(
        classes = SpringBootReactiveRetrofitTest.TestApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE)
class SpringBootReactiveRetrofitTest {
    @Autowired
    private MockApi api;

    @Test
    void springManagedClientHitsSpringManagedMockServer() {
        StepVerifier.create(api.user())
                .expectNext(new User(42, "Ada"))
                .verifyComplete();

        StepVerifier.create(api.users())
                .expectNext(new User(1, "Ada"), new User(2, "Grace"))
                .verifyComplete();

        StepVerifier.create(api.create(new NewUser("Created")))
                .expectNext(new User(7, "Created"))
                .verifyComplete();
    }

    @Test
    void springManagedClientConsumesStreamingEndpoints() {
        StepVerifier.create(api.ndjson())
                .expectNext(new User(1, "first"), new User(2, "second"), new User(3, "third"))
                .verifyComplete();

        StepVerifier.create(api.sse())
                .expectNext(new User(1, "first"), new User(2, "second"), new User(3, "third"))
                .verifyComplete();
    }

    @Test
    void springManagedClientHandlesEmptyAndErrorResponses() {
        StepVerifier.create(api.empty()).verifyComplete();

        StepVerifier.create(api.error())
                .expectErrorSatisfies(error -> assertEquals(422, ((HttpException) error).code()))
                .verify();

        StepVerifier.create(api.slow())
                .thenAwait(Duration.ofMillis(100))
                .thenCancel()
                .verify(Duration.ofSeconds(1));
    }

    interface MockApi {
        @GET("users/42")
        Mono<User> user();

        @GET("users")
        Flux<User> users();

        @POST("users")
        Mono<User> create(@Body NewUser user);

        @Streaming
        @GET("ndjson")
        Flux<User> ndjson();

        @Streaming
        @GET("sse")
        Flux<User> sse();

        @GET("empty")
        Mono<Void> empty();

        @GET("error")
        Mono<User> error();

        @GET("slow")
        Mono<User> slow();
    }

    record User(long id, String name) {}
    record NewUser(String name) {}

    @SpringBootConfiguration
    @EnableAutoConfiguration
    static class TestApplication {
        @Bean(destroyMethod = "close")
        ReactiveRetrofitMockServer mockWebServer() throws IOException {
            ReactiveRetrofitMockServer server = new ReactiveRetrofitMockServer(0);
            server.start();
            return server;
        }

        @Bean
        Retrofit retrofit(ReactiveRetrofitMockServer server) {
            return new Retrofit.Builder()
                    .baseUrl(server.baseUrl())
                    .addConverterFactory(JacksonConverterFactory.create())
                    .addCallAdapterFactory(ReactorCallAdapterFactory.create())
                    .build();
        }

        @Bean
        MockApi mockApi(Retrofit retrofit) {
            return retrofit.create(MockApi.class);
        }
    }
}
