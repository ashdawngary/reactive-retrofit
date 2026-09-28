# Reactive Retrofit

A Project Reactor call adapter for Retrofit. It lets ordinary Retrofit service interfaces return
`Mono<T>` and `Flux<T>` while retaining Retrofit's annotations, converters, OkHttp integration,
interceptors, and response types.

The project requires Java 17 or newer. It compiles against Reactor 3.5.20 and is tested with the
latest Reactor 3.5, 3.6, 3.7, and 3.8 releases. Retrofit 3.0.0 is the current compile target.

## Compatibility and versioning

Reactive Retrofit is distributed as one adapter artifact across compatible Reactor releases. The
version in `pom.xml` is the oldest Reactor version used to compile production code; newer supported
versions are exercised by the compatibility test matrix.

| Reactor line | Tested version |
| --- | --- |
| 3.5 | 3.5.20 |
| 3.6 | 3.6.18 |
| 3.7 | 3.7.19 |
| 3.8 | 3.8.7 |

Adapter releases follow Semantic Versioning:

- Patch releases contain compatible bug and dependency fixes.
- Minor releases add backward-compatible capabilities or expand compatibility.
- Major releases may change public behavior or raise minimum Java, Reactor, or Retrofit versions.

Applications remain free to select a supported Reactor version directly or through their
framework's BOM. Reactor is not shaded and Maven version ranges are not used.

Once the first release is published to Maven Central, applications can use the permanent
coordinates below, replacing the version with the desired release:

```xml
<dependency>
    <groupId>io.github.ashdawngary</groupId>
    <artifactId>reactive-retrofit</artifactId>
    <version>0.1.0</version>
</dependency>
```

## Current development status

- Retrofit-native `Mono<T>`, `Mono<Response<T>>`, `Flux<T>`, and `Flux<Response<T>>` adaptation.
- JSON-object, JSON-array, NDJSON, and server-sent-event handling.
- Spring Boot integration coverage using a lifecycle-managed MockWebServer and injected Retrofit
  client.

## Quick start

Define a normal Retrofit interface using `retrofit2.http` annotations:

```java
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import retrofit2.Response;
import retrofit2.http.Body;
import retrofit2.http.GET;
import retrofit2.http.POST;
import retrofit2.http.Path;

interface UsersApi {
    @GET("users/{id}")
    Mono<User> user(@Path("id") long id);

    @GET("users")
    Flux<User> users();

    @POST("users")
    Mono<User> create(@Body NewUser user);

    @GET("users/{id}")
    Mono<Response<User>> userResponse(@Path("id") long id);
}
```

Install the adapter alongside any Retrofit converter:

```java
Retrofit retrofit = new Retrofit.Builder()
    .baseUrl("https://api.example.com/v1/")
    .addConverterFactory(JacksonConverterFactory.create())
    .addCallAdapterFactory(ReactorCallAdapterFactory.create())
    .build();

UsersApi users = retrofit.create(UsersApi.class);

Mono<User> ada = users.user(42);
Flux<User> allUsers = users.users();
```

Calls are lazy. Each subscription clones and enqueues a new Retrofit `Call`, and cancelling the
subscription cancels that call.

## Return types

- `Mono<T>` emits the converted body of a successful response. A non-2xx response emits
  Retrofit's `HttpException`.
- `Mono<Response<T>>` emits the complete Retrofit response for every HTTP status.
- `Flux<Response<T>>` emits the single complete Retrofit response.
- `Flux<T>` uses the response `Content-Type` to adapt the body:
  - A JSON object emits one value.
  - A JSON array emits each array element.
  - NDJSON emits one converted value per non-empty line.
  - Server-sent events emit one converted value per JSON `data:` event.

NDJSON and SSE endpoints should use Retrofit's standard `@Streaming` annotation so values can be
delivered before the response finishes:

```java
@Streaming
@GET("events")
Flux<Event> events();
```

Without `@Streaming`, Retrofit buffers a raw response body before handing it to the adapter.

## Scheduler

By default, Retrofit invokes response signals on its OkHttp callback threads. To schedule
subscription through a Reactor scheduler, configure:

```java
.addCallAdapterFactory(
    ReactorCallAdapterFactory.createWithScheduler(Schedulers.boundedElastic())
)
```

Normal Reactor operators such as `publishOn` and `subscribeOn` remain available to callers.

## Build

```shell
mvn verify
```

Run the complete Reactor compatibility matrix with:

```shell
./scripts/verify-reactor-matrix.sh
```

## Spring Boot integration test

`SpringBootReactiveRetrofitTest` starts a Spring Boot application context containing:

- A lifecycle-managed MockWebServer bean.
- A Retrofit bean configured with Jackson and `ReactorCallAdapterFactory`.
- A typed Retrofit API bean consumed by the test.

The mock server exposes JSON object, JSON array, POST, NDJSON, SSE, HTTP 204, delayed, and error
responses. The test calls those endpoints through injected `Mono<T>` and `Flux<T>` contracts,
including streaming methods annotated with Retrofit's `@Streaming`.

Run it independently with:

```shell
mvn -Dtest=SpringBootReactiveRetrofitTest test
```

See
[`SpringBootReactiveRetrofitTest.java`](src/test/java/io/github/reactiveretrofit/support/SpringBootReactiveRetrofitTest.java)
for the complete Spring configuration and API contract.

## Try the public IP example

[WhatIsMyIpExample.java](src/test/java/io/github/reactiveretrofit/example/WhatIsMyIpExample.java)
calls `https://api.ipify.org/?format=json` through Retrofit and this adapter:

```shell
mvn -q test-compile org.codehaus.mojo:exec-maven-plugin:3.6.2:java \
  -Dexec.mainClass=io.github.reactiveretrofit.example.WhatIsMyIpExample \
  -Dexec.classpathScope=test
```

## License

Reactive Retrofit is available under the [Apache License 2.0](LICENSE).
