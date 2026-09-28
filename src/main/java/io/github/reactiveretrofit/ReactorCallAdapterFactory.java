package io.github.reactiveretrofit;

import okhttp3.MediaType;
import okhttp3.ResponseBody;
import okio.BufferedSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.core.scheduler.Scheduler;
import retrofit2.Call;
import retrofit2.CallAdapter;
import retrofit2.Callback;
import retrofit2.Converter;
import retrofit2.HttpException;
import retrofit2.Response;
import retrofit2.Retrofit;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Adapts Retrofit calls to lazy Project Reactor {@link Mono} and {@link Flux} publishers.
 *
 * <p>A {@code Mono<T>} emits a successful response body and turns non-2xx responses into
 * {@link HttpException}. A {@code Mono<Response<T>>} exposes every HTTP response directly.
 * A body-only {@code Flux<T>} supports a single JSON value, a JSON array, NDJSON, and JSON values
 * carried by server-sent-event {@code data:} fields. Use Retrofit's {@code @Streaming} annotation
 * for NDJSON and SSE endpoints so Retrofit does not buffer the response before adaptation.
 */
public final class ReactorCallAdapterFactory extends CallAdapter.Factory {
    private final Scheduler scheduler;

    private ReactorCallAdapterFactory(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    /**
     * Creates an adapter which signals on Retrofit/OkHttp callback threads.
     *
     * @return a call-adapter factory using Retrofit's callback threads
     */
    public static ReactorCallAdapterFactory create() {
        return new ReactorCallAdapterFactory(null);
    }

    /**
     * Creates an adapter whose subscription and response processing run on {@code scheduler}.
     *
     * @param scheduler scheduler used for subscription and response processing
     * @return a call-adapter factory using the supplied scheduler
     */
    public static ReactorCallAdapterFactory createWithScheduler(Scheduler scheduler) {
        return new ReactorCallAdapterFactory(Objects.requireNonNull(scheduler, "scheduler"));
    }

    @Override
    public CallAdapter<?, ?> get(Type returnType, Annotation[] annotations, Retrofit retrofit) {
        Class<?> rawReturnType = getRawType(returnType);
        boolean mono = rawReturnType == Mono.class;
        boolean flux = rawReturnType == Flux.class;
        if (!mono && !flux) {
            return null;
        }
        if (!(returnType instanceof ParameterizedType parameterizedType)) {
            throw new IllegalStateException(rawReturnType.getSimpleName()
                    + " return type must be parameterized as " + rawReturnType.getSimpleName() + "<Foo>");
        }

        Type declaredType = getParameterUpperBound(0, parameterizedType);
        if (getRawType(declaredType) == Response.class) {
            if (!(declaredType instanceof ParameterizedType responseType)) {
                throw new IllegalStateException("Response must be parameterized as Response<Foo>");
            }
            Type bodyType = getParameterUpperBound(0, responseType);
            return new ResponseAdapter<>(bodyType, mono, scheduler);
        }

        if (mono) {
            return new MonoBodyAdapter<>(declaredType, scheduler);
        }

        Converter<ResponseBody, Object> elementConverter = responseConverter(retrofit, declaredType, annotations);
        Type listType = new ParameterizedTypeValue(List.class, new Type[]{declaredType});
        Converter<ResponseBody, List<Object>> listConverter = responseConverter(retrofit, listType, annotations);
        return new FluxBodyAdapter<>(declaredType, elementConverter, listConverter, scheduler);
    }

    @SuppressWarnings("unchecked")
    private static <T> Converter<ResponseBody, T> responseConverter(
            Retrofit retrofit, Type type, Annotation[] annotations) {
        return (Converter<ResponseBody, T>) retrofit.responseBodyConverter(type, annotations);
    }

    private static final class MonoBodyAdapter<R> implements CallAdapter<R, Mono<R>> {
        private final Type responseType;
        private final Scheduler scheduler;

        private MonoBodyAdapter(Type responseType, Scheduler scheduler) {
            this.responseType = responseType;
            this.scheduler = scheduler;
        }

        @Override
        public Type responseType() {
            return responseType;
        }

        @Override
        public Mono<R> adapt(Call<R> call) {
            Mono<R> result = responseMono(call).flatMap(response -> {
                if (!response.isSuccessful()) {
                    return Mono.error(new HttpException(response));
                }
                R body = response.body();
                return body == null ? Mono.empty() : Mono.just(body);
            });
            return schedule(result, scheduler);
        }
    }

    private static final class ResponseAdapter<R> implements CallAdapter<R, Object> {
        private final Type responseType;
        private final boolean mono;
        private final Scheduler scheduler;

        private ResponseAdapter(Type responseType, boolean mono, Scheduler scheduler) {
            this.responseType = responseType;
            this.mono = mono;
            this.scheduler = scheduler;
        }

        @Override
        public Type responseType() {
            return responseType;
        }

        @Override
        public Object adapt(Call<R> call) {
            Mono<Response<R>> response = schedule(responseMono(call), scheduler);
            return mono ? response : response.flux();
        }
    }

    private static final class FluxBodyAdapter<T> implements CallAdapter<ResponseBody, Flux<T>> {
        private final Type elementType;
        private final Converter<ResponseBody, T> elementConverter;
        private final Converter<ResponseBody, List<T>> listConverter;
        private final Scheduler scheduler;

        @SuppressWarnings("unchecked")
        private FluxBodyAdapter(Type elementType,
                                Converter<ResponseBody, Object> elementConverter,
                                Converter<ResponseBody, List<Object>> listConverter,
                                Scheduler scheduler) {
            this.elementType = elementType;
            this.elementConverter = (Converter<ResponseBody, T>) (Converter<?, ?>) elementConverter;
            this.listConverter = (Converter<ResponseBody, List<T>>) (Converter<?, ?>) listConverter;
            this.scheduler = scheduler;
        }

        @Override
        public Type responseType() {
            // Keeping the raw body lets the adapter choose array, NDJSON, or SSE decoding after
            // the response Content-Type is known.
            return ResponseBody.class;
        }

        @Override
        public Flux<T> adapt(Call<ResponseBody> call) {
            Flux<T> result = responseMono(call).flatMapMany(response -> {
                if (!response.isSuccessful()) {
                    return Flux.error(new HttpException(response));
                }
                ResponseBody body = response.body();
                if (body == null) {
                    return Flux.empty();
                }
                return Flux.using(() -> body, this::decode, ResponseBody::close);
            });
            return schedule(result, scheduler);
        }

        private Flux<T> decode(ResponseBody body) {
            String contentType = body.contentType() == null
                    ? ""
                    : body.contentType().toString().toLowerCase(Locale.ROOT);
            if (contentType.contains("text/event-stream")) {
                return decodeSse(body);
            }
            if (contentType.contains("ndjson")) {
                return decodeLines(body);
            }
            if (contentType.contains("json") && startsWithJsonArray(body.source())) {
                return Mono.fromCallable(() -> listConverter.convert(body))
                        .flatMapMany(values -> values == null ? Flux.empty() : Flux.fromIterable(values));
            }
            return Mono.fromCallable(() -> elementConverter.convert(body)).flatMapMany(value ->
                    value == null ? Flux.empty() : Flux.just(value));
        }

        private Flux<T> decodeLines(ResponseBody body) {
            BufferedSource source = body.source();
            MediaType contentType = body.contentType();
            return Flux.generate(sink -> readNextJsonLine(source, contentType, sink));
        }

        private void readNextJsonLine(BufferedSource source, MediaType contentType, SynchronousSink<T> sink) {
            try {
                while (!source.exhausted()) {
                    String line = source.readUtf8Line();
                    if (line != null && !line.isBlank()) {
                        T value = elementConverter.convert(ResponseBody.create(line, contentType));
                        if (value != null) {
                            sink.next(value);
                            return;
                        }
                    }
                }
                sink.complete();
            } catch (Throwable error) {
                sink.error(error);
            }
        }

        private Flux<T> decodeSse(ResponseBody body) {
            BufferedSource source = body.source();
            MediaType contentType = body.contentType();
            return Flux.generate(sink -> readNextSseEvent(source, contentType, sink));
        }

        private void readNextSseEvent(BufferedSource source, MediaType contentType, SynchronousSink<T> sink) {
            try {
                StringBuilder data = new StringBuilder();
                while (!source.exhausted()) {
                    String line = source.readUtf8Line();
                    if (line == null || line.isEmpty()) {
                        if (!data.isEmpty()) {
                            T value = elementConverter.convert(ResponseBody.create(data.toString(), contentType));
                            if (value != null) sink.next(value);
                            return;
                        }
                        continue;
                    }
                    if (line.startsWith("data:")) {
                        if (!data.isEmpty()) data.append('\n');
                        data.append(line.substring(5).stripLeading());
                    }
                }
                if (!data.isEmpty()) {
                    T value = elementConverter.convert(ResponseBody.create(data.toString(), contentType));
                    if (value != null) sink.next(value);
                } else {
                    sink.complete();
                }
            } catch (Throwable error) {
                sink.error(error);
            }
        }

        private boolean startsWithJsonArray(BufferedSource source) {
            try (BufferedSource peek = source.peek()) {
                while (!peek.exhausted()) {
                    int next = peek.readByte() & 0xff;
                    if (!Character.isWhitespace(next)) {
                        return next == '[';
                    }
                }
                return false;
            } catch (IOException error) {
                throw new ReactiveAdapterException("Could not inspect response for Flux<" + elementType + ">", error);
            }
        }
    }

    private static <R> Mono<Response<R>> responseMono(Call<R> original) {
        return Mono.defer(() -> Mono.create(sink -> {
            Call<R> call = original.clone();
            sink.onCancel(call::cancel);
            call.enqueue(new Callback<>() {
                @Override
                public void onResponse(Call<R> ignored, Response<R> response) {
                    sink.success(response);
                }

                @Override
                public void onFailure(Call<R> ignored, Throwable error) {
                    if (!call.isCanceled()) {
                        sink.error(error);
                    }
                }
            });
        }));
    }

    private static <T> Mono<T> schedule(Mono<T> publisher, Scheduler scheduler) {
        return scheduler == null ? publisher : publisher.subscribeOn(scheduler);
    }

    private static <T> Flux<T> schedule(Flux<T> publisher, Scheduler scheduler) {
        return scheduler == null ? publisher : publisher.subscribeOn(scheduler);
    }

    private record ParameterizedTypeValue(Type rawType, Type[] actualTypeArguments) implements ParameterizedType {
        private ParameterizedTypeValue {
            actualTypeArguments = actualTypeArguments.clone();
        }

        @Override
        public Type[] getActualTypeArguments() {
            return actualTypeArguments.clone();
        }

        @Override
        public Type getRawType() {
            return rawType;
        }

        @Override
        public Type getOwnerType() {
            return null;
        }
    }
}
