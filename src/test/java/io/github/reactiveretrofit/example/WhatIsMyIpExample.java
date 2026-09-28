package io.github.reactiveretrofit.example;

import io.github.reactiveretrofit.ReactorCallAdapterFactory;
import okhttp3.OkHttpClient;
import reactor.core.publisher.Mono;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;
import retrofit2.http.GET;
import retrofit2.http.Query;

/** Run manually to exercise Reactive Retrofit against the public ipify API. */
public final class WhatIsMyIpExample {
    private WhatIsMyIpExample() {
    }

    interface IpifyApi {
        @GET(".")
        Mono<IpResponse> currentIp(@Query("format") String format);
    }

    record IpResponse(String ip) {
    }

    public static void main(String[] args) {
        OkHttpClient client = new OkHttpClient();
        try {
            IpifyApi ipify = new Retrofit.Builder()
                    .baseUrl("https://api.ipify.org/")
                    .client(client)
                    .addConverterFactory(JacksonConverterFactory.create())
                    .addCallAdapterFactory(ReactorCallAdapterFactory.create())
                    .build()
                    .create(IpifyApi.class);

            IpResponse response = ipify.currentIp("json").block();
            System.out.println("Public IP reported by ipify: " + response.ip());
        } finally {
            client.dispatcher().executorService().shutdown();
            client.connectionPool().evictAll();
        }
    }
}
