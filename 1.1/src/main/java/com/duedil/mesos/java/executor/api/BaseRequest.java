package com.duedil.mesos.java.executor.api;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.protobuf.ProtoObjectParser;

import java.io.IOException;
import java.net.URI;

import static com.google.common.base.Preconditions.checkNotNull;

abstract class BaseRequest implements Requestable {

    static final HttpRequestFactory REQUEST_FACTORY = new NetHttpTransport()
            .createRequestFactory(new HttpRequestInitializer() {
                @Override
                public void initialize(HttpRequest request) throws IOException {
                    request.setParser(new ProtoObjectParser());
                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentType(CONTENT_TYPE);
                    headers.setAccept(CONTENT_TYPE);
                    request.setHeaders(headers);
                }});

    final GenericUrl baseUrl;

    BaseRequest(URI baseUrl) {
        this.baseUrl = new GenericUrl(checkNotNull(baseUrl));
    }

    @Override
    public GenericUrl getBaseUrl() {
        return baseUrl;
    }

    private static final String CONTENT_TYPE = "application/json";

}
