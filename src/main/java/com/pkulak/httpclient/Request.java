package com.pkulak.httpclient;

import com.google.common.base.CharMatcher;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.pkulak.httpclient.util.Form;
import org.asynchttpclient.Param;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Request {
    private final String method;
    private final String url;
    private final String path;
    private final Multimap<String, String> queryParams;
    private final Map<String, String> pathParams;
    private final Multimap<CharSequence, String> headers;

    private Request(
            String method,
            String url,
            String path,
            Multimap<String, String> queryParams,
            Map<String, String> pathParams,
            Multimap<CharSequence, String> headers) {

        this.method = method;
        this.headers = headers;
        this.pathParams = pathParams;

        // can we skip all the parsing?
        if (url == null || !url.contains("?") && CharMatcher.is('/').countIn(url) == 2) {
            this.url = url;
            this.path = path;
            this.queryParams = queryParams;
            return;
        }

        URL parsedUrl;

        try {
            parsedUrl = new URL(url);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("invalid URL: " + url);
        }

        if (Strings.isNullOrEmpty(path)) {
            this.path = parsedUrl.getPath();
        } else {
            this.path = path;
        }

        ImmutableMultimap.Builder<String, String> paramBuilder = ImmutableMultimap.builder();

        if (!Strings.isNullOrEmpty(parsedUrl.getQuery())) {
            paramBuilder.putAll(Form.decode(parsedUrl.getQuery()));
        }

        paramBuilder.putAll(queryParams);

        this.queryParams = paramBuilder.build();
        this.url = parsedUrl.getProtocol() + "://" + parsedUrl.getAuthority();
    }

    static Builder builder(String url) {
        return new Builder(url);
    }

    public String getMethod() {
        return method;
    }

    public String getUrl() {
        String replacedPath = path;

        for (Map.Entry<String, String> replacement : pathParams.entrySet()) {
            replacedPath = replacedPath.replace("{" + replacement.getKey() + "}", Form.encode(replacement.getValue()));
        }

        return url + replacedPath;
    }

    public boolean isUrlSet() {
        return !Strings.isNullOrEmpty(url);
    }

    @Override
    public String toString() {
        if (queryParams.isEmpty()) return method + " " + getUrl();
        return method + " " + getUrl() + "?" + Form.encode(queryParams);
    }

    public List<Param> getQueryParams() {
        return queryParams.entries().stream()
                .map(entry -> new Param(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

    public Map<CharSequence, Collection<String>> getHeaders() {
        return headers.asMap();
    }

    Builder toBuilder() {
        Builder builder = new Builder(url);
        builder.method = method;
        builder.path = path;
        builder.queryParams = queryParams;
        builder.pathParams = pathParams;
        builder.headers = headers;
        return builder;
    }

    static class Builder {
        private String method = "GET";
        private String url;
        private String path = "/";
        private Multimap<String, String> queryParams = ImmutableMultimap.of();
        private Map<String, String> pathParams = ImmutableMap.of();
        private Multimap<CharSequence, String> headers = ImmutableMultimap.of();

        private Builder(String url) {
            this.url = url;
        }

        public Builder url(String url) {
            this.url = url;
            this.path = "";
            this.queryParams = ImmutableMultimap.of();
            return this;
        }

        public Builder method(String method) {
            this.method = method.toUpperCase();
            return this;
        }

        public Builder path(String path) {
            this.path = path;
            return this;
        }

        public Builder addPath(String path) {
            this.path = this.path + path;
            return this;
        }

        public Builder pathParam(String key, Object val) {
            this.pathParams = ImmutableMap.<String, String>builder()
                    .putAll(pathParams)
                    .put(key, val.toString())
                    .build();
            return this;
        }

        public Builder setQueryParam(String key, Object val) {
            this.queryParams = ImmutableMultimap.<String, String>builder()
                    .putAll(queryParams)
                    .putAll(key, val.toString())
                    .build();
            return this;
        }

        public Builder addQueryParam(String key, Object val) {
            this.queryParams = ImmutableMultimap.<String, String>builder()
                    .putAll(queryParams)
                    .put(key, val.toString())
                    .build();
            return this;
        }

        public Builder setHeader(CharSequence key, Object val) {
            this.headers = ImmutableMultimap.<CharSequence, String>builder()
                    .putAll(headers)
                    .putAll(key, val.toString())
                    .build();
            return this;
        }

        public Builder addHeader(CharSequence key, Object val) {
            this.headers = ImmutableMultimap.<CharSequence, String>builder()
                    .putAll(headers)
                    .put(key, val.toString())
                    .build();
            return this;
        }

        public Request build() {
            return new Request(method, url, path, queryParams, pathParams, headers);
        }
    }
}

