package com.pkulak.httpclient;

import com.google.common.base.CharMatcher;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.pkulak.httpclient.util.Form;
import org.asynchttpclient.Param;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Strings.isNullOrEmpty;

public class Request {
    private final String method;
    private final String url;
    private final String path;
    private final Multimap<String, Supplier<String>> queryParams;
    private final Map<String, Supplier<String>> pathParams;
    private final Multimap<CharSequence, Supplier<String>> headers;

    private Request(
            String method,
            String url,
            String path,
            Multimap<String, Supplier<String>> queryParams,
            Map<String, Supplier<String>> pathParams,
            Multimap<CharSequence, Supplier<String>> headers) {

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

        if (isNullOrEmpty(path)) {
            this.path = parsedUrl.getPath();
        } else {
            this.path = path;
        }

        ImmutableMultimap.Builder<String, Supplier<String>> paramBuilder = ImmutableMultimap.builder();

        if (!isNullOrEmpty(parsedUrl.getQuery())) {
            for (Map.Entry<String, String> entry : Form.decode(parsedUrl.getQuery()).entries()) {
                paramBuilder.put(entry.getKey(), entry::getValue);
            }
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

        for (Map.Entry<String, Supplier<String>> replacement : pathParams.entrySet()) {
            replacedPath = replacedPath.replace(
                    "{" + replacement.getKey() + "}",
                    Form.encode(replacement.getValue().get()));
        }

        return url + replacedPath;
    }

    public boolean isUrlSet() {
        return !isNullOrEmpty(url);
    }

    @Override
    public String toString() {
        if (queryParams.isEmpty()) return method + " " + getUrl();
        return method + " " + getUrl() + "?" + Form.encodeSupplied(queryParams);
    }

    public List<Param> getQueryParams() {
        return queryParams.entries().stream()
                .map(entry -> new Param(entry.getKey(), entry.getValue().get()))
                .collect(Collectors.toList());
    }

    public Map<CharSequence, List<String>> getHeaders() {
        Map<CharSequence, List<String>> retMap = new TreeMap<>();

        for (Map.Entry<CharSequence, Supplier<String>> entry : headers.entries()) {
            if (!retMap.containsKey(entry.getKey())) {
                retMap.put(entry.getKey(), new ArrayList<>());
            }

            retMap.get(entry.getKey()).add(entry.getValue().get());
        }

        return retMap;
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
        private Multimap<String, Supplier<String>> queryParams = ImmutableMultimap.of();
        private Map<String, Supplier<String>> pathParams = ImmutableMap.of();
        private Multimap<CharSequence, Supplier<String>> headers = ImmutableMultimap.of();

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

        public Builder appendPath(String path) {
            if (isNullOrEmpty(this.path)) {
                this.path = "/";
            }

            this.path = this.path + path;
            return this;
        }

        public Builder pathParam(String key, Supplier<String> val) {
            this.pathParams = ImmutableMap.<String, Supplier<String>>builder()
                    .putAll(pathParams)
                    .put(key, val)
                    .build();
            return this;
        }

        public Builder setQueryParam(String key, Supplier<String> val) {
            this.queryParams = set(this.queryParams, key, val);
            return this;
        }

        public Builder addQueryParam(String key, Supplier<String> val) {
            this.queryParams = ImmutableMultimap.<String, Supplier<String>>builder()
                    .putAll(queryParams)
                    .put(key, val)
                    .build();
            return this;
        }

        public Builder setHeader(CharSequence key, Supplier<String> val) {
            this.headers = set(this.headers, key, val);
            return this;
        }

        public Builder addHeader(CharSequence key, Supplier<String> val) {
            this.headers = ImmutableMultimap.<CharSequence, Supplier<String>>builder()
                    .putAll(headers)
                    .put(key, val)
                    .build();
            return this;
        }

        public Request build() {
            return new Request(method, url, path, queryParams, pathParams, headers);
        }


        private <T, U> Multimap<T, U> set(Multimap<T, U> map, T key, U val) {
            return ImmutableMultimap.<T, U>builder()
                    .putAll(map.entries().stream()
                            .filter(entry -> !entry.getKey().equals(key))
                            .collect(Collectors.toList()))
                    .put(key, val)
                    .build();
        }

    }
}
