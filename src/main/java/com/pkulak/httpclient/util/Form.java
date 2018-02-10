package com.pkulak.httpclient.util;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.stream.Collectors;

public class Form {
    public static String encode(String s) {
        try {
            return URLEncoder.encode(s, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("encoding not supported: UTF-8", e);
        }
    }

    public static String encode(Multimap<String, ?> map) {
        return map.entries().stream()
                .map(entry -> encode(entry.getKey()) + "=" + encode(entry.getValue().toString()))
                .collect(Collectors.joining("&"));
    }

    public static Multimap<String, String> decode(String input) {
        Multimap<String, String> accum = LinkedListMultimap.create();

        for (String part : input.split("&")) {
            String[] keyValue = part.split("=");

            try {
                accum.put(
                        URLDecoder.decode(keyValue[0], "UTF-8"),
                        URLDecoder.decode(keyValue[1], "UTF-8"));
            } catch (UnsupportedEncodingException e) { /* oh well... */ }
        }

        return accum;
    }
}
