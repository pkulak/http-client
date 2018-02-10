package com.pkulak.httpclient.mapper;

import com.google.common.collect.Multimap;
import com.pkulak.httpclient.util.Form;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class FormRequestMapper implements RequestMapper<Multimap<String, Object>> {
    private static final String CONTENT_TYPE = "application/x-www-form-urlencoded";

    public static FormRequestMapper INSTANCE = new FormRequestMapper();

    private FormRequestMapper() {}

    @Override
    public InputStream map(Multimap<String, Object> object) throws Exception {
        return new ByteArrayInputStream(Form.encode(object).getBytes());
    }

    @Override
    public String defaultContentType() {
        return CONTENT_TYPE;
    }
}
