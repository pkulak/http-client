package com.pkulak.httpclient.mapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class JacksonRequestMapper implements RequestMapper<Object> {
    private static final String CONTENT_TYPE = "application/json";

    private final ObjectMapper mapper;

    public JacksonRequestMapper(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public InputStream map(Object object) throws JsonProcessingException {
        return new ByteArrayInputStream(mapper.writeValueAsBytes(object));
    }

    @Override
    public String defaultContentType() {
        return CONTENT_TYPE;
    }
}
