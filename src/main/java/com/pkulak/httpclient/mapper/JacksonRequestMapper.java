package com.pkulak.httpclient.mapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class JacksonRequestMapper implements RequestMapper<Object> {
    private static final String CONTENT_TYPE = "application/json";

    private final ObjectWriter writer;

    public JacksonRequestMapper(ObjectWriter writer) {
        this.writer = writer;
    }

    @Override
    public InputStream map(Object object) throws JsonProcessingException {
        return new ByteArrayInputStream(writer.writeValueAsBytes(object));
    }

    @Override
    public String defaultContentType() {
        return CONTENT_TYPE;
    }
}
