package com.pkulak.httpclient.mapper;

import java.io.InputStream;

public interface RequestMapper<T> {
    /**
     * Turn a model object into a request body.
     *
     * @param object the object to map
     * @return a input stream representing the request body
     */
    InputStream map(T object) throws Exception;

    /**
     * Provide a default content type to use when the user doesn't explicitly provide one.
     *
     * @return a MIME type describing the content of this body
     */
    String defaultContentType();
}
