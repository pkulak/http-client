package com.pkulak.httpclient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import com.google.common.collect.ImmutableMultimap;
import com.pkulak.httpclient.response.FullResponse;
import com.pkulak.httpclient.response.HeaderResponse;
import net.minidev.json.JSONArray;
import org.apache.http.HttpHeaders;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class HttpClientTest {
    private HttpClient<Object, JsonNode> client;

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(8089);

    @Before
    public void initialize() {
        client = HttpClient.createDefault("http://localhost:8089");
    }

    @Test(expected = HttpClient.HttpException.class)
    public void noUrl() throws Exception {
        HttpClient.createDefault().get();
    }

    @Test
    public void stringing() throws Exception {
        assertEquals(
                client.method("GET").setPath("/testering").setQueryParam("c", "a&w").toString(),
                "GET http://localhost:8089/testering?c=a%26w");
    }

    @Test
    public void simpleGet() throws Exception {
        String body = "[\"hi there!\"]";

        stubFor(get(urlEqualTo("/simple_get"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withHeader("Content-Length", Integer.toString(body.length()))
                        .withBody(body)));

        JsonNode response = client.setPath("/simple_get").get();
        assertEquals(response.get(0).asText(), "hi there!");
    }

    @Test
    public void getWithBadResponse() {
        stubFor(get(urlEqualTo("/get_with_bad_response")).willReturn(aResponse().withStatus(500)));

        HttpClient.HttpException exception = null;

        try {
            client.setPath("/get_with_bad_response").get();
        } catch (HttpClient.HttpException e) {
            exception = e;
        }

        assertNotNull(exception);

        assertTrue(exception.getCause().getMessage().startsWith("invalid response status"));
    }

    @Test
    public void getWithVoidResponse() {
        stubFor(get(urlEqualTo("/get_with_void_response")).willReturn(aResponse().withStatus(200)));
        client.setPath("/get_with_void_response").voidResponse().get();
    }

    @Test
    public void getWithSuppliedPath() {
        String body = "[\"hi there!\"]";

        stubFor(get(urlEqualTo("/get_with_supplied_path/6755"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withHeader("Content-Length", Integer.toString(body.length()))
                        .withBody(body)));

        int status = client.setPath("/get_with_supplied_path/{id}")
                .pathParam("id", () -> "6755")
                .statusOnly()
                .get();

        assertEquals(200, status);
    }

    @Test
    public void simpleDelete() throws Exception {
        stubFor(delete(urlEqualTo("/simple_delete")).willReturn(aResponse()));
        client.setPath("/simple_delete").delete();
        assertEquals(findAll(deleteRequestedFor(urlEqualTo("/simple_delete"))).size(), 1);
    }

    @Test
    public void simpleHead() throws Exception {
        stubFor(head(urlEqualTo("/simple_head")).willReturn(aResponse()));
        int status = client.setPath("/simple_head").head().getStatusCode();
        assertEquals(findAll(headRequestedFor(urlEqualTo("/simple_head"))).size(), 1);
        assertEquals(200, status);
    }

    @Test
    public void simplePut() throws Exception {
        stubFor(put(urlEqualTo("/simple_put")).willReturn(aResponse()));
        client.setPath("/simple_put").put(User.FRY);
        assertEquals(findAll(putRequestedFor(urlEqualTo("/simple_put"))).size(), 1);
    }

    @Test
    public void simplePatch() throws Exception {
        stubFor(patch(urlEqualTo("/simple_patch")).willReturn(aResponse()));
        client.setPath("/simple_patch").statusOnly().patch(User.FRY);
        assertEquals(findAll(patchRequestedFor(urlEqualTo("/simple_patch"))).size(), 1);
    }

    @Test
    public void pathReplaceGet() throws Exception {
        stubFor(get(urlEqualTo("/path_replace/42"))
                .willReturn(aResponse().withStatus(200)));

        int status = client.statusOnly().setPath("/path_replace/{id}").pathParam("id", 42).get();
        assertEquals(200, status);
    }

    @Test
    public void concurrentGet() throws Exception {
        stubFor(get(urlEqualTo("/concurrent_get"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withFixedDelay(100)
                        .withBody("[\"hi there!\"]")));

        HttpClient<Object, JsonNode> throttledClient = client.maxConcurrency(2);
        AtomicInteger leaseCount = new AtomicInteger();

        // create one for json arrays
        HttpClient<Object, JSONArray> arrayClient = throttledClient.forModelType(JSONArray.class);

        // and one to just get statuses
        HttpClient<Object, Integer> statusClient = throttledClient.statusOnly();

        Instant start = Instant.now();
        CountDownLatch latch = new CountDownLatch(20);

        for (int i = 0; i < 10; i++) {
            arrayClient
                    .setPath("/concurrent_get")
                    .getAsync()
                    .whenComplete((a, b) -> latch.countDown());
        }

        for (int i = 0; i < 10; i++) {
            statusClient
                    .setPath("/concurrent_get")
                    .getAsync()
                    .whenComplete((a, b) -> latch.countDown());
        }

        latch.await();

        long millis = Duration.between(start, Instant.now()).toMillis();

        // should have taken about 1 second plus overhead
        assertTrue(millis > 1000);
        assertTrue(millis < 1500);

        // now, make a new client with a much higher throughput
        HttpClient<Object, JSONArray> firehose = arrayClient.maxConcurrency(10);

        start = Instant.now();
        CountDownLatch latchTwo = new CountDownLatch(10);

        for (int i = 0; i < 10; i++) {
            firehose
                    .setPath("/concurrent_get")
                    .getAsync()
                    .whenComplete((a, b) -> latchTwo.countDown());
        }

        latchTwo.await();

        millis = Duration.between(start, Instant.now()).toMillis();

        System.out.println(millis);

        // should have taken only 1/10th of a second, but we'll round up to account for overhead
        assertTrue(millis < 500);
    }

    @Test
    public void simplePost() throws Exception {
        stubFor(post(urlEqualTo("/simple_post"))
                .willReturn(aResponse().withStatus(201)));

        int status = client.setPath("/simple_post").statusOnly().post(new User("Philip", 33));

        assertEquals(status, 201);

        LoggedRequest request = findAll(postRequestedFor(urlEqualTo("/simple_post"))).get(0);

        assertEquals(request.getBodyAsString(), "{\"name\":\"Philip\",\"age\":33,\"favoriteColor\":null}");
        assertEquals(request.getHeader("Content-Type"), "application/json");
    }

    @Test
    public void lowerCasePost() throws Exception {
        stubFor(post(urlEqualTo("/lower_case_post"))
                .willReturn(aResponse().withStatus(200)));

        client
                .objectMapper(new ObjectMapper().setPropertyNamingStrategy(PropertyNamingStrategy.LOWER_CASE))
                .setPath("/lower_case_post")
                .voidResponse()
                .post(new User("Philip", 33, "purple"));

        LoggedRequest req = findAll(postRequestedFor(anyUrl())).get(0);

        assertTrue(req.getBodyAsString().contains("favoritecolor"));
    }

    @Test
    public void formPost() throws Exception {
        stubFor(post(urlEqualTo("/form_post"))
                .willReturn(aResponse().withStatus(200)));

        int status = client.setPath("/form_post")
                .withForm()
                .statusOnly()
                .post(ImmutableMultimap.of("a", 6, "b", "y&p"));

        assertEquals(status, 200);

        LoggedRequest lastRequest = findAll(postRequestedFor(urlEqualTo("/form_post"))).get(0);
        assertEquals(lastRequest.getBodyAsString(), "a=6&b=y%26p");
    }

    @Test
    public void multipleQueryAndHeader() throws Exception {
        stubFor(get(urlPathEqualTo("/multimap"))
                .willReturn(aResponse().withStatus(200)));

        int status = client.setPath("/multimap")
                .statusOnly()
                .setQueryParam("a", "b")
                .addQueryParam("a", "c")
                .setQueryParam("b", "a")
                .setQueryParam("b", "b")
                .setHeader("pkulak-test", "b")
                .addHeader("pkulak-test", "c")
                .setHeader("user-agent", "bloop/1.0")
                .setHeader("user-agent", "blap/1.2")
                .get();

        assertEquals(status, 200);

        LoggedRequest lastRequest = findAll(getRequestedFor(urlPathEqualTo("/multimap"))).get(0);

        assertEquals("/multimap?a=b&a=c&b=b", lastRequest.getUrl());
        assertEquals(Arrays.asList("b", "c"), lastRequest.getHeaders().getHeader("pkulak-test").values());
        assertEquals(Collections.singletonList("blap/1.2"), lastRequest.getHeaders().getHeader("user-agent").values());
    }

    @Test
    public void urlParamGet() throws Exception {
        stubFor(get(urlEqualTo("/simple_get?a=b&c=54"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody("[\"hi there!\"]")));

        JsonNode response = client
                .url("http://localhost:8089/simple_get?a=b")
                .setQueryParam("c", 54)
                .get();

        assertEquals(response.get(0).asText(), "hi there!");
    }

    @Test
    public void customMapping() throws Exception {
        stubFor(get(urlEqualTo("/simple_get"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"name\":\"Philip J. Fry\",\"age\":33}")));

        User user = client.setPath("/simple_get")
                .forModelType(User.class)
                .get();

        assertEquals(user.name, "Philip J. Fry");
        assertEquals(user.age, 33);
    }

    @Test
    public void appendToEmptyPath() throws Exception {
        stubFor(get(urlEqualTo("/append_empty")).willReturn(aResponse()));

        HttpClient<Object, JsonNode> client = HttpClient
                .createDefault(null)
                .url("http://localhost:8089");

        assertEquals(200, (long) client.appendPath("append_empty").statusOnly().get());
    }

    @Test
    public void rawResponses() {
        stubFor(get(urlEqualTo("/raw_response"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "text/plain")
                        .withBody("stupid, sexy flanders")));

        FullResponse resp = client.setPath("/raw_response")
                .rawResponse()
                .get();

        assertEquals(200, resp.getStatus().getStatusCode());
        assertEquals("text/plain", resp.getHeaders().get("Content-Type"));
        assertEquals("stupid, sexy flanders", new String(resp.getBody()));
    }

    @Test
    public void headersOnly() {
        stubFor(get(urlEqualTo("/headers_only"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "text/plain")
                        .withBody("stupid, sexy flanders")));

        HeaderResponse resp = client.setPath("/headers_only")
                .headersOnly()
                .get();

        assertEquals(200, resp.getStatus().getStatusCode());
        assertEquals("text/plain", resp.getHeaders().get("Content-Type"));
    }

    static class User {
        public static final User FRY = new User("Philip J. Fry", 33);

        public String name;
        public int age;
        public String favoriteColor;

        public User() {}

        public User(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public User(String name, int age, String favoriteColor) {
            this.name = name;
            this.age = age;
            this.favoriteColor = favoriteColor;
        }
    }
}
