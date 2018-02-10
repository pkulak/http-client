package com.pkulak.httpclient;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import com.google.common.collect.ImmutableMultimap;
import net.minidev.json.JSONArray;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class HttpClientTest {
    private HttpClient<JsonNode, Object> client;

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
    public void simpleDelete() throws Exception {
        stubFor(delete(urlEqualTo("/simple_delete")).willReturn(aResponse()));
        client.setPath("/simple_delete").delete();
        assertEquals(findAll(deleteRequestedFor(urlEqualTo("/simple_delete"))).size(), 1);
    }

    @Test
    public void simpleHead() throws Exception {
        stubFor(head(urlEqualTo("/simple_head")).willReturn(aResponse()));
        int status = client.setPath("/simple_head").head();
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

        HttpClient<JsonNode, Object> throttledClient = client.maxConcurrency(2);
        AtomicInteger leaseCount = new AtomicInteger();

        // create one for json arrays
        HttpClient<JSONArray, Object> arrayClient = throttledClient.forModelType(JSONArray.class);

        // and one to just get statuses
        HttpClient<Integer, Object> statusClient = throttledClient.statusOnly();

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
        HttpClient<JSONArray, Object> firehose = arrayClient.maxConcurrency(10);

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

        assertEquals(request.getBodyAsString(), "{\"name\":\"Philip\",\"age\":33}");
        assertEquals(request.getHeader("Content-Type"), "application/json");
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
                .setHeader("pkulak-test", "b")
                .addHeader("pkulak-test", "c")
                .get();

        assertEquals(status, 200);

        LoggedRequest lastRequest = findAll(getRequestedFor(urlPathEqualTo("/multimap"))).get(0);

        assertEquals(lastRequest.getUrl(), "/multimap?a=b&a=c");
        assertEquals(lastRequest.getHeaders().getHeader("pkulak-test").values(), Arrays.asList("b", "c"));
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

    static class User {
        public static final User FRY = new User("Philip J. Fry", 33);

        public String name;
        public int age;

        public User() {}

        public User(String name, int age) {
            this.name = name;
            this.age = age;
        }
    }
}