HTTP Client
===========

A wrapper around the amazing [AsyncHttpClient](https://github.com/AsyncHttpClient/async-http-client) that uses the
"mutant factory" pattern. This means that instances are immutable, and can be shared with abandon, and all modification
methods return new, still immutable instances. This allows you to cascade configuration down your app as it becomes
more specific.

Example
-------

```java
public class Main {
    public static void main(String[] args) {
        // Create a single client for the entire application. This way,
        // everything can share the same connection and thread pools, plus
        // user-agent.
        HttpClient<Object, JsonNode> client = HttpClient.createDefault()
                .setHeader(HttpHeaders.USER_AGENT, "MyApp/1.1");

        // Google can handle whatever we throw at them, so we we'll set a high
        // concurrency for our Google client.
        HttpClient<Object, JsonNode> googleClient = client
                .url("https://www.google.com")
                .maxConcurrency(100);

        // However, _our_ service was built on a TI-83, so we really need to
        // take it easy.
        HttpClient<Object, JsonNode> myClient = client
                .url("https://fragile.example.com")
                .maxConcurrency(2);

        // Here we'll set the return type to something more specific.
        HttpClient<Object, User> myUserClient = myClient.forModelType(User.class);

        // If you're using header versioning, it can be helpful to have a new
        // client for each HTTP method, with the proper headers and paths set
        // up before hand.
        HttpClient<Object, User> myGetUserClient = myUserClient
                .method("GET")
                .setPath("/users/{id}")
                .setHeader(HttpHeaders.ACCEPT, "application/vnd.myapp.users.v1+json");

        // For posts we'll just peek at the status instead of waiting around
        // for a body.
        HttpClient<Object, Integer> myPostUserClient = myUserClient
                .method("POST")
                .setPath("/users")
                .setHeader(HttpHeaders.CONTENT_TYPE, "application/vnd.myapp.users.v1+json")
                .statusOnly();

        // Fetch a user by setting the path param and executing the request.
        User user = myGetUserClient.pathParam("id", 42).execute();

        user.age += 1;

        // And finally, save our change.
        int statusCode = myPostUserClient.execute(user);
    }
}
```

More Examples
-------------

```java
public class Main {
    public static void main(String[] args) {
        HttpClient<Object, JsonNode> redditClient = HttpClient.createDefault()
                .setHeader(HttpHeaders.USER_AGENT, "HttpClient/1.0 (https://github.com/pkulak/http-client)")
                .url("https://www.reddit.com/r/ripcity.json");

        // There are, of course, shortcuts for the most popular HTTP methods
        JsonNode threads = redditClient.get();

        // And a HEAD request only gets you the headers and status
        HeaderResponse headers = redditClient.head();

        // Everything can also be done asynchronously
        redditClient.statusOnly().deleteAsync()
                .thenAccept(status -> {
                    System.out.println("the status was " + status);
                });

        // If you go over your max concurrency, requests will back up in a
        // queue. To avoid this and add your own back-pressure instead, wait
        // before every new request; the current thread will only continue when
        // there's a slot available.
        HttpClient<Object, Integer> throttledReddit = redditClient.maxConcurrency(2).statusOnly();

        for (int i = 0; i < 10; i++) {
            throttledReddit.await();
            throttledReddit.headAsync().thenAccept(System.out::println);
        }

        // When you're done, drain the queue before you exit.
        throttledReddit.awaitAll();
    }
}
```
