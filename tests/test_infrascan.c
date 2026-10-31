#include "test_framework.h"
#include "graph_buffer/graph_buffer.h"
#include "pipeline/pipeline_internal.h"
#include "service_patterns.h"
#include "yyjson/yyjson.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

static int has_data_flow(cbm_gbuf_t *gb, int64_t source_id, int64_t target_id) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_source_type(gb, source_id, "DATA_FLOWS", &edges, &count);
    for (int i = 0; i < count; i++) {
        if (edges[i]->target_id == target_id) {
            return 1;
        }
    }
    return 0;
}

TEST(infrascan_http_route_literal_guard_rejects_filesystem_paths) {
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal("/etc/crio/crio.conf", "requests.get"));
    ASSERT_FALSE(
        cbm_service_pattern_is_http_route_literal("/root/.aws/credentials", "requests.get"));
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal("/var/run/app.json", "requests.get"));
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal("/locations/", "str.split"));
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal("/api", "os.path.join"));
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal("/html/g", "template.replace"));
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal(NULL, "requests.get"));
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal("", "requests.get"));
    ASSERT_TRUE(cbm_service_pattern_is_http_route_literal("/api/orders", "requests.get"));
    ASSERT_TRUE(cbm_service_pattern_is_http_route_literal("https://orders.example/api/orders",
                                                          "requests.get"));
    /* A comment that leads an argument list is not a route (elasticsearch
     * RestHandler routes: three Java block comments became Route nodes). */
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal(
        "/*\n                 * Deprecated in #64227, 7.12/8.0.\n                 */",
        "Route.builder"));
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal("// legacy path", "Route.builder"));
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal("/_cat\n/indices", "Route.builder"));
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal("/* all */", "Route.builder"));
    ASSERT_TRUE(cbm_service_pattern_is_http_route_literal("/_cat/indices", "Route.builder"));
    /* The wildcard path (slash-star alone) is a route, not a comment:
     * elasticsearch's RestClient.buildUri(null, wildcard) registers one. */
    ASSERT_TRUE(cbm_service_pattern_is_http_route_literal("/*", "RestClient.buildUri"));
    ASSERT_TRUE(cbm_service_pattern_is_http_route_literal("/api/*", "app.get"));
    PASS();
}

/* #1250: client-side navigation is recognised only in the JS/TS family; genuine
 * HTTP clients and route registrations are never mistaken for it, and with the
 * file-language gate off nothing is suppressed (no other language's recall). */
TEST(infrascan_client_navigation_is_js_family_only) {
    const char *nav[] = {"this.router.navigateByUrl",
                         "this.router.navigate",
                         "navigateTo",
                         "navigate",
                         "this.navCtrl.navigateForward",
                         "this.navCtrl.navigateRoot",
                         "this.navCtrl.navigateBack",
                         "router.push",
                         "this.$router.push",
                         "Router.replace",
                         "props.history.push",
                         "browserHistory.replace",
                         "router.prefetch",
                         "history.pushState",
                         "window.history.replaceState",
                         "this.location.go",
                         "window.location.assign",
                         "location.replace",
                         "goto",
                         "redirect",
                         "permanentRedirect"};
    for (size_t i = 0; i < sizeof(nav) / sizeof(nav[0]); i++) {
        ASSERT_TRUE(cbm_service_pattern_is_client_navigation(true, nav[i]));
        ASSERT_FALSE(cbm_service_pattern_is_client_navigation(false, nav[i]));
    }
    const char *http[] = {"fetch",
                          "axios.get",
                          "axios.post",
                          "this.http.get",
                          "this.http.post",
                          "httpClient.put",
                          "http",
                          "api.patch",
                          "router.get",
                          "app.post",
                          "items.push",
                          "ky.get",
                          "res.redirect",
                          "client.go",
                          "this.gotoApi",
                          "$http.get",
                          "navigateApi",
                          "client.navigateRequest",
                          "navigateToEndpoint"};
    for (size_t i = 0; i < sizeof(http) / sizeof(http[0]); i++) {
        ASSERT_FALSE(cbm_service_pattern_is_client_navigation(true, http[i]));
    }
    ASSERT_FALSE(cbm_service_pattern_is_client_navigation(true, NULL));
    ASSERT_FALSE(cbm_service_pattern_is_client_navigation(true, ""));
    PASS();
}

/* ── Service-pattern QN boundaries (distilled from PR #1245) ─────────
 * A library id must sit on an identifier boundary inside the QN. A raw
 * substring match classified "proj.plugin.loader" as gin ("gin.") and
 * "proj.studio.x" as Dio ("dio"), which minted bogus Route / HTTP_CALLS
 * edges. The rule must stay open for CamelCase glue ("GuzzleHttp",
 * "FeignClient", "KafkaProducer", "IHttpClientFactory") and for version
 * suffixes ("urllib2", "amqp091-go"). */
typedef struct {
    const char *qn;
    cbm_svc_kind_t want;
} svc_case_t;

static int svc_case_mismatches(const svc_case_t *cases) {
    int bad = 0;
    for (int i = 0; cases[i].qn != NULL; i++) {
        cbm_svc_kind_t got = cbm_service_pattern_match(cases[i].qn);
        if (got != cases[i].want) {
            printf("  service-pattern mismatch: \"%s\" -> %d, expected %d\n", cases[i].qn, (int)got,
                   (int)cases[i].want);
            bad++;
        }
    }
    return bad;
}

TEST(infrascan_service_pattern_match_rejects_ids_inside_words) {
    static const svc_case_t cases[] = {
        /* "gin." inside plugin. */
        {"proj.plugins.autorun.tests.test_plugin._dispatch", CBM_SVC_NONE},
        {"proj.plugin.loader", CBM_SVC_NONE},
        /* "chi." / "echo." / "fiber." */
        {"proj.hibachi.grill", CBM_SVC_NONE},
        {"sound.reecho.apply", CBM_SVC_NONE},
        {"textile.microfiber.weave", CBM_SVC_NONE},
        /* "dio" inside studio / radio */
        {"proj.studio.x", CBM_SVC_NONE},
        {"radio.tune", CBM_SVC_NONE},
        /* "surf" / "resty" / "curl" / "phin" / "hyper" / "sling" / "needle" / "treq" */
        {"app.surface.draw", CBM_SVC_NONE},
        {"ui.restyle.apply", CBM_SVC_NONE},
        /* control: "forestry" spells "restr", never contained "resty" */
        {"pkg.forestry.trees", CBM_SVC_NONE},
        {"util.curly.brace", CBM_SVC_NONE},
        {"sea.dolphin.swim", CBM_SVC_NONE},
        {"math.hyperbolic.tanh", CBM_SVC_NONE},
        {"farm.gosling.walk", CBM_SVC_NONE},
        {"sewing.needles.sort", CBM_SVC_NONE},
        {"util.streq", CBM_SVC_NONE},
        /* "requests" glued to a lowercase prefix (upstream's case) */
        {"proj.myrequests.client.get", CBM_SVC_NONE},
        /* "express" / "rocket" / "tonic" / "nconf" */
        {"ast.expression.eval", CBM_SVC_NONE},
        {"mq.rocketmq.send", CBM_SVC_NONE},
        {"geo.tectonic.plates", CBM_SVC_NONE},
        {"app.unconfigured.warn", CBM_SVC_NONE},
        {"cli.runconfig.load", CBM_SVC_NONE},
        {"aws.s3express.session", CBM_SVC_NONE},
        /* not clients of the id they contain: ASGI server, URL library,
         * nginx platform, Memgraph OGM, Celery settings module, a word */
        {"hypercorn.run.serve", CBM_SVC_NONE},
        {"hyperlink.URL.from_text", CBM_SVC_NONE},
        {"openresty.core.request", CBM_SVC_NONE},
        {"gqlalchemy.Memgraph.execute", CBM_SVC_NONE},
        {"celeryconfig.broker_url", CBM_SVC_NONE},
        {"kafkaesque.story.tell", CBM_SVC_NONE},
        /* "Axios" glued to a lowercase continuation */
        {"geo.Axiosphere.map", CBM_SVC_NONE},
        {NULL, CBM_SVC_NONE},
    };
    ASSERT_EQ(svc_case_mismatches(cases), 0);
    PASS();
}

TEST(infrascan_service_pattern_match_keeps_real_library_qns) {
    static const svc_case_t cases[] = {
        /* Route registration: ids that end in a separator, own segment */
        {"github.com/gin-gonic/gin.Default", CBM_SVC_ROUTE_REG},
        {"proj.gin.router.GET", CBM_SVC_ROUTE_REG},
        {"gin.GET", CBM_SVC_ROUTE_REG},
        {"chi.NewRouter", CBM_SVC_ROUTE_REG},
        {"github.com/labstack/echo.New", CBM_SVC_ROUTE_REG},
        {"fiber.New", CBM_SVC_ROUTE_REG},
        {"proj.express.router.get", CBM_SVC_ROUTE_REG},
        {"@hapi/hapi.server.route", CBM_SVC_ROUTE_REG},
        /* HTTP: own segment / separator-joined wrapper */
        {"proj.venv.requests.api.get", CBM_SVC_HTTP},
        {"proj.service.requests_get", CBM_SVC_HTTP},
        {"package:dio/dio.dart", CBM_SVC_HTTP},
        {"dio.Dio.get", CBM_SVC_HTTP},
        {"pkg.net.curl.get", CBM_SVC_HTTP},
        /* axios under a capitalized default-import binding
         * (`import Axios from "axios"`) and axios's own instance type */
        {"Axios.get", CBM_SVC_HTTP},
        {"Axios.post", CBM_SVC_HTTP},
        {"AxiosInstance.get", CBM_SVC_HTTP},
        {"curl_exec", CBM_SVC_HTTP},
        {"surf.get", CBM_SVC_HTTP},
        {"hyper.Client.request", CBM_SVC_HTTP},
        {"phin.promisified", CBM_SVC_HTTP},
        {"needle.get", CBM_SVC_HTTP},
        {"github.com/go-resty/resty.New", CBM_SVC_HTTP},
        {"github.com/dghubble/sling.New", CBM_SVC_HTTP},
        /* HTTP: CamelCase glue after the id */
        {"GuzzleHttp\\Client", CBM_SVC_HTTP},
        {"proj.GuzzleHttp.Client.get", CBM_SVC_HTTP},
        {"org.springframework.cloud.openfeign.FeignClientBuilder.build", CBM_SVC_HTTP},
        {"com.acme.net.HttpClientFactory.create", CBM_SVC_HTTP},
        /* HTTP: CamelCase glue before the id */
        {"System.Net.Http.IHttpClientFactory.CreateClient", CBM_SVC_HTTP},
        {"org.asynchttpclient.AsyncHttpClient.prepareGet", CBM_SVC_HTTP},
        {"Foundation.NSURLSession.dataTask", CBM_SVC_HTTP},
        /* HTTP: version suffix after the id */
        {"urllib2.urlopen", CBM_SVC_HTTP},
        {"Mint.HTTP2.request", CBM_SVC_HTTP},
        /* Async */
        {"com.acme.events.KafkaProducer.send", CBM_SVC_ASYNC},
        {"this.kafkaProducer.send", CBM_SVC_ASYNC},
        {"org.apache.kafka.clients.producer.KafkaProducer.send", CBM_SVC_ASYNC},
        {"github.com/rabbitmq/amqp091-go.Channel.Publish", CBM_SVC_ASYNC},
        {"github.com/nats-io/nats.go.Conn.Publish", CBM_SVC_ASYNC},
        /* Config */
        {"os.getenv", CBM_SVC_CONFIG},
        {"os.Getenv", CBM_SVC_CONFIG},
        /* Glued-name libraries: the library's own name glues a lowercase
         * prefix/suffix onto another id, so each has an explicit table entry.
         * Every QN below contains NO other id — only the entry can match. */
        {"grequests.map", CBM_SVC_HTTP},
        {"txrequests.Session.send", CBM_SVC_HTTP},
        {"redaxios.create", CBM_SVC_HTTP},
        {"gaxios.request", CBM_SVC_HTTP},
        {"libcurl.version", CBM_SVC_HTTP},
        {"node-libcurl.Easy.perform", CBM_SVC_HTTP},
        {"curlpp.Easy.perform", CBM_SVC_HTTP},
        {"curlcpp.easy.perform", CBM_SVC_HTTP},
        {"hyperlocal.UnixConnector.call", CBM_SVC_HTTP},
        {"guzzlehttp/psr7.Utils.streamFor", CBM_SVC_HTTP},
        {"aiokafka.helpers.create_ssl_context", CBM_SVC_ASYNC},
        {"pykafka.topic.Topic.get_producer", CBM_SVC_ASYNC},
        {"librdkafka.rd_version", CBM_SVC_ASYNC},
        {"rdkafkacpp.version", CBM_SVC_ASYNC},
        {"rskafka.client.partition.write", CBM_SVC_ASYNC},
        {"aioamqp.connect", CBM_SVC_ASYNC},
        {"amqpstorm.Connection.channel", CBM_SVC_ASYNC},
        {"pamqp.frame.marshal", CBM_SVC_ASYNC},
        {"kombu.transport.pyamqp.Transport.establish_connection", CBM_SVC_ASYNC},
        {"amqprs.channel.basic_publish", CBM_SVC_ASYNC},
        {"amqpcpp.TcpChannel.publish", CBM_SVC_ASYNC},
        {"pynats.client.send", CBM_SVC_ASYNC},
        {"jnats.options.build", CBM_SVC_ASYNC},
        {"aiomqtt.client.publish", CBM_SVC_ASYNC},
        {"amqtt.session.deliver", CBM_SVC_ASYNC},
        {"hbmqtt.session.deliver", CBM_SVC_ASYNC},
        {"umqtt.simple.publish", CBM_SVC_ASYNC},
        {"mqttools.client.publish", CBM_SVC_ASYNC},
        {"emqtt.publish", CBM_SVC_ASYNC},
        {"rumqtt.client.publish", CBM_SVC_ASYNC},
        {"github.com/256dpi/gomqtt/client.Publish", CBM_SVC_ASYNC},
        {"libmosquitto.publish", CBM_SVC_ASYNC},
        {"mosquittopp.publish", CBM_SVC_ASYNC},
        {"google.cloud.pubsublite.admin.create_topic", CBM_SVC_ASYNC},
        {"gocelery.client.delay", CBM_SVC_ASYNC},
        {"_wgetenv", CBM_SVC_CONFIG},
        {"_wgetenv_s", CBM_SVC_CONFIG},
        {"os.getenvb", CBM_SVC_CONFIG},
        {"qgetenv", CBM_SVC_CONFIG},
        {"@dotenvx/dotenvx.config", CBM_SVC_CONFIG},
        {"vlucas/phpdotenv.load", CBM_SVC_CONFIG},
        {"apiflask.scaffold.get", CBM_SVC_ROUTE_REG},
        {"honox.factory.createRoute", CBM_SVC_ROUTE_REG},
        {"fasthttprouter.Router.GET", CBM_SVC_ROUTE_REG},
        {"github.com/vektah/gqlparser/v2.LoadSchema", CBM_SVC_GRAPHQL},
        {"github.com/Yamashou/gqlgenc.client.Post", CBM_SVC_GRAPHQL},
        {"aiogqlc.client.execute", CBM_SVC_GRAPHQL},
        {NULL, CBM_SVC_NONE},
    };
    ASSERT_EQ(svc_case_mismatches(cases), 0);
    ASSERT_STR_EQ(cbm_service_pattern_broker("aiokafka.helpers.create_ssl_context"), "kafka");
    ASSERT_STR_EQ(cbm_service_pattern_broker("umqtt.simple.publish"), "mqtt");
    ASSERT_STR_EQ(cbm_service_pattern_broker("com.acme.events.KafkaProducer.send"), "kafka");
    ASSERT_STR_EQ(cbm_service_pattern_broker("github.com/rabbitmq/amqp091-go.Channel.Publish"),
                  "rabbitmq");
    PASS();
}

/* Document/log extensions are never HTTP routes (distilled from PR #1245):
 * the arg-URL detector runs for every resolved call, so without the
 * extension guard `open("/new/file.txt")` minted a Route + HTTP_CALLS.
 * First segments deliberately avoid the filesystem-root list so ONLY the
 * extension guard can reject them; `/api/...` marker paths stay routes. */
TEST(infrascan_http_route_literal_guard_rejects_document_extensions) {
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal("/new/file.txt", "open"));
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal("/docs/guide.md", "read"));
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal("/data/app.log", "write"));
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal("/fake/path.pdf", "open"));
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal("/docs/index.rst", "open"));
    /* Same extensions under a query string are still files. */
    ASSERT_FALSE(cbm_service_pattern_is_http_route_literal("/docs/guide.md?raw=1", "open"));
    /* Positive controls: extension-less routes and the same callees. */
    ASSERT_TRUE(cbm_service_pattern_is_http_route_literal("/api/items", "requests.get"));
    ASSERT_TRUE(cbm_service_pattern_is_http_route_literal("/docs/guide", "open"));
    PASS();
}

TEST(infrascan_route_nodes_skip_bad_http_url_paths) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp/cbm_infrascan_route_guard");
    ASSERT_NOT_NULL(gb);
    int64_t caller =
        cbm_gbuf_upsert_node(gb, "Function", "client", "test.client", "client.py", 1, 3, "{}");
    int64_t fs_callee =
        cbm_gbuf_upsert_node(gb, "Function", "requests.get", "requests.get", "", 0, 0, "{}");
    int64_t split_callee =
        cbm_gbuf_upsert_node(gb, "Function", "str.split", "str.split", "", 0, 0, "{}");
    int64_t empty_callee =
        cbm_gbuf_upsert_node(gb, "Function", "requests.post", "requests.post", "", 0, 0, "{}");
    ASSERT_GT(caller, 0);
    ASSERT_GT(fs_callee, 0);
    ASSERT_GT(split_callee, 0);
    ASSERT_GT(empty_callee, 0);

    cbm_gbuf_insert_edge(gb, caller, fs_callee, "HTTP_CALLS",
                         "{\"callee\":\"requests.get\",\"url_path\":\"/etc/crio/crio.conf\","
                         "\"method\":\"GET\"}");
    cbm_gbuf_insert_edge(gb, caller, split_callee, "HTTP_CALLS",
                         "{\"callee\":\"str.split\",\"url_path\":\"/locations/\","
                         "\"method\":\"ANY\"}");
    cbm_gbuf_insert_edge(gb, caller, empty_callee, "HTTP_CALLS",
                         "{\"callee\":\"requests.get\",\"method\":\"GET\"}");

    cbm_pipeline_create_route_nodes(gb);

    ASSERT_NULL(cbm_gbuf_find_by_qn(gb, "__route__GET__/etc/crio/crio.conf"));
    ASSERT_NULL(cbm_gbuf_find_by_qn(gb, "__route__ANY__/locations/"));
    ASSERT_NULL(cbm_gbuf_find_by_qn(gb, "__route__GET__"));

    cbm_gbuf_free(gb);
    PASS();
}

TEST(infrascan_http_calls_join_matching_handler_route) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp/cbm_infrascan_route_join");
    ASSERT_NOT_NULL(gb);

    int64_t route = cbm_gbuf_upsert_node(gb, "Route", "/api/orders", "__route__GET__/api/orders",
                                         "", 0, 0, "{\"method\":\"GET\"}");
    int64_t handler = cbm_gbuf_upsert_node(gb, "Function", "get_orders", "test.get_orders",
                                           "server.py", 1, 3, "{}");
    int64_t client =
        cbm_gbuf_upsert_node(gb, "Function", "client", "test.client", "client.py", 1, 3, "{}");
    int64_t bad_route =
        cbm_gbuf_upsert_node(gb, "Route", "/etc/crio/crio.conf",
                             "__route__GET__/etc/crio/crio.conf", "", 0, 0, "{\"method\":\"GET\"}");
    int64_t bad_handler = cbm_gbuf_upsert_node(gb, "Function", "bad_handler", "test.bad_handler",
                                               "server.py", 5, 7, "{}");
    int64_t bad_client = cbm_gbuf_upsert_node(gb, "Function", "bad_client", "test.bad_client",
                                              "client.py", 5, 7, "{}");
    ASSERT_GT(route, 0);
    ASSERT_GT(handler, 0);
    ASSERT_GT(client, 0);
    ASSERT_GT(bad_route, 0);
    ASSERT_GT(bad_handler, 0);
    ASSERT_GT(bad_client, 0);

    cbm_gbuf_insert_edge(gb, handler, route, "HANDLES", "{\"handler\":\"test.get_orders\"}");
    cbm_gbuf_insert_edge(gb, client, route, "HTTP_CALLS",
                         "{\"callee\":\"requests.get\",\"url_path\":\"/api/orders\","
                         "\"method\":\"GET\"}");
    cbm_gbuf_insert_edge(gb, bad_handler, bad_route, "HANDLES",
                         "{\"handler\":\"test.bad_handler\"}");
    cbm_gbuf_insert_edge(gb, bad_client, bad_route, "HTTP_CALLS",
                         "{\"callee\":\"requests.get\",\"url_path\":\"/etc/crio/crio.conf\","
                         "\"method\":\"GET\"}");

    cbm_pipeline_create_route_nodes(gb);

    ASSERT_TRUE(has_data_flow(gb, client, handler));
    ASSERT_FALSE(has_data_flow(gb, bad_client, bad_handler));

    cbm_gbuf_free(gb);
    PASS();
}

/* An unchanged handler's Route is rebuilt from its stored route metadata. The
 * metadata is JSON, so a route path with a quote or a backslash (a legal POSIX
 * directory name) is stored escaped; reading it up to the first quote named a
 * second Route "/api/a\" beside the extracted one, and the handler QN went into
 * the HANDLES properties unescaped. Running the pass twice proves the rebuild
 * finds the Route it made instead of adding another edge. */
TEST(infrascan_decorator_route_metadata_keeps_its_escapes) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp/cbm_infrascan_route_escapes");
    ASSERT_NOT_NULL(gb);
    int64_t handler = cbm_gbuf_upsert_node(
        gb, "Function", "GET", "test.app.api.a\"b.route.GET", "app/api/a\"b/route.ts", 1, 1,
        "{\"route_path\":\"/api/a\\\"b\\\\c\",\"route_method\":\"GET\"}");
    ASSERT_GT(handler, 0);

    cbm_pipeline_create_route_nodes(gb);
    cbm_pipeline_create_route_nodes(gb);

    const cbm_gbuf_node_t **routes = NULL;
    int route_count = 0;
    ASSERT_EQ(cbm_gbuf_find_by_label(gb, "Route", &routes, &route_count), 0);
    ASSERT_EQ(route_count, 1);
    const cbm_gbuf_node_t *route = cbm_gbuf_find_by_qn(gb, "__route__GET__/api/a\"b\\c");
    ASSERT_NOT_NULL(route);
    ASSERT_STR_EQ(route->name, "/api/a\"b\\c");
    ASSERT_STR_EQ(route->file_path, "app/api/a\"b/route.ts");
    const cbm_gbuf_edge_t **edges = NULL;
    int edge_count = 0;
    ASSERT_EQ(cbm_gbuf_find_edges_by_target_type(gb, route->id, "HANDLES", &edges, &edge_count), 0);
    ASSERT_EQ(edge_count, 1);
    ASSERT_EQ(edges[0]->source_id, handler);
    yyjson_doc *doc = yyjson_read(edges[0]->properties_json, strlen(edges[0]->properties_json), 0);
    ASSERT_NOT_NULL(doc);
    const char *qn = yyjson_get_str(yyjson_obj_get(yyjson_doc_get_root(doc), "handler"));
    bool same_handler = qn && strcmp(qn, "test.app.api.a\"b.route.GET") == 0;
    yyjson_doc_free(doc);
    ASSERT_TRUE(same_handler);

    cbm_gbuf_free(gb);
    PASS();
}

/* Does the one HANDLES edge into Route route_qn hold properties that parse to
 * {"handler": qn}? */
static bool handles_names_handler(cbm_gbuf_t *gb, const char *route_qn, const char *qn) {
    const cbm_gbuf_node_t *route = cbm_gbuf_find_by_qn(gb, route_qn);
    const cbm_gbuf_edge_t **edges = NULL;
    int edge_count = 0;
    if (!route ||
        cbm_gbuf_find_edges_by_target_type(gb, route->id, "HANDLES", &edges, &edge_count) != 0 ||
        edge_count != 1) {
        return false;
    }
    yyjson_doc *doc = yyjson_read(edges[0]->properties_json, strlen(edges[0]->properties_json), 0);
    const char *handler = yyjson_get_str(yyjson_obj_get(yyjson_doc_get_root(doc), "handler"));
    bool same = handler && strcmp(handler, qn) == 0;
    yyjson_doc_free(doc);
    return same;
}

/* The HANDLES properties hold the handler's QN escaped. A QN that fills the
 * escape buffer left the closing `"}` off them, and one malformed properties
 * row fails every query that reads edge properties. The edge extraction emits
 * and the one an unchanged handler's metadata rebuilds must both parse. */
TEST(infrascan_handles_properties_hold_a_long_handler_qn) {
    char qn[CBM_SZ_512];
    memset(qn, 'q', sizeof(qn) - SKIP_ONE);
    qn[sizeof(qn) - SKIP_ONE] = '\0';

    cbm_gbuf_t *emitted = cbm_gbuf_new("test", "/tmp/cbm_infrascan_long_qn_emit");
    ASSERT_NOT_NULL(emitted);
    int64_t handler = cbm_gbuf_upsert_node(emitted, "Function", "GET", qn, "server.ts", 1, 1, "{}");
    ASSERT_GT(handler, 0);
    CBMDefinition def = {0};
    def.qualified_name = qn;
    def.file_path = "server.ts";
    def.route_path = "/long";
    def.route_method = "GET";
    cbm_pipeline_emit_def_route(emitted, handler, &def, "server.ts");
    bool emitted_parses = handles_names_handler(emitted, "__route__GET__/long", qn);
    cbm_gbuf_free(emitted);

    cbm_gbuf_t *rebuilt = cbm_gbuf_new("test", "/tmp/cbm_infrascan_long_qn_rebuild");
    ASSERT_NOT_NULL(rebuilt);
    ASSERT_GT(cbm_gbuf_upsert_node(rebuilt, "Function", "GET", qn, "server.ts", 1, 1,
                                   "{\"route_path\":\"/long\",\"route_method\":\"GET\"}"),
              0);
    cbm_pipeline_create_route_nodes(rebuilt);
    bool rebuilt_parses = handles_names_handler(rebuilt, "__route__GET__/long", qn);
    cbm_gbuf_free(rebuilt);

    ASSERT_TRUE(emitted_parses);
    ASSERT_TRUE(rebuilt_parses);
    PASS();
}

/* Does Route route_qn hold properties that parse to {"method": method}? */
static bool route_names_method(cbm_gbuf_t *gb, const char *route_qn, const char *method) {
    const cbm_gbuf_node_t *route = cbm_gbuf_find_by_qn(gb, route_qn);
    if (!route || !route->properties_json) {
        return false;
    }
    yyjson_doc *doc = yyjson_read(route->properties_json, strlen(route->properties_json), 0);
    const char *got = yyjson_get_str(yyjson_obj_get(yyjson_doc_get_root(doc), "method"));
    bool same = got && strcmp(got, method) == 0;
    yyjson_doc_free(doc);
    return same;
}

/* A route method is source text where a Python DRF action names it
 * (`@action(methods=['a"b'])`), so the Route's properties hold it escaped:
 * both the Route extraction emits and the one rebuilt from the handler's
 * metadata must parse. */
TEST(infrascan_route_properties_escape_the_method) {
    cbm_gbuf_t *emitted = cbm_gbuf_new("test", "/tmp/cbm_infrascan_method_emit");
    ASSERT_NOT_NULL(emitted);
    int64_t handler =
        cbm_gbuf_upsert_node(emitted, "Function", "act", "test.views.act", "views.py", 1, 1, "{}");
    ASSERT_GT(handler, 0);
    CBMDefinition def = {0};
    def.qualified_name = "test.views.act";
    def.file_path = "views.py";
    def.route_path = "/m";
    def.route_method = "A\"B";
    cbm_pipeline_emit_def_route(emitted, handler, &def, "views.py");
    bool emitted_parses = route_names_method(emitted, "__route__A\"B__/m", "A\"B");
    cbm_gbuf_free(emitted);

    cbm_gbuf_t *rebuilt = cbm_gbuf_new("test", "/tmp/cbm_infrascan_method_rebuild");
    ASSERT_NOT_NULL(rebuilt);
    ASSERT_GT(cbm_gbuf_upsert_node(rebuilt, "Function", "act", "test.views.act", "views.py", 1, 1,
                                   "{\"route_path\":\"/m\",\"route_method\":\"A\\\"B\"}"),
              0);
    cbm_pipeline_create_route_nodes(rebuilt);
    bool rebuilt_parses = route_names_method(rebuilt, "__route__A\"B__/m", "A\"B");
    cbm_gbuf_free(rebuilt);

    ASSERT_TRUE(emitted_parses);
    ASSERT_TRUE(rebuilt_parses);
    PASS();
}

/* A SvelteKit +server handler's HANDLES properties hold its QN escaped, as
 * every other HANDLES edge's do: a quote in a route directory, or a QN longer
 * than the old 256-byte properties buffer, made them malformed. */
TEST(infrascan_sveltekit_handles_properties_escape_the_handler_qn) {
    char dir[CBM_SZ_512];
    snprintf(dir, sizeof(dir), "a\"b/%0300d", 0);
    char file[CBM_SZ_1K];
    snprintf(file, sizeof(file), "src/routes/%s/+server.ts", dir);
    char qn[CBM_SZ_1K];
    snprintf(qn, sizeof(qn), "test.src.routes.%s.+server.GET", dir);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp/cbm_infrascan_sveltekit_handles");
    ASSERT_NOT_NULL(gb);
    int64_t file_id =
        cbm_gbuf_upsert_node(gb, "File", "+server.ts", "test.server", file, 1, 1, "{}");
    int64_t handler = cbm_gbuf_upsert_node(gb, "Function", "GET", qn, file, 1, 1, "{}");
    ASSERT_GT(file_id, 0);
    ASSERT_GT(handler, 0);
    cbm_gbuf_insert_edge(gb, file_id, handler, "DEFINES", "{}");
    cbm_pipeline_create_route_nodes(gb);

    const cbm_gbuf_edge_t **edges = NULL;
    int edge_count = 0;
    bool same = false;
    if (cbm_gbuf_find_edges_by_source_type(gb, handler, "HANDLES", &edges, &edge_count) == 0 &&
        edge_count == 1) {
        yyjson_doc *doc =
            yyjson_read(edges[0]->properties_json, strlen(edges[0]->properties_json), 0);
        yyjson_val *root = yyjson_doc_get_root(doc);
        const char *got = yyjson_get_str(yyjson_obj_get(root, "handler"));
        const char *framework = yyjson_get_str(yyjson_obj_get(root, "framework"));
        same = got && strcmp(got, qn) == 0 && framework && strcmp(framework, "sveltekit") == 0;
        yyjson_doc_free(doc);
    }
    cbm_gbuf_free(gb);
    ASSERT_TRUE(same);
    PASS();
}

/* Route metadata that does not parse, is not a top-level string, holds a NUL or
 * does not fit the path buffer names no Route at all, rather than whatever text
 * happens to sit between two quotes. The 255-byte path is the longest that fits
 * and must still route. */
TEST(infrascan_decorator_route_metadata_rejects_malformed_values) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp/cbm_infrascan_route_malformed");
    ASSERT_NOT_NULL(gb);
    char longest[CBM_SZ_256 + CBM_SZ_64];
    char too_long[CBM_SZ_256 + CBM_SZ_64];
    char path[CBM_SZ_256 + 1];
    path[0] = '/';
    memset(path + 1, 'x', CBM_SZ_256 - 1);
    path[CBM_SZ_256] = '\0';
    snprintf(too_long, sizeof(too_long), "{\"route_path\":\"%s\"}", path);
    path[CBM_SZ_256 - 1] = '\0';
    snprintf(longest, sizeof(longest), "{\"route_path\":\"%s\"}", path);
    const char *props[] = {
        "{\"route_path\":\"/truncated\"",
        "{\"meta\":{\"route_path\":\"/nested\"}}",
        "{\"route_path\":[\"/array\"]}",
        "{\"route_path\":42}",
        "{\"route_path\":\"/a\\u0000b\"}",
        "{\"route_path\":\"\"}",
        "{\"route_path\":\"relative\"}",
        "[\"route_path\",\"/not-an-object\"]",
        too_long,
    };
    const int prop_count = (int)(sizeof(props) / sizeof(props[0]));
    for (int i = 0; i < prop_count; i++) {
        char qn[CBM_SZ_32];
        snprintf(qn, sizeof(qn), "test.bad%d", i);
        ASSERT_GT(cbm_gbuf_upsert_node(gb, "Function", qn, qn, "server.ts", 1, 1, props[i]), 0);
    }
    cbm_pipeline_create_route_nodes(gb);
    ASSERT_EQ(cbm_gbuf_edge_count_by_type(gb, "HANDLES"), 0);
    const cbm_gbuf_node_t **routes = NULL;
    int route_count = 0;
    ASSERT_EQ(cbm_gbuf_find_by_label(gb, "Route", &routes, &route_count), 0);
    ASSERT_EQ(route_count, 0);

    int64_t fits =
        cbm_gbuf_upsert_node(gb, "Function", "fits", "test.fits", "server.ts", 1, 1, longest);
    ASSERT_GT(fits, 0);
    cbm_pipeline_create_route_nodes(gb);
    ASSERT_EQ(cbm_gbuf_find_by_label(gb, "Route", &routes, &route_count), 0);
    ASSERT_EQ(route_count, 1);
    ASSERT_STR_EQ(routes[0]->name, path);
    ASSERT_EQ(cbm_gbuf_edge_count_by_type(gb, "HANDLES"), 1);

    cbm_gbuf_free(gb);
    PASS();
}

/* Properties hold source text as its bytes, which need not be UTF-8: a
 * Latin-1 docstring beside the route metadata, or a Latin-1 route path. The
 * Route extraction emits for them must come back from the stored metadata too,
 * as it did before the metadata was parsed. */
TEST(infrascan_decorator_route_metadata_survives_invalid_utf8) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp/cbm_infrascan_route_latin1");
    ASSERT_NOT_NULL(gb);
    ASSERT_GT(cbm_gbuf_upsert_node(gb, "Function", "doc", "test.doc", "views.py", 1, 1,
                                   "{\"docstring\":\"caf\xe9\",\"route_path\":\"/doc\","
                                   "\"route_method\":\"GET\"}"),
              0);
    ASSERT_GT(cbm_gbuf_upsert_node(gb, "Function", "GET", "test.app.caf\xe9.route.GET",
                                   "app/caf\xe9/route.ts", 1, 1,
                                   "{\"route_path\":\"/caf\xe9\",\"route_method\":\"GET\"}"),
              0);
    cbm_pipeline_create_route_nodes(gb);
    const cbm_gbuf_node_t *doc_route = cbm_gbuf_find_by_qn(gb, "__route__GET__/doc");
    const cbm_gbuf_node_t *dir_route = cbm_gbuf_find_by_qn(gb, "__route__GET__/caf\xe9");
    bool both = doc_route && dir_route && strcmp(dir_route->name, "/caf\xe9") == 0 &&
                cbm_gbuf_edge_count_by_type(gb, "HANDLES") == 2;
    cbm_gbuf_free(gb);
    ASSERT_TRUE(both);
    PASS();
}

/* FD16: a route's DATA_FLOWS were built from the first 64 callers and the first 32 infra handlers
 * in edge-insertion order (parallel worker interleaving), so the flow set changed between two
 * indexes of the same tree and every caller past the 64th lost its flow. */
enum { FD16_CALLERS = 70, FD16_INFRA_HANDLERS = 40 };

static int64_t fd16_route_with_callers(cbm_gbuf_t *gb, int64_t *callers, bool reverse) {
    int64_t route = cbm_gbuf_upsert_node(gb, "Route", "/api/orders", "__route__GET__/api/orders",
                                         "", 0, 0, "{\"method\":\"GET\"}");
    int64_t handler = cbm_gbuf_upsert_node(gb, "Function", "get_orders", "test.get_orders",
                                           "server.py", 1, 3, "{}");
    cbm_gbuf_insert_edge(gb, handler, route, "HANDLES", "{\"handler\":\"test.get_orders\"}");
    for (int i = 0; i < FD16_CALLERS; i++) {
        char name[32];
        char qn[48];
        snprintf(name, sizeof(name), "client_%02d", i);
        snprintf(qn, sizeof(qn), "test.client_%02d", i);
        callers[i] =
            cbm_gbuf_upsert_node(gb, "Function", name, qn, "client.py", i + 1, i + 1, "{}");
    }
    for (int k = 0; k < FD16_CALLERS; k++) {
        int i = reverse ? FD16_CALLERS - 1 - k : k;
        cbm_gbuf_insert_edge(gb, callers[i], route, "HTTP_CALLS",
                             "{\"callee\":\"requests.get\",\"url_path\":\"/api/orders\","
                             "\"method\":\"GET\"}");
    }
    return handler;
}

TEST(infrascan_data_flows_link_every_caller_of_a_route) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp/cbm_infrascan_fd16_callers");
    ASSERT_NOT_NULL(gb);
    int64_t callers[FD16_CALLERS];
    int64_t handler = fd16_route_with_callers(gb, callers, false);
    cbm_pipeline_create_route_nodes(gb);
    for (int i = 0; i < FD16_CALLERS; i++) {
        ASSERT_TRUE(has_data_flow(gb, callers[i], handler));
    }
    cbm_gbuf_free(gb);
    PASS();
}

TEST(infrascan_data_flows_do_not_depend_on_caller_insertion_order) {
    cbm_gbuf_t *fwd = cbm_gbuf_new("test", "/tmp/cbm_infrascan_fd16_fwd");
    cbm_gbuf_t *rev = cbm_gbuf_new("test", "/tmp/cbm_infrascan_fd16_rev");
    ASSERT_NOT_NULL(fwd);
    ASSERT_NOT_NULL(rev);
    int64_t fwd_callers[FD16_CALLERS];
    int64_t rev_callers[FD16_CALLERS];
    int64_t fwd_handler = fd16_route_with_callers(fwd, fwd_callers, false);
    int64_t rev_handler = fd16_route_with_callers(rev, rev_callers, true);
    cbm_pipeline_create_route_nodes(fwd);
    cbm_pipeline_create_route_nodes(rev);
    for (int i = 0; i < FD16_CALLERS; i++) {
        ASSERT_EQ(has_data_flow(fwd, fwd_callers[i], fwd_handler),
                  has_data_flow(rev, rev_callers[i], rev_handler));
    }
    cbm_gbuf_free(fwd);
    cbm_gbuf_free(rev);
    PASS();
}

TEST(infrascan_data_flows_reach_every_infra_handler) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp/cbm_infrascan_fd16_infra");
    ASSERT_NOT_NULL(gb);
    int64_t route = cbm_gbuf_upsert_node(gb, "Route", "/api/orders", "__route__GET__/api/orders",
                                         "", 0, 0, "{\"method\":\"GET\"}");
    int64_t endpoint = cbm_gbuf_upsert_node(gb, "Route", "/orders", "__route__GET__/orders", "", 0,
                                            0, "{\"method\":\"GET\"}");
    int64_t client =
        cbm_gbuf_upsert_node(gb, "Function", "client", "test.client", "client.py", 1, 3, "{}");
    cbm_gbuf_insert_edge(gb, route, endpoint, "INFRA_MAPS", "{}");
    cbm_gbuf_insert_edge(gb, client, route, "HTTP_CALLS",
                         "{\"callee\":\"requests.get\",\"url_path\":\"/api/orders\","
                         "\"method\":\"GET\"}");
    int64_t handlers[FD16_INFRA_HANDLERS];
    for (int i = 0; i < FD16_INFRA_HANDLERS; i++) {
        char name[32];
        char qn[48];
        snprintf(name, sizeof(name), "orders_%02d", i);
        snprintf(qn, sizeof(qn), "test.orders_%02d", i);
        handlers[i] =
            cbm_gbuf_upsert_node(gb, "Function", name, qn, "server.py", i + 1, i + 1, "{}");
        cbm_gbuf_insert_edge(gb, handlers[i], endpoint, "HANDLES", "{}");
    }
    cbm_pipeline_create_route_nodes(gb);
    for (int i = 0; i < FD16_INFRA_HANDLERS; i++) {
        ASSERT_TRUE(has_data_flow(gb, client, handlers[i]));
    }
    cbm_gbuf_free(gb);
    PASS();
}

/* R1 + R2 (user decision, 2026-10-18): a caller in a test file pairs only with handlers in that same
 * file, and a handler in a test file pairs only with callers in that same file. A test's request to
 * the application, a production caller matched to a test's mock server, and the private mock servers
 * of two tests pairing across files are not data flows of the application. */
static int64_t r12_fn(cbm_gbuf_t *gb, const char *name, const char *file) {
    char qn[96];
    snprintf(qn, sizeof(qn), "test.%s", name);
    return cbm_gbuf_upsert_node(gb, "Function", name, qn, file, 1, 3, "{}");
}

static int64_t r12_route(cbm_gbuf_t *gb, const char *path) {
    char qn[128];
    snprintf(qn, sizeof(qn), "__route__GET__%s", path);
    return cbm_gbuf_upsert_node(gb, "Route", path, qn, "", 0, 0, "{\"method\":\"GET\"}");
}

static void r12_call(cbm_gbuf_t *gb, int64_t caller, int64_t route, const char *path) {
    char props[160];
    snprintf(props, sizeof(props),
             "{\"callee\":\"requests.get\",\"url_path\":\"%s\",\"method\":\"GET\"}", path);
    cbm_gbuf_insert_edge(gb, caller, route, "HTTP_CALLS", props);
}

TEST(infrascan_data_flows_test_callers_pair_only_within_their_file) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp/cbm_infrascan_r12_callers");
    ASSERT_NOT_NULL(gb);
    int64_t route = r12_route(gb, "/api/orders");
    int64_t handler = r12_fn(gb, "list_orders", "app/views.py");
    cbm_gbuf_insert_edge(gb, handler, route, "HANDLES", "{}");
    /* Test files by each language's own rule: a tests/ directory, a Java *Test.java, a TS .spec.ts. */
    int64_t py_test = r12_fn(gb, "test_list_orders", "tests/test_orders.py");
    int64_t java_test = r12_fn(gb, "listsOrders", "app/OrderServiceTest.java");
    int64_t ts_spec = r12_fn(gb, "fetchesOrders", "web/orders.spec.ts");
    int64_t prod = r12_fn(gb, "load_orders", "app/client.py");
    r12_call(gb, py_test, route, "/api/orders");
    r12_call(gb, java_test, route, "/api/orders");
    r12_call(gb, ts_spec, route, "/api/orders");
    r12_call(gb, prod, route, "/api/orders");
    cbm_pipeline_create_route_nodes(gb);
    ASSERT_FALSE(has_data_flow(gb, py_test, handler));
    ASSERT_FALSE(has_data_flow(gb, java_test, handler));
    ASSERT_FALSE(has_data_flow(gb, ts_spec, handler));
    /* Control: the production caller keeps its flow. */
    ASSERT_TRUE(has_data_flow(gb, prod, handler));
    cbm_gbuf_free(gb);
    PASS();
}

TEST(infrascan_data_flows_test_handlers_pair_only_within_their_file) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp/cbm_infrascan_r12_handlers");
    ASSERT_NOT_NULL(gb);
    int64_t route = r12_route(gb, "/api/s3");
    /* Two tests each run their own mock server on the same path. */
    int64_t mock_a = r12_fn(gb, "mock_s3_a", "tests/test_upload.py");
    int64_t mock_b = r12_fn(gb, "mock_s3_b", "tests/conftest.py");
    cbm_gbuf_insert_edge(gb, mock_a, route, "HANDLES", "{}");
    cbm_gbuf_insert_edge(gb, mock_b, route, "HANDLES", "{}");
    int64_t prod = r12_fn(gb, "upload", "app/storage.py");
    int64_t test_a = r12_fn(gb, "test_upload", "tests/test_upload.py");
    r12_call(gb, prod, route, "/api/s3");
    r12_call(gb, test_a, route, "/api/s3");
    cbm_pipeline_create_route_nodes(gb);
    /* A production caller never reaches a test's mock server. */
    ASSERT_FALSE(has_data_flow(gb, prod, mock_a));
    ASSERT_FALSE(has_data_flow(gb, prod, mock_b));
    /* A test reaches its own file's mock server, not another test file's. */
    ASSERT_TRUE(has_data_flow(gb, test_a, mock_a));
    ASSERT_FALSE(has_data_flow(gb, test_a, mock_b));
    cbm_gbuf_free(gb);
    PASS();
}

/* Issue #2537: the Go 1.22 ServeMux grammar "[METHOD ][HOST]/[PATH]". The path
 * is found inside the written pattern; a plain path, a lowercase word, a
 * pattern without a path and a method too long for the buffer are no split.
 * Only a Handle / HandleFunc registration is read with it. */
TEST(infrascan_servemux_pattern_split_issue2537) {
    char m[16];
    const char *pat = "POST /v1/payments";
    ASSERT_TRUE(cbm_service_pattern_mux_split(pat, m, sizeof(m)) == pat + 5);
    ASSERT_STR_EQ(m, "POST");
    pat = "GET \t /v1/payments/{id}";
    ASSERT_TRUE(cbm_service_pattern_mux_split(pat, m, sizeof(m)) == pat + 6);
    ASSERT_STR_EQ(m, "GET");
    pat = "GET api.example.com/v1/x";
    ASSERT_TRUE(cbm_service_pattern_mux_split(pat, m, sizeof(m)) == pat + 19);
    ASSERT_STR_EQ(m, "GET");
    pat = "api.example.com:8443/v1/x";
    ASSERT_TRUE(cbm_service_pattern_mux_split(pat, m, sizeof(m)) == pat + 20);
    ASSERT_STR_EQ(m, "");
    pat = "DELETE /";
    ASSERT_TRUE(cbm_service_pattern_mux_split(pat, NULL, 0) == pat + 7);

    ASSERT_NULL(cbm_service_pattern_mux_split("/v1/x", m, sizeof(m)));
    ASSERT_NULL(cbm_service_pattern_mux_split("get /v1/x", m, sizeof(m)));
    ASSERT_NULL(cbm_service_pattern_mux_split("POST", m, sizeof(m)));
    ASSERT_NULL(cbm_service_pattern_mux_split("POST ", m, sizeof(m)));
    ASSERT_NULL(cbm_service_pattern_mux_split("GET host", m, sizeof(m)));
    ASSERT_NULL(cbm_service_pattern_mux_split("GET a b/x", m, sizeof(m)));
    ASSERT_NULL(cbm_service_pattern_mux_split("", m, sizeof(m)));
    ASSERT_NULL(cbm_service_pattern_mux_split(NULL, m, sizeof(m)));
    ASSERT_NULL(cbm_service_pattern_mux_split("ABCDEFGHIJKLMNOP /x", m, sizeof(m)));
    ASSERT_STR_EQ(m, "");

    /* only Handle / HandleFunc register a pattern, and never with a URL */
    pat = "POST /v1/payments";
    ASSERT_TRUE(cbm_service_pattern_mux_route_path("mux.HandleFunc", pat) == pat + 5);
    pat = "api.example.com/v1/x";
    ASSERT_TRUE(cbm_service_pattern_mux_route_path("s.mux.Handle", pat) == pat + 15);
    ASSERT_NULL(cbm_service_pattern_mux_route_path("st.Get", "users/123"));
    ASSERT_NULL(cbm_service_pattern_mux_route_path("c.Get", "https://api.example.com/v1/x"));
    ASSERT_NULL(cbm_service_pattern_mux_route_path("mux.Handle", "https://api.example.com/v1/x"));
    ASSERT_NULL(cbm_service_pattern_mux_route_path("PatchCollector.Delete", "storage.k8s.io/v1"));
    ASSERT_NULL(cbm_service_pattern_mux_route_path("r.Route", "GET /v1/x"));
    ASSERT_NULL(cbm_service_pattern_mux_route_path("mux.HandleFunc", "/v1/x"));
    ASSERT_NULL(cbm_service_pattern_mux_route_path(NULL, "GET /v1/x"));
    ASSERT_NULL(cbm_service_pattern_mux_route_path("mux.HandleFunc", NULL));
    PASS();
}

SUITE(infrascan) {
    RUN_TEST(infrascan_http_route_literal_guard_rejects_filesystem_paths);
    RUN_TEST(infrascan_http_route_literal_guard_rejects_document_extensions);
    RUN_TEST(infrascan_client_navigation_is_js_family_only);
    RUN_TEST(infrascan_service_pattern_match_rejects_ids_inside_words);
    RUN_TEST(infrascan_service_pattern_match_keeps_real_library_qns);
    RUN_TEST(infrascan_servemux_pattern_split_issue2537);
    RUN_TEST(infrascan_route_nodes_skip_bad_http_url_paths);
    RUN_TEST(infrascan_http_calls_join_matching_handler_route);
    RUN_TEST(infrascan_decorator_route_metadata_keeps_its_escapes);
    RUN_TEST(infrascan_decorator_route_metadata_rejects_malformed_values);
    RUN_TEST(infrascan_decorator_route_metadata_survives_invalid_utf8);
    RUN_TEST(infrascan_handles_properties_hold_a_long_handler_qn);
    RUN_TEST(infrascan_route_properties_escape_the_method);
    RUN_TEST(infrascan_sveltekit_handles_properties_escape_the_handler_qn);
    RUN_TEST(infrascan_data_flows_link_every_caller_of_a_route);
    RUN_TEST(infrascan_data_flows_do_not_depend_on_caller_insertion_order);
    RUN_TEST(infrascan_data_flows_reach_every_infra_handler);
    RUN_TEST(infrascan_data_flows_test_callers_pair_only_within_their_file);
    RUN_TEST(infrascan_data_flows_test_handlers_pair_only_within_their_file);
}
