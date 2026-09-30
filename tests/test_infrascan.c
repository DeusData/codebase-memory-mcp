#include "test_framework.h"
#include "graph_buffer/graph_buffer.h"
#include "pipeline/pipeline_internal.h"
#include "service_patterns.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

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

SUITE(infrascan) {
    RUN_TEST(infrascan_http_route_literal_guard_rejects_filesystem_paths);
    RUN_TEST(infrascan_service_pattern_match_rejects_ids_inside_words);
    RUN_TEST(infrascan_service_pattern_match_keeps_real_library_qns);
    RUN_TEST(infrascan_route_nodes_skip_bad_http_url_paths);
    RUN_TEST(infrascan_http_calls_join_matching_handler_route);
}
