/*
 * service_patterns.c — Classify call edges by library identity in resolved QN.
 *
 * Instead of matching callee names (ambiguous: "get", "post", "send"),
 * we match library identifiers in the RESOLVED qualified name. The QN
 * contains the full module path, so import aliases are transparent:
 *   r.get("/api") → QN: project.venv.requests.api.get → match "requests" → HTTP_CALLS
 *
 * Two-level matching:
 *   1. Library identifier in QN → determines edge type (HTTP/ASYNC/CONFIG)
 *   2. Method suffix → determines HTTP method (get→GET, post→POST)
 */
#include "service_patterns.h"

#include <stdbool.h>
#include <stddef.h>
#include <string.h>

/* ── Library identifier → edge type ────────────────────────────── */

typedef struct {
    const char *library_id; /* substring to find in resolved QN */
    cbm_svc_kind_t kind;    /* HTTP_CALLS, ASYNC_CALLS, CONFIGURES */
    const char *broker;     /* for ASYNC: broker name (NULL otherwise) */
} lib_pattern_t;

/* HTTP client libraries — match these substrings in the resolved QN.
 * Sources: github.com/easybase/awesome-http, official SDK docs, agent research */
static const lib_pattern_t http_libraries[] = {
    /* Python */
    {"requests", CBM_SVC_HTTP, NULL},
    {"httpx", CBM_SVC_HTTP, NULL},
    {"aiohttp", CBM_SVC_HTTP, NULL},
    {"urllib", CBM_SVC_HTTP, NULL},
    {"urllib3", CBM_SVC_HTTP, NULL},
    {"httplib2", CBM_SVC_HTTP, NULL},
    {"pycurl", CBM_SVC_HTTP, NULL},
    {"treq", CBM_SVC_HTTP, NULL},
    {"uplink", CBM_SVC_HTTP, NULL},

    /* JavaScript / TypeScript */
    {"axios", CBM_SVC_HTTP, NULL},
    {"superagent", CBM_SVC_HTTP, NULL},
    {"needle", CBM_SVC_HTTP, NULL},
    {"node-fetch", CBM_SVC_HTTP, NULL},
    {"undici", CBM_SVC_HTTP, NULL},
    {"ofetch", CBM_SVC_HTTP, NULL},
    {"wretch", CBM_SVC_HTTP, NULL},
    {"sindresorhus/ky", CBM_SVC_HTTP, NULL},
    {"phin", CBM_SVC_HTTP, NULL},

    /* Go */
    {"net/http", CBM_SVC_HTTP, NULL},
    {"resty", CBM_SVC_HTTP, NULL},
    {"sling", CBM_SVC_HTTP, NULL},
    {"heimdall", CBM_SVC_HTTP, NULL},
    {"gentleman", CBM_SVC_HTTP, NULL},
    {"retryablehttp", CBM_SVC_HTTP, NULL},

    /* Java / Kotlin */
    {"HttpClient", CBM_SVC_HTTP, NULL},
    {"OkHttp", CBM_SVC_HTTP, NULL},
    {"okhttp3", CBM_SVC_HTTP, NULL},
    {"RestTemplate", CBM_SVC_HTTP, NULL},
    {"WebClient", CBM_SVC_HTTP, NULL},
    {"Unirest", CBM_SVC_HTTP, NULL},
    {"AsyncHttpClient", CBM_SVC_HTTP, NULL},
    {"apache.http", CBM_SVC_HTTP, NULL},
    {"Retrofit", CBM_SVC_HTTP, NULL},
    {"Feign", CBM_SVC_HTTP, NULL},
    {"ktor.client", CBM_SVC_HTTP, NULL},
    {"kittinunf.fuel", CBM_SVC_HTTP, NULL},

    /* Rust */
    {"reqwest", CBM_SVC_HTTP, NULL},
    {"hyper", CBM_SVC_HTTP, NULL},
    {"surf", CBM_SVC_HTTP, NULL},
    {"ureq", CBM_SVC_HTTP, NULL},
    {"isahc", CBM_SVC_HTTP, NULL},
    {"attohttpc", CBM_SVC_HTTP, NULL},

    /* C# */
    {"HttpClient", CBM_SVC_HTTP, NULL},
    {"RestSharp", CBM_SVC_HTTP, NULL},
    {"Flurl", CBM_SVC_HTTP, NULL},
    {"Refit", CBM_SVC_HTTP, NULL},

    /* Ruby */
    {"HTTParty", CBM_SVC_HTTP, NULL},
    {"Faraday", CBM_SVC_HTTP, NULL},
    {"RestClient", CBM_SVC_HTTP, NULL},
    {"Typhoeus", CBM_SVC_HTTP, NULL},
    {"Excon", CBM_SVC_HTTP, NULL},
    {"Net::HTTP", CBM_SVC_HTTP, NULL},

    /* PHP */
    {"Guzzle", CBM_SVC_HTTP, NULL},
    {"guzzle", CBM_SVC_HTTP, NULL},
    {"curl", CBM_SVC_HTTP, NULL},
    {"Symfony\\HttpClient", CBM_SVC_HTTP, NULL},

    /* C/C++ */
    {"cpr", CBM_SVC_HTTP, NULL},
    {"cpp-httplib", CBM_SVC_HTTP, NULL},
    {"Poco.Net", CBM_SVC_HTTP, NULL},
    {"Beast", CBM_SVC_HTTP, NULL},

    /* Swift */
    {"Alamofire", CBM_SVC_HTTP, NULL},
    {"Moya", CBM_SVC_HTTP, NULL},
    {"URLSession", CBM_SVC_HTTP, NULL},

    /* Dart */
    {"Dio", CBM_SVC_HTTP, NULL},
    {"dio", CBM_SVC_HTTP, NULL},
    {"package:http", CBM_SVC_HTTP, NULL},
    {"Chopper", CBM_SVC_HTTP, NULL},

    /* Elixir */
    {"HTTPoison", CBM_SVC_HTTP, NULL},
    {"Tesla", CBM_SVC_HTTP, NULL},
    {"Finch", CBM_SVC_HTTP, NULL},
    {"Mint.HTTP", CBM_SVC_HTTP, NULL},

    /* Scala */
    {"sttp", CBM_SVC_HTTP, NULL},
    {"akka.http", CBM_SVC_HTTP, NULL},
    {"http4s", CBM_SVC_HTTP, NULL},
    {"scalaj", CBM_SVC_HTTP, NULL},

    /* Haskell */
    {"wreq", CBM_SVC_HTTP, NULL},
    {"http-client", CBM_SVC_HTTP, NULL},
    {"http-conduit", CBM_SVC_HTTP, NULL},
    {"servant-client", CBM_SVC_HTTP, NULL},
    {"Network.HTTP", CBM_SVC_HTTP, NULL},

    /* Lua */
    {"socket.http", CBM_SVC_HTTP, NULL},
    {"resty.http", CBM_SVC_HTTP, NULL},

    {NULL, CBM_SVC_NONE, NULL},
};

/* Async dispatch / message broker libraries */
static const lib_pattern_t async_libraries[] = {
    /* GCP */
    {"cloudtasks", CBM_SVC_ASYNC, "cloud_tasks"},
    {"cloud_tasks", CBM_SVC_ASYNC, "cloud_tasks"},
    {"cloud.tasks", CBM_SVC_ASYNC, "cloud_tasks"},
    {"CloudTasks", CBM_SVC_ASYNC, "cloud_tasks"},
    {"pubsub", CBM_SVC_ASYNC, "pubsub"},
    {"cloud.pubsub", CBM_SVC_ASYNC, "pubsub"},
    {"PubSub", CBM_SVC_ASYNC, "pubsub"},

    /* AWS — use SDK module paths to avoid false positives */
    {"aws-sdk-go/service/sqs", CBM_SVC_ASYNC, "sqs"},
    {"aws_sdk_sqs", CBM_SVC_ASYNC, "sqs"},
    {"Amazon.SQS", CBM_SVC_ASYNC, "sqs"},
    {"@aws-sdk/client-sqs", CBM_SVC_ASYNC, "sqs"},
    {"boto3.client.sqs", CBM_SVC_ASYNC, "sqs"},
    {"aws-sdk-go/service/sns", CBM_SVC_ASYNC, "sns"},
    {"aws_sdk_sns", CBM_SVC_ASYNC, "sns"},
    {"Amazon.SNS", CBM_SVC_ASYNC, "sns"},
    {"@aws-sdk/client-sns", CBM_SVC_ASYNC, "sns"},
    {"eventbridge", CBM_SVC_ASYNC, "eventbridge"},
    {"EventBridge", CBM_SVC_ASYNC, "eventbridge"},
    {"aws-sdk-go/service/lambda", CBM_SVC_ASYNC, "lambda"},
    {"aws_sdk_lambda", CBM_SVC_ASYNC, "lambda"},
    {"@aws-sdk/client-lambda", CBM_SVC_ASYNC, "lambda"},
    {"stepfunctions", CBM_SVC_ASYNC, "stepfunctions"},

    /* Azure */
    {"ServiceBus", CBM_SVC_ASYNC, "servicebus"},
    {"Azure.Messaging", CBM_SVC_ASYNC, "servicebus"},

    /* Kafka */
    {"kafka", CBM_SVC_ASYNC, "kafka"},
    {"Kafka", CBM_SVC_ASYNC, "kafka"},
    {"kafkajs", CBM_SVC_ASYNC, "kafka"},
    {"sarama", CBM_SVC_ASYNC, "kafka"},
    {"rdkafka", CBM_SVC_ASYNC, "kafka"},
    {"confluent", CBM_SVC_ASYNC, "kafka"},
    {"Confluent.Kafka", CBM_SVC_ASYNC, "kafka"},

    /* RabbitMQ */
    {"amqp", CBM_SVC_ASYNC, "rabbitmq"},
    {"AMQP", CBM_SVC_ASYNC, "rabbitmq"},
    {"amqplib", CBM_SVC_ASYNC, "rabbitmq"},
    {"RabbitMQ", CBM_SVC_ASYNC, "rabbitmq"},
    {"lapin", CBM_SVC_ASYNC, "rabbitmq"},
    {"MassTransit", CBM_SVC_ASYNC, "rabbitmq"},

    /* NATS */
    {"nats", CBM_SVC_ASYNC, "nats"},
    {"NATS", CBM_SVC_ASYNC, "nats"},

    /* Redis pub/sub */
    {"ioredis", CBM_SVC_ASYNC, "redis"},

    /* Task queues */
    {"celery", CBM_SVC_ASYNC, "celery"},
    {"Celery", CBM_SVC_ASYNC, "celery"},
    {"dramatiq", CBM_SVC_ASYNC, "dramatiq"},
    {"huey", CBM_SVC_ASYNC, "huey"},
    {"python-rq", CBM_SVC_ASYNC, "rq"},
    {"rq.Queue", CBM_SVC_ASYNC, "rq"},
    {"bullmq", CBM_SVC_ASYNC, "bullmq"},
    {"BullMQ", CBM_SVC_ASYNC, "bullmq"},
    {"bull.Queue", CBM_SVC_ASYNC, "bull"},
    {"Sidekiq", CBM_SVC_ASYNC, "sidekiq"},
    {"sidekiq", CBM_SVC_ASYNC, "sidekiq"},
    {"Resque", CBM_SVC_ASYNC, "resque"},
    {"GoodJob", CBM_SVC_ASYNC, "goodjob"},
    {"DelayedJob", CBM_SVC_ASYNC, "delayed_job"},
    {"Hangfire", CBM_SVC_ASYNC, "hangfire"},
    {"NServiceBus", CBM_SVC_ASYNC, "nservicebus"},
    {"asynq", CBM_SVC_ASYNC, "asynq"},
    {"RichardKnop/machinery", CBM_SVC_ASYNC, "machinery"},

    /* Workflow engines — use specific module paths to avoid "Temporal" in Django etc. */
    {"temporalio", CBM_SVC_ASYNC, "temporal"},
    {"@temporalio", CBM_SVC_ASYNC, "temporal"},
    {"temporal.client", CBM_SVC_ASYNC, "temporal"},
    {"temporal.worker", CBM_SVC_ASYNC, "temporal"},
    {"inngest", CBM_SVC_ASYNC, "inngest"},

    /* Elixir */
    {"Oban", CBM_SVC_ASYNC, "oban"},
    {"Broadway", CBM_SVC_ASYNC, "broadway"},
    {"GenStage", CBM_SVC_ASYNC, "genstage"},
    {"Phoenix.PubSub", CBM_SVC_ASYNC, "phoenix_pubsub"},

    /* Scala */
    {"Alpakka", CBM_SVC_ASYNC, "alpakka"},

    /* MQTT */
    {"mqtt", CBM_SVC_ASYNC, "mqtt"},
    {"paho.mqtt", CBM_SVC_ASYNC, "mqtt"},
    {"MQTTClient", CBM_SVC_ASYNC, "mqtt"},
    {"mosquitto", CBM_SVC_ASYNC, "mqtt"},
    {"asyncio_mqtt", CBM_SVC_ASYNC, "mqtt"},
    {"gmqtt", CBM_SVC_ASYNC, "mqtt"},
    {"rumqttc", CBM_SVC_ASYNC, "mqtt"},

    /* NATS */
    {"nats.go", CBM_SVC_ASYNC, "nats"},
    {"nats-py", CBM_SVC_ASYNC, "nats"},
    {"nats.ws", CBM_SVC_ASYNC, "nats"},
    {"nats.java", CBM_SVC_ASYNC, "nats"},
    {"nats.net", CBM_SVC_ASYNC, "nats"},
    {"async-nats", CBM_SVC_ASYNC, "nats"},
    {"nats.rs", CBM_SVC_ASYNC, "nats"},

    /* Dapr pub/sub */
    {"dapr.clients.grpc", CBM_SVC_ASYNC, "dapr"},
    {"DaprClient", CBM_SVC_ASYNC, "dapr"},

    {NULL, CBM_SVC_NONE, NULL},
};

/* Config accessor libraries */
static const lib_pattern_t config_libraries[] = {
    /* Universal */
    {"getenv", CBM_SVC_CONFIG, NULL},
    {"Getenv", CBM_SVC_CONFIG, NULL},
    {"getEnv", CBM_SVC_CONFIG, NULL},
    {"LookupEnv", CBM_SVC_CONFIG, NULL},
    {"lookupEnv", CBM_SVC_CONFIG, NULL},
    {"get_env", CBM_SVC_CONFIG, NULL},
    {"fetch_env", CBM_SVC_CONFIG, NULL},
    {"GetEnvironmentVariable", CBM_SVC_CONFIG, NULL},
    {"getProperty", CBM_SVC_CONFIG, NULL},
    {"getEnvironment", CBM_SVC_CONFIG, NULL},

    /* Go */
    {"viper", CBM_SVC_CONFIG, NULL},
    {"envconfig", CBM_SVC_CONFIG, NULL},
    {"godotenv", CBM_SVC_CONFIG, NULL},

    /* Python */
    {"decouple", CBM_SVC_CONFIG, NULL},
    {"dynaconf", CBM_SVC_CONFIG, NULL},
    {"dotenv", CBM_SVC_CONFIG, NULL},

    /* JS/TS */
    {"nconf", CBM_SVC_CONFIG, NULL},
    {"convict", CBM_SVC_CONFIG, NULL},
    {"envalid", CBM_SVC_CONFIG, NULL},

    /* Rust */
    {"dotenvy", CBM_SVC_CONFIG, NULL},
    {"figment", CBM_SVC_CONFIG, NULL},
    {"config-rs", CBM_SVC_CONFIG, NULL},

    /* Java/Scala */
    {"ConfigFactory", CBM_SVC_CONFIG, NULL},
    {"ConfigurationProperties", CBM_SVC_CONFIG, NULL},

    /* Elixir */
    {"Application.get_env", CBM_SVC_CONFIG, NULL},
    {"Application.fetch_env", CBM_SVC_CONFIG, NULL},

    {NULL, CBM_SVC_NONE, NULL},
};

/* Route registration frameworks — callee resolves to one of these AND
 * has an HTTP method suffix → CBM_SVC_ROUTE_REG.
 * Distinguished from HTTP clients: "gin.GET" registers a handler,
 * "requests.get" makes an outbound HTTP call. */
static const lib_pattern_t route_reg_libraries[] = {
    /* Go */
    {"gin-gonic/gin", CBM_SVC_ROUTE_REG, NULL},
    {"gin.", CBM_SVC_ROUTE_REG, NULL},
    {"go-chi/chi", CBM_SVC_ROUTE_REG, NULL},
    {"chi.", CBM_SVC_ROUTE_REG, NULL},
    {"gorilla/mux", CBM_SVC_ROUTE_REG, NULL},
    {"labstack/echo", CBM_SVC_ROUTE_REG, NULL},
    {"echo.", CBM_SVC_ROUTE_REG, NULL},
    {"gofiber/fiber", CBM_SVC_ROUTE_REG, NULL},
    {"fiber.", CBM_SVC_ROUTE_REG, NULL},
    {"net/http.ServeMux", CBM_SVC_ROUTE_REG, NULL},
    {"http.ServeMux", CBM_SVC_ROUTE_REG, NULL},
    {"httprouter", CBM_SVC_ROUTE_REG, NULL},

    /* JavaScript / TypeScript */
    {"express", CBM_SVC_ROUTE_REG, NULL},
    {"fastify", CBM_SVC_ROUTE_REG, NULL},
    {"koa-router", CBM_SVC_ROUTE_REG, NULL},
    {"hono", CBM_SVC_ROUTE_REG, NULL},
    {"hapi", CBM_SVC_ROUTE_REG, NULL},

    /* Python (non-decorator, e.g., Flask add_url_rule) */
    {"flask", CBM_SVC_ROUTE_REG, NULL},
    {"FastAPI", CBM_SVC_ROUTE_REG, NULL},
    {"starlette", CBM_SVC_ROUTE_REG, NULL},

    /* PHP */
    {"Laravel", CBM_SVC_ROUTE_REG, NULL},
    {"Illuminate.Routing", CBM_SVC_ROUTE_REG, NULL},
    {"Symfony.Routing", CBM_SVC_ROUTE_REG, NULL},

    /* Kotlin */
    {"ktor.server", CBM_SVC_ROUTE_REG, NULL},
    {"ktor.routing", CBM_SVC_ROUTE_REG, NULL},

    /* Rust */
    {"actix-web", CBM_SVC_ROUTE_REG, NULL},
    {"actix_web", CBM_SVC_ROUTE_REG, NULL},
    {"axum", CBM_SVC_ROUTE_REG, NULL},
    {"rocket", CBM_SVC_ROUTE_REG, NULL},

    /* Java */
    {"Spring", CBM_SVC_ROUTE_REG, NULL},
    {"jakarta.ws.rs", CBM_SVC_ROUTE_REG, NULL},

    /* C# */
    {"Microsoft.AspNetCore", CBM_SVC_ROUTE_REG, NULL},
    {"MapGet", CBM_SVC_ROUTE_REG, NULL},
    {"MapPost", CBM_SVC_ROUTE_REG, NULL},

    /* Ruby */
    {"ActionDispatch", CBM_SVC_ROUTE_REG, NULL},
    {"Sinatra", CBM_SVC_ROUTE_REG, NULL},

    /* Elixir */
    {"Phoenix.Router", CBM_SVC_ROUTE_REG, NULL},

    /* Scala */
    {"akka.http.scaladsl.server", CBM_SVC_ROUTE_REG, NULL},
    {"play.api.routing", CBM_SVC_ROUTE_REG, NULL},

    {NULL, CBM_SVC_NONE, NULL},
};

/* gRPC client libraries — protobuf stub invocations */
static const lib_pattern_t grpc_libraries[] = {
    /* Go */
    {"google.golang.org/grpc", CBM_SVC_GRPC, NULL},
    {"grpc.Dial", CBM_SVC_GRPC, NULL},
    {"grpc.NewClient", CBM_SVC_GRPC, NULL},
    {"grpc.DialContext", CBM_SVC_GRPC, NULL},

    /* Python */
    {"grpc.insecure_channel", CBM_SVC_GRPC, NULL},
    {"grpc.secure_channel", CBM_SVC_GRPC, NULL},
    {"grpcio", CBM_SVC_GRPC, NULL},
    {"grpc.aio", CBM_SVC_GRPC, NULL},

    /* Java/Kotlin */
    {"io.grpc", CBM_SVC_GRPC, NULL},
    {"ManagedChannelBuilder", CBM_SVC_GRPC, NULL},
    {"ManagedChannel", CBM_SVC_GRPC, NULL},
    {"newBlockingStub", CBM_SVC_GRPC, NULL},
    {"newFutureStub", CBM_SVC_GRPC, NULL},

    /* C# */
    {"Grpc.Net.Client", CBM_SVC_GRPC, NULL},
    {"GrpcChannel", CBM_SVC_GRPC, NULL},
    {"Grpc.Core", CBM_SVC_GRPC, NULL},

    /* JS/TS */
    {"@grpc/grpc-js", CBM_SVC_GRPC, NULL},
    {"grpc-web", CBM_SVC_GRPC, NULL},

    /* Rust */
    {"tonic", CBM_SVC_GRPC, NULL},

    /* Dart/Flutter */
    {"package:grpc", CBM_SVC_GRPC, NULL},

    {NULL, CBM_SVC_NONE, NULL},
};

/* GraphQL client libraries */
static const lib_pattern_t graphql_libraries[] = {
    /* JS/TS */
    {"graphql-request", CBM_SVC_GRAPHQL, NULL},
    {"@apollo/client", CBM_SVC_GRAPHQL, NULL},
    {"apollo-client", CBM_SVC_GRAPHQL, NULL},
    {"urql", CBM_SVC_GRAPHQL, NULL},
    {"graphql-tag", CBM_SVC_GRAPHQL, NULL},

    /* Python */
    {"gql", CBM_SVC_GRAPHQL, NULL},
    {"sgqlc", CBM_SVC_GRAPHQL, NULL},
    {"graphene", CBM_SVC_GRAPHQL, NULL},

    /* Java */
    {"graphql-java", CBM_SVC_GRAPHQL, NULL},
    {"DgsQueryExecutor", CBM_SVC_GRAPHQL, NULL},

    /* Go */
    {"graphql-go", CBM_SVC_GRAPHQL, NULL},
    {"gqlgen", CBM_SVC_GRAPHQL, NULL},

    /* Ruby */
    {"graphql-ruby", CBM_SVC_GRAPHQL, NULL},

    /* Rust */
    {"async-graphql", CBM_SVC_GRAPHQL, NULL},
    {"juniper", CBM_SVC_GRAPHQL, NULL},

    {NULL, CBM_SVC_NONE, NULL},
};

/* tRPC libraries (TypeScript only) */
static const lib_pattern_t trpc_libraries[] = {
    {"@trpc/server", CBM_SVC_TRPC, NULL},
    {"@trpc/client", CBM_SVC_TRPC, NULL},
    {"@trpc/react-query", CBM_SVC_TRPC, NULL},
    {"createTRPCRouter", CBM_SVC_TRPC, NULL},
    {"createTRPCProxyClient", CBM_SVC_TRPC, NULL},

    {NULL, CBM_SVC_NONE, NULL},
};

/* Method suffix type (used by both route registration and HTTP client tables) */
typedef struct {
    const char *suffix;
    const char *method;
} method_suffix_t;

/* Route registration method suffixes — matched on callee name.
 * These are methods on router objects that register handlers. */
static const method_suffix_t route_reg_suffixes[] = {
    /* HTTP method registrations */
    {".GET", "GET"},
    {".Get", "GET"},
    {".get", "GET"},
    {".POST", "POST"},
    {".Post", "POST"},
    {".post", "POST"},
    {".PUT", "PUT"},
    {".Put", "PUT"},
    {".put", "PUT"},
    {".DELETE", "DELETE"},
    {".Delete", "DELETE"},
    {".delete", "DELETE"},
    {".PATCH", "PATCH"},
    {".Patch", "PATCH"},
    {".patch", "PATCH"},
    /* Handle/HandleFunc (Go stdlib, gorilla) */
    {".Handle", "ANY"},
    {".HandleFunc", "ANY"},
    {".handle", "ANY"},
    /* Framework-specific route registration */
    {".Route", "ANY"},
    {".route", "ANY"},
    {"::get", "GET"},
    {"::post", "POST"},
    {"::put", "PUT"},
    {"::delete", "DELETE"},
    {"::patch", "PATCH"},
    /* Minimal API (C# ASP.NET) */
    {".MapGet", "GET"},
    {".MapPost", "POST"},
    {".MapPut", "PUT"},
    {".MapDelete", "DELETE"},
    /* Router mounting / prefix registration (any method) */
    {".include_router", "ANY"},
    {".mount", "ANY"},
    {".add_url_rule", "ANY"},
    {".register_blueprint", "ANY"},
    {".use", "ANY"},
    {".register", "ANY"},
    {".add_route", "ANY"},
    {".add_api_route", "ANY"},
    {".add_api_websocket_route", "ANY"},
    {NULL, NULL},
};

/* ── HTTP method inference from function/method name suffix ───── */

static const method_suffix_t method_suffixes[] = {
    {".get", "GET"},           {".Get", "GET"},           {".GET", "GET"},
    {".post", "POST"},         {".Post", "POST"},         {".POST", "POST"},
    {".put", "PUT"},           {".Put", "PUT"},           {".PUT", "PUT"},
    {".delete", "DELETE"},     {".Delete", "DELETE"},     {".DELETE", "DELETE"},
    {".patch", "PATCH"},       {".Patch", "PATCH"},       {".PATCH", "PATCH"},
    {".head", "HEAD"},         {".Head", "HEAD"},         {".HEAD", "HEAD"},
    {".options", "OPTIONS"},   {".Options", "OPTIONS"},   {"GetAsync", "GET"},
    {"PostAsync", "POST"},     {"PutAsync", "PUT"},       {"DeleteAsync", "DELETE"},
    {"SendAsync", NULL},       {"getForObject", "GET"},   {"getForEntity", "GET"},
    {"postForObject", "POST"}, {"postForEntity", "POST"}, {NULL, NULL},
};

/* ── Matching implementation ───────────────────────────────────── */

/* Check if any library identifier appears as a substring in the QN.
 * Case-sensitive: "requests" matches "project.venv.requests.api.get"
 * but not "Requests". Library names are specific enough to avoid
 * false positives even with substring matching. */
static const lib_pattern_t *match_qn(const char *qn, const lib_pattern_t *patterns) {
    if (!qn || !qn[0]) {
        return NULL;
    }
    for (int i = 0; patterns[i].library_id != NULL; i++) {
        if (strstr(qn, patterns[i].library_id) != NULL) {
            return &patterns[i];
        }
    }
    return NULL;
}

/* ── Public API ────────────────────────────────────────────────── */

void cbm_service_patterns_init(void) {
    /* No-op — tables are static const */
}

cbm_svc_kind_t cbm_service_pattern_match(const char *resolved_qn) {
    if (!resolved_qn || !resolved_qn[0]) {
        return CBM_SVC_NONE;
    }

    /* Route registration checked first — prevents gin/echo from matching
     * as HTTP clients (both have .get/.post suffixes). */
    const lib_pattern_t *p = match_qn(resolved_qn, route_reg_libraries);
    if (p) {
        return p->kind;
    }

    p = match_qn(resolved_qn, http_libraries);
    if (p) {
        return p->kind;
    }

    p = match_qn(resolved_qn, async_libraries);
    if (p) {
        return p->kind;
    }

    p = match_qn(resolved_qn, config_libraries);
    if (p) {
        return p->kind;
    }

    p = match_qn(resolved_qn, grpc_libraries);
    if (p) {
        return p->kind;
    }

    p = match_qn(resolved_qn, graphql_libraries);
    if (p) {
        return p->kind;
    }

    p = match_qn(resolved_qn, trpc_libraries);
    if (p) {
        return p->kind;
    }

    return CBM_SVC_NONE;
}

const char *cbm_service_pattern_http_method(const char *callee_name) {
    if (!callee_name) {
        return NULL;
    }
    for (int i = 0; method_suffixes[i].suffix != NULL; i++) {
        size_t slen = strlen(method_suffixes[i].suffix);
        size_t clen = strlen(callee_name);
        if (clen >= slen && strcmp(callee_name + clen - slen, method_suffixes[i].suffix) == 0) {
            return method_suffixes[i].method;
        }
    }
    return NULL;
}

const char *cbm_service_pattern_route_method(const char *callee_name) {
    if (!callee_name) {
        return NULL;
    }
    size_t clen = strlen(callee_name);
    for (int i = 0; route_reg_suffixes[i].suffix != NULL; i++) {
        size_t slen = strlen(route_reg_suffixes[i].suffix);
        if (clen >= slen && strcmp(callee_name + clen - slen, route_reg_suffixes[i].suffix) == 0) {
            return route_reg_suffixes[i].method;
        }
    }
    return NULL;
}

const char *cbm_service_pattern_broker(const char *resolved_qn) {
    if (!resolved_qn) {
        return NULL;
    }
    const lib_pattern_t *p = match_qn(resolved_qn, async_libraries);
    return p ? p->broker : NULL;
}
