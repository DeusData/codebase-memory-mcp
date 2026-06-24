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
#include <stdlib.h>
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

    /* AWS — use SDK module paths to avoid false positives.  cbm_fqn_compute
     * converts path slashes to '.', so a resolved local Go QN reads
     * "aws-sdk-go.service.sqs..."; include both slash and dot forms so the
     * substring match fires whether the id comes from an import path or a QN. */
    {"aws-sdk-go/service/sqs", CBM_SVC_ASYNC, "sqs"},
    {"aws-sdk-go.service.sqs", CBM_SVC_ASYNC, "sqs"},
    {"aws_sdk_sqs", CBM_SVC_ASYNC, "sqs"},
    {"Amazon.SQS", CBM_SVC_ASYNC, "sqs"},
    {"@aws-sdk/client-sqs", CBM_SVC_ASYNC, "sqs"},
    {"boto3.client.sqs", CBM_SVC_ASYNC, "sqs"},
    {"aws-sdk-go/service/sns", CBM_SVC_ASYNC, "sns"},
    {"aws-sdk-go.service.sns", CBM_SVC_ASYNC, "sns"},
    {"aws_sdk_sns", CBM_SVC_ASYNC, "sns"},
    {"Amazon.SNS", CBM_SVC_ASYNC, "sns"},
    {"@aws-sdk/client-sns", CBM_SVC_ASYNC, "sns"},
    {"eventbridge", CBM_SVC_ASYNC, "eventbridge"},
    {"EventBridge", CBM_SVC_ASYNC, "eventbridge"},
    {"aws-sdk-go/service/lambda", CBM_SVC_ASYNC, "lambda"},
    {"aws-sdk-go.service.lambda", CBM_SVC_ASYNC, "lambda"},
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

static bool starts_with_segment(const char *path, const char *segment) {
    if (!path || path[0] != '/' || !segment) {
        return false;
    }
    size_t seg_len = strlen(segment);
    const char *p = path + 1;
    return strncmp(p, segment, seg_len) == 0 && (p[seg_len] == '\0' || p[seg_len] == '/');
}

static bool contains_segment(const char *path, const char *segment) {
    if (!path || !segment) {
        return false;
    }
    size_t seg_len = strlen(segment);
    const char *p = path;
    while ((p = strchr(p, '/')) != NULL) {
        p++;
        if (strncmp(p, segment, seg_len) == 0 && (p[seg_len] == '\0' || p[seg_len] == '/')) {
            return true;
        }
    }
    return false;
}

static bool is_digit_char(char ch) {
    return ch >= '0' && ch <= '9';
}

static bool has_http_route_marker(const char *path) {
    if (starts_with_segment(path, "api") || starts_with_segment(path, "apis") ||
        starts_with_segment(path, "graphql") || starts_with_segment(path, "health") ||
        starts_with_segment(path, "metrics")) {
        return true;
    }
    return path && path[0] == '/' && path[1] == 'v' && is_digit_char(path[2]) &&
           (path[3] == '\0' || path[3] == '/');
}

static bool has_filesystem_root(const char *path) {
    static const char *const roots[] = {"etc",     "root", "var",   "usr",     "home", "tmp",
                                        "private", "opt",  "bin",   "sbin",    "dev",  "proc",
                                        "sys",     "run",  "lib",   "lib64",   "mnt",  "media",
                                        "boot",    "srv",  "Users", "Volumes", NULL};
    for (int i = 0; roots[i]; i++) {
        if (starts_with_segment(path, roots[i])) {
            return true;
        }
    }
    return false;
}

static bool has_hidden_config_segment(const char *path) {
    static const char *const segments[] = {".aws", ".azure", ".config", ".docker", ".env",
                                           ".git", ".gnupg", ".kube",   ".ssh",    NULL};
    for (int i = 0; segments[i]; i++) {
        if (contains_segment(path, segments[i])) {
            return true;
        }
    }
    return false;
}

static bool path_ext_matches(const char *ext, const char *wanted) {
    return ext && wanted && strcmp(ext, wanted) == 0;
}

static bool has_filesystem_extension(const char *path) {
    if (!path) {
        return false;
    }
    const char *end = strpbrk(path, "?#");
    if (!end) {
        end = path + strlen(path);
    }
    const char *last_slash = path;
    for (const char *p = path; p < end; p++) {
        if (*p == '/') {
            last_slash = p;
        }
    }
    const char *dot = NULL;
    for (const char *p = last_slash + 1; p < end; p++) {
        if (*p == '.') {
            dot = p;
        }
    }
    if (!dot || dot == end - 1) {
        return false;
    }
    char ext[32];
    size_t ext_len = (size_t)(end - dot);
    if (ext_len >= sizeof(ext)) {
        return false;
    }
    memcpy(ext, dot, ext_len);
    ext[ext_len] = '\0';

    static const char *const hard_file_exts[] = {
        ".cfg",  ".conf",   ".credentials", ".crt",  ".db",         ".env",
        ".ini",  ".key",    ".pem",         ".pid",  ".properties", ".service",
        ".sock", ".socket", ".sqlite",      ".toml", NULL};
    for (int i = 0; hard_file_exts[i]; i++) {
        if (path_ext_matches(ext, hard_file_exts[i])) {
            return true;
        }
    }
    if ((path_ext_matches(ext, ".json") || path_ext_matches(ext, ".yaml") ||
         path_ext_matches(ext, ".yml") || path_ext_matches(ext, ".xml")) &&
        !has_http_route_marker(path)) {
        return true;
    }
    return false;
}

static bool callee_is_delimiter_or_filesystem_builder(const char *callee_name) {
    if (!callee_name) {
        return false;
    }
    const char *last_dot = strrchr(callee_name, '.');
    const char *last_colon = strstr(callee_name, "::");
    const char *method = callee_name;
    if (last_dot && last_dot[1]) {
        method = last_dot + 1;
    }
    if (last_colon && last_colon[2]) {
        method = last_colon + 2;
    }
    if (strcmp(method, "split") == 0 || strcmp(method, "rsplit") == 0 ||
        strcmp(method, "partition") == 0 || strcmp(method, "join") == 0) {
        return true;
    }
    return strstr(callee_name, "os.path.join") != NULL || strstr(callee_name, "path.join") != NULL;
}

static const char *strip_string_delimiters(const char *literal, char *buf, size_t buf_sz) {
    if (!literal || !literal[0]) {
        return NULL;
    }
    const char *start = literal;
    while (*start == ' ' || *start == '\t' || *start == '\n' || *start == '\r') {
        start++;
    }
    size_t len = strlen(start);
    while (len > 0 && (start[len - 1] == ' ' || start[len - 1] == '\t' || start[len - 1] == '\n' ||
                       start[len - 1] == '\r')) {
        len--;
    }
    if (len >= 2 && (start[0] == '"' || start[0] == '\'' || start[0] == '`') &&
        start[len - 1] == start[0]) {
        start++;
        len -= 2;
    }
    if (len == 0 || len >= buf_sz) {
        return NULL;
    }
    memcpy(buf, start, len);
    buf[len] = '\0';
    return buf;
}

bool cbm_service_pattern_is_http_route_literal(const char *literal, const char *callee_name) {
    char path_buf[1024];
    const char *path = strip_string_delimiters(literal, path_buf, sizeof(path_buf));
    if (!path || !path[0]) {
        return false;
    }
    if (strncmp(path, "http://", 7) == 0 || strncmp(path, "https://", 8) == 0) {
        return true;
    }
    if (strstr(path, "://") != NULL) {
        return false;
    }
    if (path[0] != '/') {
        return false;
    }
    if (callee_is_delimiter_or_filesystem_builder(callee_name)) {
        return false;
    }
    if (has_filesystem_root(path) || has_hidden_config_segment(path) ||
        has_filesystem_extension(path)) {
        return false;
    }
    return true;
}

/* ── Public API ────────────────────────────────────────────────── */

/* Per-worker TLS cache of cbm_service_pattern_match results.
 * The hot path in resolve_file_calls invokes pattern matching for
 * EVERY resolved CALL (via emit_service_edge) — that's 6 pattern-list
 * scans × ~30 patterns × strstr per call. On kubernetes (~600k
 * resolved call edges), the same resolved QN (e.g. "context.Context.
 * Done", "fmt.Errorf", "errors.New") repeats hundreds of thousands of
 * times. A simple TLS hash cache turns the linear scan into one
 * lookup after the first miss for that QN. Lifetime is per-worker for
 * the duration of the parallel_resolve phase. */
#include "foundation/hash_table.h"
#include "foundation/compat.h"

static CBM_TLS CBMHashTable *_svc_cache = NULL;
/* Encode the enum + 1 in the pointer so 0/NULL means "miss". */
static inline void *svc_enum_to_ptr(cbm_svc_kind_t k) {
    return (void *)(uintptr_t)((unsigned)k + 1u);
}
static inline cbm_svc_kind_t svc_ptr_to_enum(void *p) {
    return (cbm_svc_kind_t)((uintptr_t)p - 1u);
}

static void svc_cache_free_key(const char *key, void *val, void *ud) {
    (void)val;
    (void)ud;
    free((char *)key);
}

void cbm_service_pattern_cache_begin(void) {
    if (_svc_cache)
        return; /* idempotent */
    _svc_cache = cbm_ht_create(8192);
}

void cbm_service_pattern_cache_end(void) {
    if (!_svc_cache)
        return;
    cbm_ht_foreach(_svc_cache, svc_cache_free_key, NULL);
    cbm_ht_free(_svc_cache);
    _svc_cache = NULL;
}

void cbm_service_patterns_init(void) {
    /* No-op — tables are static const */
}

cbm_svc_kind_t cbm_service_pattern_match(const char *resolved_qn) {
    if (!resolved_qn || !resolved_qn[0]) {
        return CBM_SVC_NONE;
    }

    if (_svc_cache) {
        void *cached = cbm_ht_get(_svc_cache, resolved_qn);
        if (cached) {
            return svc_ptr_to_enum(cached);
        }
    }

    cbm_svc_kind_t result = CBM_SVC_NONE;
    const lib_pattern_t *p;

    /* Route registration checked first — prevents gin/echo from matching
     * as HTTP clients (both have .get/.post suffixes). */
    if ((p = match_qn(resolved_qn, route_reg_libraries)))
        result = p->kind;
    else if ((p = match_qn(resolved_qn, http_libraries)))
        result = p->kind;
    else if ((p = match_qn(resolved_qn, async_libraries)))
        result = p->kind;
    else if ((p = match_qn(resolved_qn, config_libraries)))
        result = p->kind;
    else if ((p = match_qn(resolved_qn, grpc_libraries)))
        result = p->kind;
    else if ((p = match_qn(resolved_qn, graphql_libraries)))
        result = p->kind;
    else if ((p = match_qn(resolved_qn, trpc_libraries)))
        result = p->kind;

    if (_svc_cache) {
        char *kdup = strdup(resolved_qn);
        if (kdup)
            cbm_ht_set(_svc_cache, kdup, svc_enum_to_ptr(result));
    }
    return result;
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
