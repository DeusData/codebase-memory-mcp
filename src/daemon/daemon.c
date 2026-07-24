/*
 * daemon.c — Process-local coordination and wire framing for the CBM daemon.
 */
#include "daemon/daemon.h"

#include "foundation/compat_thread.h"

#include <stdlib.h>
#include <string.h>

typedef struct cbm_daemon_subscription {
    cbm_daemon_subscription_id_t id;
    cbm_daemon_client_id_t client_id;
    struct cbm_daemon_subscription *next;
} cbm_daemon_subscription_t;

typedef struct cbm_daemon_client {
    cbm_daemon_client_id_t id;
    uint64_t last_heartbeat_ms;
    struct cbm_daemon_client *next;
} cbm_daemon_client_t;

typedef struct cbm_daemon_job {
    char *project_key;
    cbm_daemon_job_state_t state;
    cbm_daemon_subscription_t *subscriptions;
    size_t subscription_count;
    bool cancel_callback_inflight;
    bool detached;
    struct cbm_daemon_job *next;
    struct cbm_daemon_job *action_next;
} cbm_daemon_job_t;

typedef struct cbm_daemon_watch {
    char *project_key;
    cbm_daemon_subscription_t *subscriptions;
    size_t subscription_count;
    struct cbm_daemon_watch *next;
    struct cbm_daemon_watch *action_next;
} cbm_daemon_watch_t;

struct cbm_daemon_coordinator {
    cbm_mutex_t mutex;
    cbm_daemon_client_t *clients;
    cbm_daemon_job_t *jobs;
    cbm_daemon_watch_t *watches;
    size_t client_count;
    /* See cbm_daemon_coordinator_set_permanent. */
    bool permanent;
    size_t job_count;
    size_t watch_count;
    size_t callback_count;
    uint64_t lease_timeout_ms;
    cbm_daemon_client_id_t last_client_id;
    cbm_daemon_subscription_id_t last_subscription_id;
    cbm_daemon_coordinator_state_t state;
    cbm_daemon_coordinator_hooks_t hooks;
};

typedef struct {
    cbm_daemon_job_t *jobs;
    cbm_daemon_watch_t *watches;
    cbm_daemon_job_cancel_fn cancel_job;
    cbm_daemon_watch_release_fn release_watch;
    void *context;
} cbm_daemon_callback_batch_t;

enum {
    FRAME_MAGIC_0 = 0,
    FRAME_MAGIC_1 = 1,
    FRAME_MAGIC_2 = 2,
    FRAME_MAGIC_3 = 3,
    FRAME_VERSION = 4,
    FRAME_TYPE = 5,
    FRAME_FLAGS_HI = 6,
    FRAME_FLAGS_LO = 7,
    FRAME_LENGTH_3 = 8,
    FRAME_LENGTH_2 = 9,
    FRAME_LENGTH_1 = 10,
    FRAME_LENGTH_0 = 11,
};

static bool frame_type_valid(cbm_daemon_frame_type_t type) {
    return type == CBM_DAEMON_FRAME_REQUEST || type == CBM_DAEMON_FRAME_RESPONSE;
}

static char *daemon_string_dup(const char *value) {
    size_t length = strlen(value);
    char *copy = malloc(length + 1);
    if (copy) {
        memcpy(copy, value, length + 1);
    }
    return copy;
}

static void free_subscriptions(cbm_daemon_subscription_t *subscription) {
    while (subscription) {
        cbm_daemon_subscription_t *next = subscription->next;
        free(subscription);
        subscription = next;
    }
}

static void free_job(cbm_daemon_job_t *job) {
    if (job) {
        free_subscriptions(job->subscriptions);
        free(job->project_key);
        free(job);
    }
}

static void free_watch(cbm_daemon_watch_t *watch) {
    if (watch) {
        free_subscriptions(watch->subscriptions);
        free(watch->project_key);
        free(watch);
    }
}

static cbm_daemon_client_id_t issue_client_id_locked(cbm_daemon_coordinator_t *coordinator) {
    if (coordinator->last_client_id == UINT64_MAX) {
        return CBM_DAEMON_CLIENT_ID_INVALID;
    }
    coordinator->last_client_id++;
    return coordinator->last_client_id;
}

static cbm_daemon_subscription_id_t issue_subscription_id_locked(
    cbm_daemon_coordinator_t *coordinator) {
    if (coordinator->last_subscription_id == UINT64_MAX) {
        return CBM_DAEMON_SUBSCRIPTION_ID_INVALID;
    }
    coordinator->last_subscription_id++;
    return coordinator->last_subscription_id;
}

static cbm_daemon_client_t *find_client_locked(cbm_daemon_coordinator_t *coordinator,
                                               cbm_daemon_client_id_t client_id) {
    for (cbm_daemon_client_t *client = coordinator->clients; client; client = client->next) {
        if (client->id == client_id) {
            return client;
        }
    }
    return NULL;
}

static cbm_daemon_job_t *find_job_locked(cbm_daemon_coordinator_t *coordinator,
                                         const char *project_key) {
    for (cbm_daemon_job_t *job = coordinator->jobs; job; job = job->next) {
        if (strcmp(job->project_key, project_key) == 0) {
            return job;
        }
    }
    return NULL;
}

static cbm_daemon_watch_t *find_watch_locked(cbm_daemon_coordinator_t *coordinator,
                                             const char *project_key) {
    for (cbm_daemon_watch_t *watch = coordinator->watches; watch; watch = watch->next) {
        if (strcmp(watch->project_key, project_key) == 0) {
            return watch;
        }
    }
    return NULL;
}

static bool remove_subscription_locked(cbm_daemon_subscription_t **subscriptions,
                                       size_t *subscription_count, cbm_daemon_client_id_t client_id,
                                       cbm_daemon_subscription_id_t subscription_id) {
    cbm_daemon_subscription_t **cursor = subscriptions;
    while (*cursor) {
        cbm_daemon_subscription_t *subscription = *cursor;
        if (subscription->id == subscription_id && subscription->client_id == client_id) {
            *cursor = subscription->next;
            free(subscription);
            (*subscription_count)--;
            return true;
        }
        cursor = &subscription->next;
    }
    return false;
}

static void remove_client_subscriptions_locked(cbm_daemon_subscription_t **subscriptions,
                                               size_t *subscription_count,
                                               cbm_daemon_client_id_t client_id) {
    cbm_daemon_subscription_t **cursor = subscriptions;
    while (*cursor) {
        cbm_daemon_subscription_t *subscription = *cursor;
        if (subscription->client_id == client_id) {
            *cursor = subscription->next;
            free(subscription);
            (*subscription_count)--;
        } else {
            cursor = &subscription->next;
        }
    }
}

static void callback_batch_init_locked(cbm_daemon_coordinator_t *coordinator,
                                       cbm_daemon_callback_batch_t *batch) {
    memset(batch, 0, sizeof(*batch));
    batch->cancel_job = coordinator->hooks.cancel_job;
    batch->release_watch = coordinator->hooks.release_watch;
    batch->context = coordinator->hooks.context;
}

static void request_job_cancel_locked(cbm_daemon_coordinator_t *coordinator, cbm_daemon_job_t *job,
                                      cbm_daemon_callback_batch_t *batch) {
    if (job->subscription_count != 0 || job->state != CBM_DAEMON_JOB_RUNNING) {
        return;
    }
    job->state = CBM_DAEMON_JOB_CANCEL_REQUESTED;
    if (batch->cancel_job) {
        job->cancel_callback_inflight = true;
        job->action_next = batch->jobs;
        batch->jobs = job;
        coordinator->callback_count++;
    }
}

static void queue_watch_release_locked(cbm_daemon_coordinator_t *coordinator,
                                       cbm_daemon_watch_t *watch,
                                       cbm_daemon_callback_batch_t *batch) {
    watch->action_next = batch->watches;
    batch->watches = watch;
    if (batch->release_watch) {
        coordinator->callback_count++;
    }
}

static void callback_batch_run(cbm_daemon_coordinator_t *coordinator,
                               cbm_daemon_callback_batch_t *batch) {
    cbm_daemon_job_t *job = batch->jobs;
    while (job) {
        cbm_daemon_job_t *next = job->action_next;
        batch->cancel_job(job->project_key, batch->context);

        cbm_mutex_lock(&coordinator->mutex);
        coordinator->callback_count--;
        job->cancel_callback_inflight = false;
        bool detached = job->detached;
        cbm_mutex_unlock(&coordinator->mutex);
        if (detached) {
            free_job(job);
        }
        job = next;
    }

    cbm_daemon_watch_t *watch = batch->watches;
    while (watch) {
        cbm_daemon_watch_t *next = watch->action_next;
        if (batch->release_watch) {
            batch->release_watch(watch->project_key, batch->context);
            cbm_mutex_lock(&coordinator->mutex);
            coordinator->callback_count--;
            cbm_mutex_unlock(&coordinator->mutex);
        }
        free_watch(watch);
        watch = next;
    }
}

static void release_client_resources_locked(cbm_daemon_coordinator_t *coordinator,
                                            cbm_daemon_client_id_t client_id,
                                            cbm_daemon_callback_batch_t *batch) {
    for (cbm_daemon_job_t *job = coordinator->jobs; job; job = job->next) {
        remove_client_subscriptions_locked(&job->subscriptions, &job->subscription_count,
                                           client_id);
        request_job_cancel_locked(coordinator, job, batch);
    }

    cbm_daemon_watch_t **watch_cursor = &coordinator->watches;
    while (*watch_cursor) {
        cbm_daemon_watch_t *watch = *watch_cursor;
        remove_client_subscriptions_locked(&watch->subscriptions, &watch->subscription_count,
                                           client_id);
        if (watch->subscription_count == 0) {
            *watch_cursor = watch->next;
            watch->next = NULL;
            coordinator->watch_count--;
            queue_watch_release_locked(coordinator, watch, batch);
        } else {
            watch_cursor = &watch->next;
        }
    }
}

static void release_client_locked(cbm_daemon_coordinator_t *coordinator,
                                  cbm_daemon_client_t *client, cbm_daemon_callback_batch_t *batch) {
    release_client_resources_locked(coordinator, client->id, batch);
    free(client);
    coordinator->client_count--;
    if (coordinator->client_count == 0 && !coordinator->permanent) {
        coordinator->state = CBM_DAEMON_COORDINATOR_STOPPING;
    }
}

static bool terminal_job_locked(cbm_daemon_coordinator_t *coordinator, const char *project_key,
                                bool require_cancellation, cbm_daemon_job_t **free_after_unlock) {
    cbm_daemon_job_t **cursor = &coordinator->jobs;
    while (*cursor && strcmp((*cursor)->project_key, project_key) != 0) {
        cursor = &(*cursor)->next;
    }
    if (!*cursor || (require_cancellation && (*cursor)->state == CBM_DAEMON_JOB_RUNNING)) {
        return false;
    }

    cbm_daemon_job_t *job = *cursor;
    *cursor = job->next;
    job->next = NULL;
    job->detached = true;
    coordinator->job_count--;
    free_subscriptions(job->subscriptions);
    job->subscriptions = NULL;
    job->subscription_count = 0;
    if (!job->cancel_callback_inflight) {
        *free_after_unlock = job;
    }
    return true;
}

void cbm_daemon_coordinator_set_permanent(cbm_daemon_coordinator_t *coordinator, bool permanent) {
    if (!coordinator) {
        return;
    }
    cbm_mutex_lock(&coordinator->mutex);
    coordinator->permanent = permanent;
    cbm_mutex_unlock(&coordinator->mutex);
}

cbm_daemon_coordinator_t *cbm_daemon_coordinator_new(uint64_t lease_timeout_ms) {
    cbm_daemon_coordinator_t *coordinator = calloc(1, sizeof(*coordinator));
    if (!coordinator) {
        return NULL;
    }
    cbm_mutex_init(&coordinator->mutex);
    coordinator->lease_timeout_ms = lease_timeout_ms;
    coordinator->state = CBM_DAEMON_COORDINATOR_RUNNING;
    return coordinator;
}

void cbm_daemon_coordinator_free(cbm_daemon_coordinator_t *coordinator) {
    if (!coordinator) {
        return;
    }

    cbm_daemon_client_t *client = coordinator->clients;
    while (client) {
        cbm_daemon_client_t *next = client->next;
        free(client);
        client = next;
    }

    cbm_daemon_job_t *job = coordinator->jobs;
    while (job) {
        cbm_daemon_job_t *next = job->next;
        free_job(job);
        job = next;
    }

    cbm_daemon_watch_t *watch = coordinator->watches;
    while (watch) {
        cbm_daemon_watch_t *next = watch->next;
        free_watch(watch);
        watch = next;
    }
    cbm_mutex_destroy(&coordinator->mutex);
    free(coordinator);
}

bool cbm_daemon_coordinator_set_hooks(cbm_daemon_coordinator_t *coordinator,
                                      const cbm_daemon_coordinator_hooks_t *hooks) {
    if (!coordinator || !hooks) {
        return false;
    }
    cbm_mutex_lock(&coordinator->mutex);
    coordinator->hooks = *hooks;
    cbm_mutex_unlock(&coordinator->mutex);
    return true;
}

cbm_daemon_coordinator_state_t cbm_daemon_coordinator_state(cbm_daemon_coordinator_t *coordinator) {
    if (!coordinator) {
        return CBM_DAEMON_COORDINATOR_STOPPING;
    }
    cbm_mutex_lock(&coordinator->mutex);
    cbm_daemon_coordinator_state_t state = coordinator->state;
    cbm_mutex_unlock(&coordinator->mutex);
    return state;
}

cbm_daemon_client_id_t cbm_daemon_client_connected(cbm_daemon_coordinator_t *coordinator,
                                                   uint64_t now_ms) {
    if (!coordinator) {
        return CBM_DAEMON_CLIENT_ID_INVALID;
    }

    cbm_daemon_client_t *client = malloc(sizeof(*client));
    if (!client) {
        return CBM_DAEMON_CLIENT_ID_INVALID;
    }

    cbm_mutex_lock(&coordinator->mutex);
    if (coordinator->state != CBM_DAEMON_COORDINATOR_RUNNING) {
        cbm_mutex_unlock(&coordinator->mutex);
        free(client);
        return CBM_DAEMON_CLIENT_ID_INVALID;
    }
    cbm_daemon_client_id_t client_id = issue_client_id_locked(coordinator);
    if (client_id == CBM_DAEMON_CLIENT_ID_INVALID) {
        cbm_mutex_unlock(&coordinator->mutex);
        free(client);
        return CBM_DAEMON_CLIENT_ID_INVALID;
    }
    client->id = client_id;
    client->last_heartbeat_ms = now_ms;
    client->next = coordinator->clients;
    coordinator->clients = client;
    coordinator->client_count++;
    cbm_mutex_unlock(&coordinator->mutex);
    return client_id;
}

bool cbm_daemon_client_disconnected(cbm_daemon_coordinator_t *coordinator,
                                    cbm_daemon_client_id_t client_id, uint64_t now_ms) {
    (void)now_ms;
    if (!coordinator || client_id == CBM_DAEMON_CLIENT_ID_INVALID) {
        return false;
    }

    cbm_daemon_callback_batch_t batch;
    cbm_mutex_lock(&coordinator->mutex);
    callback_batch_init_locked(coordinator, &batch);
    cbm_daemon_client_t **cursor = &coordinator->clients;
    while (*cursor && (*cursor)->id != client_id) {
        cursor = &(*cursor)->next;
    }
    if (!*cursor) {
        cbm_mutex_unlock(&coordinator->mutex);
        return false;
    }
    cbm_daemon_client_t *client = *cursor;
    *cursor = client->next;
    release_client_locked(coordinator, client, &batch);
    cbm_mutex_unlock(&coordinator->mutex);

    callback_batch_run(coordinator, &batch);
    return true;
}

bool cbm_daemon_client_heartbeat(cbm_daemon_coordinator_t *coordinator,
                                 cbm_daemon_client_id_t client_id, uint64_t now_ms) {
    if (!coordinator || client_id == CBM_DAEMON_CLIENT_ID_INVALID) {
        return false;
    }
    cbm_mutex_lock(&coordinator->mutex);
    cbm_daemon_client_t *client = find_client_locked(coordinator, client_id);
    bool found = client != NULL;
    if (client && now_ms > client->last_heartbeat_ms) {
        client->last_heartbeat_ms = now_ms;
    }
    cbm_mutex_unlock(&coordinator->mutex);
    return found;
}

size_t cbm_daemon_expire_leases(cbm_daemon_coordinator_t *coordinator, uint64_t now_ms) {
    if (!coordinator) {
        return 0;
    }

    size_t expired_count = 0;
    cbm_daemon_callback_batch_t batch;
    cbm_mutex_lock(&coordinator->mutex);
    callback_batch_init_locked(coordinator, &batch);
    cbm_daemon_client_t **cursor = &coordinator->clients;
    while (*cursor) {
        cbm_daemon_client_t *client = *cursor;
        bool expired = now_ms >= client->last_heartbeat_ms &&
                       now_ms - client->last_heartbeat_ms >= coordinator->lease_timeout_ms;
        if (!expired) {
            cursor = &client->next;
            continue;
        }
        *cursor = client->next;
        release_client_locked(coordinator, client, &batch);
        expired_count++;
    }
    cbm_mutex_unlock(&coordinator->mutex);

    callback_batch_run(coordinator, &batch);
    return expired_count;
}

size_t cbm_daemon_active_clients(cbm_daemon_coordinator_t *coordinator) {
    if (!coordinator) {
        return 0;
    }
    cbm_mutex_lock(&coordinator->mutex);
    size_t count = coordinator->client_count;
    cbm_mutex_unlock(&coordinator->mutex);
    return count;
}

cbm_daemon_subscription_result_t cbm_daemon_job_subscribe(
    cbm_daemon_coordinator_t *coordinator, cbm_daemon_client_id_t client_id,
    const char *project_key, cbm_daemon_subscription_id_t *subscription_id) {
    if (subscription_id) {
        *subscription_id = CBM_DAEMON_SUBSCRIPTION_ID_INVALID;
    }
    if (!coordinator || !subscription_id || !project_key || project_key[0] == '\0' ||
        client_id == CBM_DAEMON_CLIENT_ID_INVALID) {
        return CBM_DAEMON_SUBSCRIPTION_REJECTED;
    }

    cbm_mutex_lock(&coordinator->mutex);
    if (coordinator->state != CBM_DAEMON_COORDINATOR_RUNNING ||
        !find_client_locked(coordinator, client_id)) {
        cbm_mutex_unlock(&coordinator->mutex);
        return CBM_DAEMON_SUBSCRIPTION_REJECTED;
    }
    cbm_daemon_job_t *job = find_job_locked(coordinator, project_key);
    if (job && job->state != CBM_DAEMON_JOB_RUNNING) {
        cbm_mutex_unlock(&coordinator->mutex);
        return CBM_DAEMON_SUBSCRIPTION_REJECTED;
    }

    bool started = job == NULL;
    cbm_daemon_job_t *new_job = NULL;
    char *key_copy = NULL;
    cbm_daemon_subscription_t *subscription = malloc(sizeof(*subscription));
    if (started) {
        new_job = calloc(1, sizeof(*new_job));
        key_copy = daemon_string_dup(project_key);
    }
    if (!subscription || (started && (!new_job || !key_copy))) {
        free(subscription);
        free(new_job);
        free(key_copy);
        cbm_mutex_unlock(&coordinator->mutex);
        return CBM_DAEMON_SUBSCRIPTION_REJECTED;
    }

    cbm_daemon_subscription_id_t id = issue_subscription_id_locked(coordinator);
    if (id == CBM_DAEMON_SUBSCRIPTION_ID_INVALID) {
        free(subscription);
        free(new_job);
        free(key_copy);
        cbm_mutex_unlock(&coordinator->mutex);
        return CBM_DAEMON_SUBSCRIPTION_REJECTED;
    }
    if (started) {
        new_job->project_key = key_copy;
        new_job->state = CBM_DAEMON_JOB_RUNNING;
        new_job->next = coordinator->jobs;
        coordinator->jobs = new_job;
        coordinator->job_count++;
        job = new_job;
    }
    subscription->id = id;
    subscription->client_id = client_id;
    subscription->next = job->subscriptions;
    job->subscriptions = subscription;
    job->subscription_count++;
    *subscription_id = id;
    cbm_mutex_unlock(&coordinator->mutex);
    return started ? CBM_DAEMON_SUBSCRIPTION_STARTED : CBM_DAEMON_SUBSCRIPTION_JOINED;
}

cbm_daemon_subscription_result_t cbm_daemon_watch_subscribe(
    cbm_daemon_coordinator_t *coordinator, cbm_daemon_client_id_t client_id,
    const char *project_key, cbm_daemon_subscription_id_t *subscription_id) {
    if (subscription_id) {
        *subscription_id = CBM_DAEMON_SUBSCRIPTION_ID_INVALID;
    }
    if (!coordinator || !subscription_id || !project_key || project_key[0] == '\0' ||
        client_id == CBM_DAEMON_CLIENT_ID_INVALID) {
        return CBM_DAEMON_SUBSCRIPTION_REJECTED;
    }

    cbm_mutex_lock(&coordinator->mutex);
    if (coordinator->state != CBM_DAEMON_COORDINATOR_RUNNING ||
        !find_client_locked(coordinator, client_id)) {
        cbm_mutex_unlock(&coordinator->mutex);
        return CBM_DAEMON_SUBSCRIPTION_REJECTED;
    }
    cbm_daemon_watch_t *watch = find_watch_locked(coordinator, project_key);
    bool started = watch == NULL;
    cbm_daemon_watch_t *new_watch = NULL;
    char *key_copy = NULL;
    cbm_daemon_subscription_t *subscription = malloc(sizeof(*subscription));
    if (started) {
        new_watch = calloc(1, sizeof(*new_watch));
        key_copy = daemon_string_dup(project_key);
    }
    if (!subscription || (started && (!new_watch || !key_copy))) {
        free(subscription);
        free(new_watch);
        free(key_copy);
        cbm_mutex_unlock(&coordinator->mutex);
        return CBM_DAEMON_SUBSCRIPTION_REJECTED;
    }

    cbm_daemon_subscription_id_t id = issue_subscription_id_locked(coordinator);
    if (id == CBM_DAEMON_SUBSCRIPTION_ID_INVALID) {
        free(subscription);
        free(new_watch);
        free(key_copy);
        cbm_mutex_unlock(&coordinator->mutex);
        return CBM_DAEMON_SUBSCRIPTION_REJECTED;
    }
    if (started) {
        new_watch->project_key = key_copy;
        new_watch->next = coordinator->watches;
        coordinator->watches = new_watch;
        coordinator->watch_count++;
        watch = new_watch;
    }
    subscription->id = id;
    subscription->client_id = client_id;
    subscription->next = watch->subscriptions;
    watch->subscriptions = subscription;
    watch->subscription_count++;
    *subscription_id = id;
    cbm_mutex_unlock(&coordinator->mutex);
    return started ? CBM_DAEMON_SUBSCRIPTION_STARTED : CBM_DAEMON_SUBSCRIPTION_JOINED;
}

bool cbm_daemon_job_unsubscribe(cbm_daemon_coordinator_t *coordinator,
                                cbm_daemon_client_id_t client_id,
                                cbm_daemon_subscription_id_t subscription_id) {
    if (!coordinator || client_id == CBM_DAEMON_CLIENT_ID_INVALID ||
        subscription_id == CBM_DAEMON_SUBSCRIPTION_ID_INVALID) {
        return false;
    }

    cbm_daemon_callback_batch_t batch;
    cbm_mutex_lock(&coordinator->mutex);
    callback_batch_init_locked(coordinator, &batch);
    if (!find_client_locked(coordinator, client_id)) {
        cbm_mutex_unlock(&coordinator->mutex);
        return false;
    }
    bool removed = false;
    for (cbm_daemon_job_t *job = coordinator->jobs; job; job = job->next) {
        removed = remove_subscription_locked(&job->subscriptions, &job->subscription_count,
                                             client_id, subscription_id);
        if (removed) {
            request_job_cancel_locked(coordinator, job, &batch);
            break;
        }
    }
    cbm_mutex_unlock(&coordinator->mutex);
    callback_batch_run(coordinator, &batch);
    return removed;
}

bool cbm_daemon_watch_unsubscribe(cbm_daemon_coordinator_t *coordinator,
                                  cbm_daemon_client_id_t client_id,
                                  cbm_daemon_subscription_id_t subscription_id) {
    if (!coordinator || client_id == CBM_DAEMON_CLIENT_ID_INVALID ||
        subscription_id == CBM_DAEMON_SUBSCRIPTION_ID_INVALID) {
        return false;
    }

    cbm_daemon_callback_batch_t batch;
    cbm_mutex_lock(&coordinator->mutex);
    callback_batch_init_locked(coordinator, &batch);
    if (!find_client_locked(coordinator, client_id)) {
        cbm_mutex_unlock(&coordinator->mutex);
        return false;
    }
    bool removed = false;
    cbm_daemon_watch_t **cursor = &coordinator->watches;
    while (*cursor) {
        cbm_daemon_watch_t *watch = *cursor;
        removed = remove_subscription_locked(&watch->subscriptions, &watch->subscription_count,
                                             client_id, subscription_id);
        if (!removed) {
            cursor = &watch->next;
            continue;
        }
        if (watch->subscription_count == 0) {
            *cursor = watch->next;
            watch->next = NULL;
            coordinator->watch_count--;
            queue_watch_release_locked(coordinator, watch, &batch);
        }
        break;
    }
    cbm_mutex_unlock(&coordinator->mutex);
    callback_batch_run(coordinator, &batch);
    return removed;
}

size_t cbm_daemon_job_subscribers(cbm_daemon_coordinator_t *coordinator, const char *project_key) {
    if (!coordinator || !project_key) {
        return 0;
    }
    cbm_mutex_lock(&coordinator->mutex);
    cbm_daemon_job_t *job = find_job_locked(coordinator, project_key);
    size_t count = job ? job->subscription_count : 0;
    cbm_mutex_unlock(&coordinator->mutex);
    return count;
}

size_t cbm_daemon_watch_subscribers(cbm_daemon_coordinator_t *coordinator,
                                    const char *project_key) {
    if (!coordinator || !project_key) {
        return 0;
    }
    cbm_mutex_lock(&coordinator->mutex);
    cbm_daemon_watch_t *watch = find_watch_locked(coordinator, project_key);
    size_t count = watch ? watch->subscription_count : 0;
    cbm_mutex_unlock(&coordinator->mutex);
    return count;
}

size_t cbm_daemon_active_jobs(cbm_daemon_coordinator_t *coordinator) {
    if (!coordinator) {
        return 0;
    }
    cbm_mutex_lock(&coordinator->mutex);
    size_t count = coordinator->job_count;
    cbm_mutex_unlock(&coordinator->mutex);
    return count;
}

size_t cbm_daemon_active_watches(cbm_daemon_coordinator_t *coordinator) {
    if (!coordinator) {
        return 0;
    }
    cbm_mutex_lock(&coordinator->mutex);
    size_t count = coordinator->watch_count;
    cbm_mutex_unlock(&coordinator->mutex);
    return count;
}

cbm_daemon_job_state_t cbm_daemon_job_state(cbm_daemon_coordinator_t *coordinator,
                                            const char *project_key) {
    if (!coordinator || !project_key) {
        return CBM_DAEMON_JOB_NONE;
    }
    cbm_mutex_lock(&coordinator->mutex);
    cbm_daemon_job_t *job = find_job_locked(coordinator, project_key);
    cbm_daemon_job_state_t state = job ? job->state : CBM_DAEMON_JOB_NONE;
    cbm_mutex_unlock(&coordinator->mutex);
    return state;
}

bool cbm_daemon_job_reaping(cbm_daemon_coordinator_t *coordinator, const char *project_key) {
    if (!coordinator || !project_key) {
        return false;
    }
    cbm_mutex_lock(&coordinator->mutex);
    cbm_daemon_job_t *job = find_job_locked(coordinator, project_key);
    bool transitioned = job && job->state == CBM_DAEMON_JOB_CANCEL_REQUESTED;
    if (transitioned) {
        job->state = CBM_DAEMON_JOB_REAPING;
    }
    cbm_mutex_unlock(&coordinator->mutex);
    return transitioned;
}

bool cbm_daemon_job_reaped(cbm_daemon_coordinator_t *coordinator, const char *project_key,
                           uint64_t now_ms) {
    (void)now_ms;
    if (!coordinator || !project_key) {
        return false;
    }
    cbm_daemon_job_t *free_after_unlock = NULL;
    cbm_mutex_lock(&coordinator->mutex);
    bool removed = terminal_job_locked(coordinator, project_key, true, &free_after_unlock);
    cbm_mutex_unlock(&coordinator->mutex);
    free_job(free_after_unlock);
    return removed;
}

bool cbm_daemon_job_completed(cbm_daemon_coordinator_t *coordinator, const char *project_key,
                              uint64_t now_ms) {
    (void)now_ms;
    if (!coordinator || !project_key) {
        return false;
    }
    cbm_daemon_job_t *free_after_unlock = NULL;
    cbm_mutex_lock(&coordinator->mutex);
    bool removed = terminal_job_locked(coordinator, project_key, false, &free_after_unlock);
    cbm_mutex_unlock(&coordinator->mutex);
    free_job(free_after_unlock);
    return removed;
}

bool cbm_daemon_should_exit(cbm_daemon_coordinator_t *coordinator, uint64_t now_ms) {
    (void)now_ms;
    if (!coordinator) {
        return false;
    }
    cbm_mutex_lock(&coordinator->mutex);
    bool should_exit = coordinator->state == CBM_DAEMON_COORDINATOR_STOPPING &&
                       coordinator->client_count == 0 && coordinator->job_count == 0 &&
                       coordinator->watch_count == 0 && coordinator->callback_count == 0;
    cbm_mutex_unlock(&coordinator->mutex);
    return should_exit;
}

bool cbm_daemon_frame_header_encode(uint8_t header[CBM_DAEMON_FRAME_HEADER_SIZE],
                                    cbm_daemon_frame_type_t type, uint16_t flags, uint32_t length) {
    if (!header || !frame_type_valid(type) || length > CBM_DAEMON_MAX_FRAME_SIZE) {
        return false;
    }
    header[FRAME_MAGIC_0] = 'C';
    header[FRAME_MAGIC_1] = 'B';
    header[FRAME_MAGIC_2] = 'M';
    header[FRAME_MAGIC_3] = 'D';
    header[FRAME_VERSION] = CBM_DAEMON_RENDEZVOUS_FRAME_VERSION;
    header[FRAME_TYPE] = (uint8_t)type;
    header[FRAME_FLAGS_HI] = (uint8_t)(flags >> 8);
    header[FRAME_FLAGS_LO] = (uint8_t)flags;
    header[FRAME_LENGTH_3] = (uint8_t)(length >> 24);
    header[FRAME_LENGTH_2] = (uint8_t)(length >> 16);
    header[FRAME_LENGTH_1] = (uint8_t)(length >> 8);
    header[FRAME_LENGTH_0] = (uint8_t)length;
    return true;
}

bool cbm_daemon_frame_header_decode(const uint8_t header[CBM_DAEMON_FRAME_HEADER_SIZE],
                                    cbm_daemon_frame_t *frame) {
    if (!header || !frame || header[FRAME_MAGIC_0] != 'C' || header[FRAME_MAGIC_1] != 'B' ||
        header[FRAME_MAGIC_2] != 'M' || header[FRAME_MAGIC_3] != 'D' ||
        header[FRAME_VERSION] != CBM_DAEMON_RENDEZVOUS_FRAME_VERSION) {
        return false;
    }

    cbm_daemon_frame_type_t type = (cbm_daemon_frame_type_t)header[FRAME_TYPE];
    uint32_t length = ((uint32_t)header[FRAME_LENGTH_3] << 24) |
                      ((uint32_t)header[FRAME_LENGTH_2] << 16) |
                      ((uint32_t)header[FRAME_LENGTH_1] << 8) | (uint32_t)header[FRAME_LENGTH_0];
    if (!frame_type_valid(type) || length > CBM_DAEMON_MAX_FRAME_SIZE) {
        return false;
    }

    frame->type = type;
    frame->flags =
        (uint16_t)(((uint16_t)header[FRAME_FLAGS_HI] << 8) | (uint16_t)header[FRAME_FLAGS_LO]);
    frame->length = length;
    return true;
}
