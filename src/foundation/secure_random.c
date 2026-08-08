#include "foundation/secure_random.h"

#include <stdint.h>

void cbm_secure_zero(void *buffer, size_t length) {
    volatile uint8_t *cursor = buffer;
    while (cursor && length > 0) {
        *cursor++ = 0;
        length--;
    }
}

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <bcrypt.h>

bool cbm_secure_random(void *buffer, size_t length) {
    if (!buffer && length != 0) {
        return false;
    }
    uint8_t *cursor = buffer;
    while (length > 0) {
        ULONG chunk = length > UINT32_MAX ? UINT32_MAX : (ULONG)length;
        if (BCryptGenRandom(NULL, cursor, chunk, BCRYPT_USE_SYSTEM_PREFERRED_RNG) < 0) {
            return false;
        }
        cursor += chunk;
        length -= chunk;
    }
    return true;
}

#else

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

bool cbm_secure_random(void *buffer, size_t length) {
    if (!buffer && length != 0) {
        return false;
    }
    int flags = O_RDONLY;
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
    int fd;
    do {
        fd = open("/dev/urandom", flags);
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) {
        return false;
    }

    uint8_t *cursor = buffer;
    bool ok = true;
    while (length > 0) {
        ssize_t count = read(fd, cursor, length);
        if (count > 0) {
            cursor += (size_t)count;
            length -= (size_t)count;
        } else if (count < 0 && errno == EINTR) {
            continue;
        } else {
            ok = false;
            break;
        }
    }
    /* Do not retry close(2) after EINTR: on platforms that have already
     * released the descriptor, a retry could close an unrelated fd reused by
     * another thread. The random bytes are valid once the read completed. */
    int close_result = close(fd);
    return ok && (close_result == 0 || errno == EINTR);
}

#endif
