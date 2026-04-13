// Copyright (c) 2004-2013 Sergey Lyubka
// Copyright (c) 2013-2025 Cesanta Software Limited
// All rights reserved
//
// This software is dual-licensed: you can redistribute it and/or modify
// it under the terms of the GNU General Public License version 2 as
// published by the Free Software Foundation. For the terms of this
// license, see http://www.gnu.org/licenses/
//
// You are free to use this software under the terms of the GNU General
// Public License, but WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
// See the GNU General Public License for more details.
//
// Alternatively, you can license this software under a commercial
// license, as set out in https://www.mongoose.ws/licensing/
//
// SPDX-License-Identifier: GPL-2.0-only or commercial

#ifndef MONGOOSE_H
#define MONGOOSE_H

#define MG_VERSION "7.21"

#ifdef __cplusplus
extern "C" {
#endif


#define MG_ARCH_CUSTOM 0        // User creates its own mongoose_config.h
#define MG_ARCH_UNIX 1          // Linux, BSD, Mac, ...
#define MG_ARCH_WIN32 2         // Windows
#define MG_ARCH_ESP32 3         // ESP32
#define MG_ARCH_ESP8266 4       // ESP8266
#define MG_ARCH_FREERTOS 5      // FreeRTOS
#define MG_ARCH_THREADX 6       // Eclipse ThreadX (former MS Azure RTOS)
#define MG_ARCH_ZEPHYR 7        // Zephyr RTOS
#define MG_ARCH_ARMGCC 8        // Plain ARM GCC
#define MG_ARCH_CMSIS_RTOS1 9   // CMSIS-RTOS API v1 (Keil RTX)
#define MG_ARCH_TIRTOS 10       // Texas Semi TI-RTOS
#define MG_ARCH_PICOSDK 11      // Raspberry Pi Pico-SDK (RP2040, RP2350)
#define MG_ARCH_ARMCC 12        // Keil MDK-Core with Configuration Wizard
#define MG_ARCH_CMSIS_RTOS2 13  // CMSIS-RTOS API v2 (Keil RTX5, FreeRTOS)
#define MG_ARCH_RTTHREAD 14     // RT-Thread RTOS
#define MG_ARCH_ARMCGT 15       // Texas Semi ARM-CGT
#define MG_ARCH_CUBE 16	        // STM32Cube environment

#define MG_ARCH_NEWLIB MG_ARCH_ARMGCC  // Alias, deprecate in 2025

#if !defined(MG_ARCH)
#if defined(__unix__) || defined(__APPLE__)
#define MG_ARCH MG_ARCH_UNIX
#elif defined(_WIN32)
#define MG_ARCH MG_ARCH_WIN32
#endif
#endif  // !defined(MG_ARCH)

#if !defined(MG_ARCH) || (MG_ARCH == MG_ARCH_CUSTOM)
#include "mongoose_config.h"  // keep this include
#endif

#if !defined(MG_ARCH)
#error "MG_ARCH is not specified and we couldn't guess it. Define MG_ARCH=... in mongoose_config.h"
#endif

// http://esr.ibiblio.org/?p=5095
#define MG_BIG_ENDIAN (*(uint16_t *) "\0\xff" < 0x100)

















#if MG_ARCH == MG_ARCH_ARMCGT

#include <ctype.h>
#include <errno.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>

#define MG_PATH_MAX 100
#define MG_ENABLE_SOCKET 0
#define MG_ENABLE_DIRLIST 0

#endif


#if MG_ARCH == MG_ARCH_ARMGCC
#define _POSIX_TIMERS

#include <ctype.h>
#include <errno.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define MG_PATH_MAX 100
#define MG_ENABLE_SOCKET 0
#define MG_ENABLE_DIRLIST 0

#endif


#if MG_ARCH == MG_ARCH_CUBE

#include <ctype.h>
#include <errno.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

// Cube-generated header, includes ST Cube HAL
// NOTE: use angle brackets to prevent amalgamator ditching it
#include <main.h>

#ifndef MG_PATH_MAX
#define MG_PATH_MAX 100
#endif

#ifndef MG_ENABLE_DIRLIST
#define MG_ENABLE_DIRLIST 0
#endif

#ifndef MG_ENABLE_SOCKET
#define MG_ENABLE_SOCKET 0
#endif

#ifndef MG_ENABLE_TCPIP
#define MG_ENABLE_TCPIP 1  // Enable built-in TCP/IP stack
#endif

#if MG_ENABLE_TCPIP && !defined(MG_ENABLE_DRIVER_STM32F) && \
    !defined(MG_ENABLE_DRIVER_STM32H) && !defined(MG_ENABLE_DRIVER_STM32N)
#if defined(STM32F1) || defined(STM32F2) || defined(STM32F4) || defined(STM32F7)
#define MG_ENABLE_DRIVER_STM32F 1
#elif defined(STM32H5) || defined(STM32H7)
#define MG_ENABLE_DRIVER_STM32H 1
#elif defined(STM32N6)
#define MG_ENABLE_DRIVER_STM32N 1
#else
#error Select a driver in mongoose_config.h
#endif
#endif

#ifndef MG_TLS
#define MG_TLS MG_TLS_BUILTIN
#endif

#if !defined(MG_OTA) && defined(STM32F1) || defined(STM32F2) || \
    defined(STM32F4) || defined(STM32F7)
#define MG_OTA MG_OTA_STM32F
#elif !defined(MG_OTA) && defined(STM32H5)
#define MG_OTA MG_OTA_STM32H5
#elif !defined(MG_OTA) && defined(STM32H7)
#define MG_OTA MG_OTA_STM32H7
#endif
// use HAL-defined execute-in-ram section
#define MG_IRAM __attribute__((section(".RamFunc")))

#ifndef HAL_ICACHE_MODULE_ENABLED
#define HAL_ICACHE_IsEnabled() 0
#define HAL_ICACHE_Enable() (void) 0
#define HAL_ICACHE_Disable() (void) 0
#endif

#ifndef MG_SET_MAC_ADDRESS
// Construct MAC address from UUID
#define MGUID ((uint32_t *) UID_BASE)  // Unique 96-bit chip ID
#define MG_SET_MAC_ADDRESS(mac)                                   \
  do {                                                            \
    int icache_enabled_ = HAL_ICACHE_IsEnabled();                 \
    if (icache_enabled_) HAL_ICACHE_Disable();                    \
    mac[0] = 42;                                                  \
    mac[1] = ((MGUID[0] >> 0) & 255) ^ ((MGUID[2] >> 19) & 255);  \
    mac[2] = ((MGUID[0] >> 10) & 255) ^ ((MGUID[1] >> 10) & 255); \
    mac[3] = (MGUID[0] >> 19) & 255;                              \
    mac[4] = ((MGUID[1] >> 0) & 255) ^ ((MGUID[2] >> 10) & 255);  \
    mac[5] = ((MGUID[2] >> 0) & 255) ^ ((MGUID[1] >> 19) & 255);  \
    if (icache_enabled_) HAL_ICACHE_Enable();                     \
  } while (0)
#endif

#endif


#if MG_ARCH == MG_ARCH_ESP32

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netdb.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>

#include <esp_ota_ops.h>  // Use angle brackets to avoid
#include <esp_timer.h>    // amalgamation ditching them

#define MG_PATH_MAX 128

#ifndef MG_ENABLE_POSIX_FS
#define MG_ENABLE_POSIX_FS 1
#endif

#ifndef MG_ENABLE_DIRLIST
#define MG_ENABLE_DIRLIST 1
#endif

#endif


#if MG_ARCH == MG_ARCH_ESP8266

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netdb.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>

#include <esp_system.h>

#define MG_PATH_MAX 128

#endif


#if MG_ARCH == MG_ARCH_FREERTOS

#include <ctype.h>
#if !defined(MG_ENABLE_LWIP) || !MG_ENABLE_LWIP
#include <errno.h>
#endif
#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>  // rand(), strtol(), atoi()
#include <string.h>
#if defined(__ARMCC_VERSION)
#define mode_t size_t
#include <alloca.h>
#include <time.h>
#define strdup(s) ((char *) mg_strdup(mg_str(s)).buf)
#elif defined(__CCRH__)
#else
#include <sys/stat.h>
#endif

#include <FreeRTOS.h>
#include <task.h>

#define MG_ENABLE_CUSTOM_CALLOC 1

static inline void mg_free(void *ptr) {
  vPortFree(ptr);
}

// Re-route calloc/free to the FreeRTOS's functions, don't use stdlib
static inline void *mg_calloc(size_t cnt, size_t size) {
  void *p = pvPortMalloc(cnt * size);
  if (p != NULL) memset(p, 0, size * cnt);
  return p;
}

#if !defined(MG_ENABLE_POSIX_FS) || !MG_ENABLE_POSIX_FS
#else
#define mkdir(a, b) mg_mkdir(a, b)
static inline int mg_mkdir(const char *path, mode_t mode) {
  (void) path, (void) mode;
  return -1;
}
#endif

#endif  // MG_ARCH == MG_ARCH_FREERTOS


#if MG_ARCH == MG_ARCH_PICOSDK
#if !defined(MG_ENABLE_LWIP) || !MG_ENABLE_LWIP
#include <errno.h>
#endif
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <pico/stdlib.h>
#include <pico/rand.h>
int mkdir(const char *, mode_t);

#if MG_OTA == MG_OTA_PICOSDK
#include <hardware/flash.h>
#include <pico/bootrom.h>
#endif

#endif


#if MG_ARCH == MG_ARCH_RTTHREAD

#include <rtthread.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>

#ifndef MG_IO_SIZE
#define MG_IO_SIZE 1460
#endif

#endif // MG_ARCH == MG_ARCH_RTTHREAD


#if MG_ARCH == MG_ARCH_ARMCC || MG_ARCH == MG_ARCH_CMSIS_RTOS1 || \
    MG_ARCH == MG_ARCH_CMSIS_RTOS2

#include <ctype.h>
#include <errno.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <alloca.h>
#include <string.h>
#include <time.h>
#if MG_ARCH == MG_ARCH_CMSIS_RTOS1
#include "cmsis_os.h"  // keep this include
// https://developer.arm.com/documentation/ka003821/latest
extern uint32_t rt_time_get(void);
#elif MG_ARCH == MG_ARCH_CMSIS_RTOS2
#include "cmsis_os2.h"  // keep this include
#endif

#define strdup(s) ((char *) mg_strdup(mg_str(s)).buf)

#if defined(__ARMCC_VERSION)
#define mode_t size_t
#define mkdir(a, b) mg_mkdir(a, b)
static inline int mg_mkdir(const char *path, mode_t mode) {
  (void) path, (void) mode;
  return -1;
}
#endif

#if (MG_ARCH == MG_ARCH_CMSIS_RTOS1 || MG_ARCH == MG_ARCH_CMSIS_RTOS2) &&     \
    !defined MG_ENABLE_RL && (!defined(MG_ENABLE_LWIP) || !MG_ENABLE_LWIP) && \
    (!defined(MG_ENABLE_TCPIP) || !MG_ENABLE_TCPIP)
#define MG_ENABLE_RL 1
#ifndef MG_SOCK_LISTEN_BACKLOG_SIZE
#define MG_SOCK_LISTEN_BACKLOG_SIZE 3
#endif
#endif

#endif


#if MG_ARCH == MG_ARCH_THREADX

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

// Do not include time.h and stdlib.h, since they conflict with nxd_bsd.h
// extern time_t time(time_t *);
#include <nxd_bsd.h>

#define MG_DIRSEP '\\'
#undef FOPEN_MAX

#ifndef MG_PATH_MAX
#define MG_PATH_MAX 32
#endif

#ifndef MG_SOCK_LISTEN_BACKLOG_SIZE
#define MG_SOCK_LISTEN_BACKLOG_SIZE 3
#endif

#ifndef MG_ENABLE_IPV6
#define MG_ENABLE_IPV6 0
#endif

#define socklen_t int
#define closesocket(x) soc_close(x)

// In order to enable BSD support in NetxDuo, do the following (assuming Cube):
// 1. Add nxd_bsd.h and nxd_bsd.c to the repo:
//     https://github.com/eclipse-threadx/netxduo/blob/v6.1.12_rel/addons/BSD/nxd_bsd.c
//     https://github.com/eclipse-threadx/netxduo/blob/v6.1.12_rel/addons/BSD/nxd_bsd.h
// 2. Add to tx_user.h
//     #define TX_THREAD_USER_EXTENSION int bsd_errno;
// 3. Add to nx_user.h
//     #define NX_ENABLE_EXTENDED_NOTIFY_SUPPORT
// 4. Add __CCRX__ build preprocessor constant
//   Project -> Properties -> C/C++ -> Settings -> MCU Compiler -> Preprocessor

#endif


#if MG_ARCH == MG_ARCH_TIRTOS

#include <stdlib.h>
#include <ctype.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include <serrno.h>
#include <sys/socket.h>

#include <ti/sysbios/knl/Clock.h>

#endif


#if MG_ARCH == MG_ARCH_UNIX

#define _DARWIN_UNLIMITED_SELECT 1  // No limit on file descriptors

#if defined(__APPLE__)
#include <mach/mach_time.h>
#endif

#if !defined(MG_ENABLE_EPOLL) && defined(__linux__)
#define MG_ENABLE_EPOLL 1
#elif !defined(MG_ENABLE_POLL)
#define MG_ENABLE_POLL 1
#endif

#include <arpa/inet.h>
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(MG_ENABLE_EPOLL) && MG_ENABLE_EPOLL
#include <sys/epoll.h>
#elif defined(MG_ENABLE_POLL) && MG_ENABLE_POLL
#include <poll.h>
#else
#include <sys/select.h>
#endif

#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#ifndef MG_ENABLE_DIRLIST
#define MG_ENABLE_DIRLIST 1
#endif

#ifndef MG_PATH_MAX
#define MG_PATH_MAX FILENAME_MAX
#endif

#ifndef MG_ENABLE_POSIX_FS
#define MG_ENABLE_POSIX_FS 1
#endif

#ifndef MG_IO_SIZE
#define MG_IO_SIZE 16384
#endif

#endif


#if MG_ARCH == MG_ARCH_WIN32

// Avoid name clashing; (macro expansion producing 'defined' has undefined
// behaviour). See config.h for user options
#ifndef MG_ENABLE_WINSOCK
#if (!defined(MG_ENABLE_TCPIP) || !MG_ENABLE_TCPIP) && \
    (!defined(MG_ENABLE_LWIP) || !MG_ENABLE_LWIP) &&   \
    (!defined(MG_ENABLE_FREERTOS_TCP) || !MG_ENABLE_FREERTOS_TCP)
#define MG_ENABLE_WINSOCK 1
#else
#define MG_ENABLE_WINSOCK 0
#endif
#endif

#ifndef _CRT_RAND_S
#define _CRT_RAND_S
#endif

#ifndef _WIN32_WINNT
#if defined(_MSC_VER) && _MSC_VER < 1700
#define _WIN32_WINNT 0x0400 // Let vc98 pick up wincrypt.h
#else
#define _WIN32_WINNT 0x0600
#endif
#endif
#ifndef WINVER
#define WINVER _WIN32_WINNT
#endif

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#ifndef _CRT_SECURE_NO_WARNINGS
#define _CRT_SECURE_NO_WARNINGS
#endif

#ifndef _WINSOCK_DEPRECATED_NO_WARNINGS
#define _WINSOCK_DEPRECATED_NO_WARNINGS
#endif

#include <ctype.h>
#include <direct.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>

#include <winsock2.h>       // fix missing macros and types

#if defined(_MSC_VER) && _MSC_VER < 1700
#define __func__ ""
typedef __int64 int64_t;
typedef unsigned __int64 uint64_t;
typedef unsigned char uint8_t;
typedef char int8_t;
typedef unsigned short uint16_t;
typedef short int16_t;
typedef unsigned int uint32_t;
typedef int int32_t;
typedef enum { false = 0, true = 1 } bool;
#else
#include <stdbool.h>
#include <stdint.h>
#if MG_ENABLE_WINSOCK
#include <ws2tcpip.h>
#endif
#endif

#include <process.h>
#include <winerror.h>

// For mg_random()
#if defined(_MSC_VER) && _MSC_VER < 1700
#include <wincrypt.h>
#pragma comment(lib, "advapi32.lib")
#endif

#if defined(_MSC_VER) && _MSC_VER <= 1200
  #ifndef IPPROTO_IP
    #define IPPROTO_IP 0
  #endif

  #ifndef IP_ADD_MEMBERSHIP
    struct ip_mreq {
        struct in_addr imr_multiaddr;
        struct in_addr imr_interface;
    };
    #define IP_ADD_MEMBERSHIP  12
  #endif
#endif

// Protect from calls like std::snprintf in app code
// See https://github.com/cesanta/mongoose/issues/1047
#ifndef __cplusplus
#define snprintf _snprintf
#define vsnprintf _vsnprintf
#ifndef strdup  // For MSVC with _DEBUG, see #1359
#define strdup(x) _strdup(x)
#endif
#endif

#if defined(MG_ENABLE_POLL) && MG_ENABLE_POLL && (!defined(MG_ENABLE_LWIP) || !MG_ENABLE_LWIP)
typedef unsigned long nfds_t; // see #3388
#endif

#if defined(_MSC_VER)
#if MG_ENABLE_WINSOCK
#pragma comment(lib, "ws2_32.lib")
#endif
#ifndef alloca
#define alloca(a) _alloca(a)
#endif
#endif

#define MG_DIRSEP '\\'

#ifndef MG_PATH_MAX
#define MG_PATH_MAX FILENAME_MAX
#endif

#if MG_ENABLE_WINSOCK

#define MG_INVALID_SOCKET INVALID_SOCKET
#define MG_SOCKET_TYPE SOCKET
#define poll(a, b, c) WSAPoll((a), (b), (c))
#define closesocket(x) closesocket(x)
typedef int socklen_t;

#ifndef SO_EXCLUSIVEADDRUSE
#define SO_EXCLUSIVEADDRUSE ((int) (~SO_REUSEADDR))
#endif

#define MG_SOCK_ERR(errcode) ((errcode) < 0 ? WSAGetLastError() : 0)

#define MG_SOCK_PENDING(errcode)                                            \
  (((errcode) < 0) &&                                                       \
   (WSAGetLastError() == WSAEINTR || WSAGetLastError() == WSAEINPROGRESS || \
    WSAGetLastError() == WSAEWOULDBLOCK))

#define MG_SOCK_RESET(errcode) \
  (((errcode) < 0) && (WSAGetLastError() == WSAECONNRESET))

#endif  // MG_ENABLE_WINSOCK

#define realpath(a, b) _fullpath((b), (a), MG_PATH_MAX)
#define sleep(x) Sleep((x) *1000)
#define mkdir(a, b) _mkdir(a)
#define timegm(x) _mkgmtime(x)

#ifndef S_ISDIR
#define S_ISDIR(x) (((x) &_S_IFMT) == _S_IFDIR)
#endif

#ifndef MG_ENABLE_DIRLIST
#define MG_ENABLE_DIRLIST 1
#endif

#ifndef SIGPIPE
#define SIGPIPE 0
#endif

#ifndef MG_ENABLE_POSIX_FS
#define MG_ENABLE_POSIX_FS 1
#endif

#ifndef MG_IO_SIZE
#define MG_IO_SIZE 16384
#endif

#endif


#if MG_ARCH == MG_ARCH_ZEPHYR

#include <zephyr/kernel.h>
#include <zephyr/net/socket.h>
// #include <zephyr/posix/dirent.h>
#include <zephyr/posix/fcntl.h>
#include <zephyr/posix/sys/select.h>
#include <zephyr/random/random.h>
#include <zephyr/version.h>

#include <ctype.h>
#include <errno.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>

#define MG_PUTCHAR(x) printk("%c", x)
#ifndef strdup
#define strdup(s) ((char *) mg_strdup(mg_str(s)).buf)
#endif
#define strerror(x) zsock_gai_strerror(x)

#ifndef FD_CLOEXEC
#define FD_CLOEXEC 0
#endif

#ifndef F_SETFD
#define F_SETFD 0
#endif

#define MG_ENABLE_SSI 0

int rand(void);
int sscanf(const char *, const char *, ...);

#endif


#if defined(MG_ENABLE_FREERTOS_TCP) && MG_ENABLE_FREERTOS_TCP

#include <limits.h>
#include <list.h>

#include <FreeRTOS_IP.h>
#include <FreeRTOS_Sockets.h>

#define MG_SOCKET_TYPE Socket_t
#define MG_INVALID_SOCKET FREERTOS_INVALID_SOCKET

// Why FreeRTOS-TCP did not implement a clean BSD API, but its own thing
// with FreeRTOS_ prefix, is beyond me
#define IPPROTO_TCP FREERTOS_IPPROTO_TCP
#define IPPROTO_UDP FREERTOS_IPPROTO_UDP
#define AF_INET FREERTOS_AF_INET
#define SOCK_STREAM FREERTOS_SOCK_STREAM
#define SOCK_DGRAM FREERTOS_SOCK_DGRAM
#define SO_BROADCAST 0
#define SO_ERROR 0
#define SOL_SOCKET 0
#define SO_REUSEADDR 0

#define MG_SOCK_ERR(errcode) ((errcode) < 0 ? (errcode) : 0)

#define MG_SOCK_PENDING(errcode)                 \
  ((errcode) == -pdFREERTOS_ERRNO_EWOULDBLOCK || \
   (errcode) == -pdFREERTOS_ERRNO_EISCONN ||     \
   (errcode) == -pdFREERTOS_ERRNO_EINPROGRESS || \
   (errcode) == -pdFREERTOS_ERRNO_EAGAIN)

#define MG_SOCK_RESET(errcode) ((errcode) == -pdFREERTOS_ERRNO_ENOTCONN)

// actually only if optional timeout is enabled
#define MG_SOCK_INTR(fd) (fd == NULL)

#define sockaddr_in freertos_sockaddr
#define sockaddr freertos_sockaddr
#if ipFR_TCP_VERSION_MAJOR >= 4
#define sin_addr sin_address.ulIP_IPv4
#endif
#define accept(a, b, c) FreeRTOS_accept((a), (b), (c))
#define connect(a, b, c) FreeRTOS_connect((a), (b), (c))
#define bind(a, b, c) FreeRTOS_bind((a), (b), (c))
#define listen(a, b) FreeRTOS_listen((a), (b))
#define socket(a, b, c) FreeRTOS_socket((a), (b), (c))
#define send(a, b, c, d) FreeRTOS_send((a), (b), (c), (d))
#define recv(a, b, c, d) FreeRTOS_recv((a), (b), (c), (d))
#define setsockopt(a, b, c, d, e) FreeRTOS_setsockopt((a), (b), (c), (d), (e))
#define sendto(a, b, c, d, e, f) FreeRTOS_sendto((a), (b), (c), (d), (e), (f))
#define recvfrom(a, b, c, d, e, f) \
  FreeRTOS_recvfrom((a), (b), (c), (d), (e), (f))
#define closesocket(x) FreeRTOS_closesocket(x)
#define gethostbyname(x) FreeRTOS_gethostbyname(x)
#define getsockname(a, b, c) mg_getsockname((a), (b), (c))
#define getpeername(a, b, c) mg_getpeername((a), (b), (c))

static inline int mg_getsockname(MG_SOCKET_TYPE fd, void *buf, socklen_t *len) {
  (void) fd, (void) buf, (void) len;
  return -1;
}

static inline int mg_getpeername(MG_SOCKET_TYPE fd, void *buf, socklen_t *len) {
  (void) fd, (void) buf, (void) len;
  return 0;
}
#endif


#if defined(MG_ENABLE_LWIP) && MG_ENABLE_LWIP

#if defined(__GNUC__) && !defined(__ARMCC_VERSION)
#include <sys/stat.h>
#endif

struct timeval;

#include <lwip/sockets.h>

#if !LWIP_TIMEVAL_PRIVATE
#if defined(__GNUC__) && !defined(__ARMCC_VERSION) // armclang sets both
#include <sys/time.h>
#else
struct timeval {
  time_t tv_sec;
  long tv_usec;
};
#endif
#endif

#if LWIP_SOCKET != 1
// Sockets support disabled in LWIP by default
#error Set LWIP_SOCKET variable to 1 (in lwipopts.h)
#endif
#endif


#if defined(MG_ENABLE_RL) && MG_ENABLE_RL
#include <rl_net.h>

#define closesocket(x) closesocket(x)

#define TCP_NODELAY SO_KEEPALIVE

#define MG_SOCK_ERR(errcode) ((errcode) < 0 ? (errcode) : 0)

#define MG_SOCK_PENDING(errcode)                                \
  ((errcode) == BSD_EWOULDBLOCK || (errcode) == BSD_EALREADY || \
   (errcode) == BSD_EINPROGRESS)

#define MG_SOCK_RESET(errcode) \
  ((errcode) == BSD_ECONNABORTED || (errcode) == BSD_ECONNRESET)

// In blocking mode, which is enabled by default, accept() waits for a
// connection request. In non blocking mode, you must call accept()
// again if the error code BSD_EWOULDBLOCK is returned.
#define MG_SOCK_INTR(fd) (fd == BSD_EWOULDBLOCK)

#define socklen_t int
#endif


#ifndef MG_ENABLE_LOG
#define MG_ENABLE_LOG 1
#endif

#ifndef MG_ENABLE_CUSTOM_CALLOC
#define MG_ENABLE_CUSTOM_CALLOC 0
#endif

#ifndef MG_ENABLE_CUSTOM_LOG
#define MG_ENABLE_CUSTOM_LOG 0  // Let user define their own MG_LOG
#endif

#ifndef MG_ENABLE_TCPIP
#define MG_ENABLE_TCPIP 0  // Mongoose built-in network stack
#endif

#ifndef MG_ENABLE_LWIP
#define MG_ENABLE_LWIP 0  // lWIP network stack
#endif

#ifndef MG_ENABLE_FREERTOS_TCP
#define MG_ENABLE_FREERTOS_TCP 0  // Amazon FreeRTOS-TCP network stack
#endif

#ifndef MG_ENABLE_RL
#define MG_ENABLE_RL 0  // ARM MDK network stack
#endif

#ifndef MG_ENABLE_SOCKET
#define MG_ENABLE_SOCKET !MG_ENABLE_TCPIP
#endif

#ifndef MG_ENABLE_POLL
#define MG_ENABLE_POLL 0
#endif

#ifndef MG_ENABLE_EPOLL
#define MG_ENABLE_EPOLL 0
#endif

#ifndef MG_ENABLE_FATFS
#define MG_ENABLE_FATFS 0
#endif

#ifndef MG_ENABLE_SSI
#define MG_ENABLE_SSI 0
#endif

#ifndef MG_ENABLE_IPV6
#define MG_ENABLE_IPV6 0
#endif

#ifndef MG_IPV6_V6ONLY
#define MG_IPV6_V6ONLY 0  // IPv6 socket binds only to V6, not V4 address
#endif

#ifndef MG_ENABLE_MD5
#define MG_ENABLE_MD5 1
#endif

// Set MG_ENABLE_WINSOCK=0 for Win32 builds with other external IP stack not
// mentioned in arch_win32.h
#ifndef MG_ENABLE_WINSOCK
#define MG_ENABLE_WINSOCK 1
#endif

#ifndef MG_ENABLE_DIRLIST
#define MG_ENABLE_DIRLIST 0
#endif

#ifndef MG_ENABLE_CUSTOM_RANDOM
#define MG_ENABLE_CUSTOM_RANDOM 0
#endif

#ifndef MG_ENABLE_CUSTOM_MILLIS
#define MG_ENABLE_CUSTOM_MILLIS 0
#endif

#ifndef MG_ENABLE_PACKED_FS
#define MG_ENABLE_PACKED_FS 0
#endif

#ifndef MG_ENABLE_ASSERT
#define MG_ENABLE_ASSERT 0
#endif

#ifndef MG_IO_SIZE
#define MG_IO_SIZE 512  // Granularity of the send/recv IO buffer growth
#endif

#ifndef MG_MAX_RECV_SIZE
#define MG_MAX_RECV_SIZE (3UL * 1024UL * 1024UL)  // Maximum recv IO buffer size
#endif

#ifndef MG_DATA_SIZE
#define MG_DATA_SIZE 32  // struct mg_connection :: data size
#endif

#ifndef MG_MAX_HTTP_HEADERS
#define MG_MAX_HTTP_HEADERS 30
#endif

#ifndef MG_HTTP_INDEX
#define MG_HTTP_INDEX "index.html"
#endif

#ifndef MG_PATH_MAX
#ifdef PATH_MAX
#define MG_PATH_MAX PATH_MAX
#else
#define MG_PATH_MAX 128
#endif
#endif

#ifndef MG_SOCK_LISTEN_BACKLOG_SIZE
#define MG_SOCK_LISTEN_BACKLOG_SIZE 128
#endif

#ifndef MG_DIRSEP
#define MG_DIRSEP '/'
#endif

#ifndef MG_ENABLE_POSIX_FS
#define MG_ENABLE_POSIX_FS 0
#endif

#ifndef MG_INVALID_SOCKET
#define MG_INVALID_SOCKET (-1)
#endif

#ifndef MG_SOCKET_TYPE
#define MG_SOCKET_TYPE int
#endif

#ifndef MG_SOCKET_ERRNO
#define MG_SOCKET_ERRNO errno
#endif

#if MG_ENABLE_EPOLL
#define MG_EPOLL_ADD(c)                                                    \
  do {                                                                     \
    struct epoll_event ev = {EPOLLIN | EPOLLERR | EPOLLHUP, {c}};          \
    epoll_ctl(c->mgr->epoll_fd, EPOLL_CTL_ADD, (int) (size_t) c->fd, &ev); \
  } while (0)
#define MG_EPOLL_MOD(c, wr)                                                \
  do {                                                                     \
    struct epoll_event ev = {EPOLLIN | EPOLLERR | EPOLLHUP, {c}};          \
    if (wr) ev.events |= EPOLLOUT;                                         \
    epoll_ctl(c->mgr->epoll_fd, EPOLL_CTL_MOD, (int) (size_t) c->fd, &ev); \
  } while (0)
#else
#define MG_EPOLL_ADD(c)
#define MG_EPOLL_MOD(c, wr)
#endif

#ifndef MG_ENABLE_PROFILE
#define MG_ENABLE_PROFILE 0
#endif

#ifndef MG_ENABLE_TCPIP_DRIVER_INIT    // mg_mgr_init() will also initialize
#define MG_ENABLE_TCPIP_DRIVER_INIT 1  // enabled built-in driver for
#endif                                 // Mongoose built-in network stack

#ifndef MG_TCPIP_IP                      // e.g. MG_IPV4(192, 168, 0, 223)
#define MG_TCPIP_IP MG_IPV4(0, 0, 0, 0)  // Default is 0.0.0.0 (DHCP)
#endif

#ifndef MG_TCPIP_MASK
#define MG_TCPIP_MASK MG_IPV4(0, 0, 0, 0)  // Default is 0.0.0.0 (DHCP)
#endif

#ifndef MG_TCPIP_GW
#define MG_TCPIP_GW MG_IPV4(0, 0, 0, 0)  // Default is 0.0.0.0 (DHCP)
#endif

#if MG_ENABLE_IPV6

#ifndef MG_TCPIP_GLOBAL
#define MG_TCPIP_GLOBAL MG_IPV6(0, 0, 0, 0, 0, 0, 0, 0)
#endif

#ifndef MG_TCPIP_IPV6_LINKLOCAL
#define MG_TCPIP_IPV6_LINKLOCAL MG_IPV6(0, 0, 0, 0, 0, 0, 0, 0)
#endif

#ifndef MG_TCPIP_PREFIX_LEN
#define MG_TCPIP_PREFIX_LEN 0
#endif

#ifndef MG_TCPIP_GW6
#define MG_TCPIP_GW6 MG_IPV6(0, 0, 0, 0, 0, 0, 0, 0)
#endif

#endif

#ifndef MG_SET_MAC_ADDRESS
#define MG_SET_MAC_ADDRESS(mac)
#endif

#ifndef MG_TCPIP_DHCPNAME_SIZE
#define MG_TCPIP_DHCPNAME_SIZE 18  // struct mg_tcpip_if :: dhcp_name size
#endif

#ifndef MG_SET_WIFI_CONFIG
#define MG_SET_WIFI_CONFIG(data)
#endif

#ifndef MG_ENABLE_TCPIP_PRINT_DEBUG_STATS
#define MG_ENABLE_TCPIP_PRINT_DEBUG_STATS 0
#endif

#ifndef MG_ENABLE_CHACHA20
#define MG_ENABLE_CHACHA20 1  // When set to 0, GCM is used. For MG_TLS_BUILTIN
#endif



// Macros to record timestamped events that happens with a connection.
// They are saved into a c->prof IO buffer, each event is a name and a 32-bit
// timestamp in milliseconds since connection init time.
//
// Test (run in two separate terminals):
//   make -C tutorials/http/http-server/ CFLAGS_EXTRA=-DMG_ENABLE_PROFILE=1
//   curl localhost:8000
// Output:
//   1ea1f1e7 2 net.c:150:mg_close_conn      3 profile:                                                            
//   1ea1f1e8 2 net.c:150:mg_close_conn      1ea1f1e6 init                                                         
//   1ea1f1e8 2 net.c:150:mg_close_conn          0 EV_OPEN
//   1ea1f1e8 2 net.c:150:mg_close_conn          0 EV_ACCEPT 
//   1ea1f1e8 2 net.c:150:mg_close_conn          0 EV_READ
//   1ea1f1e8 2 net.c:150:mg_close_conn          0 EV_HTTP_MSG
//   1ea1f1e8 2 net.c:150:mg_close_conn          0 EV_WRITE
//   1ea1f1e8 2 net.c:150:mg_close_conn          1 EV_CLOSE
//
// Usage:
//   Enable profiling by setting MG_ENABLE_PROFILE=1
//   Invoke MG_PROF_ADD(c, "MY_EVENT_1") in the places you'd like to measure

#if MG_ENABLE_PROFILE
struct mg_profitem {
  const char *name;    // Event name
  uint32_t timestamp;  // Milliseconds since connection creation (MG_EV_OPEN)
};

#define MG_PROFILE_ALLOC_GRANULARITY 256  // Can save 32 items wih to realloc

// Adding a profile item to the c->prof. Must be as fast as possible.
// Reallocation of the c->prof iobuf is not desirable here, that's why we
// pre-allocate c->prof with MG_PROFILE_ALLOC_GRANULARITY.
// This macro just inits and copies 8 bytes, and calls mg_millis(),
// which should be fast enough.
#define MG_PROF_ADD(c, name_)                                             \
  do {                                                                    \
    struct mg_iobuf *io = &c->prof;                                       \
    uint32_t inittime = ((struct mg_profitem *) io->buf)->timestamp;      \
    struct mg_profitem item = {name_, (uint32_t) mg_millis() - inittime}; \
    mg_iobuf_add(io, io->len, &item, sizeof(item));                       \
  } while (0)

// Initialising profile for a new connection. Not time sensitive
#define MG_PROF_INIT(c)                                          \
  do {                                                           \
    struct mg_profitem first = {"init", (uint32_t) mg_millis()}; \
    mg_iobuf_init(&(c)->prof, 0, MG_PROFILE_ALLOC_GRANULARITY);  \
    mg_iobuf_add(&c->prof, c->prof.len, &first, sizeof(first));  \
  } while (0)

#define MG_PROF_FREE(c) mg_iobuf_free(&(c)->prof)

// Dumping the profile. Not time sensitive
#define MG_PROF_DUMP(c)                                            \
  do {                                                             \
    struct mg_iobuf *io = &c->prof;                                \
    struct mg_profitem *p = (struct mg_profitem *) io->buf;        \
    struct mg_profitem *e = &p[io->len / sizeof(*p)];              \
    MG_INFO(("%lu profile:", c->id));                              \
    while (p < e) {                                                \
      MG_INFO(("%5lx %s", (unsigned long) p->timestamp, p->name)); \
      p++;                                                         \
    }                                                              \
  } while (0)

#else
#define MG_PROF_INIT(c)
#define MG_PROF_FREE(c)
#define MG_PROF_ADD(c, name)
#define MG_PROF_DUMP(c)
#endif




// Describes an arbitrary chunk of memory
struct mg_str {
  char *buf;   // String data
  size_t len;  // String length
};

// Using macro to avoid shadowing C++ struct constructor, see #1298
#define mg_str(s) mg_str_s(s)

struct mg_str mg_str(const char *s);
struct mg_str mg_str_n(const char *s, size_t n);
int mg_casecmp(const char *s1, const char *s2);
int mg_strcmp(const struct mg_str str1, const struct mg_str str2);
int mg_strcasecmp(const struct mg_str str1, const struct mg_str str2);
struct mg_str mg_strdup(const struct mg_str s);
bool mg_match(struct mg_str str, struct mg_str pattern, struct mg_str *caps);
bool mg_span(struct mg_str s, struct mg_str *a, struct mg_str *b, char delim);

bool mg_str_to_num(struct mg_str, int base, void *val, size_t val_len);




// Single producer, single consumer non-blocking queue

struct mg_queue {
  char *buf;
  size_t size;
  volatile size_t tail;
  volatile size_t head;
};

void mg_queue_init(struct mg_queue *, char *, size_t);        // Init queue
size_t mg_queue_book(struct mg_queue *, char **buf, size_t);  // Reserve space
void mg_queue_add(struct mg_queue *, size_t);                 // Add new message
size_t mg_queue_next(struct mg_queue *, char **);  // Get oldest message
void mg_queue_del(struct mg_queue *, size_t);      // Delete oldest message




typedef void (*mg_pfn_t)(char, void *);                  // Output function
typedef size_t (*mg_pm_t)(mg_pfn_t, void *, va_list *);  // %M printer

size_t mg_vxprintf(void (*)(char, void *), void *, const char *fmt, va_list *);
size_t mg_xprintf(void (*fn)(char, void *), void *, const char *fmt, ...);






// Convenience wrappers around mg_xprintf
size_t mg_vsnprintf(char *buf, size_t len, const char *fmt, va_list *ap);
size_t mg_snprintf(char *, size_t, const char *fmt, ...);
char *mg_vmprintf(const char *fmt, va_list *ap);
char *mg_mprintf(const char *fmt, ...);
size_t mg_queue_printf(struct mg_queue *, const char *fmt, ...);

// %M print helper functions
size_t mg_print_base64(void (*out)(char, void *), void *arg, va_list *ap);
size_t mg_print_esc(void (*out)(char, void *), void *arg, va_list *ap);
size_t mg_print_hex(void (*out)(char, void *), void *arg, va_list *ap);
size_t mg_print_ip(void (*out)(char, void *), void *arg, va_list *ap);
size_t mg_print_ip_port(void (*out)(char, void *), void *arg, va_list *ap);
size_t mg_print_ip4(void (*out)(char, void *), void *arg, va_list *ap);
size_t mg_print_ip6(void (*out)(char, void *), void *arg, va_list *ap);
size_t mg_print_mac(void (*out)(char, void *), void *arg, va_list *ap);
size_t mg_print_l2addr(void (*out)(char, void *), void *arg, va_list *ap);

// Various output functions
void mg_pfn_iobuf(char ch, void *param);           // param: struct mg_iobuf *
void mg_pfn_iobuf_noresize(char ch, void *param);  // param: struct mg_iobuf *
void mg_pfn_stdout(char c, void *param);           // param: ignored

// A helper macro for printing JSON: mg_snprintf(buf, len, "%m", MG_ESC("hi"))
#define MG_ESC(str) mg_print_esc, 0, (str)






enum { MG_LL_NONE, MG_LL_ERROR, MG_LL_INFO, MG_LL_DEBUG, MG_LL_VERBOSE };
extern int mg_log_level;  // Current log level, one of MG_LL_*

void mg_log(const char *fmt, ...);
void mg_log_prefix(int ll, const char *file, int line, const char *fname);
// bool mg_log2(int ll, const char *file, int line, const char *fmt, ...);
void mg_hexdump(const void *buf, size_t len);
void mg_log_set_fn(mg_pfn_t fn, void *param);

#define mg_log_set(level_) mg_log_level = (level_)

#if MG_ENABLE_LOG
#if !defined(_MSC_VER) && \
    (!defined(__STDC_VERSION__) || __STDC_VERSION__ < 199901L)
#define MG___FUNC__ ""
#else
#define MG___FUNC__ __func__  // introduced in C99
#endif
#define MG_LOG(level, args)                                    \
  do {                                                         \
    if ((level) <= mg_log_level) {                             \
      mg_log_prefix((level), __FILE__, __LINE__, MG___FUNC__); \
      mg_log args;                                             \
    }                                                          \
  } while (0)
#else
#define MG_LOG(level, args) \
  do {                      \
    if (0) mg_log args;     \
  } while (0)
#endif

#define MG_ERROR(args) MG_LOG(MG_LL_ERROR, args)
#define MG_INFO(args) MG_LOG(MG_LL_INFO, args)
#define MG_DEBUG(args) MG_LOG(MG_LL_DEBUG, args)
#define MG_VERBOSE(args) MG_LOG(MG_LL_VERBOSE, args)




struct mg_timer {
  uint64_t period_ms;          // Timer period in milliseconds
  uint64_t expire;             // Expiration timestamp in milliseconds
  unsigned flags;              // Possible flags values below
#define MG_TIMER_ONCE 0        // Call function once
#define MG_TIMER_REPEAT 1      // Call function periodically
#define MG_TIMER_RUN_NOW 2     // Call immediately when timer is set
#define MG_TIMER_CALLED 4      // Timer function was called at least once
#define MG_TIMER_AUTODELETE 8  // mg_free() timer when done
  void (*fn)(void *);          // Function to call
  void *arg;                   // Function argument
  struct mg_timer *next;       // Linkage
};

void mg_timer_init(struct mg_timer **head, struct mg_timer *timer,
                   uint64_t milliseconds, unsigned flags, void (*fn)(void *),
                   void *arg);
void mg_timer_free(struct mg_timer **head, struct mg_timer *);
void mg_timer_poll(struct mg_timer **head, uint64_t new_ms);
bool mg_timer_expired(uint64_t *expiration, uint64_t period, uint64_t now);





enum { MG_FS_READ = 1, MG_FS_WRITE = 2, MG_FS_DIR = 4 };

// Filesystem API functions
// st() returns MG_FS_* flags and populates file size and modification time
// ls() calls fn() for every directory entry, allowing to list a directory
//
// NOTE: UNIX-style shorthand names for the API functions are deliberately
// chosen to avoid conflicts with some libraries that make macros for e.g.
// stat(), write(), read() calls.
struct mg_fs {
  int (*st)(const char *path, size_t *size, time_t *mtime);  // stat file
  void (*ls)(const char *path, void (*fn)(const char *, void *),
             void *);  // List directory entries: call fn(file_name, fn_data)
                       // for each directory entry
  void *(*op)(const char *path, int flags);             // Open file
  void (*cl)(void *fd);                                 // Close file
  size_t (*rd)(void *fd, void *buf, size_t len);        // Read file
  size_t (*wr)(void *fd, const void *buf, size_t len);  // Write file
  size_t (*sk)(void *fd, size_t offset);                // Set file position
  bool (*mv)(const char *from, const char *to);         // Rename file
  bool (*rm)(const char *path);                         // Delete file
  bool (*mkd)(const char *path);                        // Create directory
};

extern struct mg_fs mg_fs_posix;   // POSIX open/close/read/write/seek
extern struct mg_fs mg_fs_packed;  // see tutorials/core/embedded-filesystem
extern struct mg_fs mg_fs_fat;     // FAT FS

// File descriptor
struct mg_fd {
  void *fd;
  struct mg_fs *fs;
};

struct mg_fd *mg_fs_open(struct mg_fs *fs, const char *path, int flags);
void mg_fs_close(struct mg_fd *fd);
bool mg_fs_ls(struct mg_fs *fs, const char *path, char *buf, size_t len);
struct mg_str mg_file_read(struct mg_fs *fs, const char *path);
bool mg_file_write(struct mg_fs *fs, const char *path, const void *, size_t);
bool mg_file_printf(struct mg_fs *fs, const char *path, const char *fmt, ...);

// Packed API
const char *mg_unpack(const char *path, size_t *size, time_t *mtime);
const char *mg_unlist(size_t no);             // Get no'th packed filename
struct mg_str mg_unpacked(const char *path);  // Packed file as mg_str







#if MG_ENABLE_ASSERT
#include <assert.h>
#elif !defined(assert)
#define assert(x)
#endif

void *mg_calloc(size_t count, size_t size);
void mg_free(void *ptr);
void mg_bzero(volatile unsigned char *buf, size_t len);
bool mg_random(void *buf, size_t len);
char *mg_random_str(char *buf, size_t len);
uint32_t mg_crc32(uint32_t crc, const char *buf, size_t len);
uint64_t mg_millis(void);  // Return milliseconds since boot
bool mg_path_is_sane(const struct mg_str path);
void mg_delayms(unsigned int ms);

#define MG_U32(a, b, c, d)                                         \
  (((uint32_t) ((a) &255) << 24) | ((uint32_t) ((b) &255) << 16) | \
   ((uint32_t) ((c) &255) << 8) | (uint32_t) ((d) &255))

#define MG_IPV4(a, b, c, d) mg_htonl(MG_U32(a, b, c, d))

#define MG_IPV6(a, b, c, d, e, f, g ,h) \
  { (uint8_t)((a)>>8),(uint8_t)(a), \
    (uint8_t)((b)>>8),(uint8_t)(b), \
    (uint8_t)((c)>>8),(uint8_t)(c), \
    (uint8_t)((d)>>8),(uint8_t)(d), \
    (uint8_t)((e)>>8),(uint8_t)(e), \
    (uint8_t)((f)>>8),(uint8_t)(f), \
    (uint8_t)((g)>>8),(uint8_t)(g), \
    (uint8_t)((h)>>8),(uint8_t)(h) }

// For printing IPv4 addresses: printf("%d.%d.%d.%d\n", MG_IPADDR_PARTS(&ip))
#define MG_U8P(ADDR) ((uint8_t *) (ADDR))
#define MG_IPADDR_PARTS(ADDR) \
  MG_U8P(ADDR)[0], MG_U8P(ADDR)[1], MG_U8P(ADDR)[2], MG_U8P(ADDR)[3]

#define MG_LOAD_BE16(p) \
  ((uint16_t) (((uint16_t) MG_U8P(p)[0] << 8U) | MG_U8P(p)[1]))
#define MG_LOAD_BE24(p)                           \
  ((uint32_t) (((uint32_t) MG_U8P(p)[0] << 16U) | \
               ((uint32_t) MG_U8P(p)[1] << 8U) | MG_U8P(p)[2]))
#define MG_LOAD_BE32(p)                           \
  ((uint32_t) (((uint32_t) MG_U8P(p)[0] << 24U) | \
               ((uint32_t) MG_U8P(p)[1] << 16U) | \
               ((uint32_t) MG_U8P(p)[2] << 8U) | MG_U8P(p)[3]))
#define MG_LOAD_BE64(p)                           \
  ((uint64_t) (((uint64_t) MG_U8P(p)[0] << 56U) | \
               ((uint64_t) MG_U8P(p)[1] << 48U) | \
               ((uint64_t) MG_U8P(p)[2] << 40U) | \
               ((uint64_t) MG_U8P(p)[3] << 32U) | \
               ((uint64_t) MG_U8P(p)[4] << 24U) | \
               ((uint64_t) MG_U8P(p)[5] << 16U) | \
               ((uint64_t) MG_U8P(p)[6] << 8U) | MG_U8P(p)[7]))
#define MG_STORE_BE16(p, n)           \
  do {                                \
    MG_U8P(p)[0] = ((n) >> 8U) & 255; \
    MG_U8P(p)[1] = (n) &255;          \
  } while (0)
#define MG_STORE_BE24(p, n)            \
  do {                                 \
    MG_U8P(p)[0] = ((n) >> 16U) & 255; \
    MG_U8P(p)[1] = ((n) >> 8U) & 255;  \
    MG_U8P(p)[2] = (n) &255;           \
  } while (0)
#define MG_STORE_BE32(p, n)            \
  do {                                 \
    MG_U8P(p)[0] = ((n) >> 24U) & 255; \
    MG_U8P(p)[1] = ((n) >> 16U) & 255; \
    MG_U8P(p)[2] = ((n) >> 8U) & 255;  \
    MG_U8P(p)[3] = (n) &255;           \
  } while (0)
#define MG_STORE_BE64(p, n)            \
  do {                                 \
    MG_U8P(p)[0] = ((n) >> 56U) & 255; \
    MG_U8P(p)[1] = ((n) >> 48U) & 255; \
    MG_U8P(p)[2] = ((n) >> 40U) & 255; \
    MG_U8P(p)[3] = ((n) >> 32U) & 255; \
    MG_U8P(p)[4] = ((n) >> 24U) & 255; \
    MG_U8P(p)[5] = ((n) >> 16U) & 255; \
    MG_U8P(p)[6] = ((n) >> 8U) & 255;  \
    MG_U8P(p)[7] = (n) &255;           \
  } while (0)

uint16_t mg_ntohs(uint16_t net);
uint32_t mg_ntohl(uint32_t net);
uint64_t mg_ntohll(uint64_t net);
#define mg_htons(x) mg_ntohs(x)
#define mg_htonl(x) mg_ntohl(x)
#define mg_htonll(x) mg_ntohll(x)

#define MG_REG(x) ((volatile uint32_t *) (x))[0]
#define MG_BIT(x) (((uint32_t) 1U) << (x))
#define MG_SET_BITS(R, CLRMASK, SETMASK) (R) = ((R) & ~(CLRMASK)) | (SETMASK)

#define MG_ROUND_UP(x, a) ((a) == 0 ? (x) : ((((x) + (a) -1) / (a)) * (a)))
#define MG_ROUND_DOWN(x, a) ((a) == 0 ? (x) : (((x) / (a)) * (a)))

#if defined(__GNUC__) && defined(__arm__)
#ifdef __ZEPHYR__
#define MG_ARM_DISABLE_IRQ() __asm__ __volatile__("cpsid i" : : : "memory")
#define MG_ARM_ENABLE_IRQ() __asm__ __volatile__("cpsie i" : : : "memory")
#else
#define MG_ARM_DISABLE_IRQ() asm volatile("cpsid i" : : : "memory")
#define MG_ARM_ENABLE_IRQ() asm volatile("cpsie i" : : : "memory")
#endif // !ZEPHYR
#elif defined(__CCRH__)
#define MG_RH850_DISABLE_IRQ() __DI()
#define MG_RH850_ENABLE_IRQ() __EI()
#else
#define MG_ARM_DISABLE_IRQ()
#define MG_ARM_ENABLE_IRQ()
#endif

#if defined(__CC_ARM)
#define MG_DSB() __dsb(0xf)
#elif defined(__ARMCC_VERSION)
#define MG_DSB() __builtin_arm_dsb(0xf)
#elif defined(__GNUC__) && defined(__arm__) && defined(__thumb__)
#ifdef __ZEPHYR__
#define MG_DSB() __asm__("DSB 0xf")
#else
#define MG_DSB() asm("DSB 0xf")
#endif // !ZEPHYR
#elif defined(__ICCARM__)
#define MG_DSB() __iar_builtin_DSB()
#else
#define MG_DSB()
#endif

struct mg_addr;
int mg_check_ip_acl(struct mg_str acl, struct mg_addr *remote_ip);

// Linked list management macros
#define LIST_ADD_HEAD(type_, head_, elem_) \
  do {                                     \
    (elem_)->next = (*head_);              \
    *(head_) = (elem_);                    \
  } while (0)

#define LIST_ADD_TAIL(type_, head_, elem_) \
  do {                                     \
    type_ **h = head_;                     \
    while (*h != NULL) h = &(*h)->next;    \
    *h = (elem_);                          \
  } while (0)

#define LIST_DELETE(type_, head_, elem_)   \
  do {                                     \
    type_ **h = head_;                     \
    while (*h != (elem_)) h = &(*h)->next; \
    *h = (elem_)->next;                    \
  } while (0)



unsigned short mg_url_port(const char *url);
int mg_url_is_ssl(const char *url);
struct mg_str mg_url_host(const char *url);
struct mg_str mg_url_user(const char *url);
struct mg_str mg_url_pass(const char *url);
const char *mg_url_uri(const char *url);




struct mg_iobuf {
  unsigned char *buf;  // Pointer to stored data
  size_t size;         // Total size available
  size_t len;          // Current number of bytes
  size_t align;        // Alignment during allocation
};

bool mg_iobuf_init(struct mg_iobuf *, size_t, size_t);
bool mg_iobuf_resize(struct mg_iobuf *, size_t);
void mg_iobuf_free(struct mg_iobuf *);
size_t mg_iobuf_add(struct mg_iobuf *, size_t, const void *, size_t);
size_t mg_iobuf_del(struct mg_iobuf *, size_t ofs, size_t len);


size_t mg_base64_update(unsigned char input_byte, char *buf, size_t len);
size_t mg_base64_final(char *buf, size_t len);
size_t mg_base64_encode(const unsigned char *p, size_t n, char *buf, size_t);
size_t mg_base64_decode(const char *src, size_t n, char *dst, size_t);




typedef struct {
  uint32_t buf[4];
  uint32_t bits[2];
  unsigned char in[64];
} mg_md5_ctx;

void mg_md5_init(mg_md5_ctx *c);
void mg_md5_update(mg_md5_ctx *c, const unsigned char *data, size_t len);
void mg_md5_final(mg_md5_ctx *c, unsigned char[16]);




typedef struct {
  uint32_t state[5];
  uint32_t count[2];
  unsigned char buffer[64];
} mg_sha1_ctx;

void mg_sha1_init(mg_sha1_ctx *);
void mg_sha1_update(mg_sha1_ctx *, const unsigned char *data, size_t len);
void mg_sha1_final(unsigned char digest[20], mg_sha1_ctx *);
// https://github.com/B-Con/crypto-algorithms
// Author:     Brad Conte (brad AT bradconte.com)
// Disclaimer: This code is presented "as is" without any guarantees.
// Details:    Defines the API for the corresponding SHA1 implementation.
// Copyright:  public domain





typedef struct {
  uint32_t state[8];
  uint64_t bits;
  uint32_t len;
  unsigned char buffer[64];
} mg_sha256_ctx;


void mg_sha256_init(mg_sha256_ctx *);
void mg_sha256_update(mg_sha256_ctx *, const unsigned char *data, size_t len);
void mg_sha256_final(unsigned char digest[32], mg_sha256_ctx *);
void mg_sha256(uint8_t dst[32], uint8_t *data, size_t datasz);
void mg_hmac_sha256(uint8_t dst[32], uint8_t *key, size_t keysz, uint8_t *data,
                    size_t datasz);

typedef struct {
    uint64_t state[8];
    uint8_t buffer[128];
    uint64_t bitlen[2];
    uint32_t datalen;
} mg_sha384_ctx;
void mg_sha384_init(mg_sha384_ctx *ctx);
void mg_sha384_update(mg_sha384_ctx *ctx, const uint8_t *data, size_t len);
void mg_sha384_final(uint8_t digest[48], mg_sha384_ctx *ctx);
void mg_sha384(uint8_t dst[48], uint8_t *data, size_t datasz);



struct mg_connection;
typedef void (*mg_event_handler_t)(struct mg_connection *, int ev,
                                   void *ev_data);
void mg_call(struct mg_connection *c, int ev, void *ev_data);
void mg_error(struct mg_connection *c, const char *fmt, ...);

enum {
  MG_EV_ERROR,      // Error                        char *error_message
  MG_EV_OPEN,       // Connection created           NULL
  MG_EV_POLL,       // mg_mgr_poll iteration        uint64_t *uptime_millis
  MG_EV_RESOLVE,    // Host name is resolved        NULL
  MG_EV_CONNECT,    // Connection established       NULL
  MG_EV_ACCEPT,     // Connection accepted          NULL
  MG_EV_TLS_HS,     // TLS handshake succeeded      NULL
  MG_EV_READ,       // Data received from socket    long *bytes_read
  MG_EV_WRITE,      // Data written to socket       long *bytes_written
  MG_EV_CLOSE,      // Connection closed            NULL
  MG_EV_HTTP_HDRS,  // HTTP headers                 struct mg_http_message *
  MG_EV_HTTP_MSG,   // Full HTTP request/response   struct mg_http_message *
  MG_EV_WS_OPEN,    // Websocket handshake done     struct mg_http_message *
  MG_EV_WS_MSG,     // Websocket msg, text or bin   struct mg_ws_message *
  MG_EV_WS_CTL,     // Websocket control msg        struct mg_ws_message *
  MG_EV_MQTT_CMD,   // MQTT low-level command       struct mg_mqtt_message *
  MG_EV_MQTT_MSG,   // MQTT PUBLISH received        struct mg_mqtt_message *
  MG_EV_MQTT_OPEN,  // MQTT CONNACK received        int *connack_status_code
  MG_EV_SNTP_TIME,  // SNTP time received           uint64_t *epoch_millis
  MG_EV_WAKEUP,     // mg_wakeup() data received    struct mg_str *data
  MG_EV_MDNS_REQ,   // mDNS request                 struct mg_mdns_req *
  MG_EV_MDNS_RESP,  // mDNS response                struct mg_mdns_resp *
  MG_EV_USER        // Starting ID for user events
};










struct mg_dns {
  const char *url;          // DNS server URL
  struct mg_connection *c;  // DNS server connection
};

struct mg_addr {
  union {  // Holds IPv4 or IPv6 address, in network byte order
    uint8_t ip[16];
    uint32_t ip4;
    uint64_t ip6[2];
  } addr;
  uint16_t port;     // TCP or UDP port in network byte order
  uint8_t scope_id;  // IPv6 scope ID
  bool is_ip6;       // True when address is IPv6 address
};

struct mg_mgr {
  struct mg_connection *conns;  // List of active connections
  struct mg_dns dns4;           // DNS for IPv4
  struct mg_dns dns6;           // DNS for IPv6
  int dnstimeout;               // DNS resolve timeout in milliseconds
  bool use_dns6;                // Use DNS6 server by default, see #1532
  unsigned long nextid;         // Next connection ID
  void *userdata;               // Arbitrary user data pointer
  void *tls_ctx;                // TLS context shared by all TLS sessions
  uint16_t mqtt_id;             // MQTT IDs for pub/sub
  void *active_dns_requests;    // DNS requests in progress
  struct mg_timer *timers;      // Active timers
  int epoll_fd;                 // Used when MG_EPOLL_ENABLE=1
  struct mg_tcpip_if *ifp;      // Builtin TCP/IP stack only. Interface pointer
  size_t extraconnsize;         // Builtin TCP/IP stack only. Extra space
  MG_SOCKET_TYPE pipe;          // Socketpair end for mg_wakeup()
#if MG_ENABLE_FREERTOS_TCP
  SocketSet_t ss;  // NOTE(lsm): referenced from socket struct
#endif
};

struct mg_connection {
  struct mg_connection *next;     // Linkage in struct mg_mgr :: connections
  struct mg_mgr *mgr;             // Our container
  struct mg_addr loc;             // Local address
  struct mg_addr rem;             // Remote address
  void *fd;                       // Connected socket, or LWIP data
  unsigned long id;               // Auto-incrementing unique connection ID
  struct mg_iobuf recv;           // Incoming data
  struct mg_iobuf send;           // Outgoing data
  struct mg_iobuf prof;           // Profile data enabled by MG_ENABLE_PROFILE
  struct mg_iobuf rtls;           // TLS only. Incoming encrypted data
  mg_event_handler_t fn;          // User-specified event handler function
  void *fn_data;                  // User-specified function parameter
  mg_event_handler_t pfn;         // Protocol-specific handler function
  void *pfn_data;                 // Protocol-specific function parameter
  char data[MG_DATA_SIZE];        // Arbitrary connection data
  void *tls;                      // TLS specific data
  unsigned is_listening : 1;      // Listening connection
  unsigned is_client : 1;         // Outbound (client) connection
  unsigned is_accepted : 1;       // Accepted (server) connection
  unsigned is_resolving : 1;      // Non-blocking DNS resolution is in progress
  unsigned is_arplooking : 1;     // Non-blocking ARP resolution is in progress
  unsigned is_connecting : 1;     // Non-blocking connect is in progress
  unsigned is_tls : 1;            // TLS-enabled connection
  unsigned is_tls_hs : 1;         // TLS handshake is in progress
  unsigned is_udp : 1;            // UDP connection
  unsigned is_websocket : 1;      // WebSocket connection
  unsigned is_mqtt5 : 1;          // For MQTT connection, v5 indicator
  unsigned is_hexdumping : 1;     // Hexdump in/out traffic
  unsigned is_draining : 1;       // Send remaining data, then close and free
  unsigned is_closing : 1;        // Close and free the connection immediately
  unsigned is_full : 1;           // Stop reads, until cleared
  unsigned is_tls_throttled : 1;  // Last TLS write: MG_SOCK_PENDING() was true
  unsigned is_resp : 1;           // Response is still being generated
  unsigned is_readable : 1;       // Connection is ready to read
  unsigned is_writable : 1;       // Connection is ready to write
};

void mg_mgr_poll(struct mg_mgr *, int ms);
void mg_mgr_init(struct mg_mgr *);
void mg_mgr_free(struct mg_mgr *);

struct mg_connection *mg_listen(struct mg_mgr *, const char *url,
                                mg_event_handler_t fn, void *fn_data);
struct mg_connection *mg_connect(struct mg_mgr *, const char *url,
                                 mg_event_handler_t fn, void *fn_data);
struct mg_connection *mg_wrapfd(struct mg_mgr *mgr, int fd,
                                mg_event_handler_t fn, void *fn_data);
void mg_connect_resolved(struct mg_connection *);
bool mg_send(struct mg_connection *, const void *, size_t);
size_t mg_printf(struct mg_connection *, const char *fmt, ...);
size_t mg_vprintf(struct mg_connection *, const char *fmt, va_list *ap);
bool mg_aton(struct mg_str str, struct mg_addr *addr);

// These functions are used to integrate with custom network stacks
struct mg_connection *mg_alloc_conn(struct mg_mgr *);
void mg_close_conn(struct mg_connection *c);
bool mg_open_listener(struct mg_connection *c, const char *url);

// Utility functions
bool mg_wakeup(struct mg_mgr *, unsigned long id, const void *buf, size_t len);
bool mg_wakeup_init(struct mg_mgr *);
struct mg_timer *mg_timer_add(struct mg_mgr *mgr, uint64_t milliseconds,
                              unsigned flags, void (*fn)(void *), void *arg);
struct mg_connection *mg_connect_svc(struct mg_mgr *mgr, const char *url,
                                     mg_event_handler_t fn, void *fn_data,
                                     mg_event_handler_t pfn, void *pfn_data);
void mg_multicast_restore(struct mg_connection *c, uint8_t *from);








struct mg_http_header {
  struct mg_str name;   // Header name
  struct mg_str value;  // Header value
};

struct mg_http_message {
  struct mg_str method, uri, query, proto;             // Request/response line
  struct mg_http_header headers[MG_MAX_HTTP_HEADERS];  // Headers
  struct mg_str body;                                  // Body
  struct mg_str head;                                  // Request + headers
  struct mg_str message;  // Request + headers + body
};

// Parameter for mg_http_serve_dir()
struct mg_http_serve_opts {
  const char *root_dir;       // Web root directory, must be non-NULL
  const char *ssi_pattern;    // SSI file name pattern, e.g. #.shtml
  const char *extra_headers;  // Extra HTTP headers to add in responses
  const char *mime_types;     // Extra mime types, ext1=type1,ext2=type2,..
  const char *page404;        // Path to the 404 page, or NULL by default
  struct mg_fs *fs;           // Filesystem implementation. Use NULL for POSIX
};

// Parameter for mg_http_next_multipart
struct mg_http_part {
  struct mg_str name;      // Form field name
  struct mg_str filename;  // Filename for file uploads
  struct mg_str body;      // Part contents
};

int mg_http_parse(const char *s, size_t len, struct mg_http_message *);
int mg_http_get_request_len(const unsigned char *buf, size_t buf_len);
void mg_http_printf_chunk(struct mg_connection *cnn, const char *fmt, ...);
void mg_http_write_chunk(struct mg_connection *c, const char *buf, size_t len);
struct mg_connection *mg_http_listen(struct mg_mgr *, const char *url,
                                     mg_event_handler_t fn, void *fn_data);
struct mg_connection *mg_http_connect(struct mg_mgr *, const char *url,
                                      mg_event_handler_t fn, void *fn_data);
void mg_http_serve_dir(struct mg_connection *, struct mg_http_message *hm,
                       const struct mg_http_serve_opts *);
void mg_http_serve_file(struct mg_connection *, struct mg_http_message *hm,
                        const char *path, const struct mg_http_serve_opts *);
void mg_http_reply(struct mg_connection *, int status_code, const char *headers,
                   const char *body_fmt, ...);
struct mg_str *mg_http_get_header(struct mg_http_message *, const char *name);
struct mg_str mg_http_var(struct mg_str buf, struct mg_str name);
int mg_http_get_var(const struct mg_str *, const char *name, char *, size_t);
int mg_url_decode(const char *s, size_t n, char *to, size_t to_len, int form);
size_t mg_url_encode(const char *s, size_t n, char *buf, size_t len);
void mg_http_creds(struct mg_http_message *, char *, size_t, char *, size_t);
long mg_http_upload(struct mg_connection *c, struct mg_http_message *hm,
                    struct mg_fs *fs, const char *dir, size_t max_size);
void mg_http_bauth(struct mg_connection *, const char *user, const char *pass);
struct mg_str mg_http_get_header_var(struct mg_str s, struct mg_str v);
size_t mg_http_next_multipart(struct mg_str, size_t, struct mg_http_part *);
int mg_http_status(const struct mg_http_message *hm);


void mg_http_serve_ssi(struct mg_connection *c, const char *root,
                       const char *fullpath);


#define MG_TLS_NONE 0     // No TLS support
#define MG_TLS_MBED 1     // mbedTLS
#define MG_TLS_OPENSSL 2  // OpenSSL
#define MG_TLS_WOLFSSL 5  // WolfSSL (based on OpenSSL)
#define MG_TLS_BUILTIN 3  // Built-in
#define MG_TLS_CUSTOM 4   // Custom implementation

#ifndef MG_TLS
#define MG_TLS MG_TLS_NONE
#endif





struct mg_tls_opts {
  struct mg_str ca;       // PEM or DER
  struct mg_str cert;     // PEM or DER
  struct mg_str key;      // PEM or DER
  struct mg_str name;     // If not empty, enable host name verification
  int skip_verification;  // Skip certificate and host name verification
};

void mg_tls_init(struct mg_connection *, const struct mg_tls_opts *opts);
void mg_tls_free(struct mg_connection *);
long mg_tls_send(struct mg_connection *, const void *buf, size_t len);
long mg_tls_recv(struct mg_connection *, void *buf, size_t len);
size_t mg_tls_pending(struct mg_connection *);
void mg_tls_flush(struct mg_connection *);
void mg_tls_handshake(struct mg_connection *);

// Private
void mg_tls_ctx_init(struct mg_mgr *);
void mg_tls_ctx_free(struct mg_mgr *);
#define MG_IS_DER(buf) (((uint8_t *) (buf))[0] == 0x30)  // DER begins with 0x30

// Low-level IO primives used by TLS layer
enum { MG_IO_ERR = -1, MG_IO_WAIT = -2, MG_IO_RESET = -3 };
long mg_io_send(struct mg_connection *c, const void *buf, size_t len);
long mg_io_recv(struct mg_connection *c, void *buf, size_t len);
#ifndef TLS_X15519_H
#define TLS_X15519_H



#define X25519_BYTES 32
extern const uint8_t X25519_BASE_POINT[X25519_BYTES];

int mg_tls_x25519(uint8_t out[X25519_BYTES], const uint8_t scalar[X25519_BYTES],
                  const uint8_t x1[X25519_BYTES], int clamp);


#endif /* TLS_X15519_H */
/******************************************************************************
 *
 * THIS SOURCE CODE IS HEREBY PLACED INTO THE PUBLIC DOMAIN FOR THE GOOD OF ALL
 *
 * This is a simple and straightforward implementation of AES-GCM authenticated
 * encryption. The focus of this work was correctness & accuracy. It is written
 * in straight 'C' without any particular focus upon optimization or speed. It
 * should be endian (memory byte order) neutral since the few places that care
 * are handled explicitly.
 *
 * This implementation of AES-GCM was created by Steven M. Gibson of GRC.com.
 *
 * It is intended for general purpose use, but was written in support of GRC's
 * reference implementation of the SQRL (Secure Quick Reliable Login) client.
 *
 * See:    http://csrc.nist.gov/publications/nistpubs/800-38D/SP-800-38D.pdf
 *         http://csrc.nist.gov/groups/ST/toolkit/BCM/documents/proposedmodes/ \
 *         gcm/gcm-revised-spec.pdf
 *
 * NO COPYRIGHT IS CLAIMED IN THIS WORK, HOWEVER, NEITHER IS ANY WARRANTY MADE
 * REGARDING ITS FITNESS FOR ANY PARTICULAR PURPOSE. USE IT AT YOUR OWN RISK.
 *
 *******************************************************************************/
#ifndef TLS_AES128_H
#define TLS_AES128_H

/******************************************************************************
 *  AES_CONTEXT : cipher context / holds inter-call data
 ******************************************************************************/
typedef struct {
  int mode;          // 1 for Encryption, 0 for Decryption
  int rounds;        // keysize-based rounds count
  uint32_t *rk;      // pointer to current round key
  uint32_t buf[68];  // key expansion buffer
} aes_context;


#define GCM_AUTH_FAILURE 0x55555555  // authentication failure

/******************************************************************************
 *  GCM_CONTEXT : MUST be called once before ANY use of this library
 ******************************************************************************/
int mg_gcm_initialize(void);

//
//  aes-gcm.h
//  MKo
//
//  Created by Markus Kosmal on 20/11/14.
//
//
int mg_aes_gcm_encrypt(unsigned char *output, const unsigned char *input,
                       size_t input_length, const unsigned char *key,
                       const size_t key_len, const unsigned char *iv,
                       const size_t iv_len, unsigned char *aead,
                       size_t aead_len, unsigned char *tag,
                       const size_t tag_len);

int mg_aes_gcm_decrypt(unsigned char *output, const unsigned char *input,
                       size_t input_length, const unsigned char *key,
                       const size_t key_len, const unsigned char *iv,
                       const size_t iv_len, unsigned char *aead,
                       size_t aead_len, const unsigned char *tag,
                       const size_t tag_len);

#endif /* TLS_AES128_H */

// End of aes128 PD



#define MG_UECC_SUPPORTS_secp256r1 1
/* Copyright 2014, Kenneth MacKay. Licensed under the BSD 2-clause license. */

#ifndef _UECC_H_
#define _UECC_H_

/* Platform selection options.
If MG_UECC_PLATFORM is not defined, the code will try to guess it based on
compiler macros. Possible values for MG_UECC_PLATFORM are defined below: */
#define mg_uecc_arch_other 0
#define mg_uecc_x86 1
#define mg_uecc_x86_64 2
#define mg_uecc_arm 3
#define mg_uecc_arm_thumb 4
#define mg_uecc_arm_thumb2 5
#define mg_uecc_arm64 6
#define mg_uecc_avr 7

/* If desired, you can define MG_UECC_WORD_SIZE as appropriate for your platform
(1, 4, or 8 bytes). If MG_UECC_WORD_SIZE is not explicitly defined then it will
be automatically set based on your platform. */

/* Optimization level; trade speed for code size.
   Larger values produce code that is faster but larger.
   Currently supported values are 0 - 4; 0 is unusably slow for most
   applications. Optimization level 4 currently only has an effect ARM platforms
   where more than one curve is enabled. */
#ifndef MG_UECC_OPTIMIZATION_LEVEL
#define MG_UECC_OPTIMIZATION_LEVEL 2
#endif

/* MG_UECC_SQUARE_FUNC - If enabled (defined as nonzero), this will cause a
specific function to be used for (scalar) squaring instead of the generic
multiplication function. This can make things faster somewhat faster, but
increases the code size. */
#ifndef MG_UECC_SQUARE_FUNC
#define MG_UECC_SQUARE_FUNC 0
#endif

/* MG_UECC_VLI_NATIVE_LITTLE_ENDIAN - If enabled (defined as nonzero), this will
switch to native little-endian format for *all* arrays passed in and out of the
public API. This includes public and private keys, shared secrets, signatures
and message hashes. Using this switch reduces the amount of call stack memory
used by uECC, since less intermediate translations are required. Note that this
will *only* work on native little-endian processors and it will treat the
uint8_t arrays passed into the public API as word arrays, therefore requiring
the provided byte arrays to be word aligned on architectures that do not support
unaligned accesses. IMPORTANT: Keys and signatures generated with
MG_UECC_VLI_NATIVE_LITTLE_ENDIAN=1 are incompatible with keys and signatures
generated with MG_UECC_VLI_NATIVE_LITTLE_ENDIAN=0; all parties must use the same
endianness. */
#ifndef MG_UECC_VLI_NATIVE_LITTLE_ENDIAN
#define MG_UECC_VLI_NATIVE_LITTLE_ENDIAN 0
#endif

/* Curve support selection. Set to 0 to remove that curve. */
#ifndef MG_UECC_SUPPORTS_secp160r1
#define MG_UECC_SUPPORTS_secp160r1 0
#endif
#ifndef MG_UECC_SUPPORTS_secp192r1
#define MG_UECC_SUPPORTS_secp192r1 0
#endif
#ifndef MG_UECC_SUPPORTS_secp224r1
#define MG_UECC_SUPPORTS_secp224r1 0
#endif
#ifndef MG_UECC_SUPPORTS_secp256r1
#define MG_UECC_SUPPORTS_secp256r1 1
#endif
#ifndef MG_UECC_SUPPORTS_secp256k1
#define MG_UECC_SUPPORTS_secp256k1 0
#endif

/* Specifies whether compressed point format is supported.
   Set to 0 to disable point compression/decompression functions. */
#ifndef MG_UECC_SUPPORT_COMPRESSED_POINT
#define MG_UECC_SUPPORT_COMPRESSED_POINT 1
#endif

struct MG_UECC_Curve_t;
typedef const struct MG_UECC_Curve_t *MG_UECC_Curve;

#ifdef __cplusplus
extern "C" {
#endif

#if MG_UECC_SUPPORTS_secp160r1
MG_UECC_Curve mg_uecc_secp160r1(void);
#endif
#if MG_UECC_SUPPORTS_secp192r1
MG_UECC_Curve mg_uecc_secp192r1(void);
#endif
#if MG_UECC_SUPPORTS_secp224r1
MG_UECC_Curve mg_uecc_secp224r1(void);
#endif
#if MG_UECC_SUPPORTS_secp256r1
MG_UECC_Curve mg_uecc_secp256r1(void);
#endif
#if MG_UECC_SUPPORTS_secp256k1
MG_UECC_Curve mg_uecc_secp256k1(void);
#endif

/* MG_UECC_RNG_Function type
The RNG function should fill 'size' random bytes into 'dest'. It should return 1
if 'dest' was filled with random data, or 0 if the random data could not be
generated. The filled-in values should be either truly random, or from a
cryptographically-secure PRNG.

A correctly functioning RNG function must be set (using mg_uecc_set_rng())
before calling mg_uecc_make_key() or mg_uecc_sign().

Setting a correctly functioning RNG function improves the resistance to
side-channel attacks for mg_uecc_shared_secret() and
mg_uecc_sign_deterministic().

A correct RNG function is set by default when building for Windows, Linux, or OS
X. If you are building on another POSIX-compliant system that supports
/dev/random or /dev/urandom, you can define MG_UECC_POSIX to use the predefined
RNG. For embedded platforms there is no predefined RNG function; you must
provide your own.
*/
typedef int (*MG_UECC_RNG_Function)(uint8_t *dest, unsigned size);

/* mg_uecc_set_rng() function.
Set the function that will be used to generate random bytes. The RNG function
should return 1 if the random data was generated, or 0 if the random data could
not be generated.

On platforms where there is no predefined RNG function (eg embedded platforms),
this must be called before mg_uecc_make_key() or mg_uecc_sign() are used.

Inputs:
    rng_function - The function that will be used to generate random bytes.
*/
void mg_uecc_set_rng(MG_UECC_RNG_Function rng_function);

/* mg_uecc_get_rng() function.

Returns the function that will be used to generate random bytes.
*/
MG_UECC_RNG_Function mg_uecc_get_rng(void);

/* mg_uecc_curve_private_key_size() function.

Returns the size of a private key for the curve in bytes.
*/
int mg_uecc_curve_private_key_size(MG_UECC_Curve curve);

/* mg_uecc_curve_public_key_size() function.

Returns the size of a public key for the curve in bytes.
*/
int mg_uecc_curve_public_key_size(MG_UECC_Curve curve);

/* mg_uecc_make_key() function.
Create a public/private key pair.

Outputs:
    public_key  - Will be filled in with the public key. Must be at least 2 *
the curve size (in bytes) long. For example, if the curve is secp256r1,
public_key must be 64 bytes long. private_key - Will be filled in with the
private key. Must be as long as the curve order; this is typically the same as
the curve size, except for secp160r1. For example, if the curve is secp256r1,
private_key must be 32 bytes long.

                  For secp160r1, private_key must be 21 bytes long! Note that
the first byte will almost always be 0 (there is about a 1 in 2^80 chance of it
being non-zero).

Returns 1 if the key pair was generated successfully, 0 if an error occurred.
*/
int mg_uecc_make_key(uint8_t *public_key, uint8_t *private_key,
                     MG_UECC_Curve curve);

/* mg_uecc_shared_secret() function.
Compute a shared secret given your secret key and someone else's public key. If
the public key is not from a trusted source and has not been previously
verified, you should verify it first using mg_uecc_valid_public_key(). Note: It
is recommended that you hash the result of mg_uecc_shared_secret() before using
it for symmetric encryption or HMAC.

Inputs:
    public_key  - The public key of the remote party.
    private_key - Your private key.

Outputs:
    secret - Will be filled in with the shared secret value. Must be the same
size as the curve size; for example, if the curve is secp256r1, secret must be
32 bytes long.

Returns 1 if the shared secret was generated successfully, 0 if an error
occurred.
*/
int mg_uecc_shared_secret(const uint8_t *public_key, const uint8_t *private_key,
                          uint8_t *secret, MG_UECC_Curve curve);

#if MG_UECC_SUPPORT_COMPRESSED_POINT
/* mg_uecc_compress() function.
Compress a public key.

Inputs:
    public_key - The public key to compress.

Outputs:
    compressed - Will be filled in with the compressed public key. Must be at
least (curve size + 1) bytes long; for example, if the curve is secp256r1,
                 compressed must be 33 bytes long.
*/
void mg_uecc_compress(const uint8_t *public_key, uint8_t *compressed,
                      MG_UECC_Curve curve);

/* mg_uecc_decompress() function.
Decompress a compressed public key.

Inputs:
    compressed - The compressed public key.

Outputs:
    public_key - Will be filled in with the decompressed public key.
*/
void mg_uecc_decompress(const uint8_t *compressed, uint8_t *public_key,
                        MG_UECC_Curve curve);
#endif /* MG_UECC_SUPPORT_COMPRESSED_POINT */

/* mg_uecc_valid_public_key() function.
Check to see if a public key is valid.

Note that you are not required to check for a valid public key before using any
other uECC functions. However, you may wish to avoid spending CPU time computing
a shared secret or verifying a signature using an invalid public key.

Inputs:
    public_key - The public key to check.

Returns 1 if the public key is valid, 0 if it is invalid.
*/
int mg_uecc_valid_public_key(const uint8_t *public_key, MG_UECC_Curve curve);

/* mg_uecc_compute_public_key() function.
Compute the corresponding public key for a private key.

Inputs:
    private_key - The private key to compute the public key for

Outputs:
    public_key - Will be filled in with the corresponding public key

Returns 1 if the key was computed successfully, 0 if an error occurred.
*/
int mg_uecc_compute_public_key(const uint8_t *private_key, uint8_t *public_key,
                               MG_UECC_Curve curve);

/* mg_uecc_sign() function.
Generate an ECDSA signature for a given hash value.

Usage: Compute a hash of the data you wish to sign (SHA-2 is recommended) and
pass it in to this function along with your private key.

Inputs:
    private_key  - Your private key.
    message_hash - The hash of the message to sign.
    hash_size    - The size of message_hash in bytes.

Outputs:
    signature - Will be filled in with the signature value. Must be at least 2 *
curve size long. For example, if the curve is secp256r1, signature must be 64
bytes long.

Returns 1 if the signature generated successfully, 0 if an error occurred.
*/
int mg_uecc_sign(const uint8_t *private_key, const uint8_t *message_hash,
                 unsigned hash_size, uint8_t *signature, MG_UECC_Curve curve);

/* MG_UECC_HashContext structure.
This is used to pass in an arbitrary hash function to
mg_uecc_sign_deterministic(). The structure will be used for multiple hash
computations; each time a new hash is computed, init_hash() will be called,
followed by one or more calls to update_hash(), and finally a call to
finish_hash() to produce the resulting hash.

The intention is that you will create a structure that includes
MG_UECC_HashContext followed by any hash-specific data. For example:

typedef struct SHA256_HashContext {
    MG_UECC_HashContext uECC;
    SHA256_CTX ctx;
} SHA256_HashContext;

void init_SHA256(MG_UECC_HashContext *base) {
    SHA256_HashContext *context = (SHA256_HashContext *)base;
    SHA256_Init(&context->ctx);
}

void update_SHA256(MG_UECC_HashContext *base,
                   const uint8_t *message,
                   unsigned message_size) {
    SHA256_HashContext *context = (SHA256_HashContext *)base;
    SHA256_Update(&context->ctx, message, message_size);
}

void finish_SHA256(MG_UECC_HashContext *base, uint8_t *hash_result) {
    SHA256_HashContext *context = (SHA256_HashContext *)base;
    SHA256_Final(hash_result, &context->ctx);
}

... when signing ...
{
    uint8_t tmp[32 + 32 + 64];
    SHA256_HashContext ctx = {{&init_SHA256, &update_SHA256, &finish_SHA256, 64,
32, tmp}}; mg_uecc_sign_deterministic(key, message_hash, &ctx.uECC, signature);
}
*/
typedef struct MG_UECC_HashContext {
  void (*init_hash)(const struct MG_UECC_HashContext *context);
  void (*update_hash)(const struct MG_UECC_HashContext *context,
                      const uint8_t *message, unsigned message_size);
  void (*finish_hash)(const struct MG_UECC_HashContext *context,
                      uint8_t *hash_result);
  unsigned
      block_size; /* Hash function block size in bytes, eg 64 for SHA-256. */
  unsigned
      result_size; /* Hash function result size in bytes, eg 32 for SHA-256. */
  uint8_t *tmp;    /* Must point to a buffer of at least (2 * result_size +
                      block_size) bytes. */
} MG_UECC_HashContext;

/* mg_uecc_sign_deterministic() function.
Generate an ECDSA signature for a given hash value, using a deterministic
algorithm (see RFC 6979). You do not need to set the RNG using mg_uecc_set_rng()
before calling this function; however, if the RNG is defined it will improve
resistance to side-channel attacks.

Usage: Compute a hash of the data you wish to sign (SHA-2 is recommended) and
pass it to this function along with your private key and a hash context. Note
that the message_hash does not need to be computed with the same hash function
used by hash_context.

Inputs:
    private_key  - Your private key.
    message_hash - The hash of the message to sign.
    hash_size    - The size of message_hash in bytes.
    hash_context - A hash context to use.

Outputs:
    signature - Will be filled in with the signature value.

Returns 1 if the signature generated successfully, 0 if an error occurred.
*/
int mg_uecc_sign_deterministic(const uint8_t *private_key,
                               const uint8_t *message_hash, unsigned hash_size,
                               const MG_UECC_HashContext *hash_context,
                               uint8_t *signature, MG_UECC_Curve curve);

/* mg_uecc_verify() function.
Verify an ECDSA signature.

Usage: Compute the hash of the signed data using the same hash as the signer and
pass it to this function along with the signer's public key and the signature
values (r and s).

Inputs:
    public_key   - The signer's public key.
    message_hash - The hash of the signed data.
    hash_size    - The size of message_hash in bytes.
    signature    - The signature value.

Returns 1 if the signature is valid, 0 if it is invalid.
*/
int mg_uecc_verify(const uint8_t *public_key, const uint8_t *message_hash,
                   unsigned hash_size, const uint8_t *signature,
                   MG_UECC_Curve curve);

#ifdef __cplusplus
} /* end of extern "C" */
#endif

#endif /* _UECC_H_ */

/* Copyright 2015, Kenneth MacKay. Licensed under the BSD 2-clause license. */

#ifndef _UECC_TYPES_H_
#define _UECC_TYPES_H_

#ifndef MG_UECC_PLATFORM
#if defined(__AVR__) && __AVR__
#define MG_UECC_PLATFORM mg_uecc_avr
#elif defined(__thumb2__) || \
    defined(_M_ARMT) /* I think MSVC only supports Thumb-2 targets */
#define MG_UECC_PLATFORM mg_uecc_arm_thumb2
#elif defined(__thumb__)
#define MG_UECC_PLATFORM mg_uecc_arm_thumb
#elif defined(__arm__) || defined(_M_ARM)
#define MG_UECC_PLATFORM mg_uecc_arm
#elif defined(__aarch64__)
#define MG_UECC_PLATFORM mg_uecc_arm64
#elif defined(__i386__) || defined(_M_IX86) || defined(_X86_) || \
    defined(__I86__)
#define MG_UECC_PLATFORM mg_uecc_x86
#elif defined(__amd64__) || defined(_M_X64)
#define MG_UECC_PLATFORM mg_uecc_x86_64
#else
#define MG_UECC_PLATFORM mg_uecc_arch_other
#endif
#endif

#ifndef MG_UECC_ARM_USE_UMAAL
#if (MG_UECC_PLATFORM == mg_uecc_arm) && (__ARM_ARCH >= 6)
#define MG_UECC_ARM_USE_UMAAL 1
#elif (MG_UECC_PLATFORM == mg_uecc_arm_thumb2) && (__ARM_ARCH >= 6) && \
    (!defined(__ARM_ARCH_7M__) || !__ARM_ARCH_7M__)
#define MG_UECC_ARM_USE_UMAAL 1
#else
#define MG_UECC_ARM_USE_UMAAL 0
#endif
#endif

#ifndef MG_UECC_WORD_SIZE
#if MG_UECC_PLATFORM == mg_uecc_avr
#define MG_UECC_WORD_SIZE 1
#elif (MG_UECC_PLATFORM == mg_uecc_x86_64 || MG_UECC_PLATFORM == mg_uecc_arm64)
#define MG_UECC_WORD_SIZE 8
#else
#define MG_UECC_WORD_SIZE 4
#endif
#endif

#if (MG_UECC_WORD_SIZE != 1) && (MG_UECC_WORD_SIZE != 4) && \
    (MG_UECC_WORD_SIZE != 8)
#error "Unsupported value for MG_UECC_WORD_SIZE"
#endif

#if ((MG_UECC_PLATFORM == mg_uecc_avr) && (MG_UECC_WORD_SIZE != 1))
#pragma message("MG_UECC_WORD_SIZE must be 1 for AVR")
#undef MG_UECC_WORD_SIZE
#define MG_UECC_WORD_SIZE 1
#endif

#if ((MG_UECC_PLATFORM == mg_uecc_arm ||         \
      MG_UECC_PLATFORM == mg_uecc_arm_thumb ||   \
      MG_UECC_PLATFORM == mg_uecc_arm_thumb2) && \
     (MG_UECC_WORD_SIZE != 4))
#pragma message("MG_UECC_WORD_SIZE must be 4 for ARM")
#undef MG_UECC_WORD_SIZE
#define MG_UECC_WORD_SIZE 4
#endif

typedef int8_t wordcount_t;
typedef int16_t bitcount_t;
typedef int8_t cmpresult_t;

#if (MG_UECC_WORD_SIZE == 1)

typedef uint8_t mg_uecc_word_t;
typedef uint16_t mg_uecc_dword_t;

#define HIGH_BIT_SET 0x80
#define MG_UECC_WORD_BITS 8
#define MG_UECC_WORD_BITS_SHIFT 3
#define MG_UECC_WORD_BITS_MASK 0x07

#elif (MG_UECC_WORD_SIZE == 4)

typedef uint32_t mg_uecc_word_t;
typedef uint64_t mg_uecc_dword_t;

#define HIGH_BIT_SET 0x80000000
#define MG_UECC_WORD_BITS 32
#define MG_UECC_WORD_BITS_SHIFT 5
#define MG_UECC_WORD_BITS_MASK 0x01F

#elif (MG_UECC_WORD_SIZE == 8)

typedef uint64_t mg_uecc_word_t;

#define HIGH_BIT_SET 0x8000000000000000U
#define MG_UECC_WORD_BITS 64
#define MG_UECC_WORD_BITS_SHIFT 6
#define MG_UECC_WORD_BITS_MASK 0x03F

#endif /* MG_UECC_WORD_SIZE */

#endif /* _UECC_TYPES_H_ */

/* Copyright 2015, Kenneth MacKay. Licensed under the BSD 2-clause license. */

#ifndef _UECC_VLI_H_
#define _UECC_VLI_H_

// 
// 

/* Functions for raw large-integer manipulation. These are only available
   if uECC.c is compiled with MG_UECC_ENABLE_VLI_API defined to 1. */
#ifndef MG_UECC_ENABLE_VLI_API
#define MG_UECC_ENABLE_VLI_API 0
#endif

#ifdef __cplusplus
extern "C" {
#endif

#if MG_UECC_ENABLE_VLI_API

void mg_uecc_vli_clear(mg_uecc_word_t *vli, wordcount_t num_words);

/* Constant-time comparison to zero - secure way to compare long integers */
/* Returns 1 if vli == 0, 0 otherwise. */
mg_uecc_word_t mg_uecc_vli_isZero(const mg_uecc_word_t *vli,
                                  wordcount_t num_words);

/* Returns nonzero if bit 'bit' of vli is set. */
mg_uecc_word_t mg_uecc_vli_testBit(const mg_uecc_word_t *vli, bitcount_t bit);

/* Counts the number of bits required to represent vli. */
bitcount_t mg_uecc_vli_numBits(const mg_uecc_word_t *vli,
                               const wordcount_t max_words);

/* Sets dest = src. */
void mg_uecc_vli_set(mg_uecc_word_t *dest, const mg_uecc_word_t *src,
                     wordcount_t num_words);

/* Constant-time comparison function - secure way to compare long integers */
/* Returns one if left == right, zero otherwise */
mg_uecc_word_t mg_uecc_vli_equal(const mg_uecc_word_t *left,
                                 const mg_uecc_word_t *right,
                                 wordcount_t num_words);

/* Constant-time comparison function - secure way to compare long integers */
/* Returns sign of left - right, in constant time. */
cmpresult_t mg_uecc_vli_cmp(const mg_uecc_word_t *left,
                            const mg_uecc_word_t *right, wordcount_t num_words);

/* Computes vli = vli >> 1. */
void mg_uecc_vli_rshift1(mg_uecc_word_t *vli, wordcount_t num_words);

/* Computes result = left + right, returning carry. Can modify in place. */
mg_uecc_word_t mg_uecc_vli_add(mg_uecc_word_t *result,
                               const mg_uecc_word_t *left,
                               const mg_uecc_word_t *right,
                               wordcount_t num_words);

/* Computes result = left - right, returning borrow. Can modify in place. */
mg_uecc_word_t mg_uecc_vli_sub(mg_uecc_word_t *result,
                               const mg_uecc_word_t *left,
                               const mg_uecc_word_t *right,
                               wordcount_t num_words);

/* Computes result = left * right. Result must be 2 * num_words long. */
void mg_uecc_vli_mult(mg_uecc_word_t *result, const mg_uecc_word_t *left,
                      const mg_uecc_word_t *right, wordcount_t num_words);

/* Computes result = left^2. Result must be 2 * num_words long. */
void mg_uecc_vli_square(mg_uecc_word_t *result, const mg_uecc_word_t *left,
                        wordcount_t num_words);

/* Computes result = (left + right) % mod.
   Assumes that left < mod and right < mod, and that result does not overlap
   mod. */
void mg_uecc_vli_modAdd(mg_uecc_word_t *result, const mg_uecc_word_t *left,
                        const mg_uecc_word_t *right, const mg_uecc_word_t *mod,
                        wordcount_t num_words);

/* Computes result = (left - right) % mod.
   Assumes that left < mod and right < mod, and that result does not overlap
   mod. */
void mg_uecc_vli_modSub(mg_uecc_word_t *result, const mg_uecc_word_t *left,
                        const mg_uecc_word_t *right, const mg_uecc_word_t *mod,
                        wordcount_t num_words);

/* Computes result = product % mod, where product is 2N words long.
   Currently only designed to work for mod == curve->p or curve_n. */
void mg_uecc_vli_mmod(mg_uecc_word_t *result, mg_uecc_word_t *product,
                      const mg_uecc_word_t *mod, wordcount_t num_words);

/* Calculates result = product (mod curve->p), where product is up to
   2 * curve->num_words long. */
void mg_uecc_vli_mmod_fast(mg_uecc_word_t *result, mg_uecc_word_t *product,
                           MG_UECC_Curve curve);

/* Computes result = (left * right) % mod.
   Currently only designed to work for mod == curve->p or curve_n. */
void mg_uecc_vli_modMult(mg_uecc_word_t *result, const mg_uecc_word_t *left,
                         const mg_uecc_word_t *right, const mg_uecc_word_t *mod,
                         wordcount_t num_words);

/* Computes result = (left * right) % curve->p. */
void mg_uecc_vli_modMult_fast(mg_uecc_word_t *result,
                              const mg_uecc_word_t *left,
                              const mg_uecc_word_t *right, MG_UECC_Curve curve);

/* Computes result = left^2 % mod.
   Currently only designed to work for mod == curve->p or curve_n. */
void mg_uecc_vli_modSquare(mg_uecc_word_t *result, const mg_uecc_word_t *left,
                           const mg_uecc_word_t *mod, wordcount_t num_words);

/* Computes result = left^2 % curve->p. */
void mg_uecc_vli_modSquare_fast(mg_uecc_word_t *result,
                                const mg_uecc_word_t *left,
                                MG_UECC_Curve curve);

/* Computes result = (1 / input) % mod.*/
void mg_uecc_vli_modInv(mg_uecc_word_t *result, const mg_uecc_word_t *input,
                        const mg_uecc_word_t *mod, wordcount_t num_words);

#if MG_UECC_SUPPORT_COMPRESSED_POINT
/* Calculates a = sqrt(a) (mod curve->p) */
void mg_uecc_vli_mod_sqrt(mg_uecc_word_t *a, MG_UECC_Curve curve);
#endif

/* Converts an integer in uECC native format to big-endian bytes. */
void mg_uecc_vli_nativeToBytes(uint8_t *bytes, int num_bytes,
                               const mg_uecc_word_t *native);
/* Converts big-endian bytes to an integer in uECC native format. */
void mg_uecc_vli_bytesToNative(mg_uecc_word_t *native, const uint8_t *bytes,
                               int num_bytes);

unsigned mg_uecc_curve_num_words(MG_UECC_Curve curve);
unsigned mg_uecc_curve_num_bytes(MG_UECC_Curve curve);
unsigned mg_uecc_curve_num_bits(MG_UECC_Curve curve);
unsigned mg_uecc_curve_num_n_words(MG_UECC_Curve curve);
unsigned mg_uecc_curve_num_n_bytes(MG_UECC_Curve curve);
unsigned mg_uecc_curve_num_n_bits(MG_UECC_Curve curve);

const mg_uecc_word_t *mg_uecc_curve_p(MG_UECC_Curve curve);
const mg_uecc_word_t *mg_uecc_curve_n(MG_UECC_Curve curve);
const mg_uecc_word_t *mg_uecc_curve_G(MG_UECC_Curve curve);
const mg_uecc_word_t *mg_uecc_curve_b(MG_UECC_Curve curve);

int mg_uecc_valid_point(const mg_uecc_word_t *point, MG_UECC_Curve curve);

/* Multiplies a point by a scalar. Points are represented by the X coordinate
   followed by the Y coordinate in the same array, both coordinates are
   curve->num_words long. Note that scalar must be curve->num_n_words long (NOT
   curve->num_words). */
void mg_uecc_point_mult(mg_uecc_word_t *result, const mg_uecc_word_t *point,
                        const mg_uecc_word_t *scalar, MG_UECC_Curve curve);

/* Generates a random integer in the range 0 < random < top.
   Both random and top have num_words words. */
int mg_uecc_generate_random_int(mg_uecc_word_t *random,
                                const mg_uecc_word_t *top,
                                wordcount_t num_words);

#endif /* MG_UECC_ENABLE_VLI_API */

#ifdef __cplusplus
} /* end of extern "C" */
#endif

#endif /* _UECC_VLI_H_ */

// End of uecc BSD-2
// portable8439 v1.0.1
// Source: https://github.com/DavyLandman/portable8439
// Licensed under CC0-1.0
// Contains poly1305-donna e6ad6e091d30d7f4ec2d4f978be1fcfcbce72781 (Public
// Domain)





#if MG_TLS == MG_TLS_BUILTIN
#ifndef __PORTABLE_8439_H
#define __PORTABLE_8439_H
#if defined(__cplusplus)
extern "C" {
#endif

// provide your own decl specificier like -DPORTABLE_8439_DECL=ICACHE_RAM_ATTR
#ifndef PORTABLE_8439_DECL
#define PORTABLE_8439_DECL
#endif

/*
 This library implements RFC 8439 a.k.a. ChaCha20-Poly1305 AEAD

 You can use this library to avoid attackers mutating or reusing your
 encrypted messages. This does assume you never reuse a nonce+key pair and,
 if possible, carefully pick your associated data.
*/

/* Make sure we are either nested in C++ or running in a C99+ compiler
#if !defined(__cplusplus) && !defined(_MSC_VER) && \
    (!defined(__STDC_VERSION__) || __STDC_VERSION__ < 199901L)
#error "C99 or newer required"
#endif */

// #if CHAR_BIT > 8
// #    error "Systems without native octals not suppoted"
// #endif

#if defined(_MSC_VER) || defined(__cplusplus)
// add restrict support is possible
#if (defined(_MSC_VER) && _MSC_VER >= 1900) || defined(__clang__) || \
    defined(__GNUC__)
#define restrict __restrict
#else
#define restrict
#endif
#endif

#define RFC_8439_TAG_SIZE (16)
#define RFC_8439_KEY_SIZE (32)
#define RFC_8439_NONCE_SIZE (12)

/*
    Encrypt/Seal plain text bytes into a cipher text that can only be
    decrypted by knowing the key, nonce and associated data.

    input:
        - key: RFC_8439_KEY_SIZE bytes that all parties have agreed
            upon beforehand
        - nonce: RFC_8439_NONCE_SIZE bytes that should never be repeated
            for the same key. A counter or a pseudo-random value are fine.
        - ad: associated data to include with calculating the tag of the
            cipher text. Can be null for empty.
        - plain_text: data to be encrypted, pointer + size should not overlap
            with cipher_text pointer

    output:
        - cipher_text: encrypted plain_text with a tag appended. Make sure to
            allocate at least plain_text_size + RFC_8439_TAG_SIZE

    returns:
        - size of bytes written to cipher_text, can be -1 if overlapping
            pointers are passed for plain_text and cipher_text
*/
PORTABLE_8439_DECL size_t mg_chacha20_poly1305_encrypt(
    uint8_t *restrict cipher_text, const uint8_t key[RFC_8439_KEY_SIZE],
    const uint8_t nonce[RFC_8439_NONCE_SIZE], const uint8_t *restrict ad,
    size_t ad_size, const uint8_t *restrict plain_text, size_t plain_text_size);

/*
    Decrypt/unseal cipher text given the right key, nonce, and additional data.

    input:
        - key: RFC_8439_KEY_SIZE bytes that all parties have agreed
            upon beforehand
        - nonce: RFC_8439_NONCE_SIZE bytes that should never be repeated for
            the same key. A counter or a pseudo-random value are fine.
        - ad: associated data to include with calculating the tag of the
            cipher text. Can be null for empty.
        - cipher_text: encrypted message.

    output:
        - plain_text: data to be encrypted, pointer + size should not overlap
            with cipher_text pointer, leave at least enough room for
            cipher_text_size - RFC_8439_TAG_SIZE

    returns:
        - size of bytes written to plain_text, -1 signals either:
            - incorrect key/nonce/ad
            - corrupted cipher_text
            - overlapping pointers are passed for plain_text and cipher_text
*/
PORTABLE_8439_DECL size_t mg_chacha20_poly1305_decrypt(
    uint8_t *restrict plain_text, const uint8_t key[RFC_8439_KEY_SIZE],
    const uint8_t nonce[RFC_8439_NONCE_SIZE],
    const uint8_t *restrict ad, size_t ad_size,
    const uint8_t *restrict cipher_text, size_t cipher_text_size);
#if defined(__cplusplus)
}
#endif
#endif
#endif
#ifndef TLS_RSA_H
#define TLS_RSA_H


int mg_rsa_mod_pow(const uint8_t *mod, size_t modsz, const uint8_t *exp, size_t expsz, const uint8_t *msg, size_t msgsz, uint8_t *out, size_t outsz);
int mg_rsa_crt_sign(const uint8_t *em, size_t em_len,
                    const uint8_t *dP, size_t dP_len,
                    const uint8_t *dQ, size_t dQ_len,
                    const uint8_t *p, size_t p_len,
                    const uint8_t *q, size_t q_len,
                    const uint8_t *qInv, size_t qInv_len,
                    uint8_t *signature, size_t sig_len);
#endif // TLS_RSA_H







#if MG_TLS == MG_TLS_MBED
#include <mbedtls/debug.h>
#include <mbedtls/error.h>
#include <mbedtls/net_sockets.h>
#include <mbedtls/ssl.h>
#include <mbedtls/ssl_ticket.h>

struct mg_tls_ctx {
  int dummy;
#ifdef MBEDTLS_SSL_SESSION_TICKETS
  mbedtls_ssl_ticket_context tickets;
#endif
};

struct mg_tls {
  mbedtls_x509_crt ca;      // Parsed CA certificate
  mbedtls_x509_crt cert;    // Parsed certificate
  mbedtls_pk_context pk;    // Private key context
  mbedtls_ssl_context ssl;  // SSL/TLS context
  mbedtls_ssl_config conf;  // SSL-TLS config
#ifdef MBEDTLS_SSL_SESSION_TICKETS
  mbedtls_ssl_ticket_context ticket;  // Session tickets context
#endif
  // https://github.com/Mbed-TLS/mbedtls/blob/3b3c652d/include/mbedtls/ssl.h#L5071C18-L5076C29
  unsigned char *throttled_buf;  // see #3074
  size_t throttled_len;
};
#endif


#if MG_TLS == MG_TLS_OPENSSL || MG_TLS == MG_TLS_WOLFSSL

#include <openssl/err.h>
#include <openssl/ssl.h>

struct mg_tls {
  BIO_METHOD *bm;
  SSL_CTX *ctx;
  SSL *ssl;
};
#endif


#define WEBSOCKET_OP_CONTINUE 0
#define WEBSOCKET_OP_TEXT 1
#define WEBSOCKET_OP_BINARY 2
#define WEBSOCKET_OP_CLOSE 8
#define WEBSOCKET_OP_PING 9
#define WEBSOCKET_OP_PONG 10



struct mg_ws_message {
  struct mg_str data;  // Websocket message data
  uint8_t flags;       // Websocket message flags
};

struct mg_connection *mg_ws_connect(struct mg_mgr *, const char *url,
                                    mg_event_handler_t fn, void *fn_data,
                                    const char *fmt, ...);
void mg_ws_upgrade(struct mg_connection *, struct mg_http_message *,
                   const char *fmt, ...);
size_t mg_ws_send(struct mg_connection *, const void *buf, size_t len, int op);
size_t mg_ws_wrap(struct mg_connection *, size_t len, int op);
size_t mg_ws_printf(struct mg_connection *c, int op, const char *fmt, ...);
size_t mg_ws_vprintf(struct mg_connection *c, int op, const char *fmt,
                     va_list *);




struct mg_connection *mg_sntp_connect(struct mg_mgr *mgr, const char *url,
                                      mg_event_handler_t fn, void *fn_data);
void mg_sntp_request(struct mg_connection *c);
int64_t mg_sntp_parse(const unsigned char *buf, size_t len);

uint64_t mg_now(void);     // Return milliseconds since Epoch





#define MQTT_CMD_CONNECT 1
#define MQTT_CMD_CONNACK 2
#define MQTT_CMD_PUBLISH 3
#define MQTT_CMD_PUBACK 4
#define MQTT_CMD_PUBREC 5
#define MQTT_CMD_PUBREL 6
#define MQTT_CMD_PUBCOMP 7
#define MQTT_CMD_SUBSCRIBE 8
#define MQTT_CMD_SUBACK 9
#define MQTT_CMD_UNSUBSCRIBE 10
#define MQTT_CMD_UNSUBACK 11
#define MQTT_CMD_PINGREQ 12
#define MQTT_CMD_PINGRESP 13
#define MQTT_CMD_DISCONNECT 14
#define MQTT_CMD_AUTH 15

#define MQTT_PROP_PAYLOAD_FORMAT_INDICATOR 0x01
#define MQTT_PROP_MESSAGE_EXPIRY_INTERVAL 0x02
#define MQTT_PROP_CONTENT_TYPE 0x03
#define MQTT_PROP_RESPONSE_TOPIC 0x08
#define MQTT_PROP_CORRELATION_DATA 0x09
#define MQTT_PROP_SUBSCRIPTION_IDENTIFIER 0x0B
#define MQTT_PROP_SESSION_EXPIRY_INTERVAL 0x11
#define MQTT_PROP_ASSIGNED_CLIENT_IDENTIFIER 0x12
#define MQTT_PROP_SERVER_KEEP_ALIVE 0x13
#define MQTT_PROP_AUTHENTICATION_METHOD 0x15
#define MQTT_PROP_AUTHENTICATION_DATA 0x16
#define MQTT_PROP_REQUEST_PROBLEM_INFORMATION 0x17
#define MQTT_PROP_WILL_DELAY_INTERVAL 0x18
#define MQTT_PROP_REQUEST_RESPONSE_INFORMATION 0x19
#define MQTT_PROP_RESPONSE_INFORMATION 0x1A
#define MQTT_PROP_SERVER_REFERENCE 0x1C
#define MQTT_PROP_REASON_STRING 0x1F
#define MQTT_PROP_RECEIVE_MAXIMUM 0x21
#define MQTT_PROP_TOPIC_ALIAS_MAXIMUM 0x22
#define MQTT_PROP_TOPIC_ALIAS 0x23
#define MQTT_PROP_MAXIMUM_QOS 0x24
#define MQTT_PROP_RETAIN_AVAILABLE 0x25
#define MQTT_PROP_USER_PROPERTY 0x26
#define MQTT_PROP_MAXIMUM_PACKET_SIZE 0x27
#define MQTT_PROP_WILDCARD_SUBSCRIPTION_AVAILABLE 0x28
#define MQTT_PROP_SUBSCRIPTION_IDENTIFIER_AVAILABLE 0x29
#define MQTT_PROP_SHARED_SUBSCRIPTION_AVAILABLE 0x2A

enum {
  MQTT_PROP_TYPE_BYTE,
  MQTT_PROP_TYPE_STRING,
  MQTT_PROP_TYPE_STRING_PAIR,
  MQTT_PROP_TYPE_BINARY_DATA,
  MQTT_PROP_TYPE_VARIABLE_INT,
  MQTT_PROP_TYPE_INT,
  MQTT_PROP_TYPE_SHORT
};

enum { MQTT_OK, MQTT_INCOMPLETE, MQTT_MALFORMED };

struct mg_mqtt_prop {
  uint8_t id;         // Enumerated at MQTT5 Reference
  uint32_t iv;        // Integer value for 8-, 16-, 32-bit integers types
  struct mg_str key;  // Non-NULL only for user property type
  struct mg_str val;  // Non-NULL only for UTF-8 types and user properties
};

struct mg_mqtt_opts {
  struct mg_str user;               // Username, can be empty
  struct mg_str pass;               // Password, can be empty
  struct mg_str client_id;          // Client ID
  struct mg_str topic;              // message/subscription topic
  struct mg_str message;            // message content
  uint8_t qos;                      // message quality of service
  uint8_t version;                  // Can be 4 (3.1.1), or 5. If 0, assume 4
  uint16_t keepalive;               // Keep-alive timer in seconds
  uint16_t retransmit_id;           // For PUBLISH, init to 0
  bool retain;                      // Retain flag
  bool clean;                       // Clean session flag
  struct mg_mqtt_prop *props;       // MQTT5 props array
  size_t num_props;                 // number of props
  struct mg_mqtt_prop *will_props;  // Valid only for CONNECT packet (MQTT5)
  size_t num_will_props;            // Number of will props
};

struct mg_mqtt_message {
  struct mg_str topic;  // Parsed topic for PUBLISH
  struct mg_str data;   // Parsed message for PUBLISH
  struct mg_str dgram;  // Whole MQTT packet, including headers
  uint16_t id;          // For PUBACK, PUBREC, PUBREL, PUBCOMP, SUBACK, PUBLISH
  uint8_t cmd;          // MQTT command, one of MQTT_CMD_*
  uint8_t qos;          // Quality of service
  uint8_t ack;          // CONNACK return code, 0 = success
  size_t props_start;   // Offset to the start of the properties (MQTT5)
  size_t props_size;    // Length of the properties
};

struct mg_connection *mg_mqtt_connect(struct mg_mgr *, const char *url,
                                      const struct mg_mqtt_opts *opts,
                                      mg_event_handler_t fn, void *fn_data);
struct mg_connection *mg_mqtt_listen(struct mg_mgr *mgr, const char *url,
                                     mg_event_handler_t fn, void *fn_data);
void mg_mqtt_login(struct mg_connection *c, const struct mg_mqtt_opts *opts);
uint16_t mg_mqtt_pub(struct mg_connection *c, const struct mg_mqtt_opts *opts);
void mg_mqtt_sub(struct mg_connection *, const struct mg_mqtt_opts *opts);
void mg_mqtt_unsub(struct mg_connection *c, const struct mg_mqtt_opts *opts);
int mg_mqtt_parse(const uint8_t *, size_t, uint8_t, struct mg_mqtt_message *);
void mg_mqtt_send_header(struct mg_connection *, uint8_t cmd, uint8_t flags,
                         uint32_t len);
void mg_mqtt_ping(struct mg_connection *);
void mg_mqtt_pong(struct mg_connection *);
void mg_mqtt_disconnect(struct mg_connection *, const struct mg_mqtt_opts *);
size_t mg_mqtt_next_prop(struct mg_mqtt_message *, struct mg_mqtt_prop *,
                         size_t ofs);





#define MG_DNS_RTYPE_A 1
#define MG_DNS_RTYPE_PTR 12
#define MG_DNS_RTYPE_TXT 16
#define MG_DNS_RTYPE_AAAA 28
#define MG_DNS_RTYPE_SRV 33

// Mongoose sends DNS queries that contain only one question:
// either A (IPv4) or AAAA (IPv6) address lookup.
// Therefore, we expect zero or one answer.
// If `resolved` is true, then `addr` contains resolved IPv4 or IPV6 address.
struct mg_dns_message {
  uint16_t txnid;       // Transaction ID
  bool resolved;        // Resolve successful, addr is set
  struct mg_addr addr;  // Resolved address
  char name[256];       // Host name
};

struct mg_dns_header {
  uint16_t txnid;  // Transaction ID
  uint16_t flags;
  uint16_t num_questions;
  uint16_t num_answers;
  uint16_t num_authority_prs;
  uint16_t num_other_prs;
};

// DNS resource record
struct mg_dns_rr {
  uint16_t nlen;    // Name or pointer length
  uint16_t atype;   // Address type
  uint16_t aclass;  // Address class
  uint16_t alen;    // Address length
};

// DNS-SD response record
struct mg_dnssd_record {
  struct mg_str srvcproto;  // service.proto, service name
  struct mg_str txt;        // TXT record contents
  uint16_t port;            // SRV record port
};

// mDNS request and response data structs passed to event handlers
struct mg_mdns_req {
  struct mg_dns_rr *rr;
  struct mg_dnssd_record *r;
  struct mg_str reqname;   // requested name in RR
  struct mg_str respname;  // actual name to use in response
  struct mg_addr *addr;    // actual address to use in response
  bool is_listing;
  bool is_resp;
  bool is_unicast;
};

struct mg_mdns_resp {
  struct mg_dns_rr *rr;
  // TODO(scaprile )struct mg_str srvcproto; struct mg_str txt; uint16_t port; ?
  struct mg_str name;
  struct mg_addr addr;
  // TODO(scaprile); bool has_A; bool has_PTR; bool has_SRV; bool has_TXT; ?
};

void mg_resolve(struct mg_connection *, const char *url);
void mg_resolve_cancel(struct mg_connection *);
bool mg_dns_parse(const uint8_t *buf, size_t len, struct mg_dns_message *);
size_t mg_dns_parse_rr(const uint8_t *buf, size_t len, size_t ofs,
                       bool is_question, struct mg_dns_rr *);

struct mg_connection *mg_mdns_listen(struct mg_mgr *mgr, mg_event_handler_t fn,
                                     void *fn_data);
bool mg_mdns_query(struct mg_connection *, const char *, unsigned int);





#ifndef MG_JSON_MAX_DEPTH
#define MG_JSON_MAX_DEPTH 30
#endif

// Error return values - negative. Successful returns are >= 0
enum { MG_JSON_TOO_DEEP = -1, MG_JSON_INVALID = -2, MG_JSON_NOT_FOUND = -3 };
int mg_json_get(struct mg_str json, const char *path, int *toklen);

struct mg_str mg_json_get_tok(struct mg_str json, const char *path);
bool mg_json_get_num(struct mg_str json, const char *path, double *v);
bool mg_json_get_bool(struct mg_str json, const char *path, bool *v);
long mg_json_get_long(struct mg_str json, const char *path, long dflt);
char *mg_json_get_str(struct mg_str json, const char *path);
char *mg_json_get_hex(struct mg_str json, const char *path, int *len);
char *mg_json_get_b64(struct mg_str json, const char *path, int *len);

bool mg_json_unescape(struct mg_str str, char *buf, size_t len);
size_t mg_json_next(struct mg_str obj, size_t ofs, struct mg_str *key,
                    struct mg_str *val);




// JSON-RPC request descriptor
struct mg_rpc_req {
  struct mg_rpc **head;  // RPC handlers list head
  struct mg_rpc *rpc;    // RPC handler being called
  mg_pfn_t pfn;          // Response printing function
  void *pfn_data;        // Response printing function data
  void *req_data;        // Arbitrary request data
  struct mg_str frame;   // Request, e.g. {"id":1,"method":"add","params":[1,2]}
};

// JSON-RPC method handler
struct mg_rpc {
  struct mg_rpc *next;              // Next in list
  struct mg_str method;             // Method pattern
  void (*fn)(struct mg_rpc_req *);  // Handler function
  void *fn_data;                    // Handler function argument
};

void mg_rpc_add(struct mg_rpc **head, struct mg_str method_pattern,
                void (*handler)(struct mg_rpc_req *), void *handler_data);
void mg_rpc_del(struct mg_rpc **head, void (*handler)(struct mg_rpc_req *));
void mg_rpc_process(struct mg_rpc_req *);

// Helper functions to print result or error frame
void mg_rpc_ok(struct mg_rpc_req *, const char *fmt, ...);
void mg_rpc_vok(struct mg_rpc_req *, const char *fmt, va_list *ap);
void mg_rpc_err(struct mg_rpc_req *, int code, const char *fmt, ...);
void mg_rpc_verr(struct mg_rpc_req *, int code, const char *fmt, va_list *);
void mg_rpc_list(struct mg_rpc_req *r);
// Copyright (c) 2023 Cesanta Software Limited
// All rights reserved





#define MG_OTA_NONE 0       // No OTA support
#define MG_OTA_STM32H5 1    // STM32 H5
#define MG_OTA_STM32H7 2    // STM32 H7
#define MG_OTA_STM32H7_DUAL_CORE 3 // STM32 H7 dual core
#define MG_OTA_STM32F  4    // STM32 F7/F4/F2
#define MG_OTA_CH32V307 100 // WCH CH32V307
#define MG_OTA_U2A 200      // Renesas U2A16, U2A8, U2A6
#define MG_OTA_RT1020 300   // IMXRT1020
#define MG_OTA_RT1050 301   // IMXRT1050
#define MG_OTA_RT1060 302   // IMXRT1060
#define MG_OTA_RT1064 303   // IMXRT1064
#define MG_OTA_RT1170 304   // IMXRT1170
#define MG_OTA_MCXN 310 	// MCXN947
#define MG_OTA_RW612 320    // FRDM-RW612
#define MG_OTA_FLASH 900    // OTA via an internal flash
#define MG_OTA_ESP32 910    // ESP32 OTA implementation
#define MG_OTA_PICOSDK 920  // RP2040/2350 using Pico-SDK hardware_flash
#define MG_OTA_CUSTOM 1000  // Custom implementation

#ifndef MG_OTA
#define MG_OTA MG_OTA_NONE
#else
#ifndef MG_IRAM
#if defined(__GNUC__)
#define MG_IRAM __attribute__((noinline, section(".iram")))
#else
#define MG_IRAM
#endif // compiler
#endif // IRAM
#endif // OTA

// Firmware update API
bool mg_ota_begin(size_t new_firmware_size);     // Start writing
bool mg_ota_write(const void *buf, size_t len);  // Write chunk, aligned to 1k
bool mg_ota_end(void);                           // Stop writing



#if MG_OTA != MG_OTA_NONE && MG_OTA != MG_OTA_CUSTOM

struct mg_flash {
  void *start;    // Address at which flash starts
  size_t size;    // Flash size
  size_t secsz;   // Sector size
  size_t align;   // Write alignment
  bool (*write_fn)(void *, const void *, size_t);  // Write function
  bool (*swap_fn)(void);                           // Swap partitions
};

bool mg_ota_flash_begin(size_t new_firmware_size, struct mg_flash *flash);
bool mg_ota_flash_write(const void *buf, size_t len, struct mg_flash *flash);
bool mg_ota_flash_end(struct mg_flash *flash);

#endif






struct mg_wifi_data {
  char *ssid, *pass;      // STA mode, SSID to connect to
  char *apssid, *appass;  // AP mode, our SSID
  uint32_t apip, apmask;  // AP mode, our IP address and mask
  uint8_t security;       // STA mode, TBD
  uint8_t apsecurity;     // AP mode, TBD
  uint8_t apchannel;      // AP mode, channel to use
  bool apmode;  // start in AP mode; 'false' -> connect to 'ssid' != NULL
};

struct mg_wifi_scan_bss_data {
  struct mg_str SSID;
  char *BSSID;
  int16_t RSSI;
  uint8_t security;
#define MG_WIFI_SECURITY_OPEN 0
#define MG_WIFI_SECURITY_WEP MG_BIT(0)
#define MG_WIFI_SECURITY_WPA MG_BIT(1)
#define MG_WIFI_SECURITY_WPA2 MG_BIT(2)
#define MG_WIFI_SECURITY_WPA3 MG_BIT(3)
#define MG_WIFI_SECURITY_WPA_ENTERPRISE MG_BIT(4)
#define MG_WIFI_SECURITY_WPA2_ENTERPRISE MG_BIT(5)
#define MG_WIFI_SECURITY_WPA3_ENTERPRISE MG_BIT(6)
  uint8_t channel;
  unsigned band : 2;
#define MG_WIFI_BAND_2G 0
#define MG_WIFI_BAND_5G 1
  unsigned has_n : 1;
  unsigned has_ac : 1;
  unsigned has_ax : 1;
};

bool mg_wifi_scan(void);
bool mg_wifi_connect(struct mg_wifi_data *);
bool mg_wifi_disconnect(void);
bool mg_wifi_ap_start(struct mg_wifi_data *);
bool mg_wifi_ap_stop(void);





#if MG_ENABLE_TCPIP

// no config defaults to 0 => Ethernet
enum mg_l2type { MG_TCPIP_L2_ETH = 0, MG_TCPIP_L2_PPP };  // MG_TCPIP_L2_PPPoE

#if defined(__DCC__)
#pragma pack(1)
#else
#pragma pack(push, 1)
#endif

struct mg_l2addr {
  union {
    uint8_t mac[6];
  } addr;
};

#if defined(__DCC__)
#pragma pack(0)
#else
#pragma pack(pop)
#endif

#if 0
TODO(): ?
struct eth_opts {
  bool enable_crc32_check;         // Do a CRC check on RX frames and strip it
  bool enable_mac_check;           // Do a MAC check on RX frames
};
struct mg_l2opts {
  union {
    struct eth_opts eth;
  };
};
#endif

enum mg_l2proto {
  MG_TCPIP_L2PROTO_IPV4 = 0,
  MG_TCPIP_L2PROTO_IPV6,
  MG_TCPIP_L2PROTO_ARP,
  MG_TCPIP_L2PROTO_PPPoE_DISC,
  MG_TCPIP_L2PROTO_PPPoE_SESS
};
enum mg_l2addrtype {
  MG_TCPIP_L2ADDR_BCAST,
  MG_TCPIP_L2ADDR_MCAST,
  MG_TCPIP_L2ADDR_MCAST6
};

#endif








#if MG_ENABLE_TCPIP

struct mg_tcpip_if;  // Mongoose TCP/IP network interface

struct mg_tcpip_driver {
  bool (*init)(struct mg_tcpip_if *);                         // Init driver
  size_t (*tx)(const void *, size_t, struct mg_tcpip_if *);   // Transmit frame
  size_t (*rx)(void *buf, size_t len, struct mg_tcpip_if *);  // Receive frame
  bool (*poll)(struct mg_tcpip_if *, bool);  // Poll, return Up/down status
};

typedef void (*mg_tcpip_event_handler_t)(struct mg_tcpip_if *ifp, int ev,
                                         void *ev_data);

enum {
  MG_TCPIP_EV_ST_CHG,  // state change                   uint8_t * (&ifp->state)
  MG_TCPIP_EV_DHCP_DNS,   // DHCP DNS assignment            uint32_t *ipaddr
  MG_TCPIP_EV_DHCP_SNTP,  // DHCP SNTP assignment           uint32_t *ipaddr
  MG_TCPIP_EV_ARP,        // Got ARP packet                 struct mg_str *
  MG_TCPIP_EV_TIMER_1S,   // 1 second timer                 NULL
  MG_TCPIP_EV_WIFI_SCAN_RESULT,  // Wi-Fi scan results             struct
                                 // mg_wifi_scan_bss_data *
  MG_TCPIP_EV_WIFI_SCAN_END,     // Wi-Fi scan has finished        NULL
  MG_TCPIP_EV_WIFI_CONNECT_ERR,  // Wi-Fi connect has failed       driver and
                                 // chip specific
  MG_TCPIP_EV_DRIVER,   // Driver event                   driver specific
  MG_TCPIP_EV_ST6_CHG,  // state6 change                  uint8_t *
                        // (&ifp->state6)
  MG_TCPIP_EV_USER      // Starting ID for user events
};

// Network interface
struct mg_tcpip_if {
  uint8_t mac[sizeof(struct mg_l2addr)];  // hw address. Set to a valid addr
  uint32_t ip, mask, gw;                  // IP address, mask, default gateway
  struct mg_str tx;                       // Output (TX) buffer
  bool enable_dhcp_client;                // Enable DCHP client
  bool enable_dhcp_server;                // Enable DCHP server
  bool enable_get_gateway;                // DCHP server sets client as gateway
  bool enable_req_dns;                    // DCHP client requests DNS server
  bool enable_req_sntp;                   // DCHP client requests SNTP server
  bool enable_crc32_check;         // Do a CRC check on RX frames and strip it
  bool enable_mac_check;           // Do a MAC check on RX frames
  bool update_mac_hash_table;      // Signal drivers to update MAC controller
  struct mg_tcpip_driver *driver;  // Low level driver
  void *driver_data;               // Driver-specific data
  mg_tcpip_event_handler_t pfn;    // Driver-specific event handler function
  mg_tcpip_event_handler_t fn;     // User-specified event handler function
  struct mg_mgr *mgr;              // Mongoose event manager
  struct mg_queue recv_queue;      // Receive queue
  char dhcp_name[MG_TCPIP_DHCPNAME_SIZE];  // Name for DHCP, "mip" if unset
  uint16_t mtu;                            // Interface link payload
  uint16_t framesize;                      // Interface frame max length
#if MG_ENABLE_IPV6
  uint64_t ip6ll[2], ip6[2];  // IPv6 link-local and global addresses,
  uint8_t prefix[8];          // prefix,
  uint8_t prefix_len;         // prefix length,
  uint64_t gw6[2];            // default gateway.
  bool enable_slaac;          // Enable IPv6 address autoconfiguration
  bool enable_dhcp6_client;   // Enable DCHPv6 client TODO()
#endif

  // Internal state, user can use it but should not change it
  uint8_t gwmac[sizeof(struct mg_l2addr)];  // Router's hw address
  enum mg_l2type l2type;                    // Ethernet, PPP, etc.
  char *dns4_url;                           // DNS server URL
  uint64_t now;                             // Current time
  uint64_t timer_1000ms;        // 1000 ms timer: for DHCP and link state
  uint64_t lease_expire;        // Lease expiration time, in ms
  uint16_t eport;               // Next ephemeral port
  volatile uint32_t ndrop;      // Number of received, but dropped frames
  volatile uint32_t nrecv;      // Number of received frames
  volatile uint32_t nsent;      // Number of transmitted frames
  volatile uint32_t nerr;       // Number of driver errors
  uint8_t state;                // Current link and IPv4 state
#define MG_TCPIP_STATE_DOWN 0   // Interface is down
#define MG_TCPIP_STATE_UP 1     // Interface is up
#define MG_TCPIP_STATE_REQ 2    // Interface is up, DHCP REQUESTING state
#define MG_TCPIP_STATE_IP 3     // Interface is up and has an IP assigned
#define MG_TCPIP_STATE_READY 4  // Interface has fully come up, ready to work
  bool gw_ready;                // We've got a hw address for the router
#if MG_ENABLE_IPV6
  uint8_t gw6mac[sizeof(struct mg_l2addr)];  // IPV6 Router's hw address
  uint8_t state6;                            // Current IPv6 state
  bool gw6_ready;  // We've got a hw address for the IPv6 router
#endif
};

void mg_tcpip_init(struct mg_mgr *, struct mg_tcpip_if *);
void mg_tcpip_free(struct mg_tcpip_if *);
void mg_tcpip_qwrite(void *buf, size_t len, struct mg_tcpip_if *ifp);
void mg_tcpip_arp_request(struct mg_tcpip_if *ifp, uint32_t ip, uint8_t *mac);

extern struct mg_tcpip_driver mg_tcpip_driver_stm32f;
extern struct mg_tcpip_driver mg_tcpip_driver_w5500;
extern struct mg_tcpip_driver mg_tcpip_driver_w5100;
extern struct mg_tcpip_driver mg_tcpip_driver_tm4c;
extern struct mg_tcpip_driver mg_tcpip_driver_tms570;
extern struct mg_tcpip_driver mg_tcpip_driver_stm32h;
extern struct mg_tcpip_driver mg_tcpip_driver_imxrt;
extern struct mg_tcpip_driver mg_tcpip_driver_same54;
extern struct mg_tcpip_driver mg_tcpip_driver_cmsis;
extern struct mg_tcpip_driver mg_tcpip_driver_ra;
extern struct mg_tcpip_driver mg_tcpip_driver_xmc;
extern struct mg_tcpip_driver mg_tcpip_driver_xmc7;
extern struct mg_tcpip_driver mg_tcpip_driver_ppp;
extern struct mg_tcpip_driver mg_tcpip_driver_pico_w;
extern struct mg_tcpip_driver mg_tcpip_driver_rw612;
extern struct mg_tcpip_driver mg_tcpip_driver_cyw;
extern struct mg_tcpip_driver mg_tcpip_driver_nxp_wifi;
extern struct mg_tcpip_driver mg_tcpip_driver_st67w6;

// Drivers that require SPI, can use this SPI abstraction
struct mg_tcpip_spi {
  void *spi;                        // Opaque SPI bus descriptor
  void (*begin)(void *);            // SPI begin: slave select low
  void (*end)(void *);              // SPI end: slave select high
  uint8_t (*txn)(void *, uint8_t);  // SPI transaction: write 1 byte, read reply
};

// Alignment and memory section requirements
#ifndef MG_8BYTE_ALIGNED
#if defined(__GNUC__)
#define MG_8BYTE_ALIGNED __attribute__((aligned((8U))))
#else
#define MG_8BYTE_ALIGNED
#endif  // compiler
#endif  // 8BYTE_ALIGNED

#ifndef MG_16BYTE_ALIGNED
#if defined(__GNUC__)
#define MG_16BYTE_ALIGNED __attribute__((aligned((16U))))
#else
#define MG_16BYTE_ALIGNED
#endif  // compiler
#endif  // 16BYTE_ALIGNED

#ifndef MG_32BYTE_ALIGNED
#if defined(__GNUC__)
#define MG_32BYTE_ALIGNED __attribute__((aligned((32U))))
#else
#define MG_32BYTE_ALIGNED
#endif  // compiler
#endif  // 32BYTE_ALIGNED

#ifndef MG_64BYTE_ALIGNED
#if defined(__GNUC__)
#define MG_64BYTE_ALIGNED __attribute__((aligned((64U))))
#else
#define MG_64BYTE_ALIGNED
#endif  // compiler
#endif  // 64BYTE_ALIGNED

#ifndef MG_ETH_RAM
#define MG_ETH_RAM
#endif

#endif


#if MG_ENABLE_TCPIP && defined(MG_ENABLE_DRIVER_CMSIS) && MG_ENABLE_DRIVER_CMSIS

#include "Driver_ETH_MAC.h"  // keep this include
#include "Driver_ETH_PHY.h"  // keep this include

#endif


#if MG_ENABLE_TCPIP &&                                          \
    ((defined(MG_ENABLE_DRIVER_CYW) && MG_ENABLE_DRIVER_CYW) || \
     (defined(MG_ENABLE_DRIVER_CYW_SDIO) && MG_ENABLE_DRIVER_CYW_SDIO))

struct mg_tcpip_spi_ {
  void *spi;              // Opaque SPI bus descriptor
  void (*begin)(void *);  // SPI begin: slave select low
  void (*end)(void *);    // SPI end: slave select high
  void (*txn)(void *, uint8_t *, uint8_t *,
              size_t len);  // SPI transaction: write-read len bytes
};

struct mg_tcpip_driver_cyw_firmware {
  const uint8_t *code_addr;
  size_t code_len;
  const uint8_t *nvram_addr;
  size_t nvram_len;
  const uint8_t *clm_addr;
  size_t clm_len;
};

struct mg_tcpip_driver_cyw_data {
  struct mg_wifi_data wifi;
  void *bus;
  struct mg_tcpip_driver_cyw_firmware *fw;
  bool hs;  // use chip "high-speed" mode; otherwise SPI CPOL0 CPHA0 (DS 4.2.3 Table 6)
};

#define MG_TCPIP_DRIVER_INIT(mgr)                              \
  do {                                                         \
    static struct mg_tcpip_driver_cyw_data driver_data_;       \
    static struct mg_tcpip_if mif_;                            \
    MG_SET_WIFI_CONFIG(&driver_data_);                         \
    mif_.ip = MG_TCPIP_IP;                                     \
    mif_.mask = MG_TCPIP_MASK;                                 \
    mif_.gw = MG_TCPIP_GW;                                     \
    mif_.driver = &mg_tcpip_driver_cyw;                        \
    mif_.driver_data = &driver_data_;                          \
    mif_.recv_queue.size = 8192;                               \
    mif_.mac[0] = 2; /* MAC read from OTP at driver init */    \
    mg_tcpip_init(mgr, &mif_);                                 \
    MG_INFO(("Driver: cyw, MAC: %M", mg_print_mac, mif_.mac)); \
  } while (0)

#endif


#if MG_ENABLE_TCPIP && \
  (defined(MG_ENABLE_DRIVER_IMXRT10) && MG_ENABLE_DRIVER_IMXRT10) || \
  (defined(MG_ENABLE_DRIVER_IMXRT11) && MG_ENABLE_DRIVER_IMXRT11) || \
  (defined(MG_ENABLE_DRIVER_MCXE) && MG_ENABLE_DRIVER_MCXE)

struct mg_tcpip_driver_imxrt_data {
  // MDC clock divider. MDC clock is derived from IPS Bus clock (ipg_clk),
  // must not exceed 2.5MHz. Configuration for clock range 2.36~2.50 MHz
  // 37.5.1.8.2, Table 37-46 : f = ipg_clk / (2(mdc_cr + 1))
  //    ipg_clk       mdc_cr VALUE
  //    --------------------------
  //                  -1  <-- TODO() tell driver to guess the value
  //    25 MHz         4
  //    33 MHz         6
  //    40 MHz         7
  //    50 MHz         9
  //    66 MHz        13
  int mdc_cr;  // Valid values: -1 to 63

  uint8_t phy_addr;  // PHY address
};

#ifndef MG_TCPIP_PHY_ADDR
#define MG_TCPIP_PHY_ADDR 2
#endif

#ifndef MG_DRIVER_MDC_CR
#define MG_DRIVER_MDC_CR 24
#endif

#define MG_TCPIP_DRIVER_INIT(mgr)                                \
  do {                                                           \
    static struct mg_tcpip_driver_imxrt_data driver_data_;       \
    static struct mg_tcpip_if mif_;                              \
    driver_data_.mdc_cr = MG_DRIVER_MDC_CR;                      \
    driver_data_.phy_addr = MG_TCPIP_PHY_ADDR;                   \
    mif_.ip = MG_TCPIP_IP;                                       \
    mif_.mask = MG_TCPIP_MASK;                                   \
    mif_.gw = MG_TCPIP_GW;                                       \
    mif_.driver = &mg_tcpip_driver_imxrt;                        \
    mif_.driver_data = &driver_data_;                            \
    MG_SET_MAC_ADDRESS(mif_.mac);                                \
    mg_tcpip_init(mgr, &mif_);                                   \
    MG_INFO(("Driver: imxrt, MAC: %M", mg_print_mac, mif_.mac)); \
  } while (0)

#endif


#if MG_ENABLE_TCPIP  && \
    defined(MG_ENABLE_DRIVER_NXP_WIFI) && MG_ENABLE_DRIVER_NXP_WIFI


struct mg_tcpip_driver_nxp_wifi_data {
  struct mg_wifi_data wifi;
};


#define MG_TCPIP_DRIVER_INIT(mgr)                                   \
  do {                                                              \
    static struct mg_tcpip_driver_nxp_wifi_data driver_data_;       \
    static struct mg_tcpip_if mif_;                                 \
    MG_SET_WIFI_CONFIG(&driver_data_);                              \
    mif_.ip = MG_TCPIP_IP;                                          \
    mif_.mask = MG_TCPIP_MASK;                                      \
    mif_.gw = MG_TCPIP_GW;                                          \
    mif_.driver = &mg_tcpip_driver_nxp_wifi;                        \
    mif_.driver_data = &driver_data_;                               \
    mif_.recv_queue.size = 8192;                                    \
    mif_.mac[0] = 2; /* MAC read from OTP at driver init */         \
    mg_tcpip_init(mgr, &mif_);                                      \
    MG_INFO(("Driver: nxp wifi, MAC: %M", mg_print_mac, mif_.mac)); \
  } while (0)

#endif




struct mg_phy {
  uint16_t (*read_reg)(uint8_t addr, uint8_t reg);
  void (*write_reg)(uint8_t addr, uint8_t reg, uint16_t value);
};

// PHY configuration settings, bitmask
enum {
  // Set if PHY LEDs are connected to ground
  MG_PHY_LEDS_ACTIVE_HIGH = (1 << 0),
  // Set when PHY clocks MAC. Otherwise, MAC clocks PHY
  MG_PHY_CLOCKS_MAC = (1 << 1)
};

enum { MG_PHY_SPEED_10M, MG_PHY_SPEED_100M, MG_PHY_SPEED_1000M };

void mg_phy_init(struct mg_phy *, uint8_t addr, uint8_t config);
bool mg_phy_up(struct mg_phy *, uint8_t addr, bool *full_duplex,
               uint8_t *speed);


#if MG_ENABLE_TCPIP && MG_ARCH == MG_ARCH_PICOSDK && \
    defined(MG_ENABLE_DRIVER_PICO_W) && MG_ENABLE_DRIVER_PICO_W

#include "cyw43.h"              // keep this include
#include "pico/cyw43_arch.h"    // keep this include
#include "pico/unique_id.h"     // keep this include

struct mg_tcpip_driver_pico_w_data {
  struct mg_wifi_data wifi;
};

#define MG_TCPIP_DRIVER_INIT(mgr)                                 \
  do {                                                            \
    static struct mg_tcpip_driver_pico_w_data driver_data_;       \
    static struct mg_tcpip_if mif_;                               \
    MG_SET_WIFI_CONFIG(&driver_data_);                            \
    mif_.ip = MG_TCPIP_IP;                                        \
    mif_.mask = MG_TCPIP_MASK;                                    \
    mif_.gw = MG_TCPIP_GW;                                        \
    mif_.driver = &mg_tcpip_driver_pico_w;                        \
    mif_.driver_data = &driver_data_;                             \
    mif_.recv_queue.size = 8192;                                  \
    mif_.mac[0] = 2; /* MAC read from OTP at driver init */       \
    mg_tcpip_init(mgr, &mif_);                                    \
    MG_INFO(("Driver: pico-w, MAC: %M", mg_print_mac, mif_.mac)); \
  } while (0)

#endif


struct mg_tcpip_driver_ppp_data {
  void *uart;                   // Opaque UART bus descriptor
  void (*reset)(void *);        // Modem hardware reset
  void (*tx)(void *, uint8_t);  // UART transmit single byte
  int (*rx)(void *);            // UART receive single byte
  const char **script;          // List of AT commands and expected replies
  int script_index;             // Index of the current AT command in the list
  uint64_t deadline;            // AT command deadline in ms
};


#if MG_ENABLE_TCPIP && \
  (defined(MG_ENABLE_DRIVER_RA6) && MG_ENABLE_DRIVER_RA6) || \
  (defined(MG_ENABLE_DRIVER_RA8) && MG_ENABLE_DRIVER_RA8)

struct mg_tcpip_driver_ra_data {
  // MDC clock "divider". MDC clock is software generated,
  uint32_t clock;    // core clock frequency in Hz
  uint16_t irqno;    // IRQn, R_ICU->IELSR[irqno]
  uint8_t phy_addr;  // PHY address
};

#ifndef MG_DRIVER_CLK_FREQ
#define MG_DRIVER_CLK_FREQ 100000000UL
#endif

#ifndef MG_DRIVER_IRQ_NO
#define MG_DRIVER_IRQ_NO 0
#endif

#ifndef MG_TCPIP_PHY_ADDR
#define MG_TCPIP_PHY_ADDR 0
#endif

#define MG_TCPIP_DRIVER_INIT(mgr)                             \
  do {                                                        \
    static struct mg_tcpip_driver_ra_data driver_data_;       \
    static struct mg_tcpip_if mif_;                           \
    driver_data_.clock = MG_DRIVER_CLK_FREQ;                  \
    driver_data_.irqno = MG_DRIVER_IRQ_NO;                    \
    driver_data_.phy_addr = MG_TCPIP_PHY_ADDR;                \
    mif_.ip = MG_TCPIP_IP;                                    \
    mif_.mask = MG_TCPIP_MASK;                                \
    mif_.gw = MG_TCPIP_GW;                                    \
    mif_.driver = &mg_tcpip_driver_ra;                        \
    mif_.driver_data = &driver_data_;                         \
    MG_SET_MAC_ADDRESS(mif_.mac);                             \
    mg_tcpip_init(mgr, &mif_);                                \
    MG_INFO(("Driver: ra, MAC: %M", mg_print_mac, mif_.mac)); \
  } while (0)

#endif


#if MG_ENABLE_TCPIP && defined(MG_ENABLE_DRIVER_RW612) && MG_ENABLE_DRIVER_RW612

struct mg_tcpip_driver_rw612_data {
  // 38.1.8 MII Speed Control Register (MSCR)
  // MDC clock frequency must not exceed 2.5 MHz and is calculated as follows:
  // MDC_freq = P_clock / ((mdc_cr + 1) * 2), where P_clock is the
  // peripheral bus clock.
  // IEEE802.3 clause 22 defines a minimum of 10 ns for the hold time on the
  // MDIO output. Depending on the host bus frequency, the setting may need
  // to be increased.
  int mdc_cr;
  int mdc_holdtime; // Valid values: [0-7]
  uint8_t phy_addr;
};

#ifndef MG_TCPIP_PHY_ADDR
#define MG_TCPIP_PHY_ADDR 2
#endif

#ifndef MG_DRIVER_MDC_CR
#define MG_DRIVER_MDC_CR 51
#endif

#ifndef MG_DRIVER_MDC_HOLDTIME
#define MG_DRIVER_MDC_HOLDTIME 3
#endif

#define MG_TCPIP_DRIVER_INIT(mgr)                                 \
  do {                                                            \
    static struct mg_tcpip_driver_rw612_data driver_data_;       \
    static struct mg_tcpip_if mif_;                               \
    driver_data_.mdc_cr = MG_DRIVER_MDC_CR;                       \
    driver_data_.phy_addr = MG_TCPIP_PHY_ADDR;                    \
    mif_.ip = MG_TCPIP_IP;                                        \
    mif_.mask = MG_TCPIP_MASK;                                    \
    mif_.gw = MG_TCPIP_GW;                                        \
    mif_.driver = &mg_tcpip_driver_rw612;                        \
    mif_.driver_data = &driver_data_;                             \
    MG_SET_MAC_ADDRESS(mif_.mac);                                 \
    mg_tcpip_init(mgr, &mif_);                                    \
    MG_INFO(("Driver: rw612, MAC: %M", mg_print_mac, mif_.mac)); \
  } while (0)

#endif


#if MG_ENABLE_TCPIP && defined(MG_ENABLE_DRIVER_SAME54) && \
    MG_ENABLE_DRIVER_SAME54

#ifndef MG_DRIVER_MDC_CR
#define MG_DRIVER_MDC_CR 5
#endif

#ifndef MG_DRIVER_PHY_ADDR
#define MG_DRIVER_PHY_ADDR 0
#endif

struct mg_tcpip_driver_same54_data {
  int mdc_cr;
  uint8_t phy_addr;
};

#define MG_TCPIP_DRIVER_INIT(mgr)                                 \
  do {                                                            \
    static struct mg_tcpip_driver_same54_data driver_data_;       \
    static struct mg_tcpip_if mif_;                               \
    driver_data_.mdc_cr = MG_DRIVER_MDC_CR;                       \
    driver_data_.phy_addr = MG_DRIVER_PHY_ADDR;                   \
    mif_.ip = MG_TCPIP_IP;                                        \
    mif_.mask = MG_TCPIP_MASK;                                    \
    mif_.gw = MG_TCPIP_GW;                                        \
    mif_.driver = &mg_tcpip_driver_same54;                        \
    mif_.driver_data = &driver_data_;                             \
    MG_SET_MAC_ADDRESS(mif_.mac);                                 \
    mg_tcpip_init(mgr, &mif_);                                    \
    MG_INFO(("Driver: same54, MAC: %M", mg_print_mac, mif_.mac)); \
  } while (0)

#endif


#if MG_ENABLE_TCPIP && \
    (defined(MG_ENABLE_DRIVER_CYW_SDIO) && MG_ENABLE_DRIVER_CYW_SDIO)

// Specific chip/card driver --> SDIO driver --> HAL --> SDIO hw controller

// API with HAL for hardware controller
// - Provide a function to init the controller (external)
// - Provide these functions:
struct mg_tcpip_sdio {
  void *sdio;                    // Opaque SDIO bus descriptor
  void (*cfg)(void *, uint8_t);  // select operating parameters
  // SDIO transaction: send cmd with a possible 1-byte read or write
  bool (*txn)(void *, uint8_t cmd, uint32_t arg, uint32_t *r);
  // SDIO extended transaction: write or read len bytes, using blksz blocks
  bool (*xfr)(void *, bool write, uint32_t arg, uint16_t blksz, uint32_t *,
              uint32_t len, uint32_t *r);
};

// API with driver (e.g.: cyw.c)
// Once the hardware controller has been initialized:
// - Init card: selects the card, sets F0 block size, sets bus width and speed
bool mg_sdio_init(struct mg_tcpip_sdio *sdio);
// - Enable other possible functions (F1 to F7)
bool mg_sdio_enable_f(struct mg_tcpip_sdio *sdio, unsigned int f);
// - Wait for them to be ready
bool mg_sdio_waitready_f(struct mg_tcpip_sdio *sdio, unsigned int f);
// - Set their transfer block length
bool mg_sdio_set_blksz(struct mg_tcpip_sdio *sdio, unsigned int f,
                       uint16_t blksz);
// - Transfer data to/from a function (abstracts from transaction type)
// - Requesting a read transfer > blocksize means block transfer will be used.
// - Drivers must have room to accomodate a whole block transfer, see sdio.c
// - Transfers of > 1 byte --> (uint32_t *) data. 1-byte --> (uint8_t *) data
bool mg_sdio_transfer(struct mg_tcpip_sdio *sdio, bool write, unsigned int f,
                      uint32_t addr, void *data, uint32_t len);

#endif


#if MG_ENABLE_TCPIP && defined(MG_ENABLE_DRIVER_ST67W6) && \
    MG_ENABLE_DRIVER_ST67W6

struct mg_tcpip_spi_ {
  void *spi;              // Opaque SPI bus descriptor
  void (*begin)(void *);  // SPI begin: slave select low
  void (*end)(void *);    // SPI end: slave select high
  void (*txn)(void *, uint8_t *, uint8_t *,
              size_t len);  // SPI transaction: write-read len bytes
};

struct mg_tcpip_driver_st67w6_data {
  struct mg_wifi_data wifi;
  void *spi;
  bool (*is_ready)(void);     // return state of module RDY pin
  struct mg_queue send_queue; // decouple tx calls from module polls
};

#define MG_TCPIP_DRIVER_INIT(mgr)                                 \
  do {                                                            \
    static struct mg_tcpip_driver_st67w6_data driver_data_;       \
    static struct mg_tcpip_if mif_;                               \
    MG_SET_WIFI_CONFIG(&driver_data_);                            \
    mif_.ip = MG_TCPIP_IP;                                        \
    mif_.mask = MG_TCPIP_MASK;                                    \
    mif_.gw = MG_TCPIP_GW;                                        \
    mif_.driver = &mg_tcpip_driver_st67w6;                        \
    mif_.driver_data = &driver_data_;                             \
    mif_.recv_queue.size = 8192;                                  \
    mif_.mac[0] = 2; /* MAC read from OTP at driver init */       \
    mg_tcpip_init(mgr, &mif_);                                    \
    MG_INFO(("Driver: st67w6, MAC: %M", mg_print_mac, mif_.mac)); \
  } while (0)

#endif


#if MG_ENABLE_TCPIP && defined(MG_ENABLE_DRIVER_STM32F) && \
    MG_ENABLE_DRIVER_STM32F

struct mg_tcpip_driver_stm32f_data {
  // MDC clock divider. MDC clock is derived from HCLK, must not exceed 2.5MHz
  //    HCLK range    DIVIDER    mdc_cr VALUE
  //    -------------------------------------
  //                                -1  <-- tell driver to guess the value
  //    60-100 MHz    HCLK/42        0
  //    100-150 MHz   HCLK/62        1
  //    20-35 MHz     HCLK/16        2
  //    35-60 MHz     HCLK/26        3
  //    150-216 MHz   HCLK/102       4  <-- value for Nucleo-F* on max speed
  //    216-310 MHz   HCLK/124       5
  //    110, 111 Reserved
  int mdc_cr;  // Valid values: -1, 0, 1, 2, 3, 4, 5

  uint8_t phy_addr;  // PHY address
};

#ifndef MG_TCPIP_PHY_ADDR
#define MG_TCPIP_PHY_ADDR 0
#endif

#ifndef MG_DRIVER_MDC_CR
#define MG_DRIVER_MDC_CR 4
#endif

#if MG_ARCH == MG_ARCH_CUBE
#define MG_ENABLE_ETH_IRQ() NVIC_EnableIRQ(ETH_IRQn)
#else
#define MG_ENABLE_ETH_IRQ()
#endif

#define MG_TCPIP_DRIVER_INIT(mgr)                                 \
  do {                                                            \
    static struct mg_tcpip_driver_stm32f_data driver_data_;       \
    static struct mg_tcpip_if mif_;                               \
    driver_data_.mdc_cr = MG_DRIVER_MDC_CR;                       \
    driver_data_.phy_addr = MG_TCPIP_PHY_ADDR;                    \
    mif_.ip = MG_TCPIP_IP;                                        \
    mif_.mask = MG_TCPIP_MASK;                                    \
    mif_.gw = MG_TCPIP_GW;                                        \
    mif_.driver = &mg_tcpip_driver_stm32f;                        \
    mif_.driver_data = &driver_data_;                             \
    MG_SET_MAC_ADDRESS(mif_.mac);                                 \
    MG_ENABLE_ETH_IRQ();                                          \
    mg_tcpip_init(mgr, &mif_);                                    \
    MG_INFO(("Driver: stm32f, MAC: %M", mg_print_mac, mif_.mac)); \
  } while (0)

#endif


#if MG_ENABLE_TCPIP
#if !defined(MG_ENABLE_DRIVER_STM32H)
#define MG_ENABLE_DRIVER_STM32H 0
#endif
#if !defined(MG_ENABLE_DRIVER_MCXN)
#define MG_ENABLE_DRIVER_MCXN 0
#endif
#if !defined(MG_ENABLE_DRIVER_STM32N)
#define MG_ENABLE_DRIVER_STM32N 0
#endif
#if MG_ENABLE_DRIVER_STM32H || MG_ENABLE_DRIVER_MCXN || MG_ENABLE_DRIVER_STM32N

struct mg_tcpip_driver_stm32h_data {
  // MDC clock divider. MDC clock is derived from HCLK, must not exceed 2.5MHz
  //    HCLK range    DIVIDER    mdc_cr VALUE
  //    -------------------------------------
  //                                -1  <-- tell driver to guess the value
  //    60-100 MHz    HCLK/42        0
  //    100-150 MHz   HCLK/62        1
  //    20-35 MHz     HCLK/16        2
  //    35-60 MHz     HCLK/26        3
  //    150-250 MHz   HCLK/102       4  <-- value for max speed HSI
  //    250-300 MHz   HCLK/124       5  <-- value for Nucleo-H* on CSI
  //    300-500 MHz   HCLK/204       6
  //    500-800 MHz   HCLK/324       7
  int mdc_cr;  // Valid values: -1, 0, 1, 2, 3, 4, 5

  uint8_t phy_addr;  // PHY address
  uint8_t phy_conf;  // PHY config
};

#ifndef MG_TCPIP_PHY_CONF
#define MG_TCPIP_PHY_CONF MG_PHY_CLOCKS_MAC
#endif

#ifndef MG_TCPIP_PHY_ADDR
#define MG_TCPIP_PHY_ADDR 0
#endif

#ifndef MG_DRIVER_MDC_CR
#define MG_DRIVER_MDC_CR 4
#endif

#if MG_ENABLE_DRIVER_STM32H && MG_ARCH == MG_ARCH_CUBE
#define MG_ENABLE_ETH_IRQ() NVIC_EnableIRQ(ETH_IRQn)
#else
#define MG_ENABLE_ETH_IRQ()
#endif

#if MG_ENABLE_IPV6
#define MG_IPV6_INIT(mif)                                         \
  do {                                                            \
    memcpy(mif.ip6ll, (uint8_t[16]) MG_TCPIP_IPV6_LINKLOCAL, 16);     \
    memcpy(mif.ip6, (uint8_t[16]) MG_TCPIP_GLOBAL, 16);           \
    memcpy(mif.gw6, (uint8_t[16]) MG_TCPIP_GW6, 16);              \
    mif.prefix_len = MG_TCPIP_PREFIX_LEN;                        \
  } while(0)
#else
#define MG_IPV6_INIT(mif)
#endif

#define MG_TCPIP_DRIVER_INIT(mgr)                                 \
  do {                                                            \
    static struct mg_tcpip_driver_stm32h_data driver_data_;       \
    static struct mg_tcpip_if mif_;                               \
    driver_data_.mdc_cr = MG_DRIVER_MDC_CR;                       \
    driver_data_.phy_addr = MG_TCPIP_PHY_ADDR;                    \
    driver_data_.phy_conf = MG_TCPIP_PHY_CONF;                    \
    mif_.ip = MG_TCPIP_IP;                                        \
    mif_.mask = MG_TCPIP_MASK;                                    \
    mif_.gw = MG_TCPIP_GW;                                        \
    mif_.driver = &mg_tcpip_driver_stm32h;                        \
    mif_.driver_data = &driver_data_;                             \
    MG_SET_MAC_ADDRESS(mif_.mac);                                 \
    MG_IPV6_INIT(mif_);                                           \
    mg_tcpip_init(mgr, &mif_);                                    \
    MG_ENABLE_ETH_IRQ();                                          \
    MG_INFO(("Driver: stm32h, MAC: %M", mg_print_mac, mif_.mac)); \
  } while (0)

#endif
#endif


#if MG_ENABLE_TCPIP && defined(MG_ENABLE_DRIVER_TM4C) && MG_ENABLE_DRIVER_TM4C

struct mg_tcpip_driver_tm4c_data {
  // MDC clock divider. MDC clock is derived from SYSCLK, must not exceed 2.5MHz
  //    SYSCLK range   DIVIDER   mdc_cr VALUE
  //    -------------------------------------
  //                                -1  <-- tell driver to guess the value
  //    60-100 MHz    SYSCLK/42      0
  //    100-150 MHz   SYSCLK/62      1  <-- value for EK-TM4C129* on max speed
  //    20-35 MHz     SYSCLK/16      2
  //    35-60 MHz     SYSCLK/26      3
  //    0x4-0xF Reserved
  int mdc_cr;  // Valid values: -1, 0, 1, 2, 3
};

#ifndef MG_DRIVER_MDC_CR
#define MG_DRIVER_MDC_CR 1
#endif

#define MG_TCPIP_DRIVER_INIT(mgr)                               \
  do {                                                          \
    static struct mg_tcpip_driver_tm4c_data driver_data_;       \
    static struct mg_tcpip_if mif_;                             \
    driver_data_.mdc_cr = MG_DRIVER_MDC_CR;                     \
    mif_.ip = MG_TCPIP_IP;                                      \
    mif_.mask = MG_TCPIP_MASK;                                  \
    mif_.gw = MG_TCPIP_GW;                                      \
    mif_.driver = &mg_tcpip_driver_tm4c;                        \
    mif_.driver_data = &driver_data_;                           \
    MG_SET_MAC_ADDRESS(mif_.mac);                               \
    mg_tcpip_init(mgr, &mif_);                                  \
    MG_INFO(("Driver: tm4c, MAC: %M", mg_print_mac, mif_.mac)); \
  } while (0)

#endif


#if MG_ENABLE_TCPIP && defined(MG_ENABLE_DRIVER_TMS570) && MG_ENABLE_DRIVER_TMS570
struct mg_tcpip_driver_tms570_data {
  int mdc_cr;
  int phy_addr;
};

#ifndef MG_TCPIP_PHY_ADDR
#define MG_TCPIP_PHY_ADDR 0
#endif

#ifndef MG_DRIVER_MDC_CR
#define MG_DRIVER_MDC_CR 1
#endif

#define MG_TCPIP_DRIVER_INIT(mgr)                               \
  do {                                                          \
    static struct mg_tcpip_driver_tms570_data driver_data_;     \
    static struct mg_tcpip_if mif_;                             \
    driver_data_.mdc_cr = MG_DRIVER_MDC_CR;                     \
    driver_data_.phy_addr = MG_TCPIP_PHY_ADDR;                  \
    mif_.ip = MG_TCPIP_IP;                                      \
    mif_.mask = MG_TCPIP_MASK;                                  \
    mif_.gw = MG_TCPIP_GW;                                      \
    mif_.driver = &mg_tcpip_driver_tms570;                      \
    mif_.driver_data = &driver_data_;                           \
    MG_SET_MAC_ADDRESS(mif_.mac);                               \
    mg_tcpip_init(mgr, &mif_);                                  \
    MG_INFO(("Driver: tms570, MAC: %M", mg_print_mac, mif_.mac));\
  } while (0)
#endif



#if MG_ENABLE_TCPIP && defined(MG_ENABLE_DRIVER_XMC) && MG_ENABLE_DRIVER_XMC

struct mg_tcpip_driver_xmc_data {
  // 13.2.8.1 Station Management Functions
  // MDC clock divider (). MDC clock is derived from ETH MAC clock
  // It must not exceed 2.5MHz
  // ETH Clock range  DIVIDER       mdc_cr VALUE
  // --------------------------------------------
  //                                     -1  <-- tell driver to guess the value
  // 60-100 MHz       ETH Clock/42        0
  // 100-150 MHz      ETH Clock/62        1
  // 20-35 MHz        ETH Clock/16        2
  // 35-60 MHz        ETH Clock/26        3
  // 150-250 MHz      ETH Clock/102       4
  // 250-300 MHz      ETH Clock/124       5
  // 110, 111 Reserved
  int mdc_cr;  // Valid values: -1, 0, 1, 2, 3, 4, 5
  uint8_t phy_addr;
};

#ifndef MG_TCPIP_PHY_ADDR
#define MG_TCPIP_PHY_ADDR 0
#endif

#ifndef MG_DRIVER_MDC_CR
#define MG_DRIVER_MDC_CR 4
#endif

#define MG_TCPIP_DRIVER_INIT(mgr)                                 \
  do {                                                            \
    static struct mg_tcpip_driver_xmc_data driver_data_;       \
    static struct mg_tcpip_if mif_;                               \
    driver_data_.mdc_cr = MG_DRIVER_MDC_CR;                       \
    driver_data_.phy_addr = MG_TCPIP_PHY_ADDR;                    \
    mif_.ip = MG_TCPIP_IP;                                        \
    mif_.mask = MG_TCPIP_MASK;                                    \
    mif_.gw = MG_TCPIP_GW;                                        \
    mif_.driver = &mg_tcpip_driver_xmc;                        \
    mif_.driver_data = &driver_data_;                             \
    MG_SET_MAC_ADDRESS(mif_.mac);                                 \
    mg_tcpip_init(mgr, &mif_);                                    \
    MG_INFO(("Driver: xmc, MAC: %M", mg_print_mac, mif_.mac)); \
  } while (0)

#endif


#if MG_ENABLE_TCPIP && defined(MG_ENABLE_DRIVER_XMC7) && MG_ENABLE_DRIVER_XMC7

struct mg_tcpip_driver_xmc7_data {
  int mdc_cr;  // Valid values: -1, 0, 1, 2, 3, 4, 5
  uint8_t phy_addr;
};

#ifndef MG_TCPIP_PHY_ADDR
#define MG_TCPIP_PHY_ADDR 0
#endif

#ifndef MG_DRIVER_MDC_CR
#define MG_DRIVER_MDC_CR 3
#endif

#define MG_TCPIP_DRIVER_INIT(mgr)                                 \
  do {                                                            \
    static struct mg_tcpip_driver_xmc7_data driver_data_;       \
    static struct mg_tcpip_if mif_;                               \
    driver_data_.mdc_cr = MG_DRIVER_MDC_CR;                       \
    driver_data_.phy_addr = MG_TCPIP_PHY_ADDR;                    \
    mif_.ip = MG_TCPIP_IP;                                        \
    mif_.mask = MG_TCPIP_MASK;                                    \
    mif_.gw = MG_TCPIP_GW;                                        \
    mif_.driver = &mg_tcpip_driver_xmc7;                        \
    mif_.driver_data = &driver_data_;                             \
    MG_SET_MAC_ADDRESS(mif_.mac);                                 \
    mg_tcpip_init(mgr, &mif_);                                    \
    MG_INFO(("Driver: xmc7, MAC: %M", mg_print_mac, mif_.mac)); \
  } while (0)

#endif


#ifdef __cplusplus
}
#endif
#endif  // MONGOOSE_H
