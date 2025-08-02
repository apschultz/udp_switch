/*
 * UDP Ethernet switch in C using libevent bufferevents and uthash
 */

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <unistd.h>
#include <signal.h>
#include <assert.h>
#include <time.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <stdbool.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <event2/event.h>
#include <event2/util.h>
#include <event2/buffer.h>
#include <jansson.h>
#include <libgen.h>
#include <uthash.h>

#define USE_PTHREAD 1
#if USE_PTHREAD
#include <pthread.h>
#include <event2/thread.h>
#endif

#define UNUSED __attribute__((unused))
#define MAC_CHECK_INTERVAL   30    /* seconds between aging runs */
#define MAC_TIMEOUT_DEFAULT  300   /* seconds before aging out a MAC */
#define MAC_MIN_AGE_DEFAULT  5     /* seconds minimum before re-learning same MAC */
#define MAX_FRAME_SIZE       2048  /* max Ethernet frame size */

#define MAX_CLIENTS          32
#define MAX_VLANS_PER_CLIENT 16
#define MAX_MACS_PER_VLAN    8

static int debug        = 0;
static int keep_clients = 0;
static int mac_timeout  = MAC_TIMEOUT_DEFAULT;
static int min_learn_age = MAC_MIN_AGE_DEFAULT;
static struct timespec last_interrupt = {0,0};
static int bind_family = AF_INET6;
static const char *bind_ip = "::";
static int port = 10000;
#if USE_PTHREAD
static int num_threads = 1;
__thread int thread_idx = 0;
__thread char thread_str[16] = "-";
#endif

/* -- Data Structures -- */

#define container_of(ptr, type, member) ({                      \
        const typeof( ((type *)0)->member ) *__mptr = (ptr);    \
        (type *)((char *)__mptr - offsetof(type,member));})

typedef struct dlist_entry {
	struct dlist_entry *prev;
	struct dlist_entry *next;
} dlist_entry;

typedef struct dlist {
	dlist_entry *head;
	dlist_entry *tail;
} dlist;

static inline void dlist_add(dlist *dl, dlist_entry *e)
{
	if (dl->head == NULL) {
		e->prev = NULL;
		e->next = NULL;
		dl->head = e;
		dl->tail = e;
	} else {
		e->prev = dl->tail;
		e->next = NULL;
		dl->tail->next = e;
		dl->tail = e;
	}
}

static inline void dlist_del(dlist *dl, dlist_entry *e)
{
	if (dl->head == e) {
		dl->head = e->next;
	}
	if (dl->tail == e) {
		dl->tail = e->prev;
	}
	if (e->prev) {
		e->prev->next = e->next;
	}
	if (e->next) {
		e->next->prev = e->prev;
	}
	e->prev = e->next = NULL;
}

#define ETHER_ADDR_LEN 6
typedef struct ether_header {
    uint8_t  ether_dhost[ETHER_ADDR_LEN];  /* destination eth addr */
    uint8_t  ether_shost[ETHER_ADDR_LEN];  /* source eth addr    */
    uint16_t ether_type;                   /* packet type ID field */
	uint8_t  data[0];
} __attribute__((packed)) ether_header;
#define ETH_P_8021Q_NET  htons(0x8100)
#define ETH_P_8021AD_NET htons(0x88A8)

typedef struct vlan_header {
    uint16_t vlan_tci;       /* vlan id/prio    */
    uint16_t ether_type;     /* packet type ID field */
	uint8_t  data[0];
} __attribute__((packed)) vlan_header;
#define VLAN_VID_MASK	0xfff

#define VLAN_BITMAP_BITS_PER_WORD (64)
#define VLAN_BITMAP_WORDS ((VLAN_VID_MASK + VLAN_BITMAP_BITS_PER_WORD-1)/VLAN_BITMAP_BITS_PER_WORD)
typedef struct vlan_bitmap {
  uint64_t bits[VLAN_BITMAP_WORDS];
} vlan_bitmap;

static inline void vlan_bitmap_set(vlan_bitmap *bm, uint16_t vlan_id) {
  bm->bits[vlan_id/VLAN_BITMAP_BITS_PER_WORD] |= 1ULL << (vlan_id&(VLAN_BITMAP_BITS_PER_WORD-1));
}
static inline void vlan_bitmap_clear(vlan_bitmap *bm, uint16_t vlan_id) {
  bm->bits[vlan_id/VLAN_BITMAP_BITS_PER_WORD] &= ~(1ULL << (vlan_id&(VLAN_BITMAP_BITS_PER_WORD-1)));
}
static inline int vlan_bitmap_test(vlan_bitmap *bm, uint16_t vlan_id) {
  return (bm->bits[vlan_id/VLAN_BITMAP_BITS_PER_WORD] >> (vlan_id&(VLAN_BITMAP_BITS_PER_WORD-1))) & 1;
}


typedef struct tenant_entry {
    uint32_t                tenant_id;
    dlist                   client_list;
    dlist_entry             global_dle;
	uint32_t                refcount;
} tenant_entry;

#if USE_PTHREAD
static inline void tenant_entry_hold(tenant_entry *te)
{
	__atomic_add_fetch(&te->refcount, 1, __ATOMIC_SEQ_CST);
}
static inline void tenant_entry_release(tenant_entry *te)
{
	if (__atomic_sub_fetch(&te->refcount, 1, __ATOMIC_SEQ_CST) == 0) {
		if (debug > 2) {
			printf("tenant_entry 0x%p freed\n", te);
		}
		free(te);
	}
}
#endif

static dlist tenant_list = {0};
static pthread_mutex_t tenant_list_lock = PTHREAD_MUTEX_INITIALIZER;

typedef struct mac_entry mac_entry;
typedef struct vlan_entry vlan_entry;

typedef struct udp_client {
	uint32_t                tenant_id;
    struct sockaddr_storage addr;
    socklen_t               addrlen;
	bool                    persistent;
	dlist                   mac_list;
	size_t                  mac_count;
	vlan_bitmap             vlans;
	dlist                   vlan_list;
	size_t                  vlan_count;
    size_t                  rxframes;
    size_t                  rxbytes;
    size_t                  txframes;
    size_t                  txbytes;
    size_t                  dropframes;
    size_t                  dropbytes;
    UT_hash_handle          hh;
	tenant_entry           *tenant;
	dlist_entry             tenant_dle;
#if USE_PTHREAD
	uint32_t                refcount;
    pthread_mutex_t         mac_list_lock;
    pthread_rwlock_t        vlan_list_lock;
#endif
} udp_client;

#if USE_PTHREAD
static inline void udp_client_hold(udp_client *cl)
{
	__atomic_add_fetch(&cl->refcount, 1, __ATOMIC_SEQ_CST);
}
static inline void udp_client_release(udp_client *cl)
{
	if (__atomic_sub_fetch(&cl->refcount, 1, __ATOMIC_SEQ_CST) == 0) {
		if (debug > 2) {
			printf("udp_client 0x%p freed\n", cl);
		}
		free(cl);
	}
}
#endif

typedef struct vlan_entry {
	uint16_t                vlan_id;
	bool                    persistent;
	dlist                   mac_list;
	dlist_entry             client_dle;
	size_t                  mac_count;
#if USE_PTHREAD
    pthread_mutex_t         mac_list_lock;
	uint32_t                refcount;
#endif
} vlan_entry;

#if USE_PTHREAD
static inline void vlan_entry_hold(vlan_entry *me)
{
	__atomic_add_fetch(&me->refcount, 1, __ATOMIC_SEQ_CST);
}
static inline void vlan_entry_release(vlan_entry *vl)
{
	if (__atomic_sub_fetch(&vl->refcount, 1, __ATOMIC_SEQ_CST) == 0) {
		if (debug > 2) {
			printf("vlan_entry 0x%p freed\n", vl);
		}
		free(vl);
	}
}
#endif

typedef struct mac_key {
	uint8_t                  mac[6];
	uint16_t                 vlan_id;
	uint32_t tenant_id;
} __attribute__((packed)) mac_key;

struct mac_entry {
	mac_key                  key;
	bool                     persistent;
    udp_client              *client;          /* pointer to client */
    vlan_entry              *vlan;            /* pointer to vlan */
    struct timespec          last_seen;       /* monotonic timestamp */
    size_t                   rxframes;
    size_t                   rxbytes;
    size_t                   txframes;
    size_t                   txbytes;
    size_t                   dropframes;
    size_t                   dropbytes;
    UT_hash_handle           hh;
	dlist_entry              client_dle;
	dlist_entry              vlan_dle;
#if USE_PTHREAD
	uint32_t                 refcount;
#endif
};

#if USE_PTHREAD
static inline void mac_entry_hold(mac_entry *me)
{
	__atomic_add_fetch(&me->refcount, 1, __ATOMIC_SEQ_CST);
}
static inline void mac_entry_release(mac_entry *me)
{
	if (__atomic_sub_fetch(&me->refcount, 1, __ATOMIC_SEQ_CST) == 0) {
		if (debug > 2) {
			printf("mac_entry 0x%p freed\n", me);
		}
		free(me);
	}
}
#endif

static udp_client *clients = NULL;
static mac_entry  *macs    = NULL;

#if USE_PTHREAD
pthread_rwlock_t clients_lock = PTHREAD_RWLOCK_INITIALIZER;
pthread_rwlock_t macs_lock = PTHREAD_RWLOCK_INITIALIZER;

#define type_hold(x)                              \
    _Generic((x),                                 \
        struct udp_client*: udp_client_hold,      \
        struct mac_entry*: mac_entry_hold,        \
        struct vlan_entry*: vlan_entry_hold       \
    )(x)
#define type_release(x)                           \
    _Generic((x),                                 \
        struct udp_client*: udp_client_release,   \
        struct mac_entry*: mac_entry_release,     \
        struct vlan_entry*: vlan_entry_release    \
    )(x)

#define _CAT(a,b)  a##b
#define CAT(a,b)   _CAT(a,b)

#define SAFE_HASH_ADD_KEYPTR(hh_name, head, key_ptr, key_len, item_ptr) \
    do {                                                                \
		pthread_rwlock_wrlock(&CAT(head,_lock));                        \
		HASH_ADD_KEYPTR(hh_name, head, key_ptr, key_len, item_ptr);     \
		if (item_ptr) {                                                 \
			type_hold(item_ptr);                                        \
		}                                                               \
		pthread_rwlock_unlock(&CAT(head,_lock));                        \
	} while(0)

#define SAFE_HASH_FIND(hh_name, head, key_ptr, key_len, item_ptr)       \
    do {                                                                \
		pthread_rwlock_rdlock(&CAT(head,_lock));                        \
		HASH_FIND(hh_name, head, key_ptr, key_len, item_ptr);           \
		if (item_ptr) {                                                 \
			type_hold(item_ptr);                                        \
		}                                                               \
		pthread_rwlock_unlock(&CAT(head,_lock));                        \
	} while(0)

#define SAFE_HASH_DEL(head, item_ptr)                                   \
    do {                                                                \
		pthread_rwlock_wrlock(&CAT(head,_lock));                        \
		HASH_DEL(head, item_ptr);                                       \
		if (item_ptr) {                                                 \
			type_release(item_ptr);                                     \
		}                                                               \
		pthread_rwlock_unlock(&CAT(head,_lock));                        \
	} while(0)
#define SAFE_HASH_ITER(hh_name, head, item_ptr, tmp_item_ptr)           \
    do {                                                                \
		pthread_rwlock_rdlock(&CAT(head,_lock));                        \
		HASH_ITER(hh_name, head, item_ptr, tmp_item_ptr)
#define SAFE_HASH_ITER_WRITE(hh_name, head, item_ptr, tmp_item_ptr)     \
    do {                                                                \
		pthread_rwlock_wrlock(&CAT(head,_lock));                        \
		HASH_ITER(hh_name, head, item_ptr, tmp_item_ptr)

#define SAFE_HASH_ITER_DONE(hh_name, head, item_ptr, tmp_item_ptr)      \
        pthread_rwlock_unlock(&CAT(head,_lock));                        \
	} while(0)

#define ATOMIC_SET(x, val) __atomic_store_n (&(x), val, __ATOMIC_RELAXED)
#define ATOMIC_ADD(x, val) __atomic_add_fetch (&(x), val, __ATOMIC_RELAXED)

#else
#define SAFE_HASH_ADD_KEYPTR HASH_ADD_KEYPTR
#define SAFE_HASH_FIND HASH_FIND
#define SAFE_HASH_DEL(head, item_ptr)                                   \
    do {                                                                \
		HASH_DEL(head, item_ptr);                                       \
		free(item_ptr);                                                 \
	} while(0)
#define SAFE_HASH_ITER HASH_ITER
#define SAFE_HASH_ITER_WRITE HASH_ITER
#define SAFE_HASH_ITER_DONE(hh_name, head, item_ptr, tmp_item_ptr) {}

#define ATOMIC_SET(x, val) x = val
#define ATOMIC_ADD(x, val) x += val

#endif

/* -- Utility Functions for RX/TX Manipulation -- */
static inline void client_add_rx(udp_client *cl, size_t bytes) {
	ATOMIC_ADD(cl->rxframes, 1);
	ATOMIC_ADD(cl->rxbytes, bytes);
}
static inline void client_add_tx(udp_client *cl, size_t bytes) {
	ATOMIC_ADD(cl->txframes, 1);
	ATOMIC_ADD(cl->txbytes, bytes);
}
static inline void client_drop(udp_client *cl, size_t bytes) {
	ATOMIC_ADD(cl->dropframes, 1);
	ATOMIC_ADD(cl->dropbytes, bytes);
}
static inline void client_reset_ctrs(udp_client *cl) {
	ATOMIC_SET(cl->rxframes, 0);
	ATOMIC_SET(cl->rxbytes, 0);
	ATOMIC_SET(cl->txframes, 0);
	ATOMIC_SET(cl->txbytes, 0);
	ATOMIC_SET(cl->dropframes, 0);
	ATOMIC_SET(cl->dropbytes, 0);
}
static inline void mac_add_rx(mac_entry *me, size_t bytes) {
	ATOMIC_ADD(me->rxframes, 1);
	ATOMIC_ADD(me->rxbytes, bytes);
}
static inline void mac_add_tx(mac_entry *me, size_t bytes) {
	ATOMIC_ADD(me->txframes, 1);
	ATOMIC_ADD(me->txbytes, bytes);
}
static inline void mac_drop(mac_entry *me, size_t bytes) {
	ATOMIC_ADD(me->dropframes, 1);
	ATOMIC_ADD(me->dropbytes, bytes);
}
static inline void mac_reset_ctrs(mac_entry *me) {
	ATOMIC_SET(me->rxframes, 0);
	ATOMIC_SET(me->rxbytes, 0);
	ATOMIC_SET(me->txframes, 0);
	ATOMIC_SET(me->txbytes, 0);
	ATOMIC_SET(me->dropframes, 0);
	ATOMIC_SET(me->dropbytes, 0);
}

/* -- Conversion Utilities -- */
#define UDPCLIENT_STR_LEN (INET6_ADDRSTRLEN + 29) //[tenant:id] [IP]:PORT\0
static size_t udp_client_to_str(udp_client *cl, char *buf, size_t buf_len) {
	assert(buf_len >= UDPCLIENT_STR_LEN);

    size_t len = 0;

	/* Add tenant prefix */
	len += snprintf(buf + len, buf_len - len, "[tenant:%u] ", cl->tenant_id);
	
    /* Build "[IP]:port" into buf directly */
    /* Add opening '[' */
    if (len < buf_len - 1) buf[len++] = '[';
    /* Write IP address */
    size_t ip_space = (len < buf_len) ? buf_len - len : 0;
    if (ip_space > 0) {
        evutil_inet_ntop(cl->addr.ss_family,
			(cl->addr.ss_family == AF_INET)
				? (void *)&((struct sockaddr_in *)&cl->addr)->sin_addr
				: (void *)&((struct sockaddr_in6 *)&cl->addr)->sin6_addr,
			buf + len, ip_space);
        size_t iplen = strnlen(buf + len, ip_space);
        len += iplen;
    }
    /* Add closing ']' and ':' */
    if (len < buf_len - 2) {
        buf[len++] = ']';
        buf[len++] = ':';
    }
    /* Write port */
    int port = ntohs(
        (cl->addr.ss_family == AF_INET)
            ? ((struct sockaddr_in *)&cl->addr)->sin_port
            : ((struct sockaddr_in6 *)&cl->addr)->sin6_port);
    if (len < buf_len) {
        int ret = snprintf(buf + len, buf_len - len, "%d", port);
        if (ret > 0) {
            size_t portlen = (size_t)ret;
            len += (portlen < buf_len - len) ? portlen : buf_len - len;
        }
    }
    /* Null-terminate if space */
    if (len < buf_len) buf[len] = '\0';
    else buf[buf_len - 1] = '\0';
    return len;
}

#define MACENTRY_STR_LEN (29) //MAC vlan VID
static size_t mac_entry_to_str(mac_entry *me, char *buf, size_t buf_len) {
	assert(buf_len >= MACENTRY_STR_LEN);
	return snprintf(buf, buf_len, 
		"%02x:%02x:%02x:%02x:%02x:%02x vlan %u",
        me->key.mac[0], me->key.mac[1], me->key.mac[2],
        me->key.mac[3], me->key.mac[4], me->key.mac[5],
		me->key.vlan_id); 
}

/* -- Logging Functions -- */
static void log_client(udp_client *cl, const char *fmt, ...) {
    char cl_str[UDPCLIENT_STR_LEN];

    udp_client_to_str(cl, cl_str, sizeof(cl_str));

#if USE_PTHREAD
	printf("[%s] %s ", thread_str, cl_str);
#else
	printf("%s ", cl_str);
#endif
    va_list ap; va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    printf("\n");
}

static void _log_mac(mac_entry *me, udp_client *cl, const char *fmt, ...) {
    char cl_str[UDPCLIENT_STR_LEN];
    char me_str[MACENTRY_STR_LEN];
    if (!cl) cl = me->client;

	udp_client_to_str(cl, cl_str, sizeof(cl_str));
	mac_entry_to_str(me, me_str, sizeof(me_str));

#if USE_PTHREAD
	printf("[%s] %s src %s ", thread_str, cl_str, me_str);
#else
	printf("%s src %s ", cl_str, me_str);
#endif
    va_list ap; va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    printf("\n");
}
#define LOG_MAC_IMPL(_me, _cl, _fmt, ...) \
    _log_mac((_me), (_cl), (_fmt), ##__VA_ARGS__)
#define log_mac(_me, _fmt, ...) \
    LOG_MAC_IMPL((_me), NULL, (_fmt), ##__VA_ARGS__)
#define log_mac_client(_me, _cl, _fmt, ...) \
    LOG_MAC_IMPL((_me), (_cl), (_fmt), ##__VA_ARGS__)


static tenant_entry *get_tenant(uint32_t tenant_id)
{
#if USE_PTHREAD
	pthread_mutex_lock(&tenant_list_lock);
#endif
    for (dlist_entry *dle = tenant_list.head; dle; dle = dle->next) {
        tenant_entry *te = container_of(dle, tenant_entry, global_dle);
        if (te->tenant_id == tenant_id) {
#if USE_PTHREAD
			tenant_entry_hold(te);
			pthread_mutex_unlock(&tenant_list_lock);
#endif
			return te;
		}
    }
    tenant_entry *te = calloc(1, sizeof(*te));
	if (!te) {
		return NULL;
	}
	if (debug > 2) {
		printf("tenant_entry 0x%p allocated\n", te);
	}
    te->tenant_id = tenant_id;
	dlist_add(&tenant_list, &te->global_dle);
#if USE_PTHREAD
	tenant_entry_hold(te); //For return
	tenant_entry_hold(te); //For tenant_list
	pthread_mutex_unlock(&tenant_list_lock);
#endif
#if USE_PTHREAD
	printf("[%s] [tenant:%u] New tenant\n", thread_str, tenant_id);
#else
	printf("[tenant:%u] New tenant\n", tenant_id);
#endif
    return te;
}

static int remove_client_from_tenant(udp_client *cl)
{
	tenant_entry *te = cl->tenant;

	dlist_del(&te->client_list, &cl->tenant_dle);
	cl->tenant = NULL;

	if (te->client_list.head == NULL) {
		//No client, delete entry
#if USE_PTHREAD
		pthread_mutex_lock(&tenant_list_lock);
#endif
		dlist_del(&tenant_list, &te->global_dle);
#if USE_PTHREAD
		pthread_mutex_unlock(&tenant_list_lock);
		tenant_entry_release(te); //For tenant_list
#endif
	}

#if USE_PTHREAD
	udp_client_release(cl); //For te->client_list
	tenant_entry_release(te); //For cl->tenant
#endif
	return 0;
}

static udp_client *get_client(uint32_t tenant_id, const struct sockaddr *addr, socklen_t addrlen)
{
    udp_client *cl;
    SAFE_HASH_FIND(hh, clients, addr, addrlen, cl);
    if (cl) {
		return cl;
	}
	if (HASH_COUNT(clients) >= MAX_CLIENTS) {
		return NULL;
	}
    cl = calloc(1, sizeof(*cl));
	if (!cl) {
		return NULL;
	}
	if (debug > 2) {
		printf("udp_client 0x%p allocated\n", cl);
	}
    memcpy(&cl->addr, addr, addrlen);
    cl->addrlen = addrlen;
	
	tenant_entry *te = get_tenant(tenant_id);
	if (!te) {
		free(cl);
		return NULL;
	}
	cl->tenant_id = tenant_id;
	cl->tenant = te;
	dlist_add(&te->client_list, &cl->tenant_dle);

    SAFE_HASH_ADD_KEYPTR(hh, clients, &cl->addr, addrlen, cl);
#if USE_PTHREAD
	pthread_rwlock_init(&cl->vlan_list_lock, NULL);
	pthread_mutex_init(&cl->mac_list_lock, NULL);
	tenant_entry_hold(te); //For cl->tenant
	udp_client_hold(cl); //For te->client_list
	udp_client_hold(cl); //For return
#endif
    log_client(cl, "New client");
    return cl;
}

static vlan_entry *get_vlan(udp_client *cl, uint16_t vlan_id)
{
    vlan_entry *vl = NULL;
#if USE_PTHREAD
	pthread_rwlock_wrlock(&cl->vlan_list_lock);
#endif
	if (vlan_bitmap_test(&cl->vlans, vlan_id)) {
		for (dlist_entry *dle = cl->vlan_list.head; dle; dle = dle->next) {
			vl = container_of(dle, vlan_entry, client_dle);
			if (vl->vlan_id == vlan_id) {
				break;
			}
			vl = NULL;
		}
		assert(vl);
	} else {
		if (cl->vlan_count >= MAX_VLANS_PER_CLIENT ||
			!(vl = calloc(1, sizeof(vlan_entry)))) {
#if USE_PTHREAD
			pthread_rwlock_unlock(&cl->vlan_list_lock);
#endif
			return NULL;
		}
		if (debug > 2) {
			printf("vlan_entry 0x%p allocated\n", vl);
		}
		vl->vlan_id = vlan_id;
#if USE_PTHREAD
		pthread_mutex_init(&vl->mac_list_lock, NULL);
		vlan_entry_hold(vl); //cl->vlan_list
#endif
		dlist_add(&cl->vlan_list, &vl->client_dle);
		cl->vlan_count++;
		vlan_bitmap_set(&cl->vlans, vlan_id);
	}
#if USE_PTHREAD
	vlan_entry_hold(vl); //local/return
	pthread_rwlock_unlock(&cl->vlan_list_lock);
#endif
	return vl;
}

static void remove_mac_from_client(udp_client *cl, mac_entry *me)
{
	if (me->client != cl) {
		return;
	}

#if USE_PTHREAD
	pthread_rwlock_wrlock(&cl->vlan_list_lock);
#endif
	vlan_entry *vl = me->vlan;
	assert(vl != NULL);

#if USE_PTHREAD
	pthread_mutex_lock(&cl->mac_list_lock);
	pthread_mutex_lock(&vl->mac_list_lock);
#endif
	dlist_del(&vl->mac_list, &me->vlan_dle);
	me->vlan = NULL;
	vl->mac_count--;

	dlist_del(&cl->mac_list, &me->client_dle);
	me->client = NULL;
	cl->mac_count--;
#if USE_PTHREAD
	pthread_mutex_unlock(&vl->mac_list_lock);
	pthread_mutex_unlock(&cl->mac_list_lock);
	mac_entry_release(me); //vl->mac_list
	mac_entry_release(me); //cl->mac_list
	//udp_client_release(cl); //me->client -- defer to end for free safety
	//vlan_entry_release(vl); //me->vlan -- defer to end for free safety
#endif

	if (vl->mac_count == 0 && !vl->persistent) {
		dlist_del(&cl->vlan_list, &vl->client_dle);
#if USE_PTHREAD
		vlan_entry_release(vl); //cl->vlan_list
#else
		free(vl);
#endif
		vlan_bitmap_clear(&cl->vlans, vl->vlan_id);
		cl->vlan_count--;
	}
#if USE_PTHREAD
	pthread_rwlock_unlock(&cl->vlan_list_lock);
	udp_client_release(cl); //me->client
	vlan_entry_release(vl); //me->vlan
#endif
}

int add_mac_to_client(udp_client *cl, mac_entry *me)
{
	if (me->client) {
		remove_mac_from_client(me->client, me);
	}

	assert(me->client == NULL);
	assert(me->vlan == NULL);

	vlan_entry *vl = get_vlan(cl, me->key.vlan_id);
	if (!vl) {
		return -1;
	}

	if (vl->mac_count >= MAX_MACS_PER_VLAN) {
#if USE_PTHREAD
		vlan_entry_release(vl);
#endif
		return -1;
	}

#if USE_PTHREAD
	pthread_mutex_lock(&cl->mac_list_lock);
	pthread_mutex_lock(&vl->mac_list_lock);
#endif
	me->client = cl;
	dlist_add(&cl->mac_list, &me->client_dle);
	cl->mac_count++;

	me->vlan = vl;
	dlist_add(&vl->mac_list, &me->vlan_dle);
	vl->mac_count++;
#if USE_PTHREAD
	pthread_mutex_unlock(&vl->mac_list_lock);
	pthread_mutex_unlock(&cl->mac_list_lock);
	udp_client_hold(cl); //me->client
	vlan_entry_hold(vl); //me->vlan
	mac_entry_hold(me); //cl->mac_list
	mac_entry_hold(me); //vl->mac_list
	vlan_entry_release(vl); //local
#endif
	return 0;
}

static mac_entry *get_mac(const uint8_t *mac,
                         udp_client *client,
						 uint16_t vlan_id)
{
	if (vlan_id > VLAN_VID_MASK) {
		return NULL;
	}
	mac_key key;
    mac_entry *me;
	memcpy(key.mac, mac, sizeof(key.mac));
	key.vlan_id = vlan_id;
	key.tenant_id = client->tenant_id;

    SAFE_HASH_FIND(hh, macs, &key, sizeof(key), me);
    if (me) {
		return me;
	}
    me = calloc(1, sizeof(*me));
	if (!me) {
		return NULL;
	}
	if (debug > 2) {
		printf("mac_entry 0x%p allocated\n", me);
	}
	me->key = key;
    me->client = NULL;
	me->persistent = mac_timeout > 0 ? 0 : 1;
	clock_gettime(CLOCK_MONOTONIC, &me->last_seen);
    mac_reset_ctrs(me);
	if (add_mac_to_client(client, me) < 0) {
		free(me);
		return NULL;
	}
#if USE_PTHREAD
	mac_entry_hold(me); //For return
#endif
    SAFE_HASH_ADD_KEYPTR(hh, macs, &me->key, sizeof(me->key), me);
    if (debug) {
		log_mac(me, "Learned");
	}
    return me;
}

// Read and process incoming UDP frames (raw socket)
static void
udp_read_cb(evutil_socket_t sockfd, short what UNUSED, void *arg UNUSED)
{
    uint8_t buf[MAX_FRAME_SIZE];
    ssize_t n;

    // Read one datagram capture source address
read:
    struct sockaddr_storage src_addr = {};
    socklen_t addrlen = sizeof(src_addr);
    n = recvfrom(sockfd, buf, sizeof(buf), 0,
                 (struct sockaddr*)&src_addr, &addrlen);
    if (n <= 0) {
		if (debug) {
			printf("recvfrom error %s (%d)", strerror(errno), errno);
		}
		if (/*errno == EAGAIN ||*/ errno == EINTR) {
			goto read;
		}
		return;
	}

	// Get UDP Client entry
    udp_client *src = get_client(0, (struct sockaddr*)&src_addr, addrlen);
	mac_entry *me = NULL;
	if (!src) {
		return;
	}
	if (n < 14) {
		if (debug) log_client(src, "Received short frame, %d bytes", n);
		client_drop(src, n);
		goto exit;
	} else if (n >= MAX_FRAME_SIZE) {
		if (debug) log_client(src, "Received long frame, %d bytes", n);
		client_drop(src, n);
		goto exit;
	}

	// Check ethernet header
	ether_header *eth_hdr = (ether_header*)buf;
	if (eth_hdr->ether_shost[0] & 0x01) {
		if (debug) {
			log_client(src, "Illegal source MAC %02x:%02x:%02x:%02x:%02x:%02x",
				eth_hdr->ether_shost[0], eth_hdr->ether_shost[1], eth_hdr->ether_shost[2],
				eth_hdr->ether_shost[3], eth_hdr->ether_shost[4], eth_hdr->ether_shost[5]);
		}
		client_drop(src, n);
		goto exit;
	}

	uint16_t vlan_id = 0;
	if (eth_hdr->ether_type == ETH_P_8021Q_NET || eth_hdr->ether_type == ETH_P_8021AD_NET) {
		vlan_header *vlan_hdr = (vlan_header *)eth_hdr->data;
		vlan_id = ntohs(vlan_hdr->vlan_tci) & VLAN_VID_MASK;
	}

    // Get MAC entry
    me = get_mac(eth_hdr->ether_shost, src, vlan_id);
	if (!me) {
		if (debug) {
			log_client(src, "Failed to get MAC object for %02x:%02x:%02x:%02x:%02x:%02x vlan %u",
				eth_hdr->ether_shost[0], eth_hdr->ether_shost[1], eth_hdr->ether_shost[2],
				eth_hdr->ether_shost[3], eth_hdr->ether_shost[4], eth_hdr->ether_shost[5],
				vlan_id); 
		}
		client_drop(src, n);
		goto exit;
	}

    // Update stats
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    double since = now.tv_sec - me->last_seen.tv_sec;
    if (me->client != src) {
        if (since < min_learn_age) {
            if (debug) {
				log_mac_client(me, src, "Skip re-learn (%.1fs < min_age)", since);
			}
			client_drop(src, n);
			mac_drop(me, n);
            goto exit;
        } else {
			if (debug) {
				log_mac_client(me, src, "Re-learning after %.1fs", since);
			}
            remove_mac_from_client(me->client, me);
            if (add_mac_to_client(src, me) < 0) {
				if (debug) {
					log_mac_client(me, src, "Failed to relearn on new client");
				}
				SAFE_HASH_DEL(macs, me);
				client_drop(src, n);
				mac_drop(me, n);
				goto exit;
			}
            mac_reset_ctrs(me);
        }
    }    
    me->last_seen = now;

	mac_add_rx(me, n);
	client_add_rx(src, n);

    // Forward packet
	bool is_group = eth_hdr->ether_dhost[0] & 0x01;
    if (is_group) {
		udp_client *cl, *ct;
		// Multicast: forward to all clients with the VLAN present
		SAFE_HASH_ITER(hh, clients, cl, ct) {
			if (cl != src && cl->tenant_id == src->tenant_id) {
#if USE_PTHREAD
				pthread_rwlock_rdlock(&cl->vlan_list_lock);
#endif
				if (vlan_bitmap_test(&cl->vlans, vlan_id)) {
					if (debug > 1) {
						char cl_str[UDPCLIENT_STR_LEN];
						udp_client_to_str(cl, cl_str, sizeof(cl_str));
						log_client(src, "multicast %u bytes to %s", n, cl_str);
					}
					if (sendto(sockfd, buf, n, 0, (struct sockaddr*)&cl->addr, cl->addrlen) > 0) {
						client_add_tx(cl, n);
					} else {
						if (debug) {
							log_client(cl, "Failed to send %d bytes", n);
						}
					}
				}
#if USE_PTHREAD
				pthread_rwlock_unlock(&cl->vlan_list_lock);
#endif
            }
        }
		SAFE_HASH_ITER_DONE(hh, clients, cl, ct);
    } else {
		// Unicast: foward only to know MACs
		mac_entry *dmac;
		mac_key key;
        memcpy(key.mac, eth_hdr->ether_dhost, sizeof(key.mac));
		key.vlan_id = vlan_id;
		key.tenant_id = src->tenant_id;
        SAFE_HASH_FIND(hh, macs, &key, sizeof(key), dmac);
        if (dmac) {
            udp_client *cl = dmac->client;
			if (cl == src) { //no need to check smac == dmac
				log_client(src, "Cannot forward to self");
				client_drop(src, n);
				mac_drop(me, n);
#if USE_PTHREAD
				mac_entry_release(dmac);
#endif				
				goto exit;
			}
			if (debug > 1) {
				char cl_str[UDPCLIENT_STR_LEN];
				udp_client_to_str(cl, cl_str, sizeof(cl_str));
				log_client(src, "forward %u bytes to %s", n, cl_str);
			}
            if (sendto(sockfd, buf, n, 0,
                   (struct sockaddr*)&cl->addr, cl->addrlen) > 0) {
			    client_add_tx(cl, n);
				mac_add_tx(dmac, n);
			} else {
				if (debug) {
					log_client(cl, "Failed to send %d bytes", n);
					client_drop(src, n);
					mac_drop(me, n);
				}
			}
#if USE_PTHREAD
	        mac_entry_release(dmac);
#endif
        } else {
			if (debug > 1) {
				log_client(src, "Unknown destination MAC %02x:%02x:%02x:%02x:%02x:%02x vlan %u",
					eth_hdr->ether_dhost[0], eth_hdr->ether_dhost[1], eth_hdr->ether_dhost[2],
					eth_hdr->ether_dhost[3], eth_hdr->ether_dhost[4], eth_hdr->ether_dhost[5],
					vlan_id); 
			}
			client_drop(src, n);
			mac_drop(me, n);
		}
    }
exit:
#if USE_PTHREAD
	if (me) {
		mac_entry_release(me);
	}
	if (src) {
		udp_client_release(src);
	}
#endif
}

static void
dump_state(void)
{
    printf("Clients:%u\n", HASH_COUNT(clients));
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
	udp_client *cl, *ct;
	SAFE_HASH_ITER(hh, clients, cl, ct) {
        char cl_str[UDPCLIENT_STR_LEN];
		udp_client_to_str(cl, cl_str, sizeof(cl_str));
        printf("  %s  rx:%zu/%zu  tx:%zu/%zu  drop:%zu/%zu  VLANs:%zu\n",
               cl_str,
               cl->rxframes, cl->rxbytes,
               cl->txframes, cl->txbytes,
			   cl->dropframes, cl->dropbytes,
               cl->vlan_count);
#if USE_PTHREAD
		pthread_rwlock_rdlock(&cl->vlan_list_lock);
#endif
		for (dlist_entry *dle = cl->vlan_list.head; dle; dle = dle->next) {
			vlan_entry *vl = container_of(dle, vlan_entry, client_dle);
			printf("    VLAN %u  MACs:%zu\n", vl->vlan_id, vl->mac_count);
#if USE_PTHREAD
			pthread_mutex_lock(&vl->mac_list_lock);
#endif
			for (dlist_entry *dle2 = vl->mac_list.head; dle2; dle2 = dle2->next) {
				mac_entry *me = container_of(dle2, mac_entry, vlan_dle);
				char me_str[MACENTRY_STR_LEN];
				mac_entry_to_str(me, me_str, sizeof(me_str));
				double age = (now.tv_sec - me->last_seen.tv_sec)
						   + (now.tv_nsec - me->last_seen.tv_nsec)/1e9;
				printf("      %s  rx:%zu/%zu  tx:%zu/%zu  drop:%zu/%zu  age=%.3fs%s\n",
					   me_str,
					   me->rxframes, me->rxbytes,
					   me->txframes, me->txbytes,
					   me->dropframes, me->dropbytes,
					   age, me->persistent?"*":"");
			}
#if USE_PTHREAD
			pthread_mutex_unlock(&vl->mac_list_lock);
#endif
		}
#if USE_PTHREAD
		pthread_rwlock_unlock(&cl->vlan_list_lock);
#endif
    }
	SAFE_HASH_ITER_DONE(hh, clients, cl, ct);
	printf("Press ctrl-c two times in two seconds to terminate\n");
}

static void
sigint_cb(evutil_socket_t sig UNUSED, short events UNUSED, void *arg)
{
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    if ((now.tv_sec - last_interrupt.tv_sec) < 2) {
        event_base_loopexit((struct event_base*)arg, NULL);
    } else {
        last_interrupt = now;
        dump_state();
    }
}

static void
mac_age_cb(evutil_socket_t fd UNUSED, short what UNUSED, void *arg UNUSED)
{
	struct event *timer_event = (struct event *)arg;
	bool to_delete = false;
	bool check_clients = false;
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    mac_entry *me, *mt;
	SAFE_HASH_ITER(hh, macs, me, mt) {
		if (debug > 1) {
			double age = (now.tv_sec - me->last_seen.tv_sec)
					+ (now.tv_nsec - me->last_seen.tv_nsec)/1e9;
			log_mac_client(me, me->client, "Checking age (%.3fs) persistent=%s",
					age, me->persistent?"true":"false");
		}
        if (!me->persistent && ((now.tv_sec - me->last_seen.tv_sec) >= mac_timeout)) {
			to_delete = true;
			break;
        }
    }
	SAFE_HASH_ITER_DONE(hh, macs, me, mt);
	
	if (!to_delete) {
		goto reschedule;
	}

	SAFE_HASH_ITER_WRITE(hh, macs, me, mt) {
        if (!me->persistent && ((now.tv_sec - me->last_seen.tv_sec) >= mac_timeout)) {
			udp_client *cl = me->client;
			log_mac_client(me, cl, "Aged out");
			// Don't use SAFE_HASH_DEL, already in lock
			HASH_DEL(macs, me);
            remove_mac_from_client(cl, me);
			if (!keep_clients && !cl->persistent && cl->vlan_count == 0) {
				// Avoid lock inversion, delete client outside of MAC loop
				check_clients = true;
			}
#if USE_PTHREAD
			mac_entry_release(me);
#endif
        }
    }
	SAFE_HASH_ITER_DONE(hh, macs, me, mt);

	if (check_clients) {
		udp_client *cl, *ct;
		SAFE_HASH_ITER_WRITE(hh, clients, cl, ct) {
			if (!cl->persistent && cl->vlan_count == 0) {
				// Don't use SAFE_HASH_DEL, already in lock
				log_client(cl, "Aged out");
				HASH_DEL(clients, cl);
				remove_client_from_tenant(cl);
#if USE_PTHREAD
				udp_client_release(cl);
#endif
			}
		}
		SAFE_HASH_ITER_DONE(hh, clients, cl, ct);
	}
reschedule:
    struct timeval tv = { .tv_sec = MAC_CHECK_INTERVAL, .tv_usec = 0 };
    evtimer_add(timer_event, &tv);
}

static mac_entry *add_persistent_mac_to_vlan(udp_client *client, vlan_entry *vl, const char *mac_str)
{
	uint8_t mac[6];
    if (sscanf(mac_str, "%hhx:%hhx:%hhx:%hhx:%hhx:%hhx",
               &mac[0], &mac[1], &mac[2],
               &mac[3], &mac[4], &mac[5]) != 6) {
        return NULL;
    }

	mac_entry *me = get_mac(mac, client, vl->vlan_id);
	if (me) {
		me->persistent = true;
		log_mac_client(me, client, "Added persistent MAC");
	}

    return me;
}

static vlan_entry *add_persistent_vlan_to_client(udp_client *cl, uint16_t vlan_id)
{
	if (vlan_id > VLAN_VID_MASK) {
		return NULL;
	}
	vlan_entry *vl = get_vlan(cl, vlan_id);
	if (!vl) {
		return NULL;
	}
	vl->persistent = true;
	log_client(cl, "Added persistent VLAN %u", vlan_id);
    return vl;
}

static udp_client *add_persistent_client(int tenant_id, const char *ip, int port)
{
	if (port < 0 || port > 65535) {
		fprintf(stderr, "Invalid port %d for client IP %s\n", port, ip);
		return NULL;
	}
	if (tenant_id < 0) {
		fprintf(stderr, "Invalid tenant %d for client %s:%d\n", tenant_id, ip, port);
		return NULL;
	}
    struct sockaddr_storage ss = {0};
    socklen_t slen;
    if (strchr(ip, ':')) {
        struct sockaddr_in6 *sin6 = (void *)&ss;
        sin6->sin6_family = AF_INET6;
		if (inet_pton(AF_INET6, ip, &sin6->sin6_addr) == 0) {
			fprintf(stderr, "Invalid client IPv6 address %s\n", ip);
			return NULL;
		}
        sin6->sin6_port = htons(port);
        slen = sizeof(*sin6);
		if (bind_family == AF_INET) {
			fprintf(stderr, "Cannot add IPv6 (%s) client on IPv4 socket.\n", ip);
			return NULL;
		}
    } else {
		struct in_addr addr = {};
		if (inet_pton(AF_INET, ip, &addr) == 0) {
			fprintf(stderr, "Invalid client IP address %s\n", ip);
			return NULL;
		}
		if (bind_family == AF_INET) {
			struct sockaddr_in *sin4 = (void *)&ss;
			sin4->sin_family = AF_INET;
			sin4->sin_addr = addr;
			sin4->sin_port = htons(port);
			slen = sizeof(*sin4);
		} else {
			struct sockaddr_in6 *sin6 = (void *)&ss;
			sin6->sin6_family = AF_INET6;
			sin6->sin6_addr.s6_addr16[5] = 0xffff;
			sin6->sin6_addr.s6_addr32[3] = addr.s_addr;
			sin6->sin6_port = htons(port);
			slen = sizeof(*sin6);
		}
    }
    udp_client *cl = get_client((uint32_t)tenant_id, (struct sockaddr *)&ss, slen);
    if (cl) {
		cl->persistent = true;
        log_client(cl, "Added persistent client");
		if (cl->tenant_id != (uint32_t)tenant_id) {
			log_client(cl, "Existing tenant_id (%u) != requested tenant id (%u)", cl->tenant_id, tenant_id);
		}
	}
	return cl;
}

static void load_clients_from_json_config(const char *cfg_file)
{
    json_error_t error;
    json_t *root = json_load_file(cfg_file, 0, &error);
    if (!root) {
        fprintf(stderr, "JSON parse error %s: line %d: %s\n",
                error.source, error.line, error.text);
        exit(EXIT_FAILURE);
    }
    json_t *clients_arr = json_object_get(root, "clients");
    if (!json_is_array(clients_arr)) {
        fprintf(stderr, "Config: 'clients' is not an array\n"); json_decref(root); exit(EXIT_FAILURE);
    }
    size_t idx;
    json_t *ent;
    json_array_foreach(clients_arr, idx, ent) {
        json_t *ip_val = json_object_get(ent, "ip");
		if (!ip_val || !json_is_string(ip_val)) {
            fprintf(stderr, "Invalid 'ip' for client index %zu\n", idx);
            continue;
		}
        json_t *port_val = json_object_get(ent, "port");
		if (!port_val || !json_is_integer(port_val)) {
            fprintf(stderr, "Invalid 'port' for client index %zu\n", idx);
            continue;
		}
		json_t *tenant_val = json_object_get(ent, "tenant");
		if (tenant_val && !json_is_integer(tenant_val)) {
            fprintf(stderr, "Invalid 'tenant' for client index %zu\n", idx);
            continue;
		}

        const char *ip = json_string_value(ip_val);
        int port = (int)json_integer_value(port_val);
		int tenant_id = tenant_val ? (int)json_integer_value(tenant_val) : 0;
		udp_client *cl = add_persistent_client(tenant_id, ip, port);
		if (!cl) {
			continue;
		}

		//Optional VLANs; if none, assume VLAN 0 for broadcasting
        json_t *vlans = json_object_get(ent, "vlans");
        if (json_is_array(vlans)) {
            size_t j;
            json_t *vlan;
            json_array_foreach(vlans, j, vlan) {
                if (json_is_integer(vlan)) {
                    uint16_t vid_i = (uint16_t)json_integer_value(vlan);
                    vlan_entry *vl UNUSED = add_persistent_vlan_to_client(cl, vid_i);
#if USE_PTHREAD
					if (vl) {
						vlan_entry_release(vl);
					}
#endif
                } else if (json_is_object(vlan)) {
					json_t *vid = json_object_get(vlan, "id");
					json_t *macs = json_object_get(vlan, "macs");
					if (json_is_integer(vid)) {
						int16_t vid_i = (uint16_t)json_integer_value(vid);
						vlan_entry *vl = add_persistent_vlan_to_client(cl, vid_i);
						if (!vl || !json_is_array(macs)) {
							continue;
						}
						size_t k;
						json_t *mac;
						json_array_foreach(macs, k, mac) {
							mac_entry *me UNUSED = add_persistent_mac_to_vlan(cl, vl, json_string_value(mac));
#if USE_PTHREAD
							if (me) {
								mac_entry_release(me);
							}
#endif
						}
#if USE_PTHREAD
						vlan_entry_release(vl);
#endif
					}
				}
            }
        } else {
			vlan_entry *vl UNUSED = add_persistent_vlan_to_client(cl, 0);
#if USE_PTHREAD
			if (vl) {
				vlan_entry_release(vl);
			}
#endif
		}
#if USE_PTHREAD
	    udp_client_release(cl);
#endif
    }
    json_decref(root);
}

int create_socket(void)
{
	struct sockaddr_storage addr = {};
	int addrlen = 0;
    if (bind_family == AF_INET6) {
		struct sockaddr_in6 *sin6 = (struct sockaddr_in6 *)&addr;
		sin6->sin6_family = bind_family;
        sin6->sin6_port = htons(port);
		inet_pton(bind_family, bind_ip, &sin6->sin6_addr);
		addrlen = sizeof(struct sockaddr_in6);
	} else { //AF_INET
		struct sockaddr_in *sin4 = (struct sockaddr_in *)&addr;
		sin4->sin_family = bind_family;
        sin4->sin_port=htons(port);
		inet_pton(bind_family, bind_ip, &sin4->sin_addr);
		addrlen = sizeof(struct sockaddr_in);
	}

    int sock = socket(bind_family, SOCK_DGRAM, 0);

#if USE_PTHREAD
	int on = 1;
#ifdef SO_REUSEPORT
    if (setsockopt(sock, SOL_SOCKET, SO_REUSEPORT, &on, sizeof(on)) < 0) {
        perror("setsockopt SO_REUSEPORT");
        close(sock);
        return -1;
    }
#else
	if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) < 0) {
        perror("setsockopt SO_REUSEADDR");
        close(sock);
        return -1;
    }
#endif
#endif
	
	int off = 0;
	setsockopt(sock, IPPROTO_IPV6, IPV6_V6ONLY, &off, sizeof(off));
	if (bind(sock, (struct sockaddr*)&addr, addrlen) < 0) {
		perror("bind");
		close(sock);
		return -1;
	}

	evutil_make_socket_nonblocking(sock);
	return sock;
}

static struct event_base *create_event_base()
{
#if defined(__CYGWIN__)
	struct event_config *cfg = event_config_new();
	event_config_set_flag(cfg, EVENT_BASE_FLAG_STARTUP_IOCP);
	struct event_base *base = event_base_new_with_config(cfg);
	event_config_free(cfg);
	return base;
#else
	struct event_base *base = event_base_new();
	return base;
#endif
}

#if USE_PTHREAD
typedef struct threadinfo {
	pthread_t tid;
	int wake_fd;
} threadinfo;
threadinfo *threads;

static void
wake_cb(evutil_socket_t fd UNUSED, short what UNUSED, void *arg)
{
    struct event_base *base = arg;
    event_base_loopbreak(base);
}

static void *
udp_thread_fn(void *arg)
{
	thread_idx = (int)(intptr_t)arg;
	snprintf(thread_str, sizeof(thread_str), "%d", thread_idx+1);

	int sock = create_socket();
	if (sock < 0) {
		return NULL;
	}
	
	printf("[%s] Starting UDP switch on port %d\n", thread_str, port);

    struct event_base *base = create_event_base();
    if (!base) {
        fprintf(stderr, "create_event_base failed\n");
        close(sock);
        return NULL;
    }
	//threads[thread_idx].base = base;

	int fds[2];
	if (evutil_socketpair(AF_UNIX, SOCK_STREAM, 0, fds) < 0) {
		perror("socketpair");
		return NULL;
	}
	evutil_make_socket_nonblocking(fds[0]);
	threads[thread_idx].wake_fd = fds[1];

	// 2) register the read side on this base
	struct event *wake_event = event_new(base, fds[0], EV_READ|EV_PERSIST, wake_cb, base);
	event_add(wake_event, NULL);

    struct event *udp_event = event_new(base, sock, EV_READ|EV_PERSIST, udp_read_cb, NULL);
    event_add(udp_event, NULL);

    event_base_dispatch(base);

	event_del(udp_event);
    event_free(udp_event);
    close(fds[0]);
    close(fds[1]);
	event_del(wake_event);
    event_free(wake_event);
    event_base_free(base);
    close(sock);
    return NULL;
}

#endif

static int
parse_client_spec(const char *spec_str)
{
    char ipbuf[INET6_ADDRSTRLEN + 1] = {0};
    const char *port_start = NULL;

    // Extract IP (handle IPv6 in brackets)
    if (*spec_str == '[') {
        const char *end = strchr(spec_str, ']');
        if (!end || *(end + 1) != ':') {
            fprintf(stderr, "Invalid IPv6 client spec (missing ]:): %s\n", spec_str);
            return -1;
        }
        size_t iplen = end - (spec_str + 1);
        if (iplen >= sizeof(ipbuf)) {
            fprintf(stderr, "IPv6 address too long: %s\n", spec_str);
            return -1;
        }
        memcpy(ipbuf, spec_str + 1, iplen);
        ipbuf[iplen] = '\0';
        port_start = end + 2;
    } else {
        const char *colon = strchr(spec_str, ':');
        if (!colon) {
            fprintf(stderr, "Missing port in client spec: %s\n", spec_str);
            return -1;
        }
        size_t iplen = colon - spec_str;
        if (iplen >= sizeof(ipbuf)) {
            fprintf(stderr, "IPv4 address too long: %s\n", spec_str);
            return -1;
        }
        memcpy(ipbuf, spec_str, iplen);
        ipbuf[iplen] = '\0';
        port_start = colon + 1;
    }

    // Parse port[:vlan[:mac]] with %n tracking
    int port = -1, vlan = 0, tenant = 0;
    char macbuf[32] = {0};
    int port_n = 0, vlan_n = 0, total_n = 0;

    int fields = sscanf(port_start, "%d%n:%d%n:%31[^@]%n",
                        &port, &port_n,
                        &vlan, &vlan_n,
                        macbuf, &total_n);

    if (fields < 1 || port_n == 0 || port <= 0 || port > 65535) {
        fprintf(stderr, "Invalid or missing port in client spec: %s\n", port_start);
        return -1;
    }

	if (fields >= 2) {
		if (vlan_n <= port_n + 1 || vlan < 0 || vlan > 4095) {
			fprintf(stderr, "Invalid VLAN in client spec (non-numeric or out of range)\n");
			return -1;
		}
	}

	const char *tenant_str = strchr(port_start, '@');
	if (tenant_str) {
		tenant = atoi(tenant_str + 1);
		if (tenant < 0) {
			fprintf(stderr, "Invalid tenant in client spec (non-numeric or out of range)\n");
			return -1;
		}
	}

    // Validate IP address
    struct in6_addr dummy6;
    struct in_addr dummy4;
    int family = strchr(ipbuf, ':') ? AF_INET6 : AF_INET;
    if ((family == AF_INET6 && inet_pton(AF_INET6, ipbuf, &dummy6) != 1) ||
        (family == AF_INET && inet_pton(AF_INET, ipbuf, &dummy4) != 1)) {
        fprintf(stderr, "Invalid IP address: %s\n", ipbuf);
        return -1;
    }

    // Add client, VLAN, optional MAC
    udp_client *cl = add_persistent_client(tenant, ipbuf, port);
    if (!cl) {
        fprintf(stderr, "Failed to add client %s:%d\n", ipbuf, port);
        return -1;
    }

	vlan_entry *vl = add_persistent_vlan_to_client(cl, vlan);
	if (!vl) {
		fprintf(stderr, "Failed to add VLAN %u to client %s:%d\n", vlan, ipbuf, port);
#if USE_PTHREAD
		udp_client_release(cl);
#endif
		return -1;
	}

	if (fields >= 3) {
		mac_entry *me = add_persistent_mac_to_vlan(cl, vl, macbuf);
		if (!me) {
			fprintf(stderr, "Invalid MAC address in client spec: %s\n", macbuf);
#if USE_PTHREAD
			vlan_entry_release(vl);
			udp_client_release(cl);
#endif
			return -1;
		}
#if USE_PTHREAD
		mac_entry_release(me);
#endif
	}
#if USE_PTHREAD
	vlan_entry_release(vl);
    udp_client_release(cl);
#endif

    return 0;
}


int main(int argc, char **argv)
{
	const char *cfg_file = NULL;
	int num_clients = 0;

	static struct option long_options[] = {
        { "config"        , required_argument, 0, 'c' },
		{ "port"          , required_argument, 0, 'p' },
		{ "bind-ip"       , required_argument, 0, 'b' },
		{ "client"        , required_argument, 0, 'C' },
		{ "keep-clients"  , no_argument      , 0, 'k' },
		{ "timeout"       , required_argument, 0, 't' },
		{ "min-age"       , required_argument, 0, 'm' },
		{ "threads"       , required_argument, 0, 'r' },
		{ "debug"         , no_argument      , 0, 'd' },
		{ "help"          , no_argument      , 0, 'h' },
		{ 0, 0, 0, 0 }
	};
	int opt, option_index = 0;
	while ((opt = getopt_long(argc, argv, "c:p:b:C:kt:m:r:dh?", long_options, &option_index)) != -1) {
		switch (opt) {
            case 'c':
                cfg_file = optarg;
                break;
			case 'p':
				port = atoi(optarg);
				break;
			case 'b':
				if (num_clients) {
					fprintf(stderr, "bind-ip must be provided before all client specs.\n");
					return 1;
				}
				bind_ip = optarg;
				if (strchr(bind_ip, ':')) {
					bind_family = AF_INET6;
				} else {
					bind_family = AF_INET;
				}
				break;
			case 'C':
				if (parse_client_spec(optarg) != 0) {
					fprintf(stderr, "Failed to parse client spec: %s\n", optarg);
					return 1;
				}
				num_clients++;
				break;
			case 'k':
				keep_clients = 1;
				break;
			case 't':
				mac_timeout = atoi(optarg);
				break;
			case 'm':
				min_learn_age = atoi(optarg);
				break;
#if USE_PTHREAD
			case 'r':
				num_threads = atoi(optarg);
				if (num_threads < 1) {
					num_threads = 1;
				}
				break;
#endif
			case 'd':
				debug++;
				break;
			case '?':
			default:
				char *name = basename(argv[0]);
				fprintf(stderr,
					"Usage: %s\n"
					"\t-c|--config <file>      - Configuration file in json format\n"
					"\t-p|--port <port>        - Listen port (default: 10000)\n"
					"\t-b|--bind <address>     - Listen IP address (default: ::)\n"
					"\t-C|--client <spec>      - Define a persistent client (repeatable)\n"
					"\t                          Format: <ip:port[:vlan[:mac]][@tenant_id]>\n"
					"\t                          Note: IPv6 address must be in brackets\n"
					"\t                          Example: -C 192.0.2.1:10000:10:aa:bb:cc:dd:ee:ff\n"
					"\t                                   -C '[2001:db8::1]:10000:20'\n"
					"\t-k|--keep-clients       - Keep all UDP clients after MACs have aged out for multicasts\n"
					"\t-t|--timeout <seconds>  - Max age to keep MAC entries (default: 300)\n"
					"\t-m|--min-age <seconds>  - Minimum age a MAC entry must be kept before moving to a new client\n"
					"\t                          (default: 5)\n"
#if USE_PTHREAD
					"\t-r|--threads <num>      - Number of threads to use for processing (default: 1)\n"
#endif
					"\t-d|--debug              - Enable debug logging (up to 3x)\n"
					"\n"
					"Configuration File Format (JSON):\n"
					"  {\n"
					"    \"clients\": [\n"
					"      {\n"
					"        \"ip\": \"192.0.2.10\",\n"
					"        \"port\": 10000,\n"
					"        \"vlans\": [10, 20]\n"
					"        \"tenant\": 1\n"
					"      },\n"
					"      {\n"
					"        \"ip\": \"192.0.2.11\",\n"
					"        \"port\": 10000,\n"
					"        \"vlans\": [\n"
					"          {\n"
					"            \"id\": 30,\n"
					"            \"macs\": [\"00:11:22:33:44:55\", \"aa:bb:cc:dd:ee:ff\"]\n"
					"          }\n"
					"        ],\n"
					"        \"tenant\": 2\n"
					"      }\n"
					"    ]\n"
					"  }\n"
					"\n"
					"  - Each client must specify 'ip' and 'port'.\n"
					"  - VLANs may be listed as integers or as objects with optional 'macs'.\n"
					"  - MACs added through the config are persistent and will not age out.\n"
					, name);
				return 1;
		}
	}
	
	if (cfg_file) {
        load_clients_from_json_config(cfg_file);
    }

#if USE_PTHREAD
    evthread_use_pthreads();

    struct event_base *base = create_event_base();
    if (!base) { fprintf(stderr, "Libevent init failed\n"); return 1; }

    threads = calloc(num_threads, sizeof(*threads));
    for (int i = 0; i < num_threads; i++) {
		threads[i].wake_fd = -1;
        if (pthread_create(&threads[i].tid, NULL, udp_thread_fn, (void*)(intptr_t)i) != 0) {
            perror("pthread_create");
            return 1;
        }
    }
#else
	printf("Starting UDP switch on port %d\n", port);
    struct event_base *base = create_event_base();
    if (!base) { fprintf(stderr, "Libevent init failed\n"); return 1; }

	int sock = create_socket();
	if (sock < 0) {
		return 1;
	}

    // Setup raw event for UDP reads
    struct event *read_event = event_new(base, sock, EV_READ|EV_PERSIST, udp_read_cb, NULL);
    event_add(read_event, NULL);
#endif

    // SIGINT handler
    struct event *signal_event = evsignal_new(base, SIGINT, sigint_cb, base);
    event_add(signal_event, NULL);

    // Schedule MAC aging
    struct event *timer_event = evtimer_new(base, mac_age_cb, event_self_cbarg());
    struct timeval initial = { .tv_sec = MAC_CHECK_INTERVAL, .tv_usec = 0 };
    evtimer_add(timer_event, &initial);

    // Start dispatcher
    event_base_dispatch(base);

#if USE_PTHREAD
    // Join threads on shutdown
    for (int i = 0; i < num_threads; i++) {
	    char one = 1;
		if (threads[i].wake_fd >= 0) {
			write(threads[i].wake_fd, &one, 1);
		}
    }
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i].tid, NULL);
    }
	free(threads);
#else
	event_del(read_event);
	event_free(read_event);
	close(sock);
#endif

    // Cleanup
    mac_entry *me, *mt;
    SAFE_HASH_ITER_WRITE(hh, macs, me, mt) {
		// Clear VLAN persistent flag so the can be automatically cleaned up
		me->vlan->persistent = false;
		remove_mac_from_client(me->client, me);
		// Don't use SAFE_HASH_DELETE, already in lock
		HASH_DEL(macs, me);
#if USE_PTHREAD
		mac_entry_release(me);
#endif
	}
	SAFE_HASH_ITER_DONE(hh, macs, me, mt);
	HASH_CLEAR(hh, macs);

    udp_client *c, *ct;
    SAFE_HASH_ITER_WRITE(hh, clients, c, ct) { 
#if USE_PTHREAD
		pthread_rwlock_wrlock(&c->vlan_list_lock);
#endif
		for (dlist_entry *dle = c->vlan_list.head; dle; ) {
			vlan_entry *vl = container_of(dle, vlan_entry, client_dle);
			dlist_entry *next = dle->next;
			dlist_del(&c->vlan_list, dle);
#if USE_PTHREAD
			vlan_entry_release(vl);
#else
			free(vl);
#endif
			dle = next;
		}
#if USE_PTHREAD
		pthread_rwlock_unlock(&c->vlan_list_lock);
#endif
		// Don't use SAFE_HASH_DELETE, already in lock
		HASH_DEL(clients, c);
		remove_client_from_tenant(c);
#if USE_PTHREAD
		udp_client_release(c);
#endif
	}
	SAFE_HASH_ITER_DONE(hh, clients, c, ct);
	HASH_CLEAR(hh, clients);
	
	event_del(timer_event);
    event_free(timer_event);
	event_del(signal_event);
    event_free(signal_event);
    event_base_free(base);
	printf("Done\n");
    return 0;
}
