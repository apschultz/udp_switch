#ifndef UDP_SWITCH_H
#define UDP_SWITCH_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>

#include "dlist.h"
#include "vlan_bitmap.h"

#ifndef USE_PTHREAD
#define USE_PTHREAD 1
#endif

#include "safe_hash.h"

#if USE_PTHREAD
#include <pthread.h>
#endif

extern int debug;

#define container_of(ptr, type, member) ({                      \
		const typeof( ((type *)0)->member ) *__mptr = (ptr);    \
		(type *)((char *)__mptr - offsetof(type,member));})

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

typedef struct tenant_entry {
	uint32_t                tenant_id;
	dlist                   client_list;
	dlist_entry             global_dle;
	uint32_t                refcount;
} tenant_entry;

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
	uint32_t                refcount;
#if USE_PTHREAD
	pthread_mutex_t         mac_list_lock;
	pthread_rwlock_t        vlan_list_lock;
#endif
} udp_client;

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

typedef struct vlan_entry {
	uint16_t                vlan_id;
	bool                    persistent;
	dlist                   mac_list;
	dlist_entry             client_dle;
	size_t                  mac_count;
	uint32_t                refcount;
#if USE_PTHREAD
	pthread_mutex_t         mac_list_lock;
#endif
} vlan_entry;

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
	uint32_t                 refcount;
};

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

#endif /* UDP_SWITCH_H */
