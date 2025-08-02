#ifndef SAFE_HASH_H
#define SAFE_HASH_H

#include <stdlib.h>
#include <uthash.h>

struct udp_client;
struct mac_entry;
struct vlan_entry;

#if USE_PTHREAD
#include <pthread.h>
#endif

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

#if USE_PTHREAD
#define SAFE_WRLOCK pthread_rwlock_wrlock
#define SAFE_RDLOCK pthread_rwlock_rdlock
#define SAFE_UNLOCK pthread_rwlock_unlock
#else
#define SAFE_WRLOCK(lock)
#define SAFE_RDLOCK(lock)
#define SAFE_UNLOCK(lock)
#endif

#define SAFE_HASH_ADD_KEYPTR(hh_name, head, key_ptr, key_len, item_ptr) \
    do {                                                                \
        SAFE_WRLOCK(&CAT(head,_lock));                        \
        HASH_ADD_KEYPTR(hh_name, head, key_ptr, key_len, item_ptr);     \
        if (item_ptr) {                                                 \
            type_hold(item_ptr);                                        \
        }                                                               \
        SAFE_UNLOCK(&CAT(head,_lock));                        \
    } while (0)

#define SAFE_HASH_FIND(hh_name, head, key_ptr, key_len, item_ptr)       \
    do {                                                                \
        SAFE_RDLOCK(&CAT(head,_lock));                        \
        HASH_FIND(hh_name, head, key_ptr, key_len, item_ptr);           \
        if (item_ptr) {                                                 \
            type_hold(item_ptr);                                        \
        }                                                               \
        SAFE_UNLOCK(&CAT(head,_lock));                        \
    } while (0)

#define SAFE_HASH_DEL(head, item_ptr)                                   \
    do {                                                                \
        SAFE_WRLOCK(&CAT(head,_lock));                        \
        HASH_DEL(head, item_ptr);                                       \
        if (item_ptr) {                                                 \
            type_release(item_ptr);                                     \
        }                                                               \
        SAFE_UNLOCK(&CAT(head,_lock));                        \
    } while (0)

#define SAFE_HASH_ITER(hh_name, head, item_ptr, tmp_item_ptr)           \
    do {                                                                \
        SAFE_RDLOCK(&CAT(head,_lock));                        \
        HASH_ITER(hh_name, head, item_ptr, tmp_item_ptr)

#define SAFE_HASH_ITER_WRITE(hh_name, head, item_ptr, tmp_item_ptr)     \
    do {                                                                \
        SAFE_WRLOCK(&CAT(head,_lock));                        \
        HASH_ITER(hh_name, head, item_ptr, tmp_item_ptr)

#define SAFE_HASH_ITER_DONE(hh_name, head, item_ptr, tmp_item_ptr)      \
        SAFE_UNLOCK(&CAT(head,_lock));                        \
    } while (0)

#endif /* SAFE_HASH_H */
