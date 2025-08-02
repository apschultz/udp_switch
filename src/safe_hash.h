#ifndef SAFE_HASH_H
#define SAFE_HASH_H

#include <stdlib.h>
#include <uthash.h>

struct udp_client;
struct mac_entry;
struct vlan_entry;

#if USE_PTHREAD
#include <pthread.h>

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
    } while (0)

#define SAFE_HASH_FIND(hh_name, head, key_ptr, key_len, item_ptr)       \
    do {                                                                \
        pthread_rwlock_rdlock(&CAT(head,_lock));                        \
        HASH_FIND(hh_name, head, key_ptr, key_len, item_ptr);           \
        if (item_ptr) {                                                 \
            type_hold(item_ptr);                                        \
        }                                                               \
        pthread_rwlock_unlock(&CAT(head,_lock));                        \
    } while (0)

#define SAFE_HASH_DEL(head, item_ptr)                                   \
    do {                                                                \
        pthread_rwlock_wrlock(&CAT(head,_lock));                        \
        HASH_DEL(head, item_ptr);                                       \
        if (item_ptr) {                                                 \
            type_release(item_ptr);                                     \
        }                                                               \
        pthread_rwlock_unlock(&CAT(head,_lock));                        \
    } while (0)

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
    } while (0)

#else
#define SAFE_HASH_ADD_KEYPTR HASH_ADD_KEYPTR
#define SAFE_HASH_FIND HASH_FIND
#define SAFE_HASH_DEL(head, item_ptr)                                   \
    do {                                                                \
        HASH_DEL(head, item_ptr);                                       \
        free(item_ptr);                                                 \
    } while (0)
#define SAFE_HASH_ITER HASH_ITER
#define SAFE_HASH_ITER_WRITE HASH_ITER
#define SAFE_HASH_ITER_DONE(hh_name, head, item_ptr, tmp_item_ptr) {}
#endif

#endif /* SAFE_HASH_H */
