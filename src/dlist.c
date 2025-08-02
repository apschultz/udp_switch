#include <stddef.h>
#include "dlist.h"

void dlist_add(dlist *dl, dlist_entry *e)
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

void dlist_del(dlist *dl, dlist_entry *e)
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
    e->prev = NULL;
    e->next = NULL;
}
