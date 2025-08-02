#ifndef DLIST_H
#define DLIST_H

typedef struct dlist_entry {
	struct dlist_entry *prev;
	struct dlist_entry *next;
} dlist_entry;

typedef struct dlist {
	dlist_entry *head;
	dlist_entry *tail;
} dlist;

void dlist_add(dlist *dl, dlist_entry *e);
void dlist_del(dlist *dl, dlist_entry *e);

#endif /* DLIST_H */
