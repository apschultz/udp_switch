#ifndef VLAN_BITMAP_H
#define VLAN_BITMAP_H

#include <stdint.h>

#define VLAN_VID_MASK   0xfff
#define VLAN_BITMAP_BITS_PER_WORD (64)
#define VLAN_BITMAP_WORDS ((VLAN_VID_MASK + VLAN_BITMAP_BITS_PER_WORD-1)/VLAN_BITMAP_BITS_PER_WORD)

typedef struct vlan_bitmap {
	uint64_t bits[VLAN_BITMAP_WORDS];
} vlan_bitmap;

void vlan_bitmap_set(vlan_bitmap *bm, uint16_t vlan_id);
void vlan_bitmap_clear(vlan_bitmap *bm, uint16_t vlan_id);
int vlan_bitmap_test(const vlan_bitmap *bm, uint16_t vlan_id);

#endif /* VLAN_BITMAP_H */
