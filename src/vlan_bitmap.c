#include "vlan_bitmap.h"

void vlan_bitmap_set(vlan_bitmap *bm, uint16_t vlan_id)
{
    bm->bits[vlan_id / VLAN_BITMAP_BITS_PER_WORD] |=
        1ULL << (vlan_id % VLAN_BITMAP_BITS_PER_WORD);
}

void vlan_bitmap_clear(vlan_bitmap *bm, uint16_t vlan_id)
{
    bm->bits[vlan_id / VLAN_BITMAP_BITS_PER_WORD] &=
        ~(1ULL << (vlan_id % VLAN_BITMAP_BITS_PER_WORD));
}

int vlan_bitmap_test(const vlan_bitmap *bm, uint16_t vlan_id)
{
    return (bm->bits[vlan_id / VLAN_BITMAP_BITS_PER_WORD] >>
            (vlan_id % VLAN_BITMAP_BITS_PER_WORD)) & 1;
}
