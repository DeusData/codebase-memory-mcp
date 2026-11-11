#ifndef CBM_INVENTORY_LIMITS_INTERNAL_H
#define CBM_INVENTORY_LIMITS_INTERNAL_H

#include "discover/inventory_filter.h"

/* Shared Boolean configuration check; arithmetic exhaustion is checked separately. */
bool cif_limits_valid(const cbm_inventory_limits_t *limits);

#endif
