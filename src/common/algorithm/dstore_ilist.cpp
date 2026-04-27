/*
 * Copyright (C) 2026 Huawei Technologies Co.,Ltd.
 *
 * dstore is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * dstore is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. if not, see <https://www.gnu.org/licenses/>.
 *
 * ---------------------------------------------------------------------------------------
 *
 * ilist.cpp
 *	  support for integrated/inline doubly- and singly- linked lists
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/lib/ilist.cpp
 *
 * NOTES
 *	  This file only contains functions that are too big to be considered
 *	  for inlining.  See ilist.h for most of the goodies.
 *
 * -------------------------------------------------------------------------
 */
#include "common/algorithm/dstore_ilist.h"
#include "common/dstore_datatype.h"


namespace DSTORE {
/*
 * Delete 'node' from list.
 *
 * It is not allowed to delete a 'node' which is not in the list 'head'
 *
 * Caution: this is O(n); consider using slist_delete_current() instead.
 */
void SListDelete(slist_head* head, slist_node* node)
{
    slist_node* last = &head->head;
    slist_node* cur = nullptr;
    bool found DSTORE_PG_USED_FOR_ASSERTS_ONLY = false;

    while ((cur = last->next) != nullptr) {
        if (cur == node) {
            last->next = cur->next;
#ifdef DSTORE_USE_ASSERT_CHECKING
            found = true;
#endif
            break;
        }
        last = cur;
    }
    StorageAssert(found);

    slist_check(head);
}

#ifdef ILIST_DEBUG
/*
 * Verify integrity of a doubly linked list
 */
void dlist_check(dlist_head* head)
{
    dlist_node* cur = nullptr;

    if (head == nullptr)
        storage_set_error(LIST_ERROR_DOUBLY_NOT_NULL_VIOLATION);
        StorageAssert(0);

    if (head->head.next == nullptr && head->head.prev == nullptr)
        return; /* OK, initialized as zeroes */

    /* iterate in forward direction */
    for (cur = head->head.next; cur != &head->head; cur = cur->next) {
        if (cur == nullptr || cur->next == nullptr || cur->prev == nullptr || cur->prev->next != cur ||
            cur->next->prev != cur)
            storage_set_error(LIST_ERROR_DOUBLY_DATA_CORRUPTED);
            StorageAssert(0);
    }

    /* iterate in backward direction */
    for (cur = head->head.prev; cur != &head->head; cur = cur->prev) {
        if (cur == nullptr || cur->next == nullptr || cur->prev == nullptr || cur->prev->next != cur ||
            cur->next->prev != cur)
            storage_set_error(LIST_ERROR_DOUBLY_DATA_CORRUPTED);
            StorageAssert(0);
    }
}

/*
 * Verify integrity of a singly linked list
 */
void slist_check(slist_head* head)
{
    slist_node* cur = nullptr;

    if (head == nullptr)
        storage_set_error(LIST_ERROR_SINGLY_NOT_NULL_VIOLATION);
        StorageAssert(0);

    /*
     * there isn't much we can test in a singly linked list except that it
     * actually ends sometime, i.e. hasn't introduced a cycle or similar
     */
    for (cur = head->head.next; cur != nullptr; cur = cur->next)
        ;
}

#endif /* ILIST_DEBUG */
}
