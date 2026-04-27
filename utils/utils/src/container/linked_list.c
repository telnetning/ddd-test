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
 * ---------------------------------------------------------------------------------
 *
 * linked_list.c
 *
 * Description:
 * 1. support for integrated/inline doubly- and singly- linked lists
 * 2. This file only contains functions that are too big to be considered
 *    for inlining.  See linked_list.h for most of the goodies.
 *
 * ---------------------------------------------------------------------------------
 */

#include "container/linked_list.h"

#ifdef LOCAL_MODULE_NAME
#undef LOCAL_MODULE_NAME
#endif
#define LOCAL_MODULE_NAME "linked_list"

UTILS_EXPORT void DListAppendMove(DListHead *dst, DListHead *src)
{
    ASSERT(dst != NULL);
    ASSERT(src != NULL);
    if (DListIsEmpty(src)) {
        return;
    }
    DListNode *srcHeadNode = DListHeadNode(src);
    DListNode *srcTailNode = DListTailNode(src);
    if (DListIsEmpty(dst)) {
        srcHeadNode->prev = &dst->head;
        srcTailNode->next = &dst->head;
        dst->head.next = srcHeadNode;
        dst->head.prev = srcTailNode;
    } else {
        DListNode *dstTailNode = DListTailNode(dst);
        dstTailNode->next = srcHeadNode;
        srcHeadNode->prev = dstTailNode;
        srcTailNode->next = &dst->head;
        dst->head.prev = srcTailNode;
    }
    /* Reset source list */
    DListInit(src);
}

/*
 * Delete 'node' from list.
 *
 * It is not allowed to delete a 'node' which is not in the list 'head'
 *
 * Caution: this is O(n); consider using slist_delete_current() instead.
 */
UTILS_EXPORT void SListDelete(SListHead *head, const SListNode *node)
{
    for (SListNode *cur = &head->head; cur->next != NULL; cur = cur->next) {
        if (cur->next == node) {
            cur->next = cur->next->next;
#ifdef LINKED_LIST_DEBUG
            SListCheck(head);
#endif
            return;
        }
    }
    /**
     * If the target 'node' is in the list, the ASSERT checking will never be executed.
     * So deleting a 'node' not in the list 'head' could be checked directly by ASSERT(false) in DEBUG version.
     */
    ASSERT(false);
}

#ifdef LINKED_LIST_DEBUG
/*
 * Verify integrity of a doubly linked list
 */
ErrorCode DListCheck(DListHead *head)
{
    DListNode *cur = NULL;

    if (head == NULL) {
        ErrLog(ERROR, ErrMsg("doubly linked list head address is NULL"));
        return ERROR_UTILS_COMMON_NOT_NULL_VIOLATION;
    }

    if (head->head.next == NULL && head->head.prev == NULL)
        return; /* OK, initialized as zeroes */

    /* iterate in forward direction */
    for (cur = head->head.next; cur != &head->head; cur = cur->next) {
        if (cur == NULL || cur->next == NULL || cur->prev == NULL || cur->prev->next != cur || cur->next->prev != cur) {
            ErrLog(ERROR, ErrCode(ERROR_UTILS_COMMON_DATA_CORRUPTED), ErrMsg("doubly linked list is corrupted."));
            return ERROR_UTILS_COMMON_DATA_CORRUPTED;
        }
    }

    /* iterate in backward direction */
    for (cur = head->head.prev; cur != &head->head; cur = cur->prev) {
        if (cur == NULL || cur->next == NULL || cur->prev == NULL || cur->prev->next != cur || cur->next->prev != cur) {
            ErrLog(ERROR, ErrCode(ERROR_UTILS_COMMON_DATA_CORRUPTED), ErrMsg("doubly linked list is corrupted."));
            return ERROR_UTILS_COMMON_DATA_CORRUPTED;
        }
    }
}

/*
 * Verify integrity of a singly linked list
 */
ErrorCode SListCheck(SListHead *head)
{
    SListNode *cur = NULL;

    if (head == NULL) {
        ErrLog(ERROR, ErrCode(ERRCODE_NOT_NULL_VIOLATION), ErrMsg("singly linked list head address is NULL"));
        return ERROR_UTILS_COMMON_NOT_NULL_VIOLATION;
    }

    /*
     * there isn't much we can test in a singly linked list except that it
     * actually ends sometime, i.e. hasn't introduced a cycle or similar
     */
    for (cur = head->head.next; cur != NULL; cur = cur->next)
        ;
}

#endif /* LINKED_LIST_DEBUG */
