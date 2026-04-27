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
 * linked_list.h
 *
 * Description:
 *     These list types are useful when there are only a predetermined set of
 *     lists that an object could be in.  List links are embedded directly into
 *     the objects, and thus no extra memory management overhead is required.
 *     (Of course, if only a small proportion of existing objects are in a list,
 *     the link fields in the remainder would be wasted space.  But usually,
 *     it saves space to not have separately-allocated list nodes.)
 *
 *     None of the functions here allocate any memory; they just manipulate
 *     externally managed memory.  The APIs for singly and doubly linked lists
 *     are identical as far as capabilities of both allow.
 *
 *     Each list has a list header, which exists even when the list is empty.
 *     An empty singly-linked list has a NULL pointer in its header.
 *     There are two kinds of empty doubly linked lists: those that have been
 *     initialized to NULL, and those that have been initialized to circularity.
 *     (If a dlist is modified and then all its elements are deleted, it will be
 *     in the circular state.)	We prefer circular dlists because there are some
 *     operations that can be done without branches (and thus faster) on lists
 *     that use circular representation.  However, it is often convenient to
 *     initialize list headers to zeroes rather than setting them up with an
 *     explicit initialization function, so we also allow the other case.
 *
 *     EXAMPLES
 *
 *     Here's a simple example demonstrating how this can be used.  Let's assume
 *     we want to store information about the tables contained in a database.
 *
 *     #include "common/slist.h"
 *
 *     // Define struct for the databases including a list header that will be
 *     // used to access the nodes in the table list later on.
 *     typedef struct my_database
 *     {
 *		    char	   *datname;
 *		    DListHead	tables;
 *		    // ...
 *     } my_database;
 *
 *     // Define struct for the tables.  Note the listNode element which stores
 *     // prev/next list links.  The listNode element need not be first.
 *     typedef struct my_table
 *     {
 *		    char	   *tablename;
 *		    DListNode	listNode;
 *		    pert		permissions;
 *		    // ...
 *     } my_table;
 *
 *     // create a database
 *     my_database *db = create_database();
 *
 *     // and add a few tables to its table list
 *     DListPushHead(&db->tables, &create_table(db, "a")->listNode);
 *     ...
 *     DListPushHead(&db->tables, &create_table(db, "b")->listNode);
 *
 *
 *     To iterate over the table list, we allocate an iterator variable and use
 *     a specialized looping construct.  Inside a DLIST_FOR_EACH, the iterator's
 *     'cur' field can be used to access the current element.  iter.cur points to
 *     a 'DListNode', but most of the time what we want is the actual table
 *     information; DLIST_CONTAINER() gives us that, like so:
 *
 *     DListIter	iter;
 *     DLIST_FOR_EACH(iter, &db->tables)
 *     {
 *		    my_table   *tbl = DLIST_CONTAINER(my_table, listNode, iter.cur);
 *		    printf("we have a table: %s in database %s\n",
 *			       tbl->tablename, db->datname);
 *     }
 *
 *
 *     While a simple iteration is useful, we sometimes also want to manipulate
 *     the list while iterating.  There is a different iterator element and looping
 *     construct for that.  Suppose we want to delete tables that meet a certain
 *     criterion:
 *
 *     DListMutableIter miter;
 *     DLIST_MODIFY_FOR_EACH(miter, &db->tables)
 *     {
 *		    my_table   *tbl = DLIST_CONTAINER(my_table, listNode, miter.cur);
 *
 *		    if (!tbl->to_be_deleted)
 *			    continue;		// don't touch this one
 *
 *		    // unlink the current table from the linked list
 *		    DListDelete(miter.cur);
 *		    // as these lists never manage memory, we can still access the table
 *		    // after it's been unlinked
 *		    drop_table(db, tbl);
 *     }
 *
 * ---------------------------------------------------------------------------------
 */

#ifndef UTILS_LINKED_LIST_H
#define UTILS_LINKED_LIST_H

#include "defines/common.h"
#include "syslog/err_log.h"
#include "types/data_types.h"

GSDB_BEGIN_C_CODE_DECLS
/*
 * Enable for extra debugging. This is rather expensive, so it's not enabled by
 * default even when USE_ASSERT_CHECKING.
 */

/*
 * Node of a doubly linked list.
 *
 * Embed this in structs that need to be part of a doubly linked list.
 */
typedef struct DListNode DListNode;
struct DListNode {
    DListNode *prev;
    DListNode *next;
};

/*
 * Head of a doubly linked list.
 *
 * Non-empty lists are internally circularly linked.  Circular lists have the
 * advantage of not needing any branches in the most common list manipulations.
 * An empty list can also be represented as a pair of NULL pointers, making
 * initialization easier.
 */
typedef struct DListHead DListHead;
struct DListHead {
    /*
     * head.next either points to the first element of the list; to &head if
     * it's a circular empty list; or to NULL if empty and not circular.
     *
     * head.prev either points to the last element of the list; to &head if
     * it's a circular empty list; or to NULL if empty and not circular.
     */
    DListNode head;
};

/*
 * Doubly linked list iterator.
 *
 * Used as state in DLIST_FOR_EACH() and DListReverseForeach(). To get the
 * current element of the iteration use the 'cur' member.
 *
 * Iterations using this are *not* allowed to change the list while iterating!
 *
 * NB: We use an extra "end" field here to avoid multiple evaluations of
 * arguments in the DLIST_FOR_EACH() macro.
 */
typedef struct DListIter DListIter;
struct DListIter {
    DListNode *cur; /* current element */
    DListNode *end; /* last node we'll iterate to */
};

/*
 * Doubly linked list iterator allowing some modifications while iterating.
 *
 * Used as state in DLIST_MODIFY_FOR_EACH(). To get the current element of the
 * iteration use the 'cur' member.
 *
 * Iterations using this are only allowed to change the list at the current
 * point of iteration. It is fine to delete the current node, but it is *not*
 * fine to insert or delete adjacent nodes.
 *
 * NB: We need a separate type for mutable iterations so that we can store
 * the 'next' node of the current node in case it gets deleted or modified.
 */
typedef struct DListMutableIter DListMutableIter;
struct DListMutableIter {
    DListNode *cur;  /* current element */
    DListNode *next; /* next node we'll iterate to */
    DListNode *end;  /* last node we'll iterate to */
};

/*
 * Node of a singly linked list.
 *
 * Embed this in structs that need to be part of a singly linked list.
 */
typedef struct SListNode SListNode;
struct SListNode {
    SListNode *next;
};

/*
 * Head of a singly linked list.
 *
 * Singly linked lists are not circularly linked, in contrast to doubly linked
 * lists; we just set head.next to NULL if empty.  This doesn't incur any
 * additional branches in the usual manipulations.
 */
typedef struct SListHead SListHead;
struct SListHead {
    SListNode head;
};

/*
 * Singly linked list iterator.
 *
 * Used as state in SListForeach(). To get the current element of the
 * iteration use the 'cur' member.
 *
 * It's allowed to modify the list while iterating, with the exception of
 * deleting the iterator's current node; deletion of that node requires
 * care if the iteration is to be continued afterward.  (Doing so and also
 * deleting or inserting adjacent list elements might misbehave; also, if
 * the user frees the current node's storage, continuing the iteration is
 * not safe.)
 *
 * NB: this wouldn't really need to be an extra struct, we could use an
 * SListNode * directly. We prefer a separate type for consistency.
 */
typedef struct SListIter SListIter;
struct SListIter {
    SListNode *cur;
};

/*
 * Singly linked list iterator allowing some modifications while iterating.
 *
 * Used as state in SListForeachModify(). To get the current element of the
 * iteration use the 'cur' member.
 *
 * The only list modification allowed while iterating is to remove the current
 * node via SListDeleteCurrent() (*not* SListDelete()).  Insertion or
 * deletion of nodes adjacent to the current node would misbehave.
 */
typedef struct SListMutableIter SListMutableIter;
struct SListMutableIter {
    SListNode *cur;  /* current element */
    SListNode *next; /* next node we'll iterate to */
    SListNode *prev; /* prev node, for deletions */
};

/* Static initializers */
#define DLIST_STATIC_INIT(name)        \
    {                                  \
        {                              \
            &(name).head, &(name).head \
        }                              \
    }
#define SLIST_STATIC_INIT(name) \
    {                           \
        {                       \
            NULL                \
        }                       \
    }

/* Prototypes for functions too big to be inline */

/* Caution: this is O(n); consider using SListDeleteCurrent() instead */
extern void SListDelete(SListHead *head, const SListNode *node);

#ifdef LINKED_LIST_DEBUG
extern void DListCheck(DListHead *head);
extern void SListCheck(SListHead *head);
#endif /* LINKED_LIST_DEBUG */

/* doubly linked list implementation */

/* init a doubly linked list node */
static inline void DListNodeInit(DListNode *node)
{
    node->next = node->prev = NULL;
}

/*
 * Initialize a doubly linked list.
 * Previous state will be thrown away without any cleanup.
 */
static inline void DListInit(DListHead *head)
{
    head->head.next = head->head.prev = &head->head;
}

/*
 * Is the list empty?
 *
 * An empty list has either its first 'next' pointer set to NULL, or to itself.
 */
static inline bool DListIsEmpty(const DListHead *head)
{
#ifdef LINKED_LIST_DEBUG
    DListCheck(head);
#endif

    return head->head.next == NULL || head->head.next == &(head->head);
}

/*
 * Insert a node at the beginning of the list.
 */
static inline void DListPushHead(DListHead *head, DListNode *node)
{
    if (head->head.next == NULL) { /* convert NULL header to circular */
        DListInit(head);
    }
    node->next = head->head.next;
    node->prev = &head->head;
    node->next->prev = node;
    head->head.next = node;

#ifdef LINKED_LIST_DEBUG
    DListCheck(head);
#endif
}

/*
 * Insert a node at the end of the list.
 */
static inline void DListPushTail(DListHead *head, DListNode *node)
{
    if (head->head.next == NULL) { /* convert NULL header to circular */
        DListInit(head);
    }
    node->next = &head->head;
    node->prev = head->head.prev;
    node->prev->next = node;
    head->head.prev = node;

#ifdef LINKED_LIST_DEBUG
    DListCheck(head);
#endif
}

/*
 * Insert a node after another *in the same list*
 */
static inline void DListInsertAfter(DListNode *after, DListNode *node)
{
    node->prev = after;
    node->next = after->next;
    after->next = node;
    node->next->prev = node;
}

/*
 * Insert a node before another *in the same list*
 */
static inline void DListInsertBefore(DListNode *before, DListNode *node)
{
    node->prev = before->prev;
    node->next = before;
    before->prev = node;
    node->prev->next = node;
}

/*
 * Delete 'node' from its list (it must be in one).
 */
static inline void DListDelete(DListNode *node)
{
    node->prev->next = node->next;
    node->next->prev = node->prev;
}

/*
 * Remove and return the first node from a list (there must be one).
 */
static inline DListNode *DListPopHeadNode(DListHead *head)
{
    DListNode *node = NULL;

    ASSERT(!DListIsEmpty(head));
    node = head->head.next;
    DListDelete(node);
    return node;
}

/*
 * Move element from its current position in the list to the head position in
 * the same list.
 *
 * Undefined behaviour if 'node' is not already part of the list.
 */
static inline void DListMoveHead(DListHead *head, DListNode *node)
{
    /* fast path if it's already at the head */
    if (head->head.next == node) {
        return;
    }
    DListDelete(node);
    DListPushHead(head, node);

#ifdef LINKED_LIST_DEBUG
    DListCheck(head);
#endif
}

/*
 * Check whether 'node' has a following node.
 * Caution: unreliable if 'node' is not in the list.
 */
static inline bool DListHasNext(const DListHead *head, const DListNode *node)
{
    return node->next != &head->head;
}

/*
 * Check whether 'node' has a preceding node.
 * Caution: unreliable if 'node' is not in the list.
 */
static inline bool DListHasPrev(const DListHead *head, const DListNode *node)
{
    return node->prev != &head->head;
}

/*
 * Return the next node in the list (there must be one).
 */
static inline DListNode *DListNextNode(SYMBOL_UNUSED DListHead *head, DListNode *node)
{
    ASSERT(DListHasNext(head, node));
    return node->next;
}

/*
 * Return previous node in the list (there must be one).
 */
static inline DListNode *DListPrevNode(SYMBOL_UNUSED DListHead *head, DListNode *node)
{
    ASSERT(DListHasPrev(head, node));
    return node->prev;
}

/* internal support function to get address of head element's struct */
static inline void *DListHeadElementOff(DListHead *head, size_t off)
{
    ASSERT(!DListIsEmpty(head));
    return (char *)head->head.next - off;
}

/*
 * Return the first node in the list (there must be one).
 */
static inline DListNode *DListHeadNode(DListHead *head)
{
    return (DListNode *)DListHeadElementOff(head, 0);
}

/* internal support function to get address of tail element's struct */
static inline void *DListTailElementOff(DListHead *head, size_t off)
{
    ASSERT(!DListIsEmpty(head));
    return (char *)head->head.prev - off;
}

/*
 * Return the last node in the list (there must be one).
 */
static inline DListNode *DListTailNode(DListHead *head)
{
    return (DListNode *)DListTailElementOff(head, 0);
}

/**
 * Append all elements from src to tail of dst in O(1)
 * @param[in] dst destination list
 * @param[in] src source list, will be empty after calling
 */
void DListAppendMove(DListHead *dst, DListHead *src);

/*
 * Return the containing struct of 'type' where 'membername' is the DListNode
 * pointed at by 'ptr'.
 *
 * This is used to convert a DListNode * back to its containing struct.
 */
#define DLIST_CONTAINER(type, membername, ptr)                                                               \
    (sizeof(typeof(ptr)) == sizeof(DListNode *) && sizeof(((type *)NULL)->membername) == sizeof(DListNode) ? \
         ((type *)(uintptr_t)(((char *)(ptr)) - (offsetof(type, membername)))) :                             \
         (type *)NULL)

/*
 * Return the address of the first element in the list.
 *
 * The list must not be empty.
 */
#define DLIST_HEAD_ELEMENT(type, membername, lhead)                      \
    (ASSERT_VAR_IS_OF_TYPE_MACRO(((type *)NULL)->membername, DListNode), \
     (type *)DListHeadElementOff(lhead, offsetof(type, membername)))

/*
 * Return the address of the last element in the list.
 *
 * The list must not be empty.
 */
#define DLIST_TAIL_ELEMENT(type, membername, lhead)                      \
    (ASSERT_VAR_IS_OF_TYPE_MACRO(((type *)NULL)->membername, DListNode), \
     ((type *)DListTailElementOff(lhead, offsetof(type, membername))))

/*
 * Iterate through the list pointed at by 'lhead' storing the state in 'iter'.
 *
 * Access the current element with iter.cur.
 *
 * It is *not* allowed to manipulate the list during iteration.
 */
#define DLIST_FOR_EACH(iter, lhead)                                                                  \
    for ((iter).end = &(lhead)->head, (iter).cur = (iter).end->next ? (iter).end->next : (iter).end; \
         (iter).cur != (iter).end; (iter).cur = (iter).cur->next)

/*
 * Iterate through the list pointed at by 'lhead' storing the state in 'iter'.
 *
 * Access the current element with iter.cur.
 *
 * Iterations using this are only allowed to change the list at the current
 * point of iteration. It is fine to delete the current node, but it is *not*
 * fine to insert or delete adjacent nodes.
 */
#define DLIST_MODIFY_FOR_EACH(iter, lhead)                                                           \
    for ((iter).end = &(lhead)->head, (iter).cur = (iter).end->next ? (iter).end->next : (iter).end, \
        (iter).next = (iter).cur->next;                                                              \
         (iter).cur != (iter).end; (iter).cur = (iter).next, (iter).next = (iter).cur->next)

/*
 * Iterate through the list in reverse order.
 *
 * It is *not* allowed to manipulate the list during iteration.
 */
#define DLIST_REVERSE_FOR_EACH(iter, lhead)                                                             \
    for (ASSERT_VAR_IS_OF_TYPE_MACRO(iter, DListIter), ASSERT_VAR_IS_OF_TYPE_MACRO(lhead, DListHead *), \
         (iter).end = &(lhead)->head, (iter).cur = (iter).end->prev ? (iter).end->prev : (iter).end;    \
         (iter).cur != (iter).end; (iter).cur = (iter).cur->prev)

/* singly linked list implementation */

/*
 * Initialize a singly linked list.
 * Previous state will be thrown away without any cleanup.
 */
static inline void SListInit(SListHead *head)
{
    head->head.next = NULL;
}

/*
 * Is the list empty?
 */
static inline bool SListIsEmpty(const SListHead *head)
{
#ifdef LINKED_LIST_DEBUG
    SListCheck(head);
#endif

    return head->head.next == NULL;
}

/*
 * Insert a node at the beginning of the list.
 */
static inline void SListPushHead(SListHead *head, SListNode *node)
{
    node->next = head->head.next;
    head->head.next = node;

#ifdef LINKED_LIST_DEBUG
    SListCheck(head);
#endif
}

/*
 * Insert a node after another *in the same list*
 */
static inline void SListInsertAfter(SListNode *after, SListNode *node)
{
    node->next = after->next;
    after->next = node;
}

/*
 * Remove and return the first node from a list (there must be one).
 */
static inline SListNode *SListPopHeadNode(SListHead *head)
{
    SListNode *node = NULL;

    ASSERT(!SListIsEmpty(head));
    node = head->head.next;
    head->head.next = node->next;
#ifdef LINKED_LIST_DEBUG
    SListCheck(head);
#endif
    return node;
}

/*
 * Check whether 'node' has a following node.
 */
static inline bool SListHasNext(SYMBOL_UNUSED const SListHead *head, const SListNode *node)
{
#ifdef LINKED_LIST_DEBUG
    SListCheck(head);
#endif

    return node->next != NULL;
}

/*
 * Return the next node in the list (there must be one).
 */
static inline SListNode *SListNextNode(SYMBOL_UNUSED SListHead *head, SListNode *node)
{
    ASSERT(SListHasNext(head, node));
    return node->next;
}

/* internal support function to get address of head element's struct */
static inline void *SListHeadElementOff(SListHead *head, size_t off)
{
    ASSERT(SListIsEmpty(head));
    return (char *)head->head.next - off;
}

/*
 * Return the first node in the list (there must be one).
 */
static inline SListNode *SListHeadNode(SListHead *head)
{
    return (SListNode *)SListHeadElementOff(head, 0);
}

/*
 * Delete the list element the iterator currently points to.
 *
 * Caution: this modifies iter->cur, so don't use that again in the current
 * loop iteration.
 */
static inline void SListDeleteCurrent(SListMutableIter *iter)
{
    /*
     * Update previous element's forward link.  If the iteration is at the
     * first list element, iter->prev will point to the list header's "head"
     * field, so we don't need a special case for that.
     */
    iter->prev->next = iter->next;

    /*
     * Reset cur to prev, so that prev will continue to point to the prior
     * valid list element after SListForeachModify() advances to the next.
     */
    iter->cur = iter->prev;
}

/*
 * Return the containing struct of 'type' where 'membername' is the SListNode
 * pointed at by 'ptr'.
 *
 * This is used to convert a SListNode * back to its containing struct.
 */
#define SLIST_CONTAINER(type, membername, ptr)                                                               \
    (sizeof(typeof(ptr)) == sizeof(SListNode *) && sizeof(((type *)NULL)->membername) == sizeof(SListNode) ? \
         ((type *)(uintptr_t)(((char *)(ptr)) - (offsetof(type, membername)))) :                             \
         (type *)NULL)

/*
 * Return the address of the first element in the list.
 *
 * The list must not be empty.
 */
#define SLIST_HEAD_ELEMENT(type, membername, lhead)                      \
    (ASSERT_VAR_IS_OF_TYPE_MACRO(((type *)NULL)->membername, SListNode), \
     (type *)SListHeadElementOff(lhead, offsetof(type, membername)))

/*
 * Iterate through the list pointed at by 'lhead' storing the state in 'iter'.
 *
 * Access the current element with iter.cur.
 *
 * It's allowed to modify the list while iterating, with the exception of
 * deleting the iterator's current node; deletion of that node requires
 * care if the iteration is to be continued afterward.  (Doing so and also
 * deleting or inserting adjacent list elements might misbehave; also, if
 * the user frees the current node's storage, continuing the iteration is
 * not safe.)
 */
#define SLIST_FOR_EACH(iter, lhead) \
    for ((iter).cur = (lhead)->head.next; (iter).cur != NULL; (iter).cur = (iter).cur->next)

/*
 * Iterate through the list pointed at by 'lhead' storing the state in 'iter'.
 *
 * Access the current element with iter.cur.
 *
 * The only list modification allowed while iterating is to remove the current
 * node via SListDeleteCurrent() (*not* SListDelete()).  Insertion or
 * deletion of nodes adjacent to the current node would misbehave.
 */
#define SLIST_MODIFY_FOR_EACH(iter, lhead)                             \
    for ((iter).prev = &(lhead)->head, (iter).cur = (iter).prev->next, \
        (iter).next = (iter).cur ? (iter).cur->next : NULL;            \
         (iter).cur != NULL;                                           \
         (iter).prev = (iter).cur, (iter).cur = (iter).next, (iter).next = (iter).next ? (iter).next->next : NULL)

GSDB_END_C_CODE_DECLS
#endif /* UTILS_LINKED_LIST_H */
