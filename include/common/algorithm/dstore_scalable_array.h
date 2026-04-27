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
 * dstore_scalable_array.h
 *    Definition of a scalable array.
 *
 * IDENTIFICATION
 *    src/gausskernel/dstore/include/common/algorithm/dstore_scalable_array.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef DSTORE_SCALABLE_ARRAY_H
#define DSTORE_SCALABLE_ARRAY_H
#include <atomic>
#include "common/concurrent/dstore_atomic.h"
#include "common/dstore_datatype.h"
#include "common/algorithm/dstore_ilist.h"

namespace DSTORE {
/* chunk size should be a power of 2 */
constexpr uint32 CHUNCK_SIZE = 64;

template<typename T>
class ScalableArray {
public:
    explicit ScalableArray(DstoreMemoryContext context = g_dstoreCurrentMemoryContext,
        uint32 chunkSizeArg = CHUNCK_SIZE);
    ScalableArray(const ScalableArray &) = default;
    ScalableArray &operator=(const ScalableArray &) = default;
    DISALLOW_MOVE(ScalableArray);
    ~ScalableArray();

    /* NOTE: there is no deconstruction for elements, user should deconstruct elements if nessasery */
    void Destroy() noexcept;
    /* Pre-allocate memory for num objects */
    RetStatus Reserve(uint64 num);
    bool IsEmpty() { return m_size == 0; }
    uint64 Size() { return m_size; }
    T& Front();
    T& Back();
    RetStatus PushBack(T& element);
    template<class... Args>
    RetStatus EmplaceBack(Args &&... args);
    void PopFront();

    T& operator[](const uint64& index);
private:
    T& At(const uint64& index);
    RetStatus ExtendNode();
    struct Node : public BaseObject {
        Node() : array(nullptr), listNode{nullptr} {}
        T* array;
        slist_node listNode;
    };
    DstoreMemoryContext m_memoryContext;
    uint32 m_chunkSize;
    uint32 m_chunkSizeBits;
    uint64 m_size;
    uint64 m_capacity;
    slist_head m_head;
    uint64 m_headIndex;
    Node* m_tailNode;
};

template<typename T>
ScalableArray<T>::ScalableArray(DstoreMemoryContext context, uint32 chunkSize)
    : m_memoryContext(context), m_chunkSize(chunkSize), m_chunkSizeBits(0), m_size(0),
      m_capacity(0), m_head{ { nullptr } }, m_headIndex(0), m_tailNode(nullptr)
{
    while (!(m_chunkSize & (1U << m_chunkSizeBits))) {
        m_chunkSizeBits++;
    }
}

template<typename T>
ScalableArray<T>::~ScalableArray()
{
    Destroy();
}

template<typename T>
void ScalableArray<T>::Destroy() noexcept
{
    slist_mutable_iter iter;
    slist_foreach_modify(iter, &m_head) {
        Node *node = slist_container(Node, listNode, iter.cur);
        SListDeleteCurrent(&iter);
        DstorePfreeExt(node->array);
        delete node;
    }
    SListInit(&m_head);
    m_size = 0;
    m_capacity = 0;
}

template<typename T>
T& ScalableArray<T>::Front()
{
    StorageReleasePanic(m_size == 0, MODULE_COMMON, ErrMsg("No element"));
    return At(0);
}

template<typename T>
T& ScalableArray<T>::Back()
{
    StorageReleasePanic(m_size == 0, MODULE_COMMON, ErrMsg("No element"));
    return At(m_size - 1);
}

template<typename T>
RetStatus ScalableArray<T>::ExtendNode()
{
    Node* node = DstoreNew(m_memoryContext) Node();
    if (!node) {
        return DSTORE_FAIL;
    }
    node->array = static_cast<T*>(DstoreMemoryContextAlloc(m_memoryContext, m_chunkSize * sizeof(T)));
    if (!node->array) {
        delete node;
        return DSTORE_FAIL;
    }
    node->listNode.next = nullptr;
    if (m_tailNode) {
        SListInsertAfter(&m_tailNode->listNode, &node->listNode);
    } else {
        SListInsertAfter(&m_head.head, &node->listNode);
    }
    m_tailNode = node;
    m_capacity += m_chunkSize;
    return DSTORE_SUCC;
}

template<typename T>
RetStatus ScalableArray<T>::Reserve(uint64 num)
{
    if (num <= m_capacity) {
        return DSTORE_SUCC;
    }
    uint64 extendChunkNum = ((num - m_capacity) < m_chunkSize) ? 1 :
        (num - m_capacity) >> m_chunkSizeBits;
    for (uint64 i = 0; i < extendChunkNum; ++i) {
        if (STORAGE_FUNC_FAIL(ExtendNode())) {
            return DSTORE_FAIL;
        }
    }
    StorageAssert(m_capacity >= num);
    return DSTORE_SUCC;
}

template<typename T>
RetStatus ScalableArray<T>::PushBack(T& element)
{
    if (m_size >= m_capacity) {
        if (STORAGE_FUNC_FAIL(ExtendNode())) {
            return DSTORE_FAIL;
        }
    }

    uint64 tailBoundary = m_headIndex + m_size;
    m_tailNode->array[tailBoundary & (m_chunkSize - 1)] = element;
    m_size++;
    return DSTORE_SUCC;
}

template<typename T>
template<typename... Args>
RetStatus ScalableArray<T>::EmplaceBack(Args&&... args)
{
    if (m_size >= m_capacity) {
        if (STORAGE_FUNC_FAIL(ExtendNode())) {
            return DSTORE_FAIL;
        }
    }

    uint64 tailBoundary = m_headIndex + m_size;
    new(&m_tailNode->array[tailBoundary & (m_chunkSize - 1)]) T(std::forward<Args>(args)...);
    m_size++;
    return DSTORE_SUCC;
}

template<typename T>
void ScalableArray<T>::PopFront()
{
    StorageReleasePanic(m_size == 0, MODULE_COMMON, ErrMsg("No element"));
    m_headIndex++;
    uint64 shrinkChunk = (m_headIndex >> m_chunkSizeBits);
    if (shrinkChunk) {
        Node *tempNode = slist_container(Node, listNode, m_head.head.next);
        m_head.head.next = tempNode->listNode.next;
        m_tailNode = (m_head.head.next == nullptr) ? nullptr : m_tailNode;
        DstorePfreeExt(tempNode->array);
        delete tempNode;
        m_headIndex = m_headIndex & (m_chunkSize - 1);
    }
    m_capacity--;
    m_size--;
}

template<typename T>
T& ScalableArray<T>::At(const uint64& index)
{
    StorageReleasePanic(index >= m_capacity, MODULE_COMMON,
        ErrMsg("index %lu exceed capacity %lu", index, m_capacity));
    slist_node* cur = m_head.head.next;
    uint64 realIndex = index + m_headIndex;
    uint64 times = realIndex >> m_chunkSizeBits;
    while (times != 0) {
        cur = cur->next;
        times--;
    }
    Node *node = slist_container(Node, listNode, cur);
    return node->array[realIndex & (m_chunkSize - 1)];
}

template<typename T>
T& ScalableArray<T>::operator[](const uint64& index)
{
    StorageReleasePanic(index >= m_size, MODULE_COMMON,
        ErrMsg("index %lu exceed size %lu", index, m_size));
    return At(index);
}

} /* namespace DSTORE */

#endif /* DSTORE_SCALABLE_ARRAY_H */