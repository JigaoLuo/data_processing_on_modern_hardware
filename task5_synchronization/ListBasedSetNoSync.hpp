#pragma once

#include "Util.hpp"
#include <cstdint>
#include <limits>

template <class T>
struct ListBasedSetNoSync {
    struct Entry {
        T key;
        Entry *next;
    };

    /// Head
    Entry staticHead;
    /// Tail
    Entry staticTail;

    ListBasedSetNoSync() {
      staticHead.key = std::numeric_limits<T>::min();
      staticHead.next = &staticTail;
      staticTail.key = std::numeric_limits<T>::max();
      staticTail.next = nullptr;
    }

#ifdef DEBUG
    // TODO(jigao): comment it out for performance evaluation
    ~ListBasedSetNoSync() {
      Entry *curr = staticHead.next;
      while (curr != &staticTail) {
          auto tmp_next = curr->next;
          delete(curr);
          curr = tmp_next;
      }
    }
#endif

    bool contains(const T& k) {
      Entry *curr = staticHead.next;
      // Iterate over all elements whose key is smaller, we stop at the first
      // element with key >= k
      while (curr->key < k)
        curr = curr->next;
      // Check if this element is the searched key and return the result
      return (curr->key == k);
    }

    void insert(const T& k) {
      // Start inserting a new element from the head
      Entry *pred = &staticHead;
      Entry *curr = staticHead.next;
      // Iterate over all elements whose key is smaller, we stop at the first
      // element with key >= k
      while (curr->key < k) {
        pred = curr;
        curr = curr->next;
      }

      // As we have a set, there is nothing to do if the key is already contained
      if (curr->key == k)
        return;

      // Then: curr->key > k

      // Create a new list element for the new key
      Entry *n = new Entry{k, curr /* next */};
      // Insert the element in the list
      pred->next = n;
    }

    void remove(const T& k) {
      Entry *pred = &staticHead;
      Entry *curr = staticHead.next;
      // Iterate over all elements whose key is smaller, we stop at the first
      // element with key >= k
      while (curr->key < k) {
        pred = curr;
        curr = curr->next;
      }

      // If the element has the searched key, remove it
      if (curr->key == k) {
        pred->next = curr->next;
        // ignore reclamation for now
#ifdef DEBUG
        delete(curr);  // TODO(jigao): comment it out for performance evaluation
#endif
      }
    }
};

// Explicit Template Instantiation
template struct ListBasedSetNoSync<int64_t>;
