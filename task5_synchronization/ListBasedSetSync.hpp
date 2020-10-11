#pragma once

#include "Util.hpp"
#include <atomic>
#include <mutex>
#include <iostream>
#include <shared_mutex>
#include <tbb/tbb.h>

// TODO(jigao): Code very copy-and-paset style. But sharing most common parts. Need refactoring. 1. define Entry outside. 2. Add call back to each function

// TODO(jigao): use read lock and upgrade it when necessary

// Defines all required functions for the synchronized implementations
template <class T>
struct ListBasedSetSync {
  virtual std::string get_name() = 0;
  virtual bool contains(const T& k) = 0;
  virtual void insert(const T& k) = 0;
  virtual void remove(const T& k) = 0;
};

// class M defines the used mutex, you can use the locks provided in tbb
template <class T, class M>
struct ListBasedSetCoarseLock : virtual public ListBasedSetSync<T> {
    // implement ListBasedSetNoSync using Coarse-Grained Locking
    struct Entry {
        T key;
        Entry *next;
    };

    /// Head
    Entry staticHead;
    /// Tail
    Entry staticTail;
    /// Mutex
    M mutex;

    ListBasedSetCoarseLock() {
        staticHead.key = std::numeric_limits<T>::min();
        staticHead.next = &staticTail;
        staticTail.key = std::numeric_limits<T>::max();
        staticTail.next = nullptr;
    }

#ifdef DEBUG
  // TODO(jigao): comment it out for performance evaluation
    ~ListBasedSetCoarseLock() {
        Entry *curr = staticHead.next;
        while (curr != &staticTail) {
            auto tmp_next = curr->next;
            delete(curr);
            curr = tmp_next;
        }
    }
#endif

    std::string get_name() override { return "Coarse Lock"; }

    bool contains(const T& k) override {
        typename M::scoped_lock lock(mutex);
//        std::lock_guard<M> lock_guard(mutex);
        Entry *curr = staticHead.next;
        // Iterate over all elements whose key is smaller, we stop at the first
        // element with key >= k
        while (curr->key < k)
            curr = curr->next;
        // Check if this element is the searched key and return the result
        return (curr->key == k);
    }

    void insert(const T& k) override {
        typename M::scoped_lock lock(mutex);
//        std::lock_guard<M> lock_guard(mutex);
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
        return;
    }

    void remove(const T& k) override {
        typename M::scoped_lock lock(mutex);
//        std::lock_guard<M> lock_guard(mutex);
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
        return;
    }
};

// Explicit Template Instantiation
template struct ListBasedSetCoarseLock<int64_t, tbb::mutex>;

// class M defines the used mutex
template <class T, class M>
struct ListBasedSetCoarseLockRW : virtual public ListBasedSetSync<T> {
    // implement ListBasedSetNoSync using Coarse-Grained Locking with read/write lock
    struct Entry {
        T key;
        Entry *next;
    };

    /// Head
    Entry staticHead;
    /// Tail
    Entry staticTail;
    /// Read Write Mutex
    M rw_mutex;

    ListBasedSetCoarseLockRW() {
        staticHead.key = std::numeric_limits<T>::min();
        staticHead.next = &staticTail;
        staticTail.key = std::numeric_limits<T>::max();
        staticTail.next = nullptr;
    }

#ifdef DEBUG
    // TODO(jigao): comment it out for performance evaluation
    ~ListBasedSetCoarseLockRW() {
        Entry *curr = staticHead.next;
        while (curr != &staticTail) {
            auto tmp_next = curr->next;
            delete(curr);
            curr = tmp_next;
        }
    }
#endif

    std::string get_name() override { return "Coarse RW Lock"; }

    bool contains(const T& k) override {
        typename M::scoped_lock lock(rw_mutex, false); // false := shared read lock
        Entry *curr = staticHead.next;
        // Iterate over all elements whose key is smaller, we stop at the first
        // element with key >= k
        while (curr->key < k)
            curr = curr->next;
        // Check if this element is the searched key and return the result
        return (curr->key == k);
    }

    void insert(const T& k) override {
        typename M::scoped_lock lock(rw_mutex, true);
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
        return;
    }

    void remove(const T& k) override {
        typename M::scoped_lock lock(rw_mutex, true);
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
        return;
    }
};

// Explicit Template Instantiation
template struct ListBasedSetCoarseLockRW<int64_t, tbb::spin_rw_mutex>;

// class M defines the used mutex
template <class T, class M>
struct ListBasedSetLockCoupling : virtual public ListBasedSetSync<T> {
    // implement ListBasedSetNoSync using Lock Coupling
    struct Entry {
        T key;
        Entry *next;
        /// Mutex in each entry
        M mutex;
    };

    /// Head
    Entry staticHead;
    /// Tail
    Entry staticTail;

    ListBasedSetLockCoupling() {
        staticHead.key = std::numeric_limits<T>::min();
        staticHead.next = &staticTail;
        staticTail.key = std::numeric_limits<T>::max();
        staticTail.next = nullptr;
    }

#ifdef DEBUG
    // TODO(jigao): comment it out for performance evaluation
    ~ListBasedSetLockCoupling() {
        Entry *curr = staticHead.next;
        while (curr != &staticTail) {
            auto tmp_next = curr->next;
            delete(curr);
            curr = tmp_next;
        }
    }
#endif

    std::string get_name() override { return "Lock Coupling"; }


    bool contains(const T& k) override {
        /// Wait for the mutex of staticHead node
        staticHead.mutex.lock();
        /// Got the mutex of staticHead node and access the staticHead node
        /// Wait for the mutex of current node
        staticHead.next->mutex.lock();
        /// Got the mutex of current node, release the mutex of staticHead node, access the current node
        staticHead.mutex.unlock();
        Entry *curr = staticHead.next;
        // Iterate over all elements whose key is smaller, we stop at the first
        // element with key >= k
        while (curr->key < k) {
            /// Wait for the mutex of next node
            curr->next->mutex.lock();
            /// Got the mutex of next node, release the mutex of current node, access the next node
            curr->mutex.unlock();
            curr = curr->next;
        }
        // Check if this element is the searched key
        const bool result = (curr->key == k);
        /// Release the mutex of current node
        curr->mutex.unlock();
        // return the result
        return result;
    }

    void insert(const T& k) override {
        /// Wait for the mutex of staticHead node
        staticHead.mutex.lock();
        /// Got the mutex of staticHead node and access the staticHead node
        // Start inserting a new element from the head
        Entry *pred = &staticHead;
        /// Wait for the mutex of current node
        staticHead.next->mutex.lock();
        /// Got the mutex of current node, access the current node
        Entry *curr = staticHead.next;
        // Iterate over all elements whose key is smaller, we stop at the first
        // element with key >= k
        while (curr->key < k) {
            /// No insertion at current node, so release the mutex of predecessor
            pred->mutex.unlock();
            pred = curr;
            /// Wait for the mutex of next node
            curr->next->mutex.lock();
            /// Got the mutex of next node, access the next node
            curr = curr->next;
        }

        /// Insertion position found: between predecessor node and current node
        /// This point has acquired these two mutex of them

        // As we have a set, there is nothing to do if the key is already contained
        if (curr->key == k) {
            /// Release two mutexes
            pred->mutex.unlock();
            curr->mutex.unlock();
            return;
        }

        // Then: curr->key > k

        // Create a new list element for the new key
        Entry *n = new Entry{k, curr /* next */};
        // Insert the element in the list
        pred->next = n;

        /// Release two mutexes
        pred->mutex.unlock();
        curr->mutex.unlock();
        return;
    }

    void remove(const T& k) override {
        /// Wait for the mutex of staticHead node
        staticHead.mutex.lock();
        /// Got the mutex of staticHead node and access the staticHead node
        Entry *pred = &staticHead;
        /// Wait for the mutex of current node
        staticHead.next->mutex.lock();
        /// Got the mutex of current node, access the current node
        Entry *curr = staticHead.next;
        // Iterate over all elements whose key is smaller, we stop at the first
        // element with key >= k
        while (curr->key < k) {
            /// No deletion at current node, so release the mutex of predecessor
            pred->mutex.unlock();
            pred = curr;
            /// Wait for the mutex of next node
            curr->next->mutex.lock();
            /// Got the mutex of next node, access the next node
            curr = curr->next;
        }

        /// Deletion position found: between predecessor node and current node
        /// This point has acquired these two mutex of them

        // If the element has the searched key, remove it
        if (curr->key == k) {
            pred->next = curr->next;
            /// Release two mutexes
            pred->mutex.unlock();
            curr->mutex.unlock();
            // ignore reclamation for now
#ifdef DEBUG
            delete(curr);  // TODO(jigao): comment it out for performance evaluation
#endif
            return;
        } else {
            /// Release two mutexes
            pred->mutex.unlock();
            curr->mutex.unlock();
            return;
        }
    }
};

// Explicit Template Instantiation
template struct ListBasedSetLockCoupling<int64_t, tbb::mutex>;

// class M defines the used mutex
template <class T, class M>
struct ListBasedSetLockCouplingRW : virtual public ListBasedSetSync<T> {
    // implement ListBasedSetNoSync using Lock Coupling with read/write locks
    struct Entry {
        T key;
        Entry *next;
        /// Mutex in each entry
        M mutex;
    };

    /// Head
    Entry staticHead;
    /// Tail
    Entry staticTail;

    ListBasedSetLockCouplingRW() {
        staticHead.key = std::numeric_limits<T>::min();
        staticHead.next = &staticTail;
        staticTail.key = std::numeric_limits<T>::max();
        staticTail.next = nullptr;
    }

#ifdef DEBUG
    // TODO(jigao): comment it out for performance evaluation
    ~ListBasedSetLockCouplingRW() {
        Entry *curr = staticHead.next;
        while (curr != &staticTail) {
            auto tmp_next = curr->next;
            delete(curr);
            curr = tmp_next;
        }
    }
#endif

    std::string get_name() override { return "RW Lock Coupling"; }

    bool contains(const T& k) override {
        /// Wait for the mutex of staticHead node
        staticHead.mutex.lock_read();
        /// Got the mutex of staticHead node and access the staticHead node
        /// Wait for the mutex of current node
        staticHead.next->mutex.lock_read();
        /// Got the mutex of current node, release the mutex of staticHead node, access the current node
        staticHead.mutex.unlock();
        Entry *curr = staticHead.next;
        // Iterate over all elements whose key is smaller, we stop at the first
        // element with key >= k
        while (curr->key < k) {
            /// Wait for the mutex of next node
            curr->next->mutex.lock_read();
            /// Got the mutex of next node, release the mutex of current node, access the next node
            curr->mutex.unlock();
            curr = curr->next;
        }
        // Check if this element is the searched key
        const bool result = (curr->key == k);
        /// Release the mutex of current node
        curr->mutex.unlock();
        // return the result
        return result;
    }

    void insert(const T& k) override {
        /// Wait for the mutex of staticHead node
        staticHead.mutex.lock();
        /// Got the mutex of staticHead node and access the staticHead node
        // Start inserting a new element from the head
        Entry *pred = &staticHead;
        /// Wait for the mutex of current node
        staticHead.next->mutex.lock();
        /// Got the mutex of current node, access the current node
        Entry *curr = staticHead.next;
        // Iterate over all elements whose key is smaller, we stop at the first
        // element with key >= k
        while (curr->key < k) {
            /// No insertion at current node, so release the mutex of predecessor
            pred->mutex.unlock();
            pred = curr;
            /// Wait for the mutex of next node
            curr->next->mutex.lock();
            /// Got the mutex of next node, access the next node
            curr = curr->next;
        }

        /// Insertion position found: between predecessor node and current node
        /// This point has acquired these two mutex of them

        // As we have a set, there is nothing to do if the key is already contained
        if (curr->key == k) {
            /// Release two mutexes
            pred->mutex.unlock();
            curr->mutex.unlock();
            return;
        }

        // Then: curr->key > k

        // Create a new list element for the new key
        Entry *n = new Entry{k, curr /* next */};
        // Insert the element in the list
        pred->next = n;

        /// Release two mutexes
        pred->mutex.unlock();
        curr->mutex.unlock();
        return;
    }

    void remove(const T& k) override {
        /// Wait for the mutex of staticHead node
        staticHead.mutex.lock();
        /// Got the mutex of staticHead node and access the staticHead node
        Entry *pred = &staticHead;
        /// Wait for the mutex of current node
        staticHead.next->mutex.lock();
        /// Got the mutex of current node, access the current node
        Entry *curr = staticHead.next;
        // Iterate over all elements whose key is smaller, we stop at the first
        // element with key >= k
        while (curr->key < k) {
            /// No deletion at current node, so release the mutex of predecessor
            pred->mutex.unlock();
            pred = curr;
            /// Wait for the mutex of next node
            curr->next->mutex.lock();
            /// Got the mutex of next node, access the next node
            curr = curr->next;
        }

        /// Deletion position found: between predecessor node and current node
        /// This point has acquired these two mutex of them

        // If the element has the searched key, remove it
        if (curr->key == k) {
            pred->next = curr->next;
            /// Release two mutexes
            pred->mutex.unlock();
            curr->mutex.unlock();
            // ignore reclamation for now
#ifdef DEBUG
            delete(curr);  // TODO(jigao): comment it out for performance evaluation
#endif
            return;
        } else {
            /// Release two mutexes
            pred->mutex.unlock();
            curr->mutex.unlock();
            return;
        }
    }
};

// Explicit Template Instantiation
template struct ListBasedSetLockCouplingRW<int64_t, tbb::spin_rw_mutex>;

static constexpr size_t MAX_ATTEMPTS = 10;

// class M defines the used mutex
template <class T, class M>
struct ListBasedSetOptimistic : virtual public ListBasedSetSync<T> {
    // implement ListBasedSetNoSync using Optimistic Locking

    struct Entry {
        T key;
        Entry *next;
        /// Mutex in each entry
        M mutex;
    };

    /// Head
    Entry staticHead;
    /// Tail
    Entry staticTail;

    ListBasedSetOptimistic() {
        staticHead.key = std::numeric_limits<T>::min();
        staticHead.next = &staticTail;
        staticTail.key = std::numeric_limits<T>::max();
        staticTail.next = nullptr;
    }

#ifdef DEBUG
    // TODO(jigao): comment it out for performance evaluation
    ~ListBasedSetOptimistic() {
        Entry *curr = staticHead.next;
        while (curr != &staticTail) {
            auto tmp_next = curr->next;
            delete(curr);
            curr = tmp_next;
        }
    }
#endif

    std::string get_name() override { return "Optimistic"; }

    bool contains(const T& k) override {
        /// Attempt to read optimistically
        for (size_t i = 0; i < MAX_ATTEMPTS; i++) {
            Entry *curr = staticHead.next;
            // Iterate over all elements whose key is smaller, we stop at the first
            // element with key >= k
            while (curr->key < k) {
                curr = curr->next;
            }
            /// Located and acquire the mutex of the current node
            const bool curr_try_lock = curr->mutex.try_lock();
            if (!curr_try_lock) {
                continue;
            }
            /// Got the mutex of next current node, access the current node

            /// Start to validate
            bool validate_result = false;
            {
                Entry *second_curr = staticHead.next;
                while (second_curr->key < k) {
                    second_curr = second_curr->next;
                }
                if (curr->key == second_curr->key && curr->next == second_curr->next) { // TODO(question): is this condition correct?
                    validate_result = true;
                }
            }
            if (!validate_result) {
                /// If validation stage fails, then restart.
                curr->mutex.unlock();
                continue;
            }

            // Check if this element is the searched key and return the result
            const bool result = (curr->key == k);
            curr->mutex.unlock();
            return result;
        }

        /// Fallback to pessimistic locking
#ifdef DEBUG
        std::cout << "Fallback: contains_pessimistic" << std::endl;
#endif
        return contains_pessimistic(k);
    }

    /// This is just the same version from lock coupling with read-write mutex
    // TODO(all): it can also be a mutex instead of read-write mutex, you can change that.
    bool contains_pessimistic(const T& k) {
        /// Wait for the mutex of staticHead node
        staticHead.mutex.lock();
        /// Got the mutex of staticHead node and access the staticHead node
        /// Wait for the mutex of current node
        staticHead.next->mutex.lock();
        /// Got the mutex of current node, release the mutex of staticHead node, access the current node
        staticHead.mutex.unlock();
        Entry *curr = staticHead.next;
        // Iterate over all elements whose key is smaller, we stop at the first
        // element with key >= k
        while (curr->key < k) {
            /// Wait for the mutex of next node
            curr->next->mutex.lock();
            /// Got the mutex of next node, release the mutex of current node, access the next node
            curr->mutex.unlock();
            curr = curr->next;
        }
        // Check if this element is the searched key
        const bool result = (curr->key == k);
        /// Release the mutex of current node
        curr->mutex.unlock();
        // return the result
        return result;
    }

    void insert(const T& k) override {
        /// Attempt to read optimistically
        for (size_t i = 0; i < MAX_ATTEMPTS; i++) {
            // Start inserting a new element from the head
            Entry *pred = &staticHead;
            Entry *curr = staticHead.next;
            // Iterate over all elements whose key is smaller, we stop at the first
            // element with key >= k
            while (curr->key < k) {
                pred = curr;
                curr = curr->next;
            }


            /// Insertion position found: between predecessor node and current node
            /// Wait for the mutex of predecessor node and current node
            const bool curr_try_lock = curr->mutex.try_lock();
            const bool pred_try_lock = pred->mutex.try_lock();
            if (!curr_try_lock || !pred_try_lock) {
                if (curr_try_lock) {
                    curr->mutex.unlock();
                }
                if (pred_try_lock) {
                    pred->mutex.unlock();
                }
                continue;
            }
            /// This point has acquired these two mutex of them

            /// Start to validate
            bool validate_result = false;
            {
                Entry *second_pred = &staticHead;
                Entry *second_curr = staticHead.next;
                // Iterate over all elements whose key is smaller, we stop at the first
                // element with key >= k
                while (second_curr->key < k) {
                    second_pred = second_curr;
                    second_curr = second_curr->next;
                }

                if (curr->key == second_curr->key && pred->key == second_pred->key && curr == second_pred->next) { // TODO(question): is this condition correct?
                    validate_result = true;
                }
            }
            if (!validate_result) {
                /// If validation stage fails, then restart.
                /// Release two mutexes
                pred->mutex.unlock();
                curr->mutex.unlock();
                continue;
            }

            // As we have a set, there is nothing to do if the key is already contained
            if (curr->key == k) {
                /// Release two mutexes
                pred->mutex.unlock();
                curr->mutex.unlock();
                return;
            }

            // Then: curr->key > k

            // Create a new list element for the new key
            Entry *n = new Entry{k, curr /* next */};
            // Insert the element in the list
            pred->next = n;

            /// Release two mutexes
            pred->mutex.unlock();
            curr->mutex.unlock();
            return;
        }

        /// Fallback to pessimistic locking
#ifdef DEBUG
        std::cout << "Fallback: insert_pessimistic" << std::endl;
#endif
        return insert_pessimistic(k);
    }

    void insert_pessimistic(const T& k) {
        /// Wait for the mutex of staticHead node
        staticHead.mutex.lock();
        /// Got the mutex of staticHead node and access the staticHead node
        // Start inserting a new element from the head
        Entry *pred = &staticHead;
        /// Wait for the mutex of current node
        staticHead.next->mutex.lock();
        /// Got the mutex of current node, access the current node
        Entry *curr = staticHead.next;
        // Iterate over all elements whose key is smaller, we stop at the first
        // element with key >= k
        while (curr->key < k) {
            /// No insertion at current node, so release the mutex of predecessor
            pred->mutex.unlock();
            pred = curr;
            /// Wait for the mutex of next node
            curr->next->mutex.lock();
            /// Got the mutex of next node, access the next node
            curr = curr->next;
        }

        /// Insertion position found: between predecessor node and current node
        /// This point has acquired these two mutex of them

        // As we have a set, there is nothing to do if the key is already contained
        if (curr->key == k) {
            /// Release two mutexes
            pred->mutex.unlock();
            curr->mutex.unlock();
            return;
        }

        // Then: curr->key > k

        // Create a new list element for the new key
        Entry *n = new Entry{k, curr /* next */};
        // Insert the element in the list
        pred->next = n;

        /// Release two mutexes
        pred->mutex.unlock();
        curr->mutex.unlock();
        return;
    }

    void remove(const T& k) override {
        if (staticHead.next == &staticTail) { return; }
        /// Attempt to read optimistically
        for (size_t i = 0; i < MAX_ATTEMPTS; i++) {
            Entry *pred = &staticHead;
            Entry *curr = staticHead.next;
            // Iterate over all elements whose key is smaller, we stop at the first
            // element with key >= k
            while (curr->key < k) {
                pred = curr;
                curr = curr->next;
            }

            /// Deletion position found: between predecessor node and current node
            /// Wait for the mutex of predecessor node and current node
            const bool curr_try_lock = curr->mutex.try_lock();
            const bool pred_try_lock = pred->mutex.try_lock();
            if (!curr_try_lock || !pred_try_lock) {
                if (curr_try_lock) {
                    curr->mutex.unlock();
                }
                if (pred_try_lock) {
                    pred->mutex.unlock();
                }
                continue;
            }
            /// This point has acquired these two mutex of them

            /// Start to validate
            bool validate_result = false;
            {
                Entry *second_pred = &staticHead;
                Entry *second_curr = staticHead.next;
                while (second_curr->key < k) {
                    second_pred = second_curr;
                    second_curr = second_curr->next;
                }

                if (curr->key == second_curr->key && pred->key == second_pred->key && curr == second_pred->next) { // TODO(question): is this condition correct?
                    validate_result = true;
                }
            }
            if (!validate_result) {
                /// Release two mutexes
                pred->mutex.unlock();
                curr->mutex.unlock();
                /// If validation stage fails, then restart.
                continue;
            }

            // If the element has the searched key, remove it
            if (curr->key == k) {
                pred->next = curr->next;
                /// Release two mutexes
                pred->mutex.unlock();
                curr->mutex.unlock();
                // ignore reclamation for now
#ifdef DEBUG
                delete(curr);  // TODO(jigao): comment it out for performance evaluation
#endif
                return;
            } else {
                /// Release two mutexes
                pred->mutex.unlock();
                curr->mutex.unlock();
                return;
            }
        }

        /// Fallback to pessimistic locking
#ifdef DEBUG
        std::cout << "Fallback: remove_pessimistic" << std::endl;
#endif
        return remove_pessimistic(k);
    }

    void remove_pessimistic(const T& k) {
        /// Wait for the mutex of staticHead node
        staticHead.mutex.lock();
        /// Got the mutex of staticHead node and access the staticHead node
        Entry *pred = &staticHead;
        /// Wait for the mutex of current node
        staticHead.next->mutex.lock();
        /// Got the mutex of current node, access the current node
        Entry *curr = staticHead.next;
        // Iterate over all elements whose key is smaller, we stop at the first
        // element with key >= k
        while (curr->key < k) {
            /// No deletion at current node, so release the mutex of predecessor
            pred->mutex.unlock();
            pred = curr;
            /// Wait for the mutex of next node
            curr->next->mutex.lock();
            /// Got the mutex of next node, access the next node
            curr = curr->next;
        }

        /// Deletion position found: between predecessor node and current node
        /// This point has acquired these two mutex of them

        // If the element has the searched key, remove it
        if (curr->key == k) {
            pred->next = curr->next;
            /// Release two mutexes
            pred->mutex.unlock();
            curr->mutex.unlock();
            // ignore reclamation for now
#ifdef DEBUG
            delete(curr);  // TODO(jigao): comment it out for performance evaluation
#endif
            return;
        } else {
            /// Release two mutexes
            pred->mutex.unlock();
            curr->mutex.unlock();
            return;
        }
    }
};

// Explicit Template Instantiation
template struct ListBasedSetOptimistic<int64_t, tbb::mutex>;

// class M defines the used mutex
template <class T, class M>
struct ListBasedSetOptimisticLockCoupling : virtual public ListBasedSetSync<T> {
    // implement ListBasedSetNoSync using Optimistic Lock Coupling
    struct Entry {
        T key;
        Entry *next;
        /// Version
        uint64_t version{0};
        /// Mutex in each entry
        M mutex;
    };

    /// Head
    Entry staticHead;
    /// Tail
    Entry staticTail;

    ListBasedSetOptimisticLockCoupling() {
        staticHead.key = std::numeric_limits<T>::min();
        staticHead.next = &staticTail;
        staticTail.key = std::numeric_limits<T>::max();
        staticTail.next = nullptr;
    }

#ifdef DEBUG
    // TODO(jigao): comment it out for performance evaluation
    ~ListBasedSetOptimisticLockCoupling() {
        Entry *curr = staticHead.next;
        while (curr != &staticTail) {
            auto tmp_next = curr->next;
            delete(curr);
            curr = tmp_next;
        }
    }
#endif

    std::string get_name() override { return "Optimistic Lock Coupling"; }

    bool contains(const T& k) override {
        /// Attempt to read optimistically
        for (size_t i = 0; i < MAX_ATTEMPTS; i++) {
            Entry *curr = staticHead.next;
            // Iterate over all elements whose key is smaller, we stop at the first
            // element with key >= k
            /// Pre Version for caching the version
            uint64_t pre_version_curr = curr->version;
            while (curr->key < k && pre_version_curr == curr->version /* validation */) {  // TODO(question): is this condition correct?
                curr = curr->next;
                pre_version_curr = curr->version;
            }

            /// Validation
            const bool curr_try_lock = curr->mutex.try_lock();
            if (!(pre_version_curr == curr->version && curr_try_lock)) {  // TODO(question): is this condition correct?
                /// If validation stage fails, then restart.
                if (curr_try_lock) {
                    curr->mutex.unlock();
                }
                continue;
            }

            const bool result = (curr->key == k);
            curr->mutex.unlock();

            // Check if this element is the searched key and return the result
            return result;
        }

        /// Fallback to pessimistic locking
#ifdef DEBUG
        std::cout << "Fallback: contains_pessimistic" << std::endl;
#endif
        return contains_pessimistic(k);
    }

    /// This is just the same version from lock coupling with read-write mutex
    // TODO(all): it can also be a mutex instead of read-write mutex, you can change that.
    bool contains_pessimistic(const T& k) {
        /// Wait for the mutex of staticHead node
        staticHead.mutex.lock_read();
        /// Got the mutex of staticHead node and access the staticHead node
        /// Wait for the mutex of current node
        staticHead.next->mutex.lock_read();
        /// Got the mutex of current node, release the mutex of staticHead node, access the current node
        staticHead.mutex.unlock();
        Entry *curr = staticHead.next;
        // Iterate over all elements whose key is smaller, we stop at the first
        // element with key >= k
        while (curr->key < k) {
            /// Wait for the mutex of next node
            curr->next->mutex.lock_read();
            /// Got the mutex of next node, release the mutex of current node, access the next node
            curr->mutex.unlock();
            curr = curr->next;
        }
        // Check if this element is the searched key
        const bool result = (curr->key == k);
        /// Release the mutex of current node
        curr->mutex.unlock();
        // return the result
        return result;
    }

    void insert(const T& k) override {
        /// Attempt to read optimistically
        for (size_t i = 0; i < MAX_ATTEMPTS; i++) {
            // Start inserting a new element from the head
            Entry *pred = &staticHead;
            Entry *curr = staticHead.next;
            // Iterate over all elements whose key is smaller, we stop at the first
            // element with key >= k
            /// Pre Version for caching the version
            uint64_t pre_version_pred = pred->version;
            uint64_t pre_version_curr = curr->version;
            while (curr->key < k && pre_version_pred == pred->version && pre_version_curr == curr->version /* validation */) { // TODO(question): is this condition correct?
                pred = curr;
                curr = curr->next;
                pre_version_pred = pred->version;
                pre_version_curr = curr->version;
            }

            /// Validation
            const bool curr_try_lock = curr->mutex.try_lock();
            const bool pred_try_lock = pred->mutex.try_lock();
            if (!(pre_version_curr == curr->version && curr_try_lock) || !(pre_version_pred == pred->version && pred_try_lock)) { // TODO(question): is this condition correct?
                /// If validation stage fails, then restart.
                if (curr_try_lock) {
                    curr->mutex.unlock();
                }
                if (pred_try_lock) {
                    pred->mutex.unlock();
                }
                continue;
            }

            /// Insertion position found: between predecessor node and current node
            /// Wait for the mutex of predecessor node and current node
            /// This point has acquired these two mutex of them

            // As we have a set, there is nothing to do if the key is already contained
            if (curr->key == k) {
                /// Release two mutexes
                pred->mutex.unlock();
                curr->mutex.unlock();
                return;
            }

            // Then: curr->key > k

            // Create a new list element for the new key
            Entry *n = new Entry{k, curr /* next */};
            // Insert the element in the list
            pred->next = n;
            /// Increment the version
            pred->version++;

            /// Release two mutexes
            pred->mutex.unlock();
            curr->mutex.unlock();
            return;
        }

        /// Fallback to pessimistic locking
#ifdef DEBUG
        std::cout << "Fallback: insert_pessimistic" << std::endl;
#endif
        return insert_pessimistic(k);
    }

    void insert_pessimistic(const T& k) {
        /// Wait for the mutex of staticHead node
        staticHead.mutex.lock();
        /// Got the mutex of staticHead node and access the staticHead node
        // Start inserting a new element from the head
        Entry *pred = &staticHead;
        /// Wait for the mutex of current node
        staticHead.next->mutex.lock();
        /// Got the mutex of current node, access the current node
        Entry *curr = staticHead.next;
        // Iterate over all elements whose key is smaller, we stop at the first
        // element with key >= k
        while (curr->key < k) {
            /// No insertion at current node, so release the mutex of predecessor
            pred->mutex.unlock();
            pred = curr;
            /// Wait for the mutex of next node
            curr->next->mutex.lock();
            /// Got the mutex of next node, access the next node
            curr = curr->next;
        }

        /// Insertion position found: between predecessor node and current node
        /// This point has acquired these two mutex of them

        // As we have a set, there is nothing to do if the key is already contained
        if (curr->key == k) {
            /// Release two mutexes
            pred->mutex.unlock();
            curr->mutex.unlock();
            return;
        }

        // Then: curr->key > k

        // Create a new list element for the new key
        Entry *n = new Entry{k, curr /* next */};
        // Insert the element in the list
        pred->next = n;
        /// Increment the version
        pred->version++;

        /// Release two mutexes
        pred->mutex.unlock();
        curr->mutex.unlock();
        return;
    }

    void remove(const T& k) override {
        if (staticHead.next == &staticTail) { return; }
        /// Attempt to read optimistically
        for (size_t i = 0; i < MAX_ATTEMPTS; i++) {
            Entry *pred = &staticHead;
            Entry *curr = staticHead.next;
            // Iterate over all elements whose key is smaller, we stop at the first
            // element with key >= k
            /// Pre Version for caching the version
            uint64_t pre_version_pred = pred->version;
            uint64_t pre_version_curr = curr->version;
            while (curr->key < k && pre_version_pred == pred->version && pre_version_curr == curr->version /* validation */) {  // TODO(question): is this condition correct?
                pred = curr;
                curr = curr->next;
                pre_version_pred = pred->version;
                pre_version_curr = curr->version;
            }

            /// Validation
            const bool curr_try_lock = curr->mutex.try_lock();
            const bool pred_try_lock = pred->mutex.try_lock();
            if (!(pre_version_curr == curr->version && curr_try_lock) || !(pre_version_pred == pred->version && pred_try_lock)) { // TODO(question): is this condition correct?
                /// If validation stage fails, then restart.
                if (curr_try_lock) {
                    curr->mutex.unlock();
                }
                if (pred_try_lock) {
                    pred->mutex.unlock();
                }
                continue;
            }

            /// Deletion position found: between predecessor node and current node
            /// Wait for the mutex of predecessor node and current node
            /// This point has acquired these two mutex of them

            // If the element has the searched key, remove it
            if (curr->key == k) {
                pred->next = curr->next;
                /// Release two mutexes
                pred->mutex.unlock();
                curr->mutex.unlock();
                // ignore reclamation for now
#ifdef DEBUG
                delete(curr);  // TODO(jigao): comment it out for performance evaluation
#endif
                return;
            } else {
                /// Release two mutexes
                pred->mutex.unlock();
                curr->mutex.unlock();
                return;
            }
        }

        /// Fallback to pessimistic locking
#ifdef DEBUG
        std::cout << "Fallback: remove_pessimistic" << std::endl;
#endif
        remove_pessimistic(k);
    }

    void remove_pessimistic(const T& k) {
        /// Wait for the mutex of staticHead node
        staticHead.mutex.lock();
        /// Got the mutex of staticHead node and access the staticHead node
        Entry *pred = &staticHead;
        /// Wait for the mutex of current node
        staticHead.next->mutex.lock();
        /// Got the mutex of current node, access the current node
        Entry *curr = staticHead.next;
        // Iterate over all elements whose key is smaller, we stop at the first
        // element with key >= k
        while (curr->key < k) {
            /// No deletion at current node, so release the mutex of predecessor
            pred->mutex.unlock();
            pred = curr;
            /// Wait for the mutex of next node
            curr->next->mutex.lock();
            /// Got the mutex of next node, access the next node
            curr = curr->next;
        }

        /// Deletion position found: between predecessor node and current node
        /// This point has acquired these two mutex of them

        // If the element has the searched key, remove it
        if (curr->key == k) {
            pred->next = curr->next;
            /// Release two mutexes
            pred->mutex.unlock();
            curr->mutex.unlock();
            // ignore reclamation for now
#ifdef DEBUG
            delete(curr);  // TODO(jigao): comment it out for performance evaluation
#endif
            return;
        } else {
            /// Release two mutexes
            pred->mutex.unlock();
            curr->mutex.unlock();
            return;
        }
    }
};

// Explicit Template Instantiation
template struct ListBasedSetOptimisticLockCoupling<int64_t, tbb::spin_rw_mutex>;