#pragma once

#include <iostream>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <functional>
#include <chrono>

// https://stackoverflow.com/a/29742586/10971650
template <typename FUN>
class ThreadPool {
    public:
    ThreadPool(int num_threads) : shutdown (false) {
        // Create the specified number of threads
        threads.reserve(num_threads);
        for (int i = 0; i < num_threads; ++i) {
            threads.emplace_back(std::bind(&ThreadPool::threadEntry, this, i));
        }
    }

    ~ThreadPool() {
        {
            // Unblock any threads and tell them to stop
            std::unique_lock<std::mutex> l(mutex);
            shutdown = true;
            cond_var.notify_all();
        }
        // Wait for all threads to stop
//        std::cerr << "Joining threads" << std::endl;
        for (auto& thread : threads) {
            thread.join();
        }
    }

    void addTask(std::function<FUN>&& func) {
        // Place a job on the queu and unblock a thread
//        std::cerr << "addTask" << std::endl;
        std::unique_lock<std::mutex> l(mutex);
        tasks.emplace (func);
        cond_var.notify_one();
    }

    protected:
    void threadEntry(int i) {
        std::function<FUN> task;
        while (true) {
            {
                std::unique_lock <std::mutex> l (mutex);
                while (!shutdown && tasks.empty()) {
                    cond_var.wait (l);
                }

                if (tasks.empty ()) {
                    // No tasks to do and we are shutting down
//                    std::cerr << "Thread " << i << " terminates" << std::endl;
                    return;
                }
//                std::cerr << "Thread " << i << " does a job" << std::endl;
                task = std::move(tasks.front());
                tasks.pop();
            }
            // Do the task without holding any locks
            task();
        }

    }

    std::mutex mutex;
    std::condition_variable cond_var;
    bool shutdown;
    std::queue<std::function<FUN>> tasks;
    std::vector<std::thread> threads;
};