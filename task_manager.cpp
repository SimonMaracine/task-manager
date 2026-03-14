#include "task_manager.hpp"

#include <algorithm>

namespace task_manager {
    template<typename ForwardIter, typename UnaryPredicate>
    static constexpr ForwardIter remove_if_swap(ForwardIter first, ForwardIter last, UnaryPredicate predicate) {
        first = std::find_if(first, last, predicate);

        if (first != last) {
            for (ForwardIter iter = first; ++iter != last;) {
                if (!predicate(*iter)) {
                    std::swap(*iter, *first);
                    ++first;
                }
            }
        }

        return first;
    }

    void AsyncTask::finish() {
        m_thread.request_stop();
    }

    void AsyncTask::finish(std::exception_ptr exception) {
        m_exception = std::move(exception);
        m_thread.request_stop();
    }

    bool AsyncTask::stop_requested() const {
        return m_thread.get_stop_token().stop_requested();
    }

    CoroutineTask::YieldInstruction CoroutineTask::wait_for(double time) {
        const double current_time = get_time();
        return current_time + time;
    }

    void TaskManager::add_immediate_task(ImmediateTaskFunction task_function) {
        m_immediate_tasks_incoming.emplace_back(std::move(task_function));
    }

    void TaskManager::add_immediate_thread_safe_task(ImmediateThreadSafeTaskFunction task_function) {
        std::lock_guard guard {m_mutex};

        m_immediate_thread_safe_tasks_incoming.emplace_back(std::move(task_function));
    }

    void TaskManager::add_repeatable_task(RepeatableTaskFunction task_function, double interval) {
        RepeatableTask& task = m_repeatable_tasks_incoming.emplace_back(std::move(task_function), interval);
        task.m_last_time = get_time();
    }

    void TaskManager::add_delayed_task(DelayedTaskFunction task_function, double delay) {

        DelayedTask& task = m_delayed_tasks_incoming.emplace_back(std::move(task_function), delay);
        task.m_last_time = get_time();
    }

    void TaskManager::add_continuous_task(ContinuousTaskFunction task_function, double interval) {
        ContinuousTask& task = m_continuous_tasks_incoming.emplace_back(std::move(task_function), interval);
        task.m_last_time = get_time();
    }

    void TaskManager::add_async_task(AsyncTaskFunction task_function) {
        m_async_tasks.emplace_front(std::move(task_function));
    }

    void TaskManager::update() {
        update_immediate_tasks();
        update_immediate_thread_safe_tasks();
        update_repeatable_tasks();
        update_delayed_tasks();
        update_continuous_tasks();
        update_async_tasks();
        update_coroutine_tasks();
    }

    void TaskManager::clear() {
        // Any pending or current tasks are simply canceled
        m_immediate_tasks_incoming.clear();
        m_immediate_tasks_active.clear();
        m_repeatable_tasks_incoming.clear();
        m_repeatable_tasks_active.clear();
        m_delayed_tasks_incoming.clear();
        m_delayed_tasks_active.clear();
        m_continuous_tasks_incoming.clear();
        m_continuous_tasks_active.clear();

        // Join the async tasks before the thread safe tasks, as the latter are usually very needed
        m_async_tasks.clear();

        // Don't cancel these tasks, just execute them
        while (!m_immediate_thread_safe_tasks_incoming.empty() || !m_immediate_thread_safe_tasks_active.empty()) {
            update_immediate_thread_safe_tasks();
        }

        for (const CoroutineTask& task : m_coroutine_tasks) {
            task.m_handle.destroy();
        }
        m_coroutine_tasks.clear();
    }

    void TaskManager::update_immediate_tasks() {
        for (ImmediateTask& task : m_immediate_tasks_active) {
            update_immediate_task(task);
        }

        m_immediate_tasks_active.clear();
        std::swap(m_immediate_tasks_incoming, m_immediate_tasks_active);
    }

    void TaskManager::update_immediate_thread_safe_tasks() {
        for (ImmediateThreadSafeTask& task : m_immediate_thread_safe_tasks_active) {
            update_immediate_thread_safe_task(task);
        }

        m_immediate_thread_safe_tasks_active.clear();
        {
            std::lock_guard guard (m_mutex);
            std::swap(m_immediate_thread_safe_tasks_incoming, m_immediate_thread_safe_tasks_active);
        }
    }

    void TaskManager::update_repeatable_tasks() {
        for (RepeatableTask& task : m_repeatable_tasks_active) {
            if (!update_repeatable_task(task)) {
                m_repeatable_tasks_incoming.push_back(std::move(task));
            }
        }

        m_repeatable_tasks_active.clear();
        std::swap(m_repeatable_tasks_incoming, m_repeatable_tasks_active);
    }

    void TaskManager::update_delayed_tasks() {
        for (DelayedTask& task : m_delayed_tasks_active) {
            if (!update_delayed_task(task)) {
                m_delayed_tasks_incoming.push_back(std::move(task));
            }
        }

        m_delayed_tasks_active.clear();
        std::swap(m_delayed_tasks_incoming, m_delayed_tasks_active);
    }

    void TaskManager::update_continuous_tasks() {
        for (ContinuousTask& task : m_continuous_tasks_active) {
            if (!update_continuous_task(task)) {
                m_continuous_tasks_incoming.push_back(std::move(task));
            }
        }

        m_continuous_tasks_active.clear();
        std::swap(m_continuous_tasks_incoming, m_continuous_tasks_active);
    }

    void TaskManager::update_async_tasks() {
        std::exception_ptr last_exception;

        for (auto before_iter = m_async_tasks.before_begin(), iter = m_async_tasks.begin(); iter != m_async_tasks.end();) {
            if (const auto& task = *iter; task.m_thread.get_stop_token().stop_requested()) {
                if (task.m_exception) {
                    // Oops, an error occurred in this task, store it
                    last_exception = task.m_exception;
                }

                // Task is auto-joined
                iter = m_async_tasks.erase_after(before_iter);

                continue;
            }

            before_iter++, iter++;
        }

        if (last_exception) {
            // Simply bubble up the error, whatever it is
            std::rethrow_exception(last_exception);
        }
    }

    void TaskManager::update_coroutine_tasks() {
        for (const CoroutineTask& task : m_coroutine_tasks) {
            if (task.m_handle.done()) {
                continue;
            }

            switch (const auto& promise = task.m_handle.promise(); promise.m_yield_instruction.index()) {
                case 0:
                    resume_coroutine_task(task);
                    break;
                case 1: {
                    if (const double target_time = std::get<1>(promise.m_yield_instruction); get_time() > target_time) {
                        resume_coroutine_task(task);
                    }

                    break;
                }
                case 2: {
                    if (const auto& condition = std::get<2>(promise.m_yield_instruction); condition()) {
                        resume_coroutine_task(task);
                    }

                    break;
                }
            }
        }

        // Must destroy tasks in a second and third pass
        // Destroying a coroutine invalidates it, which disallows calling done()

        const auto removed_iter {remove_if_swap(m_coroutine_tasks.begin(), m_coroutine_tasks.end(), [](const CoroutineTask& task) {
            return task.m_handle.done();
        })};

        for (auto iter = removed_iter; iter != m_coroutine_tasks.end(); iter++) {
            iter->m_handle.destroy();
        }

        m_coroutine_tasks.erase(removed_iter, m_coroutine_tasks.end());
    }

    void TaskManager::resume_coroutine_task(const CoroutineTask& task) {
        task.m_handle.resume();

        if (task.m_handle.promise().m_exception) {
            // Again, simply bubble up the error
            std::rethrow_exception(task.m_handle.promise().m_exception);
        }
    }

    void TaskManager::update_immediate_task(ImmediateTask& task) {
        task.m_task_function();
    }

    void TaskManager::update_immediate_thread_safe_task(ImmediateThreadSafeTask& task) {
        task.m_task_function();
    }

    bool TaskManager::update_repeatable_task(RepeatableTask& task) {
        const double current_time = get_time();
        const double elapsed_time = current_time - task.m_last_time;

        task.m_last_time = current_time;
        task.m_total_time += elapsed_time;

        // There may appear glitches, if the application stalls
        // So this is no "perfect repeatable task"

        if (task.m_total_time > task.m_interval) {
            task.m_total_time = 0.0;  // Better to just reset instead of decrement

            if (task.m_task_function()) {
                return true;
            }
        }

        return false;
    }

    bool TaskManager::update_delayed_task(DelayedTask& task) {
        const double current_time = get_time();
        const double elapsed_time = current_time - task.m_last_time;

        task.m_last_time = current_time;
        task.m_total_time += elapsed_time;

        // This type of task suffers from the same problems as the repeatable task

        if (task.m_total_time > task.m_delay) {
            task.m_task_function();
            return true;
        }

        return false;
    }

    bool TaskManager::update_continuous_task(ContinuousTask& task) {
        const double current_time = get_time();
        const double elapsed_time = current_time - task.m_last_time;

        task.m_last_time = current_time;
        task.m_total_time += elapsed_time;

        // Ensure the function is called at least once
        if (task.m_task_function()) {
            return true;
        }

        return task.m_total_time > task.m_interval;
    }
}
