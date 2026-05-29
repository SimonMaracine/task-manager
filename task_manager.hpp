#pragma once

#include <functional>
#include <thread>
#include <exception>
#include <utility>
#include <vector>
#include <forward_list>
#include <mutex>
#include <coroutine>
#include <variant>

double get_time();

namespace task_manager {
    class AsyncTask;
    class CoroutineTask;
    class TaskManager;

    using ImmediateTaskFunction = std::function<void()>;
    using ImmediateThreadSafeTaskFunction = std::function<void()>;
    using RepeatableTaskFunction = std::function<bool()>;
    using DelayedTaskFunction = std::function<void()>;
    using ContinuousTaskFunction = std::function<bool()>;
    using AsyncTaskFunction = std::function<void(AsyncTask&)>;

    template<typename... Args>
    using CoroutineTaskFunction = std::function<CoroutineTask(Args&&...)>;

    class ImmediateTask {
    public:
        explicit ImmediateTask(ImmediateTaskFunction task_function)
            : m_task_function(std::move(task_function)) {}
    private:
        ImmediateTaskFunction m_task_function;

        friend class TaskManager;
    };

    class ImmediateThreadSafeTask {
    public:
        explicit ImmediateThreadSafeTask(ImmediateThreadSafeTaskFunction task_function)
            : m_task_function(std::move(task_function)) {}
    private:
        ImmediateThreadSafeTaskFunction m_task_function;

        friend class TaskManager;
    };

    class RepeatableTask {
    public:
        RepeatableTask(RepeatableTaskFunction task_function, double interval)
            : m_task_function(std::move(task_function)), m_interval(interval) {}
    private:
        RepeatableTaskFunction m_task_function;

        double m_interval {};
        double m_last_time {};
        double m_total_time {};

        friend class TaskManager;
    };

    class DelayedTask {
    public:
        DelayedTask(DelayedTaskFunction task_function, double delay)
            : m_task_function(std::move(task_function)), m_delay(delay) {}
    private:
        DelayedTaskFunction m_task_function;

        double m_delay {};
        double m_last_time {};
        double m_total_time {};

        friend class TaskManager;
    };

    class ContinuousTask {
    public:
        ContinuousTask(ContinuousTaskFunction task_function, double interval)
            : m_task_function(std::move(task_function)), m_interval(interval) {}
    private:
        ContinuousTaskFunction m_task_function;

        double m_interval {};
        double m_last_time {};
        double m_total_time {};

        friend class TaskManager;
    };

    class AsyncTask {
    public:
        explicit AsyncTask(AsyncTaskFunction task_function)
            : m_thread(std::move(task_function), std::ref(*this)) {}

        void finish();
        void finish(std::exception_ptr exception);
        bool stop_requested() const;
    private:
        std::exception_ptr m_exception;
        std::jthread m_thread;

        friend class TaskManager;
    };

    class CoroutineTask {
    public:
        using DefaultYieldInstruction = std::monostate;
        using YieldInstruction = std::variant<DefaultYieldInstruction, double, std::function<bool()>>;

        static YieldInstruction wait_for(double time);

        static YieldInstruction wait_until(const std::function<bool()>& condition) {
            return condition;
        }

        static YieldInstruction none() {
            return DefaultYieldInstruction();
        }

        class promise_type {
        public:
            CoroutineTask get_return_object() {
                return CoroutineTask(std::coroutine_handle<promise_type>::from_promise(*this));
            }

            static std::suspend_always initial_suspend() noexcept { return {}; }
            static std::suspend_always final_suspend() noexcept { return {}; }  // Don't automatically destroy the coroutine when returning

            static void return_void() {}

            void unhandled_exception() {
                m_exception = std::current_exception();
            }

            std::suspend_always yield_value(YieldInstruction yield_instruction) {
                m_yield_instruction = std::move(yield_instruction);
                return {};
            }
        private:
            YieldInstruction m_yield_instruction;
            std::exception_ptr m_exception;

            friend class TaskManager;
        };
    private:
        explicit CoroutineTask(std::coroutine_handle<promise_type> handle)
            : m_handle(handle) {}

        std::coroutine_handle<promise_type> m_handle;

        friend class TaskManager;
    };

    class TaskManager {
    public:
        void add_immediate_task(ImmediateTaskFunction task_function);
        void add_immediate_thread_safe_task(ImmediateThreadSafeTaskFunction task_function);
        void add_repeatable_task(RepeatableTaskFunction task_function, double interval);
        void add_delayed_task(DelayedTaskFunction task_function, double delay);
        void add_continuous_task(ContinuousTaskFunction task_function, double interval);
        void add_async_task(AsyncTaskFunction task_function);

        template<typename... Args>
        void add_coroutine_task(auto&& task_function, Args&&... args) {
            m_coroutine_tasks.push_back(task_function(std::forward<Args>(args)...));  // Coroutine is created right here
        }

        void update();
        void clear();
    private:
        void update_immediate_tasks();
        void update_immediate_thread_safe_tasks();
        void update_repeatable_tasks();
        void update_delayed_tasks();
        void update_continuous_tasks();
        void update_async_tasks();
        void update_coroutine_tasks();

        static void resume_coroutine_task(const CoroutineTask& task);
        static void update_immediate_task(ImmediateTask& task);
        static void update_immediate_thread_safe_task(ImmediateThreadSafeTask& task);
        static bool update_repeatable_task(RepeatableTask& task);
        static bool update_delayed_task(DelayedTask& task);
        static bool update_continuous_task(ContinuousTask& task);

        std::vector<ImmediateTask> m_immediate_tasks_incoming;
        std::vector<ImmediateTask> m_immediate_tasks_active;
        std::vector<ImmediateThreadSafeTask> m_immediate_thread_safe_tasks_incoming;
        std::vector<ImmediateThreadSafeTask> m_immediate_thread_safe_tasks_active;
        std::vector<RepeatableTask> m_repeatable_tasks_incoming;
        std::vector<RepeatableTask> m_repeatable_tasks_active;
        std::vector<DelayedTask> m_delayed_tasks_incoming;
        std::vector<DelayedTask> m_delayed_tasks_active;
        std::vector<ContinuousTask> m_continuous_tasks_incoming;
        std::vector<ContinuousTask> m_continuous_tasks_active;
        std::forward_list<AsyncTask> m_async_tasks;
        std::vector<CoroutineTask> m_coroutine_tasks;
        std::mutex m_mutex;
    };
}
