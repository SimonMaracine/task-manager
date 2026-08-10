#include <iostream>
#include <chrono>

#include "task_manager.hpp"

// Should return some wall clock time in seconds
double get_time() {
    return std::chrono::duration<double>(std::chrono::system_clock::now().time_since_epoch()).count();
}

int main() {
    task_manager::TaskManager task_manager;

    task_manager.add_coroutine_task([]() -> task_manager::CoroutineTask {
        std::cout << "Coro1\n";

        co_yield task_manager::CoroutineTask::none();

        std::cout << "Coro2\n";
    });
    
    task_manager.add_immediate_task([]() {
        std::cout << "Hello, world!\n";
    });


    // Execute two ticks
    task_manager.update();
    task_manager.update();
}
