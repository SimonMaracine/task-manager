#include <iostream>

#include "task_manager.hpp"

double get_time() {
    // Should return wall clock time
    return 0.0;
}

int main() {
    task_manager::TaskManager task_manager;

    task_manager.add_immediate_task([]() {
        std::cout << "Hello, world!\n";
    });

    task_manager.add_coroutine_task([]() -> task_manager::CoroutineTask {
        std::cout << "Coro1\n";

        co_yield task_manager::CoroutineTask::none();

        std::cout << "Coro2\n";
    });

    task_manager.update();
    task_manager.update();
}
