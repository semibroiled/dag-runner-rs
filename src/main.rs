use std::collections::HashMap;
use std::collections::{HashSet, VecDeque};

use log::{error, info};
use simple_logger::SimpleLogger;

// Task Definition

#[derive(Debug, Clone)]
struct Task {
    name: String,
    id: String,
    command: String,
    dependencies: HashSet<String>,
}

impl Task {
    fn new(name: &str, id: &str, command: &str, dependencies: HashSet<String>) -> Task {
        Task {
            name: name.to_string(),
            id: id.to_string(),
            command: command.to_string(),
            dependencies,
        }
    }
}

// Graph Structure to represent DAG
#[derive(Debug)]
struct DAG {
    tasks: HashMap<String, Task>,
}
impl DAG {
    fn new() -> DAG {
        DAG {
            tasks: HashMap::new(),
        }
    }

    fn add_task(&mut self, task: Task) {
        self.tasks.insert(task.id.clone(), task); // Insert into hashmap tasks, with ID and task struct itself
    }
}

// Scheduler
struct Scheduler {
    dag: DAG,
    task_queue: VecDeque<Task>,
}

impl Scheduler {
    fn new(dag: DAG) -> Scheduler {
        Scheduler {
            dag,
            task_queue: VecDeque::new(),
        }
    }

    fn run(&mut self) {
        // Initialize task queue with tasks with no dependencies
        for task in self.dag.tasks.values() {
            if task.dependencies.is_empty() {
                self.task_queue.push_back(task.clone());
            }
        }

        while let Some(task) = self.task_queue.pop_front() {
            self.execute_task(&task);
        }
    }

    fn execute_task(&mut self, task: &Task) {
        // Execute task
        println!("Executing {}| {}:{}", task.id, task.name, task.command);
        // Simulate task completion
        self.complete_task(task);
    }

    fn complete_task(&mut self, completed_task: &Task) {
        for task in self.dag.tasks.values_mut() {
            if task.dependencies.contains(&completed_task.id) {
                task.dependencies.remove(&completed_task.id);
                if task.dependencies.is_empty() {
                    self.task_queue.push_back(task.clone());
                }
            }
        }
    }
}

fn main() {

    // Define Task 1
    let task1 = Task::new("task1", "abc", "This is the first command", HashSet::new());

    let mut deps_task2 = HashSet::new();
    deps_task2.insert("abc".to_string());

    let task2 = Task::new(
        "task2",
        "efg",
        "This is the second command",
        deps_task2.clone(),
    );
    let task3 = Task::new("task3", "hig", "This is the third command", deps_task2);

    // Create DAG and add tasks
    let mut dag = DAG::new();

    dag.add_task(task1);
    dag.add_task(task2);
    dag.add_task(task3);

    // Run Scheduler
    let mut scheduler = Scheduler::new(dag);
    scheduler.run()
}
