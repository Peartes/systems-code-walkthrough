use std::collections::{HashMap, HashSet};

use crate::parallel_determinism::types::{Task, TaskId};
pub struct DependencyGraph {
    pub tasks: Vec<Task>,
    pub dependencies: HashMap<TaskId, HashSet<TaskId>>, // (task_id, depends_on_task_id)
}

impl DependencyGraph {
    pub fn from_tasks(tasks: Vec<Task>) -> Self {
        let mut dependencies: HashMap<TaskId, HashSet<TaskId>> = HashMap::new();

        // For each task, find all tasks before it that it conflicts with
        for (i, task) in tasks.iter().enumerate() {
            let mut deps = HashSet::new();

            for (j, other_task) in tasks[..i].iter().enumerate() {
                if task.conflicts_with(other_task) {
                    deps.insert(j);
                }
            }

            dependencies.insert(i, deps);
        }

        Self {
            tasks,
            dependencies,
        }
    }

    pub fn execution_levels(&self) -> Vec<Vec<TaskId>> {
        let mut levels = vec![];
        let mut completed = HashSet::new();
        let mut remaining: HashSet<TaskId> = self.tasks.iter().map(|t| t.id).collect();

        while !remaining.is_empty() {
            let mut current_level = vec![];

            // Find tasks whose dependencies are all completed
            for &task_id in &remaining {
                let deps = &self.dependencies[&task_id];
                if deps.iter().all(|dep| completed.contains(dep)) {
                    current_level.push(task_id);
                }
            }

            if current_level.is_empty() {
                panic!("Circular dependency detected!");
            }

            // Mark current level as completed
            for &task_id in &current_level {
                completed.insert(task_id);
                remaining.remove(&task_id);
            }

            levels.push(current_level);
        }

        levels
    }

    pub fn visualize(&self) {
        println!("\n=== Dependency Graph ===");
        for (task_id, deps) in &self.dependencies {
            print!("Task {}: ", self.tasks[*task_id].name);
            if deps.is_empty() {
                println!("no dependencies");
            } else {
                let dep_names: Vec<_> = deps
                    .iter()
                    .map(|id| self.tasks[*id].name.as_str())
                    .collect();
                println!("depends on {:?}", dep_names);
            }
        }

        println!("\n=== Execution Levels ===");
        for (level_num, level) in self.execution_levels().iter().enumerate() {
            let task_names: Vec<_> = level
                .iter()
                .map(|id| self.tasks[*id].name.as_str())
                .collect();
            println!(
                "Level {}: {:?} (can run in parallel)",
                level_num, task_names
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_conflicts() {
        let tasks = vec![
            Task {
                id: 0,
                name: "A".to_string(),
                reads: vec!["account_1".to_string()],
                writes: vec!["account_2".to_string()],
                work: &(|| Ok("A done".to_string())),
            },
            Task {
                id: 1,
                name: "B".to_string(),
                reads: vec!["account_3".to_string()],
                writes: vec!["account_4".to_string()],
                work: &(|| Ok("B done".to_string())),
            },
        ];

        assert!(!tasks[0].conflicts_with(&tasks[1]));
        assert!(!tasks[1].conflicts_with(&tasks[0]));
    }

    #[test]
    fn test_write_write_conflict() {
        let task_a = Task {
            id: 0,
            name: "A".to_string(),
            reads: vec![],
            writes: vec!["account_1".to_string()],
            work: &(|| Ok("A".to_string())),
        };

        let task_b = Task {
            id: 1,
            name: "B".to_string(),
            reads: vec![],
            writes: vec!["account_1".to_string()],
            work: &(|| Ok("B".to_string())),
        };

        assert!(task_a.conflicts_with(&task_b));
        assert!(task_b.conflicts_with(&task_a));
    }

    #[test]
    fn test_read_write_conflict() {
        let task_a = Task {
            id: 0,
            name: "A".to_string(),
            reads: vec![],
            writes: vec!["account_1".to_string()],
            work: &(|| Ok("A".to_string())),
        };

        let task_b = Task {
            id: 1,
            name: "B".to_string(),
            reads: vec!["account_1".to_string()],
            writes: vec![],
            work: &(|| Ok("B".to_string())),
        };

        assert!(task_b.conflicts_with(&task_a));
    }

    #[test]
    fn test_execution_levels() {
        let tasks = vec![
            Task {
                id: 0,
                name: "A".to_string(),
                reads: vec![],
                writes: vec!["x".to_string()],
                work: &(|| Ok("A".to_string())),
            },
            Task {
                id: 1,
                name: "B".to_string(),
                reads: vec![],
                writes: vec!["y".to_string()],
                work: &(|| Ok("B".to_string())),
            },
            Task {
                id: 2,
                name: "C".to_string(),
                reads: vec!["x".to_string()],
                writes: vec!["z".to_string()],
                work: &(|| Ok("C".to_string())),
            },
        ];

        let graph = DependencyGraph::from_tasks(tasks);
        let levels = graph.execution_levels();

        assert_eq!(levels.len(), 2);
        assert_eq!(levels[0].len(), 2); // A and B
        assert_eq!(levels[1].len(), 1); // C
    }
}
