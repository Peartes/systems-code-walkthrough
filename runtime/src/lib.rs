//! This crate contrasts two scheduling models for async tasks:
//! Tokio for high throughput and Commonware for deterministic replay.
//!
//! The key idea is that determinism is stronger than "same final result".
//! It also means "same execution path" given the same inputs and seed.
//! That property is essential in systems that must agree on state, such as
//! blockchains, where every node must apply the exact same sequence of steps.
//!
//! Each function below focuses on a small, observable behavior so you can
//! reason about scheduling, change parameters, and predict the outcome.

mod parallel_determinism;
mod tasks;

use std::{sync::Arc, time::Duration};

use commonware_runtime::{
    Clock, Runner, Spawner,
    deterministic::{Config, Runner as DeterministicRunner},
};
use tokio::{join, runtime::Runtime, sync::RwLock, time::sleep};

/// Demonstrate Tokio's nondeterministic scheduling with simple async sleeps.
///
/// The tasks all finish, but the *order* of prints is not guaranteed. The
/// runtime is optimized for throughput, not for replaying a specific path.
fn tokio_tasks() {
    // Create multi-threaded runtime
    let rt = Runtime::new().unwrap();

    rt.block_on(async {
        // Spawn first task
        let task1 = tokio::spawn(async {
            println!("Task 1: Starting");
            sleep(Duration::from_millis(10)).await;
            println!("Task 1: Done");
        });

        // Spawn second task
        let task2 = tokio::spawn(async {
            println!("Task 2: Starting");
            sleep(Duration::from_millis(10)).await;
            println!("Task 2: Done");
        });

        // Spawn third task
        let task3 = tokio::spawn(async {
            println!("Task 3: Starting");
            // sleep(Duration::from_millis(10)).await;
            println!("Task 3: Done");
        });

        // Wait for all tasks to complete
        let _ = tokio::join!(task1, task2, task3);
    });
}

/// Demonstrate Commonware's deterministic scheduling with a fixed seed.
///
/// With the same seed, the runtime polls tasks in the same order each run.
/// This shows that determinism is about the execution path, not just outputs.
///
/// We spawn each task from a cloned context so tasks are siblings and do not
/// abort each other under Commonware's supervision rules.
fn commoware_runtime_tasks() {
    // Create deterministic runtime with a seed
    let executor = DeterministicRunner::new(
        Config::default().with_seed(12345), // Same seed = same execution order!
    );

    executor.start(|context| async move {
        // Spawn first task from a cloned context so it doesn't get aborted
        // when another root-level task completes.
        let task1 = context.clone().spawn(|context| async move {
            println!("Task 1: Starting");
            context.sleep(Duration::from_millis(10)).await;
            println!("Task 1: Done");
        });

        // Spawn second task from a cloned context as a sibling of task1.
        let task2 = context.clone().spawn(|context| async move {
            println!("Task 2: Starting");
            context.sleep(Duration::from_millis(10)).await;
            println!("Task 2: Done");
        });

        // Spawn third task from a cloned context as a sibling of task1.
        let task3 = context.clone().spawn(|_| async move {
            println!("Task 3: Starting");
            // context.sleep(Duration::from_millis(10)).await;
            println!("Task 3: Done");
        });

        // Wait for all tasks to complete
        let _ = tokio::join!(task1, task2, task3);
    });
}

/// Run a small word-selection workflow on Tokio.
///
/// The goal is to show how a typical concurrent workflow behaves when task
/// order is not fixed. The end results are valid, but the exact interleaving
/// can change between runs.
fn tokio_executor() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let words = Arc::new(tasks::read_file());
        let selected_words = Arc::new(RwLock::new(Vec::<String>::new()));

        let select_word_task_words_clone = words.clone();
        let select_word_task_selected_words_clone = selected_words.clone();
        let select_word_task = tokio::spawn(async move {
            let rand_seed = vec![12345, 67890, 54321, 98765, 11111];
            for i in 0..5 {
                let selected_word =
                    tasks::select_random_word(&select_word_task_words_clone, Some(rand_seed[i]))
                        .await;
                select_word_task_selected_words_clone
                    .write()
                    .await
                    .push(selected_word);
                sleep(Duration::from_millis(10)).await;
            }
        });

        let count_word_task_words_clone = words.clone();
        let count_word_task_selected_words = selected_words.clone();
        let count_word_task = tokio::spawn(async move {
            for _ in 0..5 {
                if let Some(word) = count_word_task_selected_words.read().await.last() {
                    tasks::count_word_occurrences(word, &count_word_task_words_clone).await;
                } else {
                    println!("No word selected yet, skipping count.");
                }
                sleep(Duration::from_millis(10)).await;
            }
        });
        let _ = tokio::join!(select_word_task, count_word_task);
    });
}

/// Run the same word-selection workflow on the deterministic runtime.
///
/// Because the seed and scheduling are fixed, the interleaving is repeatable.
/// This is the type of property needed when multiple replicas must agree on
/// every state transition.
fn commonware_executor() {
    let rt = DeterministicRunner::new(Config::default().with_seed(12345));

    rt.start(|context| async move {
        let words = Arc::new(tasks::read_file());
        let selected_words = Arc::new(RwLock::new(Vec::<String>::new()));

        let select_word_task_words_clone = words.clone();
        let select_word_task_selected_words_clone = selected_words.clone();
        let select_word_task = context.clone().spawn(|context| async move {
            let rand_seed = vec![12345, 67890, 54321, 98765, 11111];
            for i in 0..5 {
                let selected_word =
                    tasks::select_random_word(&select_word_task_words_clone, Some(rand_seed[i]))
                        .await;
                select_word_task_selected_words_clone
                    .write()
                    .await
                    .push(selected_word);
                context.sleep(Duration::from_millis(10)).await;
            }
        });

        let count_word_task_words_clone = words.clone();
        let count_word_task_selected_words = selected_words.clone();
        let count_word_task = context.clone().spawn(|context| async move {
            for _ in 0..5 {
                if let Some(word) = count_word_task_selected_words.read().await.last() {
                    tasks::count_word_occurrences(word, &count_word_task_words_clone).await;
                } else {
                    println!("No word selected yet, skipping count.");
                }
                context.sleep(Duration::from_millis(10)).await;
            }
        });
        let _ = join!(select_word_task, count_word_task);
    });
}

#[cfg(test)]
mod tests {
    use commonware_runtime::tokio::Config as TokioConfig;

    use crate::tasks::{cpu_cooperative, delayed_work, greedy_task, io_bound};

    use super::*;

    /// Basic check that the Tokio demo runs to completion.
    #[test]
    fn test_tokio_tasks() {
        tokio_tasks();
    }

    /// Basic check that the deterministic demo runs to completion.
    #[test]
    fn test_commonware_runtime_tasks() {
        commoware_runtime_tasks();
    }

    /// Exercises the Tokio workflow used for comparison.
    #[test]
    fn test_tokio_executor() {
        tokio_executor();
    }
    /// Exercises the deterministic workflow used for comparison.
    #[test]
    fn test_commonware_executor() {
        commonware_executor();
    }

    /// Run a mix of task types on Tokio to illustrate scheduling tradeoffs.
    #[test]
    fn test_tasks_types_tokio() {
        let rt =
            commonware_runtime::tokio::Runner::new(TokioConfig::default().with_worker_threads(1));
        rt.start(|context| async move {
            let greddy = context.clone().spawn(|_| async {
                greedy_task();
            });
            let cpu_cooperative_task = context.clone().spawn(|context| async move {
                cpu_cooperative(&context).await;
            });
            let io_bound_task = context.clone().spawn(|context| async move {
                io_bound(&context).await;
            });
            let delayed_task = context.clone().spawn(|context| async move {
                delayed_work(&context).await;
            });
            let _ = join!(greddy, cpu_cooperative_task, io_bound_task, delayed_task);
        });
    }

    /// Run the same mix of task types on the deterministic runtime.
    #[test]
    fn test_tasks_types_commonware() {
        let rt = commonware_runtime::deterministic::Runner::new(Config::default().with_seed(12345));
        rt.start(|context| async move {
            let greddy = context.clone().spawn(|_| async {
                greedy_task();
            });
            let cpu_cooperative_task = context.clone().spawn(|context| async move {
                cpu_cooperative(&context).await;
            });
            let io_bound_task = context.clone().spawn(|context| async move {
                io_bound(&context).await;
            });
            let delayed_task = context.clone().spawn(|context| async move {
                delayed_work(&context).await;
            });
            let _ = join!(greddy, cpu_cooperative_task, io_bound_task, delayed_task);
        });
    }
}
