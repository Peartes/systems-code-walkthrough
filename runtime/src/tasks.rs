//! Workloads used by the runtime demos.
//!
//! These tasks are intentionally simple but observable. They let us compare
//! how Tokio schedules work (nondeterministic interleaving) versus how the
//! Commonware deterministic runtime schedules work (repeatable interleaving).
//! The same data and seed should lead to the same execution path, which is
//! the property required by systems that must agree on state transitions.

use std::time::Duration;

use commonware_runtime::Clock;
use rand::{SeedableRng, seq::IndexedRandom};

/// Load a fixed corpus of words from `src/grimm.txt`.
///
/// This provides stable input for experiments so any differences in output or
/// ordering are due to scheduling, not data changes.
pub fn read_file() -> Vec<String> {
    let path = std::env::current_dir().expect("Current directory should be accessible");
    std::fs::read_to_string(format!("{}/src/grimm.txt", path.display()))
        .expect("File should be read successfully")
        .split_whitespace()
        .map(|word| word.to_string())
        .collect()
}

/// Pick a single word from the corpus.
///
/// When `seed` is provided, selection is deterministic, which makes the
/// downstream scheduling path reproducible.
pub async fn select_random_word(words: &[String], seed: Option<u64>) -> String {
    let mut rng = if let Some(seed) = seed {
        rand::rngs::StdRng::seed_from_u64(seed)
    } else {
        rand::rngs::StdRng::from_os_rng()
    };
    let word = words.choose(&mut rng).unwrap().to_string();
    println!("Selected word is: {}", word);
    word
}

/// Count how many times a selected word appears.
///
/// This is a simple, pure computation used to demonstrate repeatable task
/// ordering when the runtime is deterministic.
pub async fn count_word_occurrences(word: &str, words: &[String]) -> usize {
    let count = words.iter().filter(|&w| w == word).count();
    println!("The word '{}' appears {} times in the file.", word, count);
    count
}

/// A CPU-bound task that never yields.
///
/// This models a "bad citizen" task that can starve other work on a
/// single-threaded executor.
pub fn greedy_task() {
    println!("CPU: Starting computation");
    let mut result = 0u64;
    for i in 0..100_000_000 {
        result = result.wrapping_add(i);
    }
    println!("CPU: Done (result: {})", result);
}

/// A CPU-bound task that yields periodically.
///
/// By sleeping briefly, it cooperates with the scheduler so other tasks can
/// make progress. This shows why cooperative yielding matters.
pub async fn cpu_cooperative(context: &impl Clock) {
    println!("CPU-Coop: Starting computation");
    let mut result = 0u64;
    for i in 0..100_000_000 {
        result = result.wrapping_add(i);

        // Yield every 10M iterations
        if i % 10_000_000 == 0 {
            context.sleep(Duration::from_micros(10)).await;
        }
    }
    println!("CPU-Coop: Done (result: {})", result);
}

/// Simulate I/O by sleeping between steps.
///
/// This highlights how runtimes handle waiting tasks and time advancement.
pub async fn io_bound(context: &impl Clock) {
    println!("I/O: Starting");
    for i in 0..5 {
        println!("I/O: Step {}", i);
        context.sleep(Duration::from_millis(50)).await; // Simulates I/O wait
    }
    println!("I/O: Done");
}

/// A deliberately delayed task.
///
/// Useful for observing how long-running waits interact with scheduling.
pub async fn delayed_work(context: &impl Clock) {
    println!("Delayed: Waiting 2 seconds...");
    context.sleep(Duration::from_secs(2)).await;
    println!("Delayed: Now executing!");
}

#[cfg(test)]
mod tasks_tests {
    use super::*;
    use tokio::runtime::Runtime;

    /// Ensures the corpus is present and non-empty.
    #[test]
    fn test_read_file() {
        let words = read_file();
        assert!(!words.is_empty());
    }

    /// Verifies that random selection returns a word from the corpus.
    #[test]
    fn test_select_random_word() {
        let words = read_file();
        let word = Runtime::new()
            .unwrap()
            .block_on(async { select_random_word(&words, None).await });
        assert!(words.contains(&word));
    }

    /// Verifies that counting a selected word yields a positive count.
    #[test]
    fn test_count_word_occurrences() {
        let words = read_file();
        let count = Runtime::new().unwrap().block_on(async {
            let word = select_random_word(&words, None).await;
            let count = count_word_occurrences(&word, &words).await;
            count
        });
        assert!(count > 0);
    }
}
