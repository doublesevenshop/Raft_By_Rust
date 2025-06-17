use crate::raft::config;
use rand::{self, Rng};
use std::time::{Duration, Instant};

pub fn rand_election_timeout() -> Duration {
    let timeout = rand::random_range(config::ELECTION_TIMEOUT_MIN_MILLIS..config::ELECTION_TIMEOUT_MAX_MILLIS);
    Duration::from_millis(timeout)
}