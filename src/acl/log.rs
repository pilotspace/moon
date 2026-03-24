use std::collections::VecDeque;

#[derive(Debug, Clone)]
pub struct AclLogEntry {
    pub reason: String,
    pub object: String,
    pub username: String,
    pub client_addr: String,
    pub timestamp_ms: u64,
}

pub struct AclLog {
    entries: VecDeque<AclLogEntry>,
    max_entries: usize,
}

impl AclLog {
    pub fn new(max_entries: usize) -> Self {
        AclLog {
            entries: VecDeque::new(),
            max_entries,
        }
    }

    pub fn push(&mut self, entry: AclLogEntry) {
        if self.entries.len() >= self.max_entries {
            self.entries.pop_front();
        }
        self.entries.push_back(entry);
    }

    /// Returns up to `count` most-recent entries (all if count is None).
    pub fn entries(&self, count: Option<usize>) -> Vec<&AclLogEntry> {
        let limit = count.unwrap_or(self.entries.len());
        self.entries.iter().rev().take(limit).collect()
    }

    pub fn reset(&mut self) {
        self.entries.clear();
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(id: u64) -> AclLogEntry {
        AclLogEntry {
            reason: "command".to_string(),
            object: format!("cmd_{}", id),
            username: "alice".to_string(),
            client_addr: "127.0.0.1:1234".to_string(),
            timestamp_ms: id,
        }
    }

    #[test]
    fn test_acl_log_push_and_len() {
        let mut log = AclLog::new(128);
        assert_eq!(log.len(), 0);
        assert!(log.is_empty());
        log.push(make_entry(1));
        assert_eq!(log.len(), 1);
        assert!(!log.is_empty());
    }

    #[test]
    fn test_acl_log_circular_eviction() {
        let mut log = AclLog::new(128);
        for i in 0..200 {
            log.push(make_entry(i));
        }
        assert_eq!(log.len(), 128);
        // Most recent entry should be 199
        let entries = log.entries(Some(1));
        assert_eq!(entries[0].timestamp_ms, 199);
        // Oldest should be 72 (200 - 128)
        let all = log.entries(None);
        assert_eq!(all.last().unwrap().timestamp_ms, 72);
    }

    #[test]
    fn test_acl_log_entries_with_count() {
        let mut log = AclLog::new(128);
        for i in 0..10 {
            log.push(make_entry(i));
        }
        let entries = log.entries(Some(5));
        assert_eq!(entries.len(), 5);
        // Most recent first
        assert_eq!(entries[0].timestamp_ms, 9);
        assert_eq!(entries[4].timestamp_ms, 5);
    }

    #[test]
    fn test_acl_log_reset() {
        let mut log = AclLog::new(128);
        for i in 0..10 {
            log.push(make_entry(i));
        }
        log.reset();
        assert_eq!(log.len(), 0);
        assert!(log.is_empty());
    }
}
