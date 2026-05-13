//! MA5 — Maintenance-window scheduler.
//!
//! Operators can register cron-style expressions with budget multipliers to
//! control autovacuum aggressiveness at different times of day / week:
//!
//! ```text
//! RECLAMATION SCHEDULE 0 2 * * *   2.0   # 2x budget at 02:00 each night
//! RECLAMATION SCHEDULE * 9-17 * * 1-5  0.1  # 0.1x during business hours Mon-Fri
//! ```
//!
//! At each autovacuum tick:
//!   `actual_budget_ms = base_budget_ms × current_budget_multiplier(now)`
//!
//! When multiple windows match, the highest multiplier wins.
//! When no windows match, the multiplier is 1.0 (no change).
//!
//! ## Cron expression format (5 fields, POSIX subset)
//!
//! ```text
//! MIN  HOUR  DOM  MON  DOW
//!  *    *     *    *    *
//! ```
//!
//! Each field accepts:
//! - `*` — any value
//! - `N` — exact value
//! - `N-M` — inclusive range
//! - `N,M,...` — list (comma-separated values and/or ranges)
//!
//! Ranges: MIN 0-59, HOUR 0-23, DOM 1-31, MON 1-12, DOW 0-7 (0 and 7 = Sunday).
//!
//! ## Persistence
//!
//! [`MaintenanceSchedule::save_to_file`] writes a TOML file
//! (`reclamation-schedule.toml` by convention) that can be loaded on restart.
//! No manifest integration — the file is a standalone sidecar.
//!
//! ## No new dependencies
//!
//! The cron parser is hand-rolled using the restricted 5-field POSIX subset.
//! The `cron` crate (130 KB compiled, regex transitive dep) was evaluated and
//! rejected to keep the dependency surface minimal.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use thiserror::Error;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors from cron expression parsing.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ParseError {
    /// Wrong number of fields (expected 5).
    #[error("invalid cron field: {0}")]
    InvalidField(String),
    /// A value is out of the allowed range for its position.
    #[error("value out of range in field '{field}': {value} (allowed {min}-{max})")]
    OutOfRange {
        field: &'static str,
        value: u32,
        min: u32,
        max: u32,
    },
}

// ---------------------------------------------------------------------------
// Cron field representation
// ---------------------------------------------------------------------------

/// A parsed cron field: either wildcard or a set of matching values.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum CronField {
    Any,
    Values(Vec<u32>),
}

impl CronField {
    fn matches(&self, val: u32) -> bool {
        match self {
            CronField::Any => true,
            CronField::Values(v) => v.contains(&val),
        }
    }
}

/// Parse one cron field from text. `field_name` is used in error messages.
/// Allowed value range: `[min_val, max_val]` (inclusive).
fn parse_field(text: &str, field_name: &'static str, min_val: u32, max_val: u32) -> Result<CronField, ParseError> {
    let text = text.trim();
    if text == "*" {
        return Ok(CronField::Any);
    }

    let mut values: Vec<u32> = Vec::new();

    for part in text.split(',') {
        let part = part.trim();
        if part.contains('-') {
            let mut iter = part.splitn(2, '-');
            let lo_str = iter.next().unwrap_or("");
            let hi_str = iter.next().unwrap_or("");
            let lo = lo_str.parse::<u32>().map_err(|_| {
                ParseError::InvalidField(format!("non-numeric value '{lo_str}' in field {field_name}"))
            })?;
            let hi = hi_str.parse::<u32>().map_err(|_| {
                ParseError::InvalidField(format!("non-numeric value '{hi_str}' in field {field_name}"))
            })?;
            if lo < min_val || hi > max_val || lo > hi {
                return Err(ParseError::OutOfRange { field: field_name, value: lo.max(hi), min: min_val, max: max_val });
            }
            for v in lo..=hi {
                values.push(v);
            }
        } else {
            let v = part.parse::<u32>().map_err(|_| {
                ParseError::InvalidField(format!("non-numeric value '{part}' in field {field_name}"))
            })?;
            if v < min_val || v > max_val {
                return Err(ParseError::OutOfRange { field: field_name, value: v, min: min_val, max: max_val });
            }
            values.push(v);
        }
    }

    values.sort_unstable();
    values.dedup();
    Ok(CronField::Values(values))
}

// ---------------------------------------------------------------------------
// CronExpression
// ---------------------------------------------------------------------------

/// A parsed 5-field cron expression.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronExpression {
    raw: String,
    minute: CronField,
    hour: CronField,
    dom: CronField,
    month: CronField,
    dow: CronField,
}

impl CronExpression {
    /// Parse a 5-field cron expression string.
    pub fn parse(expr: &str) -> Result<Self, ParseError> {
        let fields: Vec<&str> = expr.split_whitespace().collect();
        if fields.len() != 5 {
            return Err(ParseError::InvalidField(format!(
                "expected 5 fields, got {} in '{expr}'",
                fields.len()
            )));
        }
        Ok(Self {
            raw: expr.to_string(),
            minute: parse_field(fields[0], "minute", 0, 59)?,
            hour:   parse_field(fields[1], "hour",   0, 23)?,
            dom:    parse_field(fields[2], "dom",    1, 31)?,
            month:  parse_field(fields[3], "month",  1, 12)?,
            dow:    parse_field(fields[4], "dow",    0, 7)?,
        })
    }

    /// Returns `true` when `t` falls within this cron schedule.
    ///
    /// Uses system UTC time decomposition. Clock-jump safe (calendar-based).
    pub fn matches(&self, t: SystemTime) -> bool {
        let Ok(duration) = t.duration_since(UNIX_EPOCH) else {
            return false;
        };
        let secs = duration.as_secs();

        // Decompose epoch seconds to (year, month, day, hour, min, dow).
        // dow: 0=Thu at epoch (1970-01-01), shift so 0=Sun.
        let (_, month, day, hour, min, dow) = epoch_to_utc(secs);

        let dow_matches = self.dow.matches(dow) || (dow == 0 && self.dow.matches(7));

        self.minute.matches(min)
            && self.hour.matches(hour)
            && self.dom.matches(day)
            && self.month.matches(month)
            && dow_matches
    }
}

/// Decompose Unix epoch seconds to (year, month 1-12, day 1-31, hour, min, dow 0-6 Sun=0).
fn epoch_to_utc(secs: u64) -> (u32, u32, u32, u32, u32, u32) {
    let min  = (secs / 60) % 60;
    let hour = (secs / 3600) % 24;
    // Day of week: 1970-01-01 was Thursday (4). 0=Sun.
    let dow  = ((secs / 86400 + 4) % 7) as u32;

    let mut days = (secs / 86400) as i64;
    let mut year = 1970i32;

    loop {
        let days_in_year = if is_leap(year) { 366 } else { 365 };
        if days < days_in_year {
            break;
        }
        days -= days_in_year;
        year += 1;
    }

    let mut month = 1u32;
    loop {
        let dim = days_in_month(year, month) as i64;
        if days < dim {
            break;
        }
        days -= dim;
        month += 1;
    }

    let day = days as u32 + 1;
    (year as u32, month, day, hour as u32, min as u32, dow)
}

fn is_leap(y: i32) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

fn days_in_month(y: i32, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap(y) => 29,
        2 => 28,
        _ => 30,
    }
}

// ---------------------------------------------------------------------------
// MaintenanceWindow — one cron expression + multiplier
// ---------------------------------------------------------------------------

/// A single maintenance window: a cron schedule + budget multiplier.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintenanceWindow {
    /// Raw cron expression string (stored for serialization).
    pub expression: String,
    /// Budget multiplier when this window is active.
    /// > 1.0 → allow more background work (e.g. 2.0x at night).
    /// < 1.0 → throttle background work (e.g. 0.1x during business hours).
    pub multiplier: f32,
    #[serde(skip)]
    parsed: Option<CronExpression>,
}

impl MaintenanceWindow {
    fn ensure_parsed(&mut self) -> Result<(), ParseError> {
        if self.parsed.is_none() {
            self.parsed = Some(CronExpression::parse(&self.expression)?);
        }
        Ok(())
    }

    fn matches(&mut self, t: SystemTime) -> bool {
        if self.parsed.is_none() {
            if let Ok(expr) = CronExpression::parse(&self.expression) {
                self.parsed = Some(expr);
            } else {
                return false;
            }
        }
        self.parsed.as_ref().map_or(false, |e| e.matches(t))
    }
}

// ---------------------------------------------------------------------------
// MaintenanceSchedule
// ---------------------------------------------------------------------------

/// Serializable container for all maintenance windows.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct MaintenanceScheduleFile {
    windows: Vec<MaintenanceWindowRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MaintenanceWindowRecord {
    expression: String,
    multiplier: f32,
}

/// Per-shard maintenance schedule: a collection of cron windows with multipliers.
///
/// At each autovacuum tick, call [`MaintenanceSchedule::current_budget_multiplier`]
/// to get the effective multiplier for the current wall-clock time.
///
/// ## Example
///
/// ```rust,ignore
/// let mut sched = MaintenanceSchedule::new();
/// sched.add("0 2 * * *",     2.0).unwrap(); // 2x budget at 02:00
/// sched.add("* 9-17 * * 1-5", 0.1).unwrap(); // 0.1x during biz hours
///
/// let multiplier = sched.current_budget_multiplier(SystemTime::now());
/// let actual_budget_ms = base_budget_ms as f64 * multiplier as f64;
/// ```
pub struct MaintenanceSchedule {
    windows: Vec<MaintenanceWindow>,
    /// True when the schedule has been modified since last save (or since creation).
    /// The event loop checks this flag and auto-saves to the persistence path.
    dirty: bool,
}

impl MaintenanceSchedule {
    /// Create an empty schedule (multiplier = 1.0 always).
    pub fn new() -> Self {
        Self { windows: Vec::new(), dirty: false }
    }

    /// Returns true if the schedule has been modified since the last save.
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Clear the dirty flag (call after a successful save).
    pub fn mark_clean(&mut self) {
        self.dirty = false;
    }

    /// Add a maintenance window.
    ///
    /// `expression` must be a valid 5-field cron expression.
    /// `multiplier` is the budget multiplier when the window is active.
    pub fn add(&mut self, expression: &str, multiplier: f32) -> Result<(), ParseError> {
        // Validate the expression up front.
        let parsed = CronExpression::parse(expression)?;
        self.windows.push(MaintenanceWindow {
            expression: expression.to_string(),
            multiplier,
            parsed: Some(parsed),
        });
        self.dirty = true;
        Ok(())
    }

    /// Return the effective budget multiplier for wall-clock time `now`.
    ///
    /// When one or more windows match, returns the highest multiplier among
    /// all matching windows. When no windows match (or schedule is empty),
    /// returns `1.0` (base budget, no change).
    ///
    /// Note: "highest multiplier" applies regardless of whether windows are
    /// boost (> 1.0) or throttle (< 1.0) windows. This means if you have
    /// a 0.1x throttle and a 2.0x boost both matching, the 2.0x wins.
    /// Design this intentionally: place non-overlapping cron expressions to
    /// get independent boost / throttle behavior.
    pub fn current_budget_multiplier(&self, now: SystemTime) -> f32 {
        let mut best: Option<f32> = None;
        for w in &self.windows {
            // parsed is always Some — we validate in add().
            if let Some(ref expr) = w.parsed {
                if expr.matches(now) {
                    best = Some(match best {
                        None => w.multiplier,
                        Some(b) => b.max(w.multiplier),
                    });
                }
            }
        }
        best.unwrap_or(1.0)
    }

    /// List all registered windows (expression, multiplier pairs).
    pub fn list(&self) -> Vec<(String, f32)> {
        self.windows
            .iter()
            .map(|w| (w.expression.clone(), w.multiplier))
            .collect()
    }

    /// Remove all windows.
    pub fn clear(&mut self) {
        self.windows.clear();
        self.dirty = true;
    }

    /// Persist the schedule to a TOML file.
    pub fn save_to_file(&self, path: &Path) -> std::io::Result<()> {
        let file = MaintenanceScheduleFile {
            windows: self.windows.iter().map(|w| MaintenanceWindowRecord {
                expression: w.expression.clone(),
                multiplier: w.multiplier,
            }).collect(),
        };
        let toml = toml_serialize(&file);
        std::fs::write(path, toml.as_bytes())
    }

    /// Load the schedule from a TOML file.
    ///
    /// Returns an empty schedule if the file does not exist.
    /// Returns an error if the file exists but is malformed.
    pub fn load_from_file(path: &Path) -> std::io::Result<Self> {
        if !path.exists() {
            return Ok(Self::new());
        }
        let data = std::fs::read_to_string(path)?;
        let file: MaintenanceScheduleFile = toml_deserialize(&data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let mut sched = Self::new();
        for rec in file.windows {
            sched.add(&rec.expression, rec.multiplier).map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{e}"))
            })?;
        }
        Ok(sched)
    }
}

impl Default for MaintenanceSchedule {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Minimal TOML serialization (avoid pulling toml crate if not already present)
// ---------------------------------------------------------------------------

fn toml_serialize(file: &MaintenanceScheduleFile) -> String {
    // Simple hand-rolled TOML array-of-tables.
    let mut out = String::from("# Moon reclamation maintenance schedule\n");
    for w in &file.windows {
        out.push_str("[[windows]]\n");
        out.push_str(&format!("expression = {:?}\n", w.expression));
        out.push_str(&format!("multiplier = {}\n", w.multiplier));
        out.push('\n');
    }
    out
}

fn toml_deserialize(s: &str) -> Result<MaintenanceScheduleFile, String> {
    // Parse [[windows]] array-of-tables manually.
    let mut windows: Vec<MaintenanceWindowRecord> = Vec::new();
    let mut current_expr: Option<String> = None;
    let mut current_mult: Option<f32> = None;

    for line in s.lines() {
        let line = line.trim();
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        if line == "[[windows]]" {
            // Flush previous entry if complete.
            if let (Some(expr), Some(mult)) = (current_expr.take(), current_mult.take()) {
                windows.push(MaintenanceWindowRecord { expression: expr, multiplier: mult });
            }
            continue;
        }
        if let Some((key, val)) = line.split_once('=') {
            let key = key.trim();
            let val = val.trim();
            match key {
                "expression" => {
                    // Strip surrounding quotes.
                    let unquoted = val
                        .trim_matches('"')
                        .to_string();
                    current_expr = Some(unquoted);
                }
                "multiplier" => {
                    let m: f32 = val.parse().map_err(|e| format!("invalid multiplier '{val}': {e}"))?;
                    current_mult = Some(m);
                }
                _ => {}
            }
        }
    }
    // Flush last entry.
    if let (Some(expr), Some(mult)) = (current_expr, current_mult) {
        windows.push(MaintenanceWindowRecord { expression: expr, multiplier: mult });
    }

    Ok(MaintenanceScheduleFile { windows })
}

// ---------------------------------------------------------------------------
// Tests (unit)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

    fn make_time(day_of_week_from_thu: u64, hour: u64, min: u64) -> SystemTime {
        // 1970-01-01 was Thursday. day_of_week_from_thu=0 → Thursday.
        // We want a specific (dow, hour, min) combination.
        let secs = day_of_week_from_thu * 86400 + hour * 3600 + min * 60;
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    #[test]
    fn test_cron_wildcard_matches_everything() {
        let expr = CronExpression::parse("* * * * *").unwrap();
        assert!(expr.matches(make_time(0, 0, 0)));
        assert!(expr.matches(make_time(3, 23, 59)));
    }

    #[test]
    fn test_cron_exact_hour() {
        let expr = CronExpression::parse("* 2 * * *").unwrap();
        assert!(expr.matches(make_time(0, 2, 0)));
        assert!(expr.matches(make_time(0, 2, 59)));
        assert!(!expr.matches(make_time(0, 3, 0)));
        assert!(!expr.matches(make_time(0, 1, 59)));
    }

    #[test]
    fn test_cron_range() {
        let expr = CronExpression::parse("* 9-17 * * *").unwrap();
        assert!(expr.matches(make_time(0, 9, 0)));
        assert!(expr.matches(make_time(0, 17, 0)));
        assert!(!expr.matches(make_time(0, 8, 59)));
        assert!(!expr.matches(make_time(0, 18, 0)));
    }

    #[test]
    fn test_cron_parse_error_too_few_fields() {
        assert!(CronExpression::parse("* * *").is_err());
    }

    #[test]
    fn test_cron_parse_error_out_of_range() {
        assert!(CronExpression::parse("60 * * * *").is_err());
        assert!(CronExpression::parse("* 24 * * *").is_err());
    }

    #[test]
    fn test_maintenance_schedule_highest_multiplier_wins() {
        let mut sched = MaintenanceSchedule::new();
        sched.add("* * * * *", 0.5).unwrap();
        sched.add("* 2 * * *", 2.0).unwrap();
        // 1970-01-01 02:00 UTC — wildcard (0.5) and hour-2 (2.0) both match.
        let t = UNIX_EPOCH + Duration::from_secs(2 * 3600);
        let mul = sched.current_budget_multiplier(t);
        assert!((mul - 2.0).abs() < 1e-6, "highest multiplier must win, got {mul}");
    }
}
