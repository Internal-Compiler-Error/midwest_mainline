//! Global transfer rate limits: one token bucket per direction, shared by every swarm in
//! the client. A swarm asks before requesting a block (download) and before sending one
//! (upload); a refusal means "not now", and the swarm tries again on its next housekeeping
//! tick, so the limit is honoured to within about a second.

use crate::config::SettingsWatch;
use std::sync::Mutex;
use std::time::Instant;

pub struct RateLimiter {
    settings: SettingsWatch,
    download: Mutex<Bucket>,
    upload: Mutex<Bucket>,
}

impl RateLimiter {
    pub fn new(settings: SettingsWatch) -> Self {
        Self {
            settings,
            download: Mutex::new(Bucket::new(Instant::now())),
            upload: Mutex::new(Bucket::new(Instant::now())),
        }
    }

    /// Whether `bytes` may be requested now under the download limit; spends them if so.
    pub fn take_download(&self, bytes: usize) -> bool {
        let rate = self.settings.borrow().download_limit;
        self.download.lock().unwrap().take(rate, bytes, Instant::now())
    }

    /// Whether `bytes` may be sent now under the upload limit; spends them if so.
    pub fn take_upload(&self, bytes: usize) -> bool {
        let rate = self.settings.borrow().upload_limit;
        self.upload.lock().unwrap().take(rate, bytes, Instant::now())
    }
}

/// Tokens accrue at `rate` bytes per second up to one second's worth, so a burst after a
/// quiet spell is bounded by the rate itself.
struct Bucket {
    tokens: f64,
    last: Instant,
}

impl Bucket {
    /// Starts full, so the first second under a limit isn't a dead one.
    fn new(now: Instant) -> Self {
        Self {
            tokens: f64::MAX,
            last: now,
        }
    }

    fn take(&mut self, rate: u64, bytes: usize, now: Instant) -> bool {
        if rate == 0 {
            return true;
        }
        let rate = rate as f64;
        self.tokens = (self.tokens + now.duration_since(self.last).as_secs_f64() * rate).min(rate);
        self.last = now;
        if self.tokens >= bytes as f64 {
            self.tokens -= bytes as f64;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;

    #[test]
    fn a_bucket_pays_out_its_rate_per_second_and_no_more() {
        let t0 = Instant::now();
        let mut bucket = Bucket::new(t0);
        assert!(bucket.take(0, 1 << 30, t0), "zero means no limit");

        let later = t0 + Duration::from_secs(1);
        assert!(bucket.take(1000, 600, later));
        assert!(!bucket.take(1000, 600, later), "only 400 left this second");
        assert!(bucket.take(1000, 400, later));

        let much_later = t0 + Duration::from_secs(60);
        assert!(bucket.take(1000, 1000, much_later));
        assert!(
            !bucket.take(1000, 1, much_later),
            "a quiet minute doesn't bank more than a second"
        );
    }
}
