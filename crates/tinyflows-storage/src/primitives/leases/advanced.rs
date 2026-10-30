use super::{Acquire, Acquisition, Body, Lease, LeaseRecord, Leases, id};
use tinystoragedrivers_types::{Result, StorageError};

impl Leases {
    /// Take `(owner, key)` with endpoint and takeover semantics.
    ///
    /// A missing record starts at epoch 1. A matching live tenure is
    /// re-entrant; a released or expired record advances the epoch. A live
    /// leftover from the same holder is taken over only when `held_epoch` is
    /// absent. A stale epoch or a competing live holder returns `Held`.
    ///
    /// # Errors
    ///
    /// `InvalidInput` for a non-positive ttl, a storage error, or a backend
    /// error when the epoch cannot be advanced.
    pub async fn acquire_with(&self, request: Acquire<'_>) -> Result<Acquisition> {
        let Acquire {
            owner,
            key,
            holder,
            ttl_ms,
            endpoint,
            held_epoch,
        } = request;
        let id = id(owner, key)?;
        let mut unclean = false;
        let mut seen: Option<Body> = None;
        let mut attempts = 0_u32;
        let stored = self
            .repo
            .update(&id, |current| {
                let (now, expires_ms) = self.deadline(ttl_ms)?;
                unclean = false;
                seen = None;
                let raced = attempts > 0;
                attempts += 1;
                let (epoch, was_unclean) = match &current {
                    None => (1, false),
                    Some(found) => {
                        let live = found.expires_ms > now && !found.released;
                        if live && found.holder == holder && held_epoch == Some(found.epoch) {
                            (found.epoch, false)
                        } else if live && (found.holder != holder || raced || held_epoch.is_some())
                        {
                            seen.clone_from(&current);
                            return Ok(None);
                        } else {
                            let next = found
                                .epoch
                                .checked_add(1)
                                .ok_or_else(|| StorageError::backend("lease epoch overflow"))?;
                            (next, !found.released)
                        }
                    }
                };
                unclean = was_unclean;
                Ok(Some(Body {
                    holder: holder.to_owned(),
                    epoch,
                    expires_ms,
                    endpoint: endpoint.map(str::to_owned),
                    released: false,
                }))
            })
            .await?;
        Ok(match stored {
            Some((body, _)) => Acquisition::Granted {
                lease: Lease::from_body(id, body),
                previous_unclean: unclean,
            },
            None => match seen {
                Some(body) => Acquisition::Held(LeaseRecord::from_body(body)),
                None => {
                    return Err(StorageError::backend(
                        "lease acquire declined without a live record",
                    ));
                }
            },
        })
    }

    /// Return the stored record, including lapsed and released leases.
    ///
    /// Unlike [`Self::current`], this lets observers distinguish a clean
    /// release from an abandoned lease and read the epoch a takeover resumes.
    ///
    /// # Errors
    ///
    /// A storage error.
    pub async fn inspect(&self, owner: &str, key: &str) -> Result<Option<LeaseRecord>> {
        let id = id(owner, key)?;
        Ok(self
            .repo
            .get(&id)
            .await?
            .map(|found| LeaseRecord::from_body(found.doc)))
    }

    /// Delete a lease only while it still belongs to `(holder, epoch)`.
    ///
    /// This compacts keys that will never be acquired again. A later acquire
    /// would restart at epoch 1, so callers must ensure no protected resource
    /// can still accept writes from that earlier epoch.
    ///
    /// # Errors
    ///
    /// A storage error, or a conflict past the retry budget.
    pub async fn compact(&self, owner: &str, key: &str, holder: &str, epoch: u64) -> Result<bool> {
        let id = id(owner, key)?;
        self.repo
            .delete_if(&id, |lease| lease.holder == holder && lease.epoch == epoch)
            .await
    }
}
