//! Outbound delivery dispatchers.
//!
//! Each module implements an HTTP-based delivery target:
//! - [`webhook`] — generic HTTP POST
//! - [`slack`] — Slack Webhook payload formatting
//! - [`telegram`] — Telegram Bot API message formatting
//!
//! All modules share a common retry pattern (3 attempts, linear backoff)
//! and return a boolean success indicator.

pub mod slack;
pub mod telegram;
pub mod webhook;
