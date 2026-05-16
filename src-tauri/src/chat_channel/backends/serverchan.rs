use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::Serialize;
use tokio::sync::{mpsc, Mutex};

use crate::chat_channel::error::ChatChannelError;
use crate::chat_channel::traits::ChatChannelBackend;
use crate::chat_channel::types::*;

const SCT_BASE: &str = "https://sctapi.ftqq.com/";
const TITLE_MAX_LEN: usize = 32;
const DESP_MAX_LEN: usize = 32 * 1024;
const SHORT_MAX_LEN: usize = 64;

/// Server酱 (Server Chan) backend — one-way push only (no incoming).
///
/// Unlike Telegram/Lark which spawn a long-polling / websocket task in
/// `start`, Server酱 has no incoming command stream. `start` therefore
/// only flips the status flag and returns; the dispatcher never sees
/// inbound messages from this backend.
pub struct ServerChanBackend {
    send_key: String,
    default_channel: Option<String>,
    noip: Option<bool>,
    client: reqwest::Client,
    status: Arc<Mutex<ChannelConnectionStatus>>,
    #[allow(dead_code)]
    channel_id: i32,
}

#[derive(Serialize)]
struct SendBody<'a> {
    title: &'a str,
    desp: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    short: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    channel: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    noip: Option<u8>,
}

impl ServerChanBackend {
    pub fn new(
        channel_id: i32,
        send_key: String,
        default_channel: Option<String>,
        noip: Option<bool>,
    ) -> Self {
        Self {
            send_key,
            default_channel,
            noip,
            client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_default(),
            status: Arc::new(Mutex::new(ChannelConnectionStatus::Disconnected)),
            channel_id,
        }
    }

    fn endpoint(&self) -> String {
        // Webhook URL is built per-request and never logged. Composing
        // it as `{base}{send_key}.send` keeps the literal `.send` suffix
        // in source (per convergence criterion) without checking in a
        // real SendKey value.
        format!("{}{}.send", SCT_BASE, self.send_key)
    }

    /// POST to Server酱 and parse `{code, message, data:{pushid, readkey}}`.
    /// `code == 0` is success; anything else (including HTTP non-2xx)
    /// becomes `ChatChannelError::SendFailed`.
    async fn send_with(
        &self,
        title: &str,
        desp: &str,
        short: Option<&str>,
    ) -> Result<SentMessageId, ChatChannelError> {
        let title = truncate_str(title, TITLE_MAX_LEN);
        let desp = truncate_str(desp, DESP_MAX_LEN);
        let short_owned = short.map(|s| truncate_str(s, SHORT_MAX_LEN));

        let body = SendBody {
            title: &title,
            desp: &desp,
            short: short_owned.as_deref(),
            channel: self.default_channel.as_deref(),
            noip: self.noip.and_then(|v| if v { Some(1) } else { None }),
        };

        let resp = self
            .client
            .post(self.endpoint())
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| ChatChannelError::SendFailed(e.to_string()))?;

        let status = resp.status();
        if !status.is_success() {
            return Err(ChatChannelError::SendFailed(format!(
                "HTTP {}",
                status.as_u16()
            )));
        }

        let parsed: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| ChatChannelError::SendFailed(e.to_string()))?;

        let code = parsed.get("code").and_then(|v| v.as_i64());
        if code != Some(0) {
            let msg = parsed
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown error");
            return Err(ChatChannelError::SendFailed(format!(
                "code={} message={msg}",
                code.unwrap_or(-1)
            )));
        }

        let pushid = parsed
            .pointer("/data/pushid")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ChatChannelError::SendFailed("missing pushid".to_string()))?;
        let readkey = parsed
            .pointer("/data/readkey")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ChatChannelError::SendFailed("missing readkey".to_string()))?;

        Ok(SentMessageId(format!("{pushid}|{readkey}")))
    }
}

#[async_trait]
impl ChatChannelBackend for ServerChanBackend {
    fn channel_type(&self) -> ChannelType {
        ChannelType::ServerChan
    }

    async fn start(
        &self,
        _command_tx: mpsc::Sender<IncomingCommand>,
    ) -> Result<(), ChatChannelError> {
        // Server酱 is a one-way webhook; no incoming stream, no background
        // task. We only flip the status flag so the manager treats this
        // channel as ready for `send_*` calls.
        *self.status.lock().await = ChannelConnectionStatus::Connected;
        Ok(())
    }

    async fn stop(&self) -> Result<(), ChatChannelError> {
        *self.status.lock().await = ChannelConnectionStatus::Disconnected;
        Ok(())
    }

    async fn status(&self) -> ChannelConnectionStatus {
        *self.status.lock().await
    }

    async fn send_message(&self, text: &str) -> Result<SentMessageId, ChatChannelError> {
        self.send_with("codeg", text, None).await
    }

    async fn send_rich_message(
        &self,
        message: &RichMessage,
    ) -> Result<SentMessageId, ChatChannelError> {
        let title_owned = message.title.clone().unwrap_or_else(|| "codeg".to_string());

        let mut desp = message.body.clone();
        if !message.fields.is_empty() {
            desp.push_str("\n\n");
            for (key, value) in &message.fields {
                desp.push_str(&format!("**{key}**: {value}\n"));
            }
        }

        let short_owned: String = message.body.chars().take(SHORT_MAX_LEN).collect();
        let short = if short_owned.is_empty() {
            None
        } else {
            Some(short_owned.as_str())
        };

        self.send_with(&title_owned, &desp, short).await
    }

    async fn test_connection(&self) -> Result<(), ChatChannelError> {
        self.send_with("codeg test", "connection test", None)
            .await
            .map(|_| ())
    }
}

/// Truncate to the given byte budget while respecting char boundaries.
/// Server酱 enforces ~32-char title and ~32 KiB desp limits; over-budget
/// payloads return `code != 0` from the upstream, so we clip locally
/// instead of relying on the remote validator.
fn truncate_str(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while !s.is_char_boundary(end) && end > 0 {
        end -= 1;
    }
    s[..end].to_string()
}
