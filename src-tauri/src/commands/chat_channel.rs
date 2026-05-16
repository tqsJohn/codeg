use crate::app_error::AppCommandError;
use crate::chat_channel::backends::weixin::{WeixinQrcodeInfo, WeixinQrcodeStatusPublic};
use crate::chat_channel::manager::ChatChannelManager;
use crate::chat_channel::types::ChannelType;
use crate::db::service::{chat_channel_message_log_service, chat_channel_service};
use crate::db::AppDatabase;
use crate::models::chat_channel::{ChannelStatusInfo, ChatChannelInfo, ChatChannelMessageLogInfo};

// ---------------------------------------------------------------------------
// Shared core functions (used by both Tauri commands and web handlers)
// ---------------------------------------------------------------------------

pub async fn list_chat_channels_core(
    db: &AppDatabase,
) -> Result<Vec<ChatChannelInfo>, AppCommandError> {
    let rows = chat_channel_service::list_all(&db.conn)
        .await
        .map_err(AppCommandError::from)?;
    Ok(rows.into_iter().map(ChatChannelInfo::from).collect())
}

pub async fn create_chat_channel_core(
    db: &AppDatabase,
    name: String,
    channel_type: String,
    config_json: String,
    enabled: bool,
    daily_report_enabled: bool,
    daily_report_time: Option<String>,
) -> Result<ChatChannelInfo, AppCommandError> {
    // Validate channel_type
    let _: ChannelType = serde_json::from_value(serde_json::Value::String(channel_type.clone()))
        .map_err(|_| {
            AppCommandError::invalid_input(format!("Invalid channel type: {channel_type}"))
        })?;

    let model = chat_channel_service::create(
        &db.conn,
        name,
        channel_type,
        config_json,
        enabled,
        daily_report_enabled,
        daily_report_time,
    )
    .await
    .map_err(AppCommandError::from)?;
    Ok(ChatChannelInfo::from(model))
}

#[allow(clippy::too_many_arguments)]
pub async fn update_chat_channel_core(
    db: &AppDatabase,
    id: i32,
    name: Option<String>,
    enabled: Option<bool>,
    config_json: Option<String>,
    event_filter_json: Option<Option<String>>,
    daily_report_enabled: Option<bool>,
    daily_report_time: Option<Option<String>>,
) -> Result<ChatChannelInfo, AppCommandError> {
    let model = chat_channel_service::update(
        &db.conn,
        id,
        name,
        enabled,
        config_json,
        event_filter_json,
        daily_report_enabled,
        daily_report_time,
    )
    .await
    .map_err(AppCommandError::from)?;
    Ok(ChatChannelInfo::from(model))
}

pub async fn delete_chat_channel_core(
    db: &AppDatabase,
    manager: &ChatChannelManager,
    id: i32,
) -> Result<(), AppCommandError> {
    // Disconnect running backend before deleting from DB (prevents orphaned task)
    let _ = manager.remove_channel(id).await;
    chat_channel_service::delete(&db.conn, id)
        .await
        .map_err(AppCommandError::from)?;
    let _ = crate::keyring_store::delete_channel_token(id);
    Ok(())
}

pub async fn connect_chat_channel_core(
    db: &AppDatabase,
    manager: &ChatChannelManager,
    id: i32,
) -> Result<(), AppCommandError> {
    let model = chat_channel_service::get_by_id(&db.conn, id)
        .await
        .map_err(AppCommandError::from)?
        .ok_or_else(|| AppCommandError::not_found(format!("Chat channel {id} not found")))?;

    let channel_type: ChannelType = serde_json::from_value(serde_json::Value::String(
        model.channel_type.clone(),
    ))
    .map_err(|_| {
        AppCommandError::configuration_invalid(format!(
            "Invalid channel type: {}",
            model.channel_type
        ))
    })?;

    let config: serde_json::Value = serde_json::from_str(&model.config_json).map_err(|e| {
        AppCommandError::configuration_invalid("Invalid config JSON").with_detail(e.to_string())
    })?;

    let token = crate::keyring_store::get_channel_token(id).ok_or_else(|| {
        eprintln!("[connect_chat_channel] channel {id}: Token not set in keyring");
        AppCommandError::configuration_missing("Token not set")
    })?;

    eprintln!(
        "[connect_chat_channel] channel {id}: creating {channel_type} backend, config={}",
        model.config_json
    );

    let backend = crate::chat_channel::backends::create_backend(id, channel_type, &config, token)
        .map_err(AppCommandError::from)?;

    manager
        .add_channel(id, model.name, channel_type, backend)
        .await
        .map_err(|e| {
            eprintln!("[connect_chat_channel] channel {id}: add_channel failed: {e}");
            AppCommandError::from(e)
        })?;

    eprintln!("[connect_chat_channel] channel {id}: connected successfully");
    Ok(())
}

pub async fn test_chat_channel_core(db: &AppDatabase, id: i32) -> Result<(), AppCommandError> {
    let model = chat_channel_service::get_by_id(&db.conn, id)
        .await
        .map_err(AppCommandError::from)?
        .ok_or_else(|| AppCommandError::not_found(format!("Chat channel {id} not found")))?;

    let channel_type: ChannelType = serde_json::from_value(serde_json::Value::String(
        model.channel_type.clone(),
    ))
    .map_err(|_| {
        AppCommandError::configuration_invalid(format!(
            "Invalid channel type: {}",
            model.channel_type
        ))
    })?;

    let config: serde_json::Value = serde_json::from_str(&model.config_json).map_err(|e| {
        AppCommandError::configuration_invalid("Invalid config JSON").with_detail(e.to_string())
    })?;

    let token = crate::keyring_store::get_channel_token(id)
        .ok_or_else(|| AppCommandError::configuration_missing("Token not set"))?;

    let backend = crate::chat_channel::backends::create_backend(id, channel_type, &config, token)
        .map_err(AppCommandError::from)?;

    backend
        .test_connection()
        .await
        .map_err(AppCommandError::from)?;

    Ok(())
}

pub fn save_chat_channel_token_core(channel_id: i32, token: &str) -> Result<(), AppCommandError> {
    crate::keyring_store::set_channel_token(channel_id, token)
        .map_err(|e| AppCommandError::io_error("Failed to save token").with_detail(e))
}

pub fn get_chat_channel_has_token_core(channel_id: i32) -> Result<bool, AppCommandError> {
    Ok(crate::keyring_store::get_channel_token(channel_id).is_some())
}

pub fn delete_chat_channel_token_core(channel_id: i32) -> Result<(), AppCommandError> {
    crate::keyring_store::delete_channel_token(channel_id)
        .map_err(|e| AppCommandError::io_error("Failed to delete token").with_detail(e))
}

pub async fn disconnect_chat_channel_core(
    manager: &ChatChannelManager,
    id: i32,
) -> Result<(), AppCommandError> {
    manager
        .remove_channel(id)
        .await
        .map_err(AppCommandError::from)?;
    Ok(())
}

pub async fn get_chat_channel_status_core(
    manager: &ChatChannelManager,
) -> Result<Vec<ChannelStatusInfo>, AppCommandError> {
    Ok(manager.get_status().await)
}

pub async fn list_chat_channel_messages_core(
    db: &AppDatabase,
    channel_id: i32,
    limit: Option<u64>,
    offset: Option<u64>,
) -> Result<Vec<ChatChannelMessageLogInfo>, AppCommandError> {
    let limit = limit.unwrap_or(50);
    let offset = offset.unwrap_or(0);
    let rows =
        chat_channel_message_log_service::list_by_channel(&db.conn, channel_id, limit, offset)
            .await
            .map_err(AppCommandError::from)?;
    Ok(rows
        .into_iter()
        .map(ChatChannelMessageLogInfo::from)
        .collect())
}

const COMMAND_PREFIX_KEY: &str = "chat_command_prefix";
const DEFAULT_COMMAND_PREFIX: &str = "/";

pub async fn get_chat_command_prefix_core(db: &AppDatabase) -> Result<String, AppCommandError> {
    let val = crate::db::service::app_metadata_service::get_value(&db.conn, COMMAND_PREFIX_KEY)
        .await
        .map_err(AppCommandError::from)?;
    Ok(val.unwrap_or_else(|| DEFAULT_COMMAND_PREFIX.to_string()))
}

pub async fn set_chat_command_prefix_core(
    db: &AppDatabase,
    prefix: String,
) -> Result<(), AppCommandError> {
    let trimmed = prefix.trim();
    if trimmed.is_empty() || trimmed.len() > 3 || trimmed.chars().any(|c| c.is_alphanumeric()) {
        return Err(AppCommandError::invalid_input(
            "Prefix must be 1-3 non-alphanumeric characters",
        ));
    }
    crate::db::service::app_metadata_service::upsert_value(&db.conn, COMMAND_PREFIX_KEY, trimmed)
        .await
        .map_err(AppCommandError::from)?;
    Ok(())
}

const MESSAGE_LANGUAGE_KEY: &str = "chat_message_language";

pub async fn get_chat_message_language_core(db: &AppDatabase) -> Result<String, AppCommandError> {
    let val = crate::db::service::app_metadata_service::get_value(&db.conn, MESSAGE_LANGUAGE_KEY)
        .await
        .map_err(AppCommandError::from)?;
    Ok(val.unwrap_or_else(|| "en".to_string()))
}

pub async fn set_chat_message_language_core(
    db: &AppDatabase,
    language: String,
) -> Result<(), AppCommandError> {
    // Validate language code
    let valid = [
        "en", "zh-cn", "zh-tw", "ja", "ko", "es", "de", "fr", "pt", "ar",
    ];
    let lang_lower = language.to_lowercase();
    if !valid.contains(&lang_lower.as_str()) {
        return Err(AppCommandError::invalid_input(format!(
            "Unsupported language: {language}. Supported: {}",
            valid.join(", ")
        )));
    }
    crate::db::service::app_metadata_service::upsert_value(
        &db.conn,
        MESSAGE_LANGUAGE_KEY,
        &lang_lower,
    )
    .await
    .map_err(AppCommandError::from)?;
    Ok(())
}

const EVENT_FILTER_KEY: &str = "chat_event_filter";

pub async fn get_chat_event_filter_core(
    db: &AppDatabase,
) -> Result<Option<Vec<String>>, AppCommandError> {
    let val = crate::db::service::app_metadata_service::get_value(&db.conn, EVENT_FILTER_KEY)
        .await
        .map_err(AppCommandError::from)?;
    match val {
        Some(json) => {
            // Parse as Option<Vec<String>> to correctly handle stored "null"
            let filter: Option<Vec<String>> = serde_json::from_str(&json)
                .map_err(|e| AppCommandError::invalid_input(e.to_string()))?;
            Ok(filter)
        }
        None => Ok(None),
    }
}

pub async fn set_chat_event_filter_core(
    db: &AppDatabase,
    filter: Option<Vec<String>>,
) -> Result<(), AppCommandError> {
    match filter {
        Some(arr) => {
            let json = serde_json::to_string(&arr)
                .map_err(|e| AppCommandError::invalid_input(e.to_string()))?;
            crate::db::service::app_metadata_service::upsert_value(
                &db.conn,
                EVENT_FILTER_KEY,
                &json,
            )
            .await
            .map_err(AppCommandError::from)?;
        }
        None => {
            // null means all events enabled — remove the key
            crate::db::service::app_metadata_service::upsert_value(
                &db.conn,
                EVENT_FILTER_KEY,
                "null",
            )
            .await
            .map_err(AppCommandError::from)?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// WeChat QR code auth
// ---------------------------------------------------------------------------

pub async fn weixin_get_qrcode_core() -> Result<WeixinQrcodeInfo, AppCommandError> {
    crate::chat_channel::backends::weixin::weixin_get_qrcode()
        .await
        .map_err(AppCommandError::from)
}

// ---------------------------------------------------------------------------
// Server酱 status query
// ---------------------------------------------------------------------------

/// Look up the WeChat delivery status of a previously-pushed Server酱
/// message and write it back into the `chat_channel_message_log` row.
///
/// Returns `Ok(None)` when:
///   * the log row does not exist or is not a `server_chan` message
///   * the SentMessageId is not in the expected `pushid|readkey` shape
///   * Server酱 has not yet produced a `wxstatus` field for this push
///
/// Returns `Ok(Some(status))` and persists the value when Server酱
/// reports a non-empty `data.wxstatus`.
pub async fn query_server_chan_status_core(
    db: &AppDatabase,
    log_id: i32,
) -> Result<Option<String>, AppCommandError> {
    // 1) Load the log row and verify it belongs to a server_chan channel.
    let log = chat_channel_message_log_service::get_by_id(&db.conn, log_id)
        .await
        .map_err(AppCommandError::from)?
        .ok_or_else(|| AppCommandError::not_found(format!("Message log {log_id} not found")))?;

    let channel = chat_channel_service::get_by_id(&db.conn, log.channel_id)
        .await
        .map_err(AppCommandError::from)?;
    let Some(channel) = channel else {
        return Ok(None);
    };
    if channel.channel_type != "server_chan" {
        return Ok(None);
    }

    // 2) SentMessageId is stored inside `content_preview` style by the
    //    dispatcher; the canonical place however is the structured
    //    `SentMessageId(pushid|readkey)` returned by the backend. In the
    //    current schema we re-derive it from the log row's preview/error
    //    slot: callers writing Server酱 logs persist `pushid|readkey`
    //    into `content_preview`. If the format does not match we treat
    //    it as "no info yet" and return None.
    let raw = &log.content_preview;
    let (pushid, readkey) = match split_pushid_readkey(raw) {
        Some(pair) => pair,
        None => return Ok(None),
    };

    // 3) Query Server酱's status endpoint. Failures are surfaced as
    //    network errors so the UI can retry; "no wxstatus yet" is not
    //    an error.
    let client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| AppCommandError::network("Failed to build HTTP client").with_detail(e.to_string()))?;

    let url = format!(
        "https://sctapi.ftqq.com/push?id={}&readkey={}",
        pushid, readkey
    );
    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| AppCommandError::network("Server酱 status request failed").with_detail(e.to_string()))?;

    if !resp.status().is_success() {
        return Err(AppCommandError::network(format!(
            "Server酱 status HTTP {}",
            resp.status().as_u16()
        )));
    }

    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| AppCommandError::network("Server酱 status decode failed").with_detail(e.to_string()))?;

    let wxstatus = body
        .pointer("/data/wxstatus")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty());

    if let Some(ref status) = wxstatus {
        chat_channel_message_log_service::update_wx_status(&db.conn, log_id, status)
            .await
            .map_err(AppCommandError::from)?;
    }

    Ok(wxstatus)
}

/// Split a `pushid|readkey` SentMessageId-shaped string. Returns `None`
/// if either side is empty or the separator is missing — the caller
/// treats that as "no Server酱 status info available".
fn split_pushid_readkey(s: &str) -> Option<(&str, &str)> {
    let (pushid, readkey) = s.split_once('|')?;
    if pushid.is_empty() || readkey.is_empty() {
        return None;
    }
    Some((pushid, readkey))
}

pub async fn weixin_check_qrcode_core(
    db: &AppDatabase,
    channel_id: i32,
    qrcode: &str,
) -> Result<WeixinQrcodeStatusPublic, AppCommandError> {
    let result = crate::chat_channel::backends::weixin::weixin_check_qrcode(qrcode)
        .await
        .map_err(AppCommandError::from)?;

    // On confirmed: save token + update config with base_url
    if result.status == "confirmed" {
        eprintln!(
            "[Weixin] QR confirmed for channel {channel_id}, bot_token={}, base_url={}",
            result
                .bot_token
                .as_deref()
                .map(|t| if t.len() > 8 { &t[..8] } else { t })
                .unwrap_or("None"),
            result.base_url.as_deref().unwrap_or("None"),
        );
        if let Some(ref token) = result.bot_token {
            save_chat_channel_token_core(channel_id, token)?;
            eprintln!("[Weixin] Token saved for channel {channel_id}");
        } else {
            eprintln!(
                "[Weixin] WARNING: No bot_token in confirmed response for channel {channel_id}"
            );
        }
        if let Some(ref base_url) = result.base_url {
            let config_json = serde_json::json!({ "base_url": base_url }).to_string();
            update_chat_channel_core(
                db,
                channel_id,
                None,
                None,
                Some(config_json),
                None,
                None,
                None,
            )
            .await?;
            eprintln!("[Weixin] Config updated with base_url for channel {channel_id}");
        }
    }

    // Return only the status — never expose bot_token to the frontend
    Ok(WeixinQrcodeStatusPublic {
        status: result.status,
    })
}

// ---------------------------------------------------------------------------
// Tauri commands (use tauri::State for injection)
// ---------------------------------------------------------------------------

#[cfg(feature = "tauri-runtime")]
#[tauri::command]
pub async fn list_chat_channels(
    db: tauri::State<'_, AppDatabase>,
) -> Result<Vec<ChatChannelInfo>, AppCommandError> {
    list_chat_channels_core(&db).await
}

#[cfg(feature = "tauri-runtime")]
#[tauri::command]
pub async fn create_chat_channel(
    db: tauri::State<'_, AppDatabase>,
    name: String,
    channel_type: String,
    config_json: String,
    enabled: bool,
    daily_report_enabled: bool,
    daily_report_time: Option<String>,
) -> Result<ChatChannelInfo, AppCommandError> {
    create_chat_channel_core(
        &db,
        name,
        channel_type,
        config_json,
        enabled,
        daily_report_enabled,
        daily_report_time,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
#[cfg(feature = "tauri-runtime")]
#[tauri::command]
pub async fn update_chat_channel(
    db: tauri::State<'_, AppDatabase>,
    id: i32,
    name: Option<String>,
    enabled: Option<bool>,
    config_json: Option<String>,
    event_filter_json: Option<Option<String>>,
    daily_report_enabled: Option<bool>,
    daily_report_time: Option<Option<String>>,
) -> Result<ChatChannelInfo, AppCommandError> {
    update_chat_channel_core(
        &db,
        id,
        name,
        enabled,
        config_json,
        event_filter_json,
        daily_report_enabled,
        daily_report_time,
    )
    .await
}

#[cfg(feature = "tauri-runtime")]
#[tauri::command]
pub async fn delete_chat_channel(
    db: tauri::State<'_, AppDatabase>,
    manager: tauri::State<'_, ChatChannelManager>,
    id: i32,
) -> Result<(), AppCommandError> {
    delete_chat_channel_core(&db, &manager, id).await
}

#[cfg(feature = "tauri-runtime")]
#[tauri::command]
pub async fn save_chat_channel_token(
    channel_id: i32,
    token: String,
) -> Result<(), AppCommandError> {
    save_chat_channel_token_core(channel_id, &token)
}

#[cfg(feature = "tauri-runtime")]
#[tauri::command]
pub async fn get_chat_channel_has_token(channel_id: i32) -> Result<bool, AppCommandError> {
    get_chat_channel_has_token_core(channel_id)
}

#[cfg(feature = "tauri-runtime")]
#[tauri::command]
pub async fn delete_chat_channel_token(channel_id: i32) -> Result<(), AppCommandError> {
    delete_chat_channel_token_core(channel_id)
}

#[cfg(feature = "tauri-runtime")]
#[tauri::command]
pub async fn connect_chat_channel(
    db: tauri::State<'_, AppDatabase>,
    manager: tauri::State<'_, ChatChannelManager>,
    id: i32,
) -> Result<(), AppCommandError> {
    connect_chat_channel_core(&db, &manager, id).await
}

#[cfg(feature = "tauri-runtime")]
#[tauri::command]
pub async fn disconnect_chat_channel(
    manager: tauri::State<'_, ChatChannelManager>,
    id: i32,
) -> Result<(), AppCommandError> {
    disconnect_chat_channel_core(&manager, id).await
}

#[cfg(feature = "tauri-runtime")]
#[tauri::command]
pub async fn test_chat_channel(
    db: tauri::State<'_, AppDatabase>,
    id: i32,
) -> Result<(), AppCommandError> {
    test_chat_channel_core(&db, id).await
}

#[cfg(feature = "tauri-runtime")]
#[tauri::command]
pub async fn get_chat_channel_status(
    manager: tauri::State<'_, ChatChannelManager>,
) -> Result<Vec<ChannelStatusInfo>, AppCommandError> {
    get_chat_channel_status_core(&manager).await
}

#[cfg(feature = "tauri-runtime")]
#[tauri::command]
pub async fn list_chat_channel_messages(
    db: tauri::State<'_, AppDatabase>,
    channel_id: i32,
    limit: Option<u64>,
    offset: Option<u64>,
) -> Result<Vec<ChatChannelMessageLogInfo>, AppCommandError> {
    list_chat_channel_messages_core(&db, channel_id, limit, offset).await
}

#[cfg(feature = "tauri-runtime")]
#[tauri::command]
pub async fn get_chat_command_prefix(
    db: tauri::State<'_, AppDatabase>,
) -> Result<String, AppCommandError> {
    get_chat_command_prefix_core(&db).await
}

#[cfg(feature = "tauri-runtime")]
#[tauri::command]
pub async fn set_chat_command_prefix(
    db: tauri::State<'_, AppDatabase>,
    prefix: String,
) -> Result<(), AppCommandError> {
    set_chat_command_prefix_core(&db, prefix).await
}

#[cfg(feature = "tauri-runtime")]
#[tauri::command]
pub async fn get_chat_event_filter(
    db: tauri::State<'_, AppDatabase>,
) -> Result<Option<Vec<String>>, AppCommandError> {
    get_chat_event_filter_core(&db).await
}

#[cfg(feature = "tauri-runtime")]
#[tauri::command]
pub async fn set_chat_event_filter(
    db: tauri::State<'_, AppDatabase>,
    filter: Option<Vec<String>>,
) -> Result<(), AppCommandError> {
    set_chat_event_filter_core(&db, filter).await
}

#[cfg(feature = "tauri-runtime")]
#[tauri::command]
pub async fn get_chat_message_language(
    db: tauri::State<'_, AppDatabase>,
) -> Result<String, AppCommandError> {
    get_chat_message_language_core(&db).await
}

#[cfg(feature = "tauri-runtime")]
#[tauri::command]
pub async fn set_chat_message_language(
    db: tauri::State<'_, AppDatabase>,
    language: String,
) -> Result<(), AppCommandError> {
    set_chat_message_language_core(&db, language).await
}

#[cfg(feature = "tauri-runtime")]
#[tauri::command]
pub async fn weixin_get_qrcode() -> Result<WeixinQrcodeInfo, AppCommandError> {
    weixin_get_qrcode_core().await
}

#[cfg(feature = "tauri-runtime")]
#[tauri::command]
pub async fn weixin_check_qrcode(
    db: tauri::State<'_, AppDatabase>,
    channel_id: i32,
    qrcode: String,
) -> Result<WeixinQrcodeStatusPublic, AppCommandError> {
    weixin_check_qrcode_core(&db, channel_id, &qrcode).await
}

#[cfg(feature = "tauri-runtime")]
#[tauri::command]
pub async fn query_server_chan_status(
    db: tauri::State<'_, AppDatabase>,
    log_id: i32,
) -> Result<Option<String>, AppCommandError> {
    query_server_chan_status_core(&db, log_id).await
}
