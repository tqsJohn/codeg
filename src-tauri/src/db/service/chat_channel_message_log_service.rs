use chrono::Utc;
use sea_orm::prelude::DateTimeUtc;
use sea_orm::{
    ActiveModelTrait, ActiveValue::NotSet, ColumnTrait, DatabaseConnection, EntityTrait,
    IntoActiveModel, QueryFilter, QueryOrder, Set,
};

use crate::db::entities::chat_channel_message_log;
use crate::db::error::DbError;

pub async fn create_log(
    conn: &DatabaseConnection,
    channel_id: i32,
    direction: &str,
    message_type: &str,
    content_preview: &str,
    status: &str,
    error_detail: Option<String>,
) -> Result<(), DbError> {
    let active = chat_channel_message_log::ActiveModel {
        id: NotSet,
        channel_id: Set(channel_id),
        direction: Set(direction.to_string()),
        message_type: Set(message_type.to_string()),
        content_preview: Set(truncate_preview(content_preview)),
        status: Set(status.to_string()),
        error_detail: Set(error_detail),
        created_at: Set(Utc::now()),
    };
    active.insert(conn).await?;
    Ok(())
}

pub async fn list_by_channel(
    conn: &DatabaseConnection,
    channel_id: i32,
    limit: u64,
    offset: u64,
) -> Result<Vec<chat_channel_message_log::Model>, DbError> {
    use sea_orm::PaginatorTrait;
    Ok(chat_channel_message_log::Entity::find()
        .filter(chat_channel_message_log::Column::ChannelId.eq(channel_id))
        .order_by_desc(chat_channel_message_log::Column::CreatedAt)
        .paginate(conn, limit)
        .fetch_page(offset / limit)
        .await?)
}

/// Look up a single message log row by primary key.
pub async fn get_by_id(
    conn: &DatabaseConnection,
    id: i32,
) -> Result<Option<chat_channel_message_log::Model>, DbError> {
    Ok(chat_channel_message_log::Entity::find_by_id(id)
        .one(conn)
        .await?)
}

/// Persist the upstream WeChat delivery status reported by Server酱's
/// `push?id=&readkey=` endpoint into the existing `error_detail` column
/// (re-used as a generic "delivery status detail" slot so we avoid a
/// schema migration).
pub async fn update_wx_status(
    conn: &DatabaseConnection,
    id: i32,
    wx_status: &str,
) -> Result<(), DbError> {
    let model = chat_channel_message_log::Entity::find_by_id(id)
        .one(conn)
        .await?
        .ok_or_else(|| DbError::Migration(format!("chat channel message log not found: {id}")))?;

    let mut active = model.into_active_model();
    active.error_detail = Set(Some(format!("wxstatus:{wx_status}")));
    active.update(conn).await?;
    Ok(())
}

pub async fn cleanup_old_logs(
    conn: &DatabaseConnection,
    older_than: DateTimeUtc,
) -> Result<u64, DbError> {
    let result = chat_channel_message_log::Entity::delete_many()
        .filter(chat_channel_message_log::Column::CreatedAt.lt(older_than))
        .exec(conn)
        .await?;
    Ok(result.rows_affected)
}

fn truncate_preview(s: &str) -> String {
    if s.len() <= 200 {
        s.to_string()
    } else {
        let mut end = 200;
        while !s.is_char_boundary(end) && end > 0 {
            end -= 1;
        }
        format!("{}...", &s[..end])
    }
}
