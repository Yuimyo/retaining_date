use anyhow::{anyhow, bail, Context, Result};
use async_recursion::async_recursion;
use chrono::{DateTime, Local};
use clap::Parser;
use sqlx::{Acquire, SqlitePool};
use std::{path::PathBuf, time::SystemTime};

#[derive(Parser)]
enum Commands {
    Apply {
        path: String,
    },
    Save {
        path: String,
        #[arg(short = 'r', long = "recursive")]
        recursive: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let database_file_name = dotenvy::var("DATABASE_PATH")?;
    let pool = SqlitePool::connect(&format!("file:{}", database_file_name)).await?;

    match Commands::parse() {
        Commands::Apply { path } => apply_dirs_props(&pool, path.into()).await,
        Commands::Save { path, recursive } => {
            if recursive {
                save_dirs_props_recursive(&pool, path.into()).await
            } else {
                save_dirs_props(&pool, path.into()).await
            }
        }
    }
}

#[async_recursion]
async fn save_dirs_props_recursive(pool: &SqlitePool, dir: PathBuf) -> Result<()> {
    save_dirs_props(pool, dir.clone()).await?;
    for entry in dir.read_dir()? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            save_dirs_props_recursive(pool, entry.path()).await?;
        }
    }
    Ok(())
}

async fn apply_dirs_props(pool: &SqlitePool, dir: PathBuf) -> Result<()> {
    if !dir.exists() {
        bail!("Doesn't exist dir: {:?}", dir);
    }
    let dir_prop_row_id: i64 = get_dir_prop_row_id(pool, dir.clone())
        .await
        .context(format!("Failed to get dir_prop_row_id: {:?}", dir))?;

    let mut tx = pool.begin().await?;
    let conn = tx.acquire().await?;

    let latest_cached_time: DateTime<Local> = match sqlx::query_as::<_, (DateTime<Local>,)>(
        "
            SELECT cached_date FROM dir_actions_log
            WHERE dir_id = $1 AND action_type = $2
            ORDER BY cached_date DESC
            LIMIT 1
        ",
    )
    .bind(dir_prop_row_id)
    .bind(LogActionType::CacheDates)
    .fetch_one(&mut *conn)
    .await
    {
        Ok((cached_date,)) => cached_date,
        Err(sqlx::Error::RowNotFound) => {
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };

    let files_props: Vec<_> = match sqlx::query_as::<_, (String, DateTime<Local>, DateTime<Local>)>(
        "
            SELECT name, created_date, modified_date FROM dir_file_props
            WHERE dir_id = $1 AND cached_date = $2
        ",
    )
    .bind(dir_prop_row_id)
    .bind(latest_cached_time)
    .fetch_all(&mut *conn)
    .await
    {
        Ok(files_props) => files_props,
        Err(sqlx::Error::RowNotFound) => return Ok(()),
        Err(e) => return Err(e.into()),
    };

    for (name, _, file_previous_modified_date) in files_props {
        let file_path = dir.clone().join(name);
        if !file_path.exists() || !file_path.is_file() {
            continue;
        }

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(file_path)?;
        file.set_modified(datetime_local_to_system_time(file_previous_modified_date))?;
    }

    tx.commit().await?;

    Ok(())
}

async fn save_dirs_props(pool: &SqlitePool, dir: PathBuf) -> Result<()> {
    if !dir.exists() {
        bail!("Doesn't exist dir: {:?}", dir);
    }
    let dir_prop_row_id: i64 = get_dir_prop_row_id(pool, dir.clone())
        .await
        .context(format!("Failed to get dir_prop_row_id: {:?}", dir))?;
    let cached_time: DateTime<Local> = Local::now();
    println!("save: {} | {:?}", dir_prop_row_id, dir);

    let mut tx = pool.begin().await?;
    let conn = tx.acquire().await?;

    // 変更ログを残す
    sqlx::query(
        "
            INSERT INTO dir_actions_log 
            (dir_id, action_type, cached_date) 
            VALUES ($1, $2, $3)
        ",
    )
    .bind(dir_prop_row_id)
    .bind(LogActionType::CacheDates)
    .bind(cached_time)
    .execute(&mut *conn)
    .await?;

    // ファイル毎にメタ情報を保存する
    for entry in dir.read_dir()?.flatten() {
        if !entry.file_type()?.is_file() {
            continue;
        }
        let file_name = entry.file_name();
        let file_name = file_name.to_str().ok_or(anyhow!(
            "Unable to convert OsString to &str: {:?}",
            file_name
        ))?;

        if let Ok(metadata) = entry.metadata() {
            let created_time: DateTime<Local> = system_time_to_datetime_local(metadata.created()?);
            let modified_time: DateTime<Local> =
                system_time_to_datetime_local(metadata.modified()?);

            match sqlx::query_as::<_, (u32,)>(
                "
                    SELECT id FROM dir_file_props
                    WHERE dir_id = $1 AND name = $2
                    LIMIT 1
                ",
            )
            .bind(dir_prop_row_id)
            .bind(file_name)
            .fetch_one(&mut *conn)
            .await
            {
                Ok((file_prop_row_id,)) => {
                    sqlx::query(
                        "
                            UPDATE dir_file_props 
                            SET cached_date = $2, created_date = $3, modified_date = $4
                            WHERE id = $1
                        ",
                    )
                    .bind(file_prop_row_id)
                    .bind(cached_time)
                    .bind(created_time)
                    .bind(modified_time)
                    .execute(&mut *conn)
                    .await?;
                }
                Err(sqlx::Error::RowNotFound) => {
                    sqlx::query(
                        "
                            INSERT INTO dir_file_props 
                            (dir_id, name, cached_date, created_date, modified_date) 
                            VALUES ($1, $2, $3, $4, $5)
                        ",
                    )
                    .bind(dir_prop_row_id)
                    .bind(file_name)
                    .bind(cached_time)
                    .bind(created_time)
                    .bind(modified_time)
                    .execute(&mut *conn)
                    .await?;
                }
                Err(e) => return Err(e.into()),
            };
        }
    }

    tx.commit().await?;

    Ok(())
}

async fn get_dir_prop_row_id(pool: &SqlitePool, dir: PathBuf) -> Result<i64> {
    let mut dir_path_str = dir.clone();
    let dir_path_str = dir_path_str
        .as_mut_os_str()
        .to_str()
        .ok_or(anyhow!("Unable to convert PathBuf to &str: {:?}", dir))?;

    let mut tx = pool.begin().await?;
    let conn = tx.acquire().await?;

    // poolのdir_propsテーブルから、dir_path_strに対応するprimary keyを取得する。存在しないなら新たに作成する。
    let dir_prop_row_id: i64 = match sqlx::query_as::<_, (i64,)>(
        "
            SELECT id FROM dir_props
            WHERE path = $1
            LIMIT 1
        ",
    )
    .bind(dir_path_str)
    .fetch_one(&mut *conn)
    .await
    {
        Ok((id,)) => id,
        Err(sqlx::Error::RowNotFound) => {
            let path_inserting_result = sqlx::query(
                "
                    INSERT INTO dir_props 
                    (path) 
                    VALUES ($1)
                ",
            )
            .bind(dir_path_str)
            .execute(&mut *conn)
            .await?;

            path_inserting_result.last_insert_rowid()
        }
        Err(e) => return Err(e.into()),
    };

    tx.commit().await?;

    Ok(dir_prop_row_id)
}

#[derive(Debug, Clone, Copy)]
enum LogActionType {
    CacheDates,
}

impl<'q> sqlx::Encode<'q, sqlx::Sqlite> for LogActionType {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Sqlite as sqlx::Database>::ArgumentBuffer<'q>,
    ) -> std::result::Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        let action_type: u8 = (*self).into();
        buf.push(sqlx::sqlite::SqliteArgumentValue::Int(action_type as _));

        Ok(sqlx::encode::IsNull::No)
    }
}

impl sqlx::Type<sqlx::Sqlite> for LogActionType {
    fn type_info() -> sqlx::sqlite::SqliteTypeInfo {
        u8::type_info()
    }
}

impl From<u8> for LogActionType {
    fn from(value: u8) -> Self {
        match value {
            0 => LogActionType::CacheDates,
            _ => unreachable!(),
        }
    }
}
impl From<LogActionType> for u8 {
    fn from(value: LogActionType) -> Self {
        match value {
            LogActionType::CacheDates => 0,
        }
    }
}

fn system_time_to_datetime_local(system_time: SystemTime) -> DateTime<Local> {
    system_time.into()
}

fn datetime_local_to_system_time(datetime: DateTime<Local>) -> SystemTime {
    datetime.into()
}
