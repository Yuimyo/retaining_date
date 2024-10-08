use anyhow::{bail, Result};
use chrono::{DateTime, TimeZone, Utc};
use clap::{arg, Parser, Subcommand};
use sqlx::{Acquire, SqlitePool};
use std::{env::args, path::PathBuf, time::SystemTime};

#[derive(Parser)]
enum Commands {
    Apply { path: String },
    Save { path: String },
}

#[tokio::main]
async fn main() -> Result<()> {
    let pool = SqlitePool::connect("file:data.db").await.unwrap();

    match Commands::parse() {
        Commands::Apply { path } => apply_dir_props(&pool, path.into()).await,
        Commands::Save { path } => save_dir_props(&pool, path.into()).await,
    }
}

async fn apply_dir_props(pool: &SqlitePool, dir: PathBuf) -> Result<()> {
    if !dir.exists() {
        bail!("Doesn't exist dir: {:?}", dir);
    }
    let dir_prop_row_id: i64 = get_dir_prop_row_id(pool, dir.clone()).await?;

    let mut tx = pool.begin().await?;
    let conn = tx.acquire().await?;

    let cached_time: u64 = match sqlx::query_as::<_, (String,)>(
        "
            SELECT cached_date FROM dir_actions_log
            WHERE dir_id = $1 AND action_type = 0
            ORDER BY cached_date DESC
            LIMIT 1
        ",
    )
    .bind(dir_prop_row_id)
    .fetch_one(&mut *conn)
    .await
    {
        Err(sqlx::Error::RowNotFound) => {
            return Ok(());
        }
        Err(e) => return Err(e.into()),
        Ok((cached_date,)) => cached_date.parse::<u64>()?,
    };

    let files_props: Vec<_> = match sqlx::query_as::<_, (String, String, String)>(
        r#"
        SELECT name, created_date, modified_date FROM dir_file_props
        WHERE dir_id = $1 AND cached_date = $2
    "#,
    )
    .bind(dir_prop_row_id)
    .bind(format!("{}", cached_time))
    .fetch_all(&mut *conn)
    .await
    {
        Err(sqlx::Error::RowNotFound) => return Ok(()),
        Err(e) => return Err(e.into()),
        Ok(files_props) => files_props,
    };

    for (name, created_date, modified_date) in files_props {
        let modified_date: u64 = modified_date.parse::<u64>()?;

        let file_path = dir.clone().join(name);
        if !file_path.exists() || !file_path.is_file() {
            continue;
        }

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(file_path)?;
        file.set_modified(unix_to_system_time(modified_date))?;
    }

    tx.commit().await?;

    Ok(())
}

async fn save_dir_props(pool: &SqlitePool, dir: PathBuf) -> Result<()> {
    if !dir.exists() {
        bail!("Doesn't exist dir: {:?}", dir);
    }
    let dir_prop_row_id: i64 = get_dir_prop_row_id(pool, dir.clone()).await?;
    let cached_time: u64 = Utc::now().timestamp() as u64;

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
    .bind(0)
    .bind(format!("{}", cached_time))
    .execute(&mut *conn)
    .await?;

    // ファイル毎にメタ情報を保存する
    for entry in dir.read_dir()?.flatten() {
        if !entry.file_type()?.is_file() {
            continue;
        }
        let entry_file_name = entry.file_name();
        let entry_file_name = entry_file_name.to_str().unwrap();

        if let Ok(metadata) = entry.metadata() {
            let created_time = system_time_to_unix(metadata.created()?)?;
            let modified_time = system_time_to_unix(metadata.modified()?)?;
            match sqlx::query_as::<_, (u32,)>(
                r#"
                SELECT id FROM dir_file_props
                WHERE dir_id = $1 AND name = $2
                LIMIT 1
            "#,
            )
            .bind(dir_prop_row_id)
            .bind(entry_file_name)
            .fetch_one(&mut *conn)
            .await
            {
                Err(sqlx::Error::RowNotFound) => {
                    sqlx::query(
                        r#"
                        INSERT INTO dir_file_props 
                        (dir_id, name, cached_date, created_date, modified_date) 
                        VALUES ($1, $2, $3, $4, $5)
                    "#,
                    )
                    .bind(dir_prop_row_id)
                    .bind(entry_file_name)
                    .bind(format!("{}", cached_time))
                    .bind(format!("{}", created_time))
                    .bind(format!("{}", modified_time))
                    .execute(&mut *conn)
                    .await?;
                }
                Err(e) => return Err(e.into()),
                Ok((file_prop_row_id,)) => {
                    sqlx::query(
                        r#"
                        UPDATE dir_file_props 
                        SET cached_date = $2, created_date = $3, modified_date = $4
                        WHERE id = $1
                    "#,
                    )
                    .bind(file_prop_row_id)
                    .bind(format!("{}", cached_time))
                    .bind(format!("{}", created_time))
                    .bind(format!("{}", modified_time))
                    .execute(&mut *conn)
                    .await?;
                }
            };
        }
    }

    tx.commit().await?;

    Ok(())
}

async fn get_dir_prop_row_id(pool: &SqlitePool, dir: PathBuf) -> Result<i64> {
    let mut dir_path_str = dir.clone();
    let dir_path_str = dir_path_str.as_mut_os_str().to_str().unwrap();

    let mut tx = pool.begin().await?;
    let conn = tx.acquire().await?;

    // poolのdir_propsテーブルから、dir_path_strに対応するprimary keyを取得する。存在しないなら新たに作成する。
    let dir_prop_row_id: i64 = match sqlx::query_as::<_, (i64,)>(
        r#"
        SELECT id FROM dir_props
        WHERE path = $1
        LIMIT 1
    "#,
    )
    .bind(dir_path_str)
    .fetch_one(&mut *conn)
    .await
    {
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
        Ok((id,)) => id,
    };

    tx.commit().await?;

    Ok(dir_prop_row_id)
}

fn system_time_to_unix(system_time: SystemTime) -> Result<u64> {
    Ok(system_time.duration_since(std::time::UNIX_EPOCH)?.as_secs())
}

fn unix_to_system_time(unix_time: u64) -> SystemTime {
    std::time::UNIX_EPOCH + std::time::Duration::from_secs(unix_time)
}

fn unix_to_date_time(unix_time: u64) -> Result<DateTime<Utc>> {
    match Utc.timestamp_opt(unix_time as i64, 0).single() {
        Some(date_time) => Ok(date_time),
        None => bail!("invalid timestamp"),
    }
}
