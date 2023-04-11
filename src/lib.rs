use std::{
    path::Path,
    thread,
};

use anyhow::Result;
use sqlx::{
    migrate::Migrator, postgres::PgPoolOptions, Connection, Executor, PgConnection, PgPool,
};
use uuid::Uuid;

struct TestDb {
    server_url: String,
    db_name: String,
}

impl TestDb {
    pub fn new(server_url: impl Into<String>, migration_path: impl AsRef<Path>) -> Result<Self> {
        let server_url = server_url.into();
        let db_name = format!("test_db_{}", Uuid::new_v4().to_string().replace('-', "_"));

        let manage_url = format!("{}/postgres", server_url);
        let db_url = format!("{}/{}", server_url, db_name);
        let migration_path = migration_path.as_ref().to_path_buf();
        let db_name_clone = db_name.clone();
        thread::spawn(move || {
            futures::executor::block_on(async move {
                let mut conn = PgConnection::connect(&manage_url).await.unwrap();
                conn.execute(format!(r#"CREATE DATABASE "{}""#, db_name_clone).as_str())
                    .await
                    .unwrap();
                let mut conn = PgConnection::connect(&db_url).await.unwrap();
                let migrator = Migrator::new(migration_path.as_ref()).await.unwrap();
                migrator.run(&mut conn).await.unwrap();
            })
        })
        .join()
        .unwrap();

        Ok(Self {
            server_url,
            db_name,
        })
    }

    fn url(&self) -> String {
        format!("{}/{}", self.server_url, self.db_name)
    }

    pub async fn get_pool(&self) -> PgPool {
        PgPoolOptions::new()
            .max_connections(5)
            .connect(&self.url())
            .await
            .unwrap()
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        let manage_url = format!("{}/postgres", self.server_url);
        let db_name = self.db_name.clone();
        thread::spawn(move || {
                futures::executor::block_on(async move {
                    let mut conn = PgConnection::connect(&manage_url).await.unwrap();
                    conn.execute(format!(r#"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND datname = '{}'"#, db_name).as_str())
                        .await
                        .unwrap();
                    conn.execute(format!(r#"DROP DATABASE "{}""#, db_name).as_str())
                        .await
                        .unwrap();
                })
            })
            .join()
            .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[futures_test::test]
    async fn test_db_migration_should_work() {
        let testdb = TestDb::new("postgres://brook@localhost:5432", "./migrations").unwrap();

        let pool = testdb.get_pool().await;
        sqlx::query("insert into todos(title) values($1)")
            .bind("test")
            .execute(&pool)
            .await
            .unwrap();

        let (id, title) = sqlx::query_as::<_, (i32, String)>("select id, title from todos")
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(id, 1);
        assert_eq!(title, "test");
    }
}
