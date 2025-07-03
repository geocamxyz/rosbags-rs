//! SQLite3 storage backend implementation

use crate::error::{ReaderError, Result};
use crate::storage::StorageReader;
use crate::types::{Connection, Message, MessageDefinition, MessageDefinitionFormat};
use rusqlite::Connection as SqliteConnection;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// SQLite3 storage reader implementation
pub struct SqliteReader {
    /// Database file paths
    db_paths: Vec<PathBuf>,
    /// Database connections (one per file)
    connections: Vec<SqliteConnection>,
    /// Topic connections from metadata
    topic_connections: Vec<Connection>,
    /// Schema version detected from database
    schema_version: u32,
    /// Message type definitions
    message_definitions: HashMap<String, MessageDefinition>,
    /// Whether the reader is currently open
    is_open: bool,
}

impl SqliteReader {
    /// Create a new SQLite reader
    pub fn new(paths: Vec<&Path>, connections: Vec<Connection>) -> Result<Self> {
        let db_paths = paths.iter().map(|p| p.to_path_buf()).collect();
        Ok(Self {
            db_paths,
            connections: Vec::new(),
            topic_connections: connections,
            schema_version: 0,
            message_definitions: HashMap::new(),
            is_open: false,
        })
    }

    /// Detect the schema version from the database
    fn detect_schema_version(conn: &SqliteConnection) -> Result<u32> {
        // Check if schema table exists
        let mut stmt = conn
            .prepare("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='schema'")?;
        let schema_table_exists: i32 = stmt.query_row([], |row| row.get(0))?;

        if schema_table_exists > 0 {
            // Schema table exists, get version from it
            let mut stmt = conn.prepare("SELECT schema_version FROM schema")?;
            let version: i32 = stmt.query_row([], |row| row.get(0))?;
            Ok(version as u32)
        } else {
            // No schema table, check for offered_qos_profiles column to distinguish v1 vs v2
            let mut stmt = conn.prepare("PRAGMA table_info(topics)")?;
            let rows = stmt.query_map([], |row| {
                let column_name: String = row.get(1)?;
                Ok(column_name)
            })?;

            let mut has_qos_profiles = false;
            for row in rows {
                if row? == "offered_qos_profiles" {
                    has_qos_profiles = true;
                    break;
                }
            }

            Ok(if has_qos_profiles { 2 } else { 1 })
        }
    }

    /// Load message definitions from the database (schema version 4+)
    fn load_message_definitions(
        &self,
        conn: &SqliteConnection,
    ) -> Result<HashMap<String, MessageDefinition>> {
        if self.schema_version < 4 {
            return Ok(HashMap::new()); // No message definitions in older schemas
        }

        let mut stmt = conn.prepare(
            "SELECT topic_type, encoding, encoded_message_definition, type_description_hash
             FROM message_definitions ORDER BY id",
        )?;

        let rows = stmt.query_map([], |row| {
            let topic_type: String = row.get(0)?;
            let encoding: String = row.get(1)?;
            let definition: String = row.get(2)?;
            let _hash: String = row.get(3)?;
            Ok((topic_type, encoding, definition))
        })?;

        let mut definitions = HashMap::new();
        for row in rows {
            let (topic_type, encoding, definition) = row?;

            let format = match encoding.as_str() {
                "ros2msg" => MessageDefinitionFormat::Msg,
                "ros2idl" => MessageDefinitionFormat::Idl,
                _ => MessageDefinitionFormat::None,
            };

            definitions.insert(
                topic_type,
                MessageDefinition {
                    format,
                    data: definition,
                },
            );
        }

        Ok(definitions)
    }

    /// Build a query for messages with optional filters
    fn build_message_query(
        &self,
        connections: Option<&[Connection]>,
        start: Option<u64>,
        stop: Option<u64>,
    ) -> (String, Vec<Box<dyn rusqlite::ToSql>>) {
        let mut query = String::from(
            "SELECT topics.id, messages.timestamp, messages.data
             FROM messages JOIN topics ON messages.topic_id = topics.id",
        );
        let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        let mut conditions = Vec::new();

        // Filter by connections (topics)
        if let Some(conns) = connections {
            if !conns.is_empty() {
                let topic_names: Vec<&str> = conns.iter().map(|c| c.topic.as_str()).collect();
                let placeholders = topic_names
                    .iter()
                    .map(|_| "?")
                    .collect::<Vec<_>>()
                    .join(",");
                conditions.push(format!("topics.name IN ({placeholders})"));
                for topic in topic_names {
                    params.push(Box::new(topic.to_string()));
                }
            }
        }

        // Filter by start time
        if let Some(start_time) = start {
            conditions.push("messages.timestamp >= ?".to_string());
            params.push(Box::new(start_time as i64));
        }

        // Filter by stop time
        if let Some(stop_time) = stop {
            conditions.push("messages.timestamp < ?".to_string());
            params.push(Box::new(stop_time as i64));
        }

        // Add WHERE clause if we have conditions
        if !conditions.is_empty() {
            query.push_str(" WHERE ");
            query.push_str(&conditions.join(" AND "));
        }

        // Order by timestamp
        query.push_str(" ORDER BY messages.timestamp");

        (query, params)
    }
}

impl StorageReader for SqliteReader {
    fn open(&mut self) -> Result<()> {
        if self.is_open {
            return Ok(());
        }

        // Open database connections
        for path in &self.db_paths {
            let conn = SqliteConnection::open_with_flags(
                path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )?;

            // Verify the database has required tables
            {
                let mut stmt = conn.prepare(
                    "SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('messages', 'topics')"
                )?;
                let table_count: i32 = stmt.query_row([], |row| row.get(0))?;

                if table_count != 2 {
                    return Err(ReaderError::generic(format!(
                        "Database {} is missing required tables",
                        path.display()
                    )));
                }
            }

            self.connections.push(conn);
        }

        // Detect schema version and load message definitions from the last database
        if !self.connections.is_empty() {
            let last_conn_idx = self.connections.len() - 1;
            let schema_version = Self::detect_schema_version(&self.connections[last_conn_idx])?;
            self.schema_version = schema_version;

            // Load message definitions
            let definitions = self.load_message_definitions(&self.connections[last_conn_idx])?;
            self.message_definitions = definitions;
        }

        self.is_open = true;
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        if !self.is_open {
            return Ok(());
        }

        self.connections.clear();
        self.message_definitions.clear();
        self.is_open = false;
        Ok(())
    }

    fn get_definitions(&self) -> Result<HashMap<String, MessageDefinition>> {
        if !self.is_open {
            return Err(ReaderError::BagNotOpen);
        }
        Ok(self.message_definitions.clone())
    }

    fn messages(
        &self,
        connections: Option<&[Connection]>,
        start: Option<u64>,
        stop: Option<u64>,
    ) -> Result<Box<dyn Iterator<Item = Result<Message>> + '_>> {
        if !self.is_open {
            return Err(ReaderError::BagNotOpen);
        }

        // Collect all messages from all database connections
        let mut all_messages = Vec::new();

        for db_conn in &self.connections {
            // Build the SQL query with filters
            let (query, params) = self.build_message_query(connections, start, stop);

            // Get topic name to connection mapping for this database
            let mut topic_map = HashMap::new();
            let mut stmt = db_conn.prepare("SELECT id, name FROM topics")?;
            let topic_rows = stmt.query_map([], |row| {
                let id: i32 = row.get(0)?;
                let name: String = row.get(1)?;
                Ok((id, name))
            })?;

            for row in topic_rows {
                let (topic_id, topic_name) = row?;
                // Find the connection for this topic
                if let Some(conn) = self
                    .topic_connections
                    .iter()
                    .find(|c| c.topic == topic_name)
                {
                    topic_map.insert(topic_id, conn.clone());
                }
            }

            // Execute the message query
            let mut stmt = db_conn.prepare(&query)?;
            let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| p.as_ref()).collect();
            let message_rows = stmt.query_map(param_refs.as_slice(), |row| {
                let topic_id: i32 = row.get(0)?;
                let timestamp: i64 = row.get(1)?;
                let data: Vec<u8> = row.get(2)?;
                Ok((topic_id, timestamp as u64, data))
            })?;

            // Convert database rows to Message objects
            for row in message_rows {
                let (topic_id, timestamp, data) = row?;

                if let Some(connection) = topic_map.get(&topic_id) {
                    let message = Message {
                        connection: connection.clone(),
                        topic: connection.topic.clone(),
                        timestamp,
                        data,
                    };
                    all_messages.push(Ok(message));
                }
            }
        }

        // Sort messages by timestamp
        all_messages.sort_by(|a, b| match (a, b) {
            (Ok(msg_a), Ok(msg_b)) => msg_a.timestamp.cmp(&msg_b.timestamp),
            _ => std::cmp::Ordering::Equal,
        });

        Ok(Box::new(all_messages.into_iter()))
    }

    fn is_open(&self) -> bool {
        self.is_open
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl SqliteReader {
    /// Get all topics and their message counts directly from the database
    pub fn get_topics_from_database(&self) -> Result<Vec<Connection>> {
        if self.connections.is_empty() {
            return Ok(Vec::new());
        }

        let mut all_connections = Vec::new();

        for db_conn in &self.connections {
            // Get topics from this database
            let mut stmt = db_conn.prepare(
                "SELECT id, name, type, serialization_format, offered_qos_profiles FROM topics ORDER BY id"
            )?;

            let topic_rows = stmt.query_map([], |row| {
                let id: i32 = row.get(0)?;
                let name: String = row.get(1)?;
                let message_type: String = row.get(2)?;
                let serialization_format: String = row.get(3)?;
                let offered_qos_profiles: String = row.get(4)?;
                Ok((
                    id,
                    name,
                    message_type,
                    serialization_format,
                    offered_qos_profiles,
                ))
            })?;

            for topic_result in topic_rows {
                let (topic_id, name, message_type, serialization_format, _qos_profiles) =
                    topic_result?;

                // Get message count for this topic
                let mut count_stmt =
                    db_conn.prepare("SELECT COUNT(*) FROM messages WHERE topic_id = ?")?;
                let message_count: u64 = count_stmt.query_row([topic_id], |row| {
                    let count: i64 = row.get(0)?;
                    Ok(count as u64)
                })?;

                // Create connection
                let connection = Connection {
                    id: (all_connections.len() + 1) as u32,
                    topic: name,
                    message_type,
                    message_definition: MessageDefinition::default(),
                    type_description_hash: String::new(),
                    message_count,
                    serialization_format,
                    offered_qos_profiles: Vec::new(),
                };

                all_connections.push(connection);
            }
        }

        Ok(all_connections)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqlite_reader_creation() {
        let reader = SqliteReader::new(vec![], vec![]);
        assert!(reader.is_ok());
        let reader = reader.unwrap();
        assert!(!reader.is_open());
    }

    #[test]
    fn test_sqlite_reader_open_close() {
        let mut reader = SqliteReader::new(vec![], vec![]).unwrap();
        assert!(!reader.is_open());

        reader.open().unwrap();
        assert!(reader.is_open());

        reader.close().unwrap();
        assert!(!reader.is_open());
    }
}

/// SQLite storage writer implementation
#[cfg(feature = "sqlite")]
pub struct SqliteWriter {
    /// Path to the database file
    db_path: PathBuf,
    /// Database connection
    connection: Option<SqliteConnection>,
    /// Whether compression is enabled (reserved for future use)
    _compression_mode: crate::types::CompressionMode,
    /// Whether the writer is currently open
    is_open: bool,
    /// Connection ID mapping: topic -> database topic_id
    topic_id_map: HashMap<String, i32>,
}

#[cfg(feature = "sqlite")]
impl SqliteWriter {
    /// Create a new SQLite writer
    pub fn new(path: &Path, compression_mode: crate::types::CompressionMode) -> Result<Self> {
        // SQLite3 doesn't support storage-level compression
        if compression_mode == crate::types::CompressionMode::Storage {
            return Err(crate::error::BagError::writer(
                "SQLite3 writer does not support storage-side compression",
            ));
        }

        let db_path = path.join(format!(
            "{}.db3",
            path.file_name().unwrap().to_string_lossy()
        ));

        Ok(Self {
            db_path,
            connection: None,
            _compression_mode: compression_mode,
            is_open: false,
            topic_id_map: HashMap::new(),
        })
    }

    /// Create the database schema
    fn create_schema(&self) -> Result<()> {
        let conn = self.connection.as_ref().unwrap();

        // ROS2 SQLite schema version 4
        let schema = r#"
            CREATE TABLE schema(
                schema_version INTEGER PRIMARY KEY,
                ros_distro TEXT NOT NULL
            );
            CREATE TABLE metadata(
                id INTEGER PRIMARY KEY,
                metadata_version INTEGER NOT NULL,
                metadata TEXT NOT NULL
            );
            CREATE TABLE topics(
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                serialization_format TEXT NOT NULL,
                offered_qos_profiles TEXT NOT NULL,
                type_description_hash TEXT NOT NULL
            );
            CREATE TABLE message_definitions(
                id INTEGER PRIMARY KEY,
                topic_type TEXT NOT NULL,
                encoding TEXT NOT NULL,
                encoded_message_definition TEXT NOT NULL,
                type_description_hash TEXT NOT NULL
            );
            CREATE TABLE messages(
                id INTEGER PRIMARY KEY,
                topic_id INTEGER NOT NULL,
                timestamp INTEGER NOT NULL,
                data BLOB NOT NULL
            );
            CREATE INDEX timestamp_idx ON messages (timestamp ASC);
            INSERT INTO schema(schema_version, ros_distro) VALUES (4, 'rosbags');
        "#;

        conn.execute_batch(schema)?;
        Ok(())
    }
}

#[cfg(feature = "sqlite")]
impl crate::storage::StorageWriter for SqliteWriter {
    fn open(&mut self) -> Result<()> {
        if self.is_open {
            return Err(crate::error::BagError::BagAlreadyOpen);
        }

        // Create the database file
        let connection = SqliteConnection::open(&self.db_path)?;
        self.connection = Some(connection);

        // Create the schema
        self.create_schema()?;

        self.is_open = true;
        Ok(())
    }

    fn close(&mut self, version: u32, metadata: &str) -> Result<()> {
        if !self.is_open {
            return Ok(());
        }

        // Write metadata to the database
        if let Some(conn) = &self.connection {
            conn.execute(
                "INSERT INTO metadata(metadata_version, metadata) VALUES (?1, ?2)",
                (version, metadata),
            )?;
        }

        // Close the database connection
        self.connection = None;
        self.is_open = false;
        self.topic_id_map.clear();

        Ok(())
    }

    fn add_msgtype(&mut self, connection: &Connection) -> Result<()> {
        if !self.is_open {
            return Err(crate::error::BagError::BagNotOpen);
        }

        let conn = self.connection.as_ref().unwrap();

        // Determine encoding based on format
        let encoding = match connection.message_definition.format {
            MessageDefinitionFormat::Msg => "ros2msg",
            MessageDefinitionFormat::Idl => "ros2idl",
            MessageDefinitionFormat::None => "ros2msg", // Default fallback
        };

        // Insert into message_definitions table
        conn.execute(
            "INSERT INTO message_definitions(topic_type, encoding, encoded_message_definition, type_description_hash) VALUES (?1, ?2, ?3, ?4)",
            (
                &connection.message_type,
                encoding,
                &connection.message_definition.data,
                &connection.type_description_hash,
            ),
        )?;

        Ok(())
    }

    fn add_connection(
        &mut self,
        connection: &Connection,
        offered_qos_profiles: &str,
    ) -> Result<()> {
        if !self.is_open {
            return Err(crate::error::BagError::BagNotOpen);
        }

        let conn = self.connection.as_ref().unwrap();

        // Insert topic into topics table
        conn.execute(
            "INSERT INTO topics(name, type, serialization_format, offered_qos_profiles, type_description_hash) VALUES (?1, ?2, ?3, ?4, ?5)",
            (
                &connection.topic,
                &connection.message_type,
                &connection.serialization_format,
                offered_qos_profiles,
                &connection.type_description_hash,
            ),
        )?;

        // Get the ID of the inserted topic
        let topic_id = conn.last_insert_rowid() as i32;
        self.topic_id_map.insert(connection.topic.clone(), topic_id);

        Ok(())
    }

    fn write(&mut self, connection: &Connection, timestamp: u64, data: &[u8]) -> Result<()> {
        if !self.is_open {
            return Err(crate::error::BagError::BagNotOpen);
        }

        let topic_id = self
            .topic_id_map
            .get(&connection.topic)
            .ok_or_else(|| crate::error::BagError::connection_not_found(&connection.topic))?;

        let conn = self.connection.as_ref().unwrap();

        // Insert message into messages table
        conn.execute(
            "INSERT INTO messages(topic_id, timestamp, data) VALUES (?1, ?2, ?3)",
            (topic_id, timestamp as i64, data),
        )?;

        Ok(())
    }

    fn write_batch(&mut self, messages: &[(Connection, u64, Vec<u8>)]) -> Result<()> {
        if !self.is_open {
            return Err(crate::error::BagError::BagNotOpen);
        }

        if messages.is_empty() {
            return Ok(());
        }

        // Take the connection temporarily to avoid borrowing issues
        let mut conn = self.connection.take().unwrap();
        
        // Start transaction for batch insert
        let tx = conn.transaction()?;

        {
            let mut stmt = tx.prepare(
                "INSERT INTO messages(topic_id, timestamp, data) VALUES (?1, ?2, ?3)"
            )?;

            for (connection, timestamp, data) in messages {
                let topic_id = self
                    .topic_id_map
                    .get(&connection.topic)
                    .ok_or_else(|| crate::error::BagError::connection_not_found(&connection.topic))?;

                stmt.execute((topic_id, *timestamp as i64, data))?;
            }
        }

        // Commit the transaction
        tx.commit()?;

        // Put the connection back
        self.connection = Some(conn);

        Ok(())
    }

    fn is_open(&self) -> bool {
        self.is_open
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
