use typedb_driver::{
    concept::{Attribute, Concept, Value},
    Connection, DatabaseManager, Error, Options, Promise, Session, SessionType, TransactionType
};
use serde_json::json;

fn main() {
    let connection = Connection::new_core("localhost:1729")?;
    let databases = DatabaseManager::new(connection);

    databases.create("access-management-db").await?;

    let session = Session::new(databases.get("access-management-db").await?, SessionType::Schema).await?;
    let tx = session.transaction(TransactionType::Write).await?;
    tx.query().define("define subject sub entity;").await?;
    tx.query().define("define subject owns name; name sub attribute, value string;").await?;
    tx.commit().await?;
    drop(session);

    let session = Session::new(databases.get("access-management-db").await?, SessionType::Data).await?;
    let tx = session.transaction(TransactionType::Write).await?;
    tx.query().insert("insert $s isa subject, has name 'Bob';").await?;
    tx.commit().await?;
    
    let tx = session.transaction(TransactionType::Read).await?;
    let mut stream = tx.query().fetch("match $s isa subject; fetch $s: name;")?;
    for item in stream {
        let answer = item.unwrap();
        println!(
            "Name: {}",
            serde_json::to_string_pretty(&answer).unwrap()            
        );
    }
    drop(session);
}
