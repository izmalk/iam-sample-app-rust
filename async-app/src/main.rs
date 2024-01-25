use typedb_driver::{
    concept::{Attribute, Concept, Value},
    Connection, DatabaseManager, Error, Options, Promise, Session, SessionType, TransactionType
};
use serde_json::json;

fn main() {

test();

}

async fn test() {
    let connection = Connection::new_core("localhost:1729");
    let databases = DatabaseManager::new(connection.unwrap());

    databases.create("access-management-db").await;

    let session = Session::new(databases.get("access-management-db").await.unwrap(), SessionType::Schema).await.unwrap();
    let tx = session.transaction(TransactionType::Write).await.unwrap();
    tx.query().define("define subject sub entity;").await;
    tx.query().define("define subject owns name; name sub attribute, value string;").await;
    tx.commit().await;
    drop(session);

    let session = Session::new(databases.get("access-management-db").await.unwrap(), SessionType::Data).await.unwrap();
    let tx = session.transaction(TransactionType::Write).await.unwrap();
    tx.query().insert("insert $s isa subject, has name 'Bob';");
    tx.commit().await;
    
    let tx = session.transaction(TransactionType::Read).await.unwrap();
    let stream = tx.query().fetch("match $s isa subject; fetch $s: name;").unwrap();
    while let Some(item) = stream.next().await {
        println!(
            "Name: {}",
            serde_json::to_string_pretty(&item.unwrap()).unwrap()            
        );
    }
    drop(session);
    return();

}