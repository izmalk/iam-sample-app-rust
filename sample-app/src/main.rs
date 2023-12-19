use typedb_driver::{Connection, DatabaseManager, Session, SessionType, TransactionType, Promise, concept::{Attribute, Concept, Value}, Options};
use std::{fs, process::exit};
use chrono::prelude::*;

fn unwrap_string(concept: Concept) -> String {
    match concept {
        Concept::Attribute(Attribute { value: Value::String(value), .. }) => value,
        _ => unreachable!(),
    }
}

fn unwrap_value_long(value: Value) -> i64 {
    match value {
        Value::Long(value) => value,
        _ => unreachable!(),
    }
}

fn main() {
    const DB_NAME: &str = "iam";
    const SERVER_ADDR: &str = "127.0.0.1:1729";

    println!("IAM Sample App");

    println!("Attempting to connect to a TypeDB Core server: {}", SERVER_ADDR);
    let driver = Connection::new_core(SERVER_ADDR).expect("Connection error."); //Connect tot TypeDB Core server
    let databases = DatabaseManager::new(driver);

    if databases.contains(DB_NAME).expect("Failed to check existence of the database.") {
        println!("Found a pre-existing database! Re-creating with the default schema and data...");
        let _ = databases.get(DB_NAME).expect("Failed to get the database for deletion.").delete();
    }
    let _ = databases.create(DB_NAME);
    if databases.contains(DB_NAME).expect("Failed to check creation of a database.") {
        println!("Empty database created.");
    }
    let db = databases.get(DB_NAME).expect("Failed to retrieve the database to open a session.");
    println!("Opening a Schema session to define a schema.");
    let session = Session::new(db, SessionType::Schema).expect("Failed to open a session.");
        let tx = session.transaction(TransactionType::Write).expect("Failed to open a transaction.");
            let data = fs::read_to_string("iam-schema.tql").expect("Failed to read a schema file.");
            let _ = tx.query().define(&data).resolve();
            let _ = tx.commit().resolve();
        let _ = session.force_close();
    println!("Opening a Data session to insert data.");
    let db = databases.get(DB_NAME).expect("Failed to retrieve the database to open a session.");
    let session = Session::new(db, SessionType::Data).expect("Failed to open a session.");
        let tx = session.transaction(TransactionType::Write).expect("Failed to open a transaction.");
            let data = fs::read_to_string("iam-data-single-query.tql").expect("Failed to read a schema file.");
            let _ = tx.query().insert(&data).expect("Failed to insert sample data.");
            let _ = tx.commit().resolve();
            println!("Testing the new database.");
        let tx2 = session.transaction(TransactionType::Read).expect("Failed to open a transaction."); //Re-using a same session to open a new transaction
            let read_query = "match $u isa user; get $u; count;";
            let count = tx2.query().get_aggregate(&read_query).resolve().expect("Failed to get query results.").unwrap();
            if unwrap_value_long(count) == 3 {
                println!("Database setup complete. Test passed.");
            } else {
                println!("Test failed with the following result: expected result: 3.");
                exit(1)
            }
            tx2.force_close();
        let _ = session.force_close();

    println!("Commencing sample requests.");

    println!();
    println!("Request #1: User listing.");
    let db = databases.get(DB_NAME).expect("Failed to retrieve the database to open a session.");
    let session = Session::new(db, SessionType::Data).expect("Failed to open a session.");
        let tx = session.transaction(TransactionType::Read).expect("Failed to open a transaction.");
            let typeql_read_query = "match $u isa user, has full-name $n, has email $e; get;";
            let iterator = tx.query().get(typeql_read_query).expect("Failed to read data."); //Executing the query
            let mut k = 0; // Counter
            for item in iterator { //Iterating through results
                k += 1;
                let answer = item.unwrap();
                let name = unwrap_string(answer.map.get("n").unwrap().clone());
                let email = unwrap_string(answer.map.get("e").unwrap().clone());
                println!("User #{}: {}, has E-Mail: {}", k.to_string(), name, email)
            }
            println!("Users found: {}", k.to_string());
        let _ = tx.force_close();
        
        println!();
        println!("Request #2: Files that Kevin Morrison has access to");
        let tx = session.transaction(TransactionType::Read).expect("Failed to open a transaction.");
            let typeql_read_query = 
            "match 
            $u isa user, has full-name 'Kevin Morrison'; 
            $p($u, $pa) isa permission; 
            $o isa object, has path $fp; 
            $pa($o, $va) isa access; 
            get $fp;";
            let iterator = tx.query().get(typeql_read_query).expect("Failed to read data.");
            let mut k = 0;
            for item in iterator {
                k += 1;
                let answer = item.unwrap();
                println!("File #{}: {}", k.to_string(), unwrap_string(answer.map.get("fp").unwrap().clone()));
            }
            println!("Files found: {}", k.to_string());
            let _ = tx.force_close();

        println!();
        println!("Request #3: Files that Kevin Morrison has view access to (with inference)");
        let tx = session.transaction_with_options(TransactionType::Read, Options::new().infer(true)).expect("Failed to open a transaction."); //Inference enabled
            let typeql_read_query = 
            "match 
            $u isa user, has full-name 'Kevin Morrison'; 
            $p($u, $pa) isa permission; 
            $o isa object, has path $fp; 
            $pa($o, $va) isa access;
            $va isa action, has name 'view_file'; 
            get $fp; 
            sort $fp asc; 
            offset 0; 
            limit 5;"; //Only the first five results
            let iterator = tx.query().get(typeql_read_query).expect("Failed to read data.");
            let mut k = 0;
            for item in iterator {
                k += 1;
                let answer = item.unwrap();
                println!("File #{}: {}", k.to_string(), unwrap_string(answer.map.get("fp").unwrap().clone()));
            }
            let typeql_read_query = 
            "match 
            $u isa user, has full-name 'Kevin Morrison';
            $p($u, $pa) isa permission;
            $o isa object, has path $fp; 
            $pa($o, $va) isa access;
            $va isa action, has name 'view_file'; 
            get $fp; 
            sort $fp asc; 
            offset 5; 
            limit 5;"; //The next five results
            let iterator = tx.query().get(typeql_read_query).expect("Failed to read data.");
            for item in iterator {
                k += 1;
                let answer = item.unwrap();
                println!("File #{}: {}", k.to_string(), unwrap_string(answer.map.get("fp").unwrap().clone()));
            }
            println!("Files found: {}", k.to_string());
            let _ = tx.force_close();

        println!();
        println!("Request #4: Add a new file and a view access to it");
        let tx = session.transaction(TransactionType::Write).expect("Failed to open a transaction."); //Open a transaction to write
            let filename = format!("{}{}", "logs/", Utc::now().to_string());
            let typeql_insert_query = format!("insert $f isa file, has path '{}';", filename);
            let _query_response: Vec<Result<typedb_driver::answer::ConceptMap, typedb_driver::Error>> = tx.query().insert(&typeql_insert_query).expect("Failed to read data.").collect(); //Inserting file
            println!("Inserted file: {}", filename);
            let typeql_insert_query = format!(
            "match 
            $f isa file, has path '{}';
            $vav isa action, has name 'view_file';
            insert 
            ($vav, $f) isa access;"
            , filename);
            let _query_response: Vec<Result<typedb_driver::answer::ConceptMap, typedb_driver::Error>> = tx.query().insert(&typeql_insert_query).expect("Failed to read data.").collect(); //The second query in the same transaction
            println!("Added view access to the file.");
            let _ = tx.commit().resolve();

}
