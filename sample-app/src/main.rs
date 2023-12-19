use typedb_driver::{Connection, DatabaseManager, Session, SessionType, TransactionType, Credential, Promise};

fn main() {
    println!("IAM Sample App");
    println!("Connecting to the server");

    let connection = Connection::new_core("127.0.0.1:1729");
    
    //let connection = Connection::new_enterprise(&["127.0.0.1:1729"],Credential::with_tls("admin", "password", None).unwrap());
    let driver = match connection {
        Ok(connection) => connection,
        Err(_error)=> panic!("Connection error!"),
    };
    
    let databases = DatabaseManager::new(driver);

    if databases.contains("iam").unwrap() {println!("Database iam exist!")}

    println!("Testing database management");
    // create database
    let create_res = databases.create("test-db");
    match create_res {
        Ok(_t) => println!("Test-db created!"),
        Err(_error)=> panic!("Error creating test-db!"),
    };     
    // get database schema
    let schema = databases.get("test-db").unwrap().schema().unwrap();
    println!("{}{}", "Database schema:", schema);
    // get all databases
    println!("List databases:");
    for db in databases.all().unwrap() {
        println!("{}", db.name());
    };
    // check if database exists
    if databases.contains("test-db").unwrap() {println!("Test-db exists!")}
    // delete database
    let del_res = databases.get("test-db").unwrap().delete();
    match del_res {
        Ok(_t) => println!("Test complete, test-db deleted!"),
        Err(_error)=> panic!("Database management test error!"),
    };    

    let session_result = Session::new(databases.get("iam").unwrap(), SessionType::Data);

    let session = match session_result {
        Ok(s) => s,
        Err(_e)=> panic!("Session error!"),
    };


    println!("Read transaction test!");
    let tx_result = session.transaction(TransactionType::Read);

    let tx = match tx_result {
        Ok(t) => t,
        Err(_e)=> panic!("Transaction error!"),
    };

    let query_response = tx.query().fetch("match $p isa person; fetch $p: attribute;");

    let response = match query_response {
        Ok(t) => t,
        Err(_e)=> panic!("Transaction error!"),
    };

    for val in response {
        let result = val.unwrap();
        println!("Got: {}", result);
    }

    tx.force_close();

    println!("Write transaction test!");

    let tx2_result = session.transaction(TransactionType::Write);

    let tx2 = match tx2_result {
        Ok(t) => t,
        Err(_e)=> panic!("Transaction error!"),
    };

    let _query_response2: Vec<Result<typedb_driver::answer::ConceptMap, typedb_driver::Error>> = tx2.query().insert("insert $f isa file, has path \"mod.rs\";").unwrap().collect();

    let _ = tx2.commit().resolve();
}
