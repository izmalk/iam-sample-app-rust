use std::error::Error;
use typedb_driver::{
    answer::{ConceptMap, JSON}, concept::{Attribute, Concept, Value}, Connection, DatabaseManager, Error as TypeDBError, Options, Session, SessionType, TransactionType
};
//use serde_json::json;
mod setup;

fn fetch_all_users(driver: Connection, db_name:String) -> Result<Vec<JSON>, Box<dyn Error>> {
    let databases = DatabaseManager::new(driver);
    let session = Session::new(databases.get(db_name)?, SessionType::Data)?;
    let tx = session.transaction(TransactionType::Read)?;
    let iterator = tx.query().fetch("match $u isa user; fetch $u: full-name, email;")?;
    let mut count = 0;
    let mut result = vec![];
    for item in iterator {
        count += 1;
        let json = item?;
        println!(
            "User #{}: {}",
            count.to_string(),
            json.to_string()
        );
        result.push(json);
    }
    if result.len() > 0 {
        Ok(result)
    }
    else {
        Err(Box::new(TypeDBError::Other("Error: No users found in a database.".to_string())))
    }
}

fn insert_new_user(driver: Connection, db_name:String, new_name:&str, new_email:&str) -> Result<Vec<ConceptMap>, Box<dyn Error>> {
    let databases = DatabaseManager::new(driver);
    let session = Session::new(databases.get(db_name)?, SessionType::Data)?;
    let tx = session.transaction(TransactionType::Write)?;
    let iterator = tx.query().insert(&format!("insert $p isa person, has full-name $fn, has email $e; $fn == '{}'; $e == '{}';", new_name, new_email))?;
    let mut count = 0;
    let mut result = vec![];
    for item in iterator {
        count += 1;
        let concept_map = item?;
        let name = unwrap_string(concept_map.get("fn").unwrap().clone());
        let email = unwrap_string(concept_map.get("e").unwrap().clone());
        println!(
            "Added new user. Name: {}, E-mail: {}",
            name,
            email
        );
        result.push(concept_map);
    }
    if result.len() > 0 {
        Ok(result)
    }
    else {
        Err(Box::new(TypeDBError::Other("Error: No users found in a database.".to_string())))
    }
}

fn get_files_by_user(driver: Connection, db_name:String, name:&str, inference:bool) -> Result<Vec<(usize, ConceptMap)>, Box<dyn Error>> {
    let databases = DatabaseManager::new(driver);
    let session = Session::new(databases.get(db_name)?, SessionType::Data)?;
    let tx = session.transaction_with_options(TransactionType::Read, Options::new().infer(inference))?;
    //let users = tx.query().get(&format!("match $u isa user, has full-name '{}'; get;", name))?.map(|x| x.unwrap().get("_0").unwrap().clone()).collect::<Vec<_>>();
    let users = tx.query().get(&format!("match $u isa user, has full-name '{}'; get;", name))?.map(|x| x.unwrap()).collect::<Vec<_>>();
    let mut response;
    if users.len() > 1 {
        return Err(Box::new(TypeDBError::Other("Found more than one user with that name.".to_string())));
    }
    else if users.len() == 1 {
        response = tx.query().get(&format!("match
                                                $fn == '{}';
                                                $u isa user, has full-name $fn;
                                                $p($u, $pa) isa permission;
                                                $o isa object, has path $fp;
                                                $pa($o, $va) isa access;
                                                $va isa action, has name 'view_file';
                                                get $fp; sort $fp asc;
                                                ", name))?.map(|x| x.unwrap()).enumerate().collect::<Vec<_>>();
    }
    else {
        println!("Warning: No users found with that name. Extending search for full-names containing the provided search string.");
        response = tx.query().get(&format!("match
                                                $fn contains '{}';
                                                $u isa user, has full-name $fn;
                                                $p($u, $pa) isa permission;
                                                $o isa object, has path $fp;
                                                $pa($o, $va) isa access;
                                                $va isa action, has name 'view_file';
                                                get $fp; sort $fp asc;
                                                ", name))?.map(|x| x.unwrap()).enumerate().collect::<Vec<_>>();
    }
    if response.len() == 0 {
        println!("No files found. Try enabling inference.");
    }
    for (count, file) in &response {
        println!("File #{}: {}", count + 1, unwrap_string(file.get("fp").unwrap().clone()));
    };
    return Ok(response);
}


fn queries(driver:Connection, db_name:String) -> Result<(), Box<dyn Error>> {
    println!("Request 1 of 6: Fetch all users as JSON objects with full names and emails");
    let users = fetch_all_users(driver.clone(), db_name.clone());
    assert!(users?.len() == 3);

    let new_name = "Jack Keeper";
    let new_email = "jk@vaticle.com";
    println!("Request 2 of 6: Add a new user with the full-name {} and email {}", new_name, new_email);
    insert_new_user(driver.clone(), db_name.clone(), new_name, new_email);

    let infer = false;
    let name = "Kevin Morrison";
    println!("Request 3 of 6: Find all files that the user {} has access to view (no inference)", name);
    let no_files = get_files_by_user(driver.clone(), db_name.clone(), name, infer);
    assert!(no_files?.len() == 0);
    
    let infer = true;
    println!("Request 4 of 6: Find all files that the user {} has access to view (with inference)", name);
    let files = get_files_by_user(driver.clone(), db_name.clone(), name, infer);
    //assert!(files?.len() == 10);

    // let old_path = "lzfkn.java";
    // let new_path = "lzfkn2.java";
    // print("Request 5 of 6: Update the path of a file from {} to {}", old_path, new_path);
    // updated_files = update_filepath(driver, old_path, new_path);

    // let path = "lzfkn2.java";
    // print("Request 6 of 6: Delete the file with path {}", path);
    // deleted = delete_file(driver, path);
    // assert!(deleted);
    
    match files {
        Ok(_) => return Ok(()),
        Err(_) => return Err(Box::new(TypeDBError::Other("Application terminated unexpectedly".to_string()))),
    };  

}


fn main() -> Result<(), Box<dyn Error>> {
    let DB_NAME: String = "sample_app_db".to_string();
    let SERVER_ADDR = "127.0.0.1:1729";
    println!("Sample App");
    let driver = Connection::new_core(SERVER_ADDR)?;
    let setup = match setup::db_setup(driver.clone(), DB_NAME.clone()) {
        Ok(()) => queries(driver, DB_NAME),
        Err(_) => return Err(Box::new(TypeDBError::Other("DB setup failed.".to_string()))),
    };  
    let result = match setup {
        Ok(_) => Ok(()),
        Err(x) => Err(x),
    };

    return result;

    // const DB_NAME: &str = "iam";
    // const SERVER_ADDR: &str = "127.0.0.1:1729";

    // println!("IAM Sample App");

    // println!(
    //     "Attempting to connect to a TypeDB Core server: {}",
    //     SERVER_ADDR
    // );
    // let driver = Connection::new_core(SERVER_ADDR)?; //Connect to TypeDB Core server
    // let databases = DatabaseManager::new(driver);

    // if databases.contains(DB_NAME)? {
    //     println!("Found a pre-existing database! Re-creating with the default schema and data...");
    //     databases.get(DB_NAME)?.delete()?;
    // }
    // databases.create(DB_NAME)?;
    // if databases.contains(DB_NAME)? {
    //     println!("Empty database created.");
    // }
    // {
    //     println!("Opening a Schema session to define a schema.");
    //     let db = databases.get(DB_NAME)?;
    //     let session = Session::new(db, SessionType::Schema)?;
    //     let tx = session.transaction(TransactionType::Write)?;
    //     let data = fs::read_to_string("iam-schema.tql")?;
    //     tx.query().define(&data).resolve()?;
    //     tx.commit().resolve()?;
    // }
    // {
    //     println!("Opening a Data session to insert data.");
    //     let db = databases.get(DB_NAME)?;
    //     let session = Session::new(db, SessionType::Data)?;
    //     {
    //         let tx = session.transaction(TransactionType::Write)?;
    //         let data = fs::read_to_string("iam-data-single-query.tql")?;
    //         let _ = tx.query().insert(&data)?;
    //         tx.commit().resolve()?;
    //     }
    //     {
    //         println!("Testing the new database.");
    //         let tx = session.transaction(TransactionType::Read)?; //Re-using a same session to open a new transaction
    //         let read_query = "match $u isa user; get $u; count;";
    //         let count = tx.query().get_aggregate(&read_query).resolve()?.unwrap();
    //         if unwrap_value_long(count.clone()) == 3 {
    //             println!("Database setup complete. Test passed.");
    //         } else {
    //             println!(
    //                 "Test failed with the following result: {} expected result: 3.",
    //                 unwrap_value_long(count).to_string()
    //             );
    //             exit(1)
    //         }
    //     }
    // }

    // println!("Commencing sample requests.");
    // {
    //     let db = databases.get(DB_NAME)?;
    //     let session = Session::new(db, SessionType::Data)?;

    //     println!();
    //     println!("Request #1: User listing.");
    //     {
    //         let tx = session.transaction(TransactionType::Read)?;
    //         let typeql_read_query = "match $u isa user, has full-name $n, has email $e; get;";
    //         let iterator = tx.query().get(typeql_read_query)?; //Executing the query
    //         let mut k = 0; // Counter
    //         for item in iterator {
    //             //Iterating through results
    //             k += 1;
    //             let answer = item.unwrap();
    //             let name = unwrap_string(answer.map.get("n").unwrap().clone());
    //             let email = unwrap_string(answer.map.get("e").unwrap().clone());
    //             println!("User #{}: {}, has E-Mail: {}", k.to_string(), name, email)
    //         }
    //         println!("Users found: {}", k.to_string());
    //     }

    //     println!();
    //     println!("Request #2: Files that Kevin Morrison has access to");
    //     {
    //         let tx = session.transaction(TransactionType::Read)?;
    //         let typeql_read_query = "match 
    //         $u isa user, has full-name 'Kevin Morrison'; 
    //         $p($u, $pa) isa permission; 
    //         $o isa object, has path $fp; 
    //         $pa($o, $va) isa access; 
    //         get $fp;";
    //         let iterator = tx.query().get(typeql_read_query)?;
    //         let mut k = 0;
    //         for item in iterator {
    //             k += 1;
    //             let answer = item.unwrap();
    //             println!(
    //                 "File #{}: {}",
    //                 k.to_string(),
    //                 unwrap_string(answer.map.get("fp").unwrap().clone())
    //             );
    //         }
    //         println!("Files found: {}", k.to_string());
    //     }

    //     println!();
    //     println!("Request #3: Files that Kevin Morrison has view access to (with inference)");
    //     {
    //         let tx = session
    //             .transaction_with_options(TransactionType::Read, Options::new().infer(true))?; //Inference enabled
    //         let typeql_read_query = "match 
    //         $u isa user, has full-name 'Kevin Morrison'; 
    //         $p($u, $pa) isa permission; 
    //         $o isa object, has path $fp; 
    //         $pa($o, $va) isa access;
    //         $va isa action, has name 'view_file'; 
    //         get $fp; 
    //         sort $fp asc; 
    //         offset 0; 
    //         limit 5;"; //Only the first five results
    //         let iterator = tx.query().get(typeql_read_query)?;
    //         let mut k = 0;
    //         for item in iterator {
    //             k += 1;
    //             let answer = item.unwrap();
    //             println!(
    //                 "File #{}: {}",
    //                 k.to_string(),
    //                 unwrap_string(answer.map.get("fp").unwrap().clone())
    //             );
    //         }
    //         let typeql_read_query = "match 
    //         $u isa user, has full-name 'Kevin Morrison';
    //         $p($u, $pa) isa permission;
    //         $o isa object, has path $fp; 
    //         $pa($o, $va) isa access;
    //         $va isa action, has name 'view_file'; 
    //         get $fp; 
    //         sort $fp asc; 
    //         offset 5; 
    //         limit 5;"; //The next five results
    //         let iterator = tx.query().get(typeql_read_query)?;
    //         for item in iterator {
    //             k += 1;
    //             let answer = item.unwrap();
    //             println!(
    //                 "File #{}: {}",
    //                 k.to_string(),
    //                 unwrap_string(answer.map.get("fp").unwrap().clone())
    //             );
    //         }
    //         println!("Files found: {}", k.to_string());
    //     }

    //     println!();
    //     println!("Request #4: Add a new file and a view access to it");
    //     {
    //         let tx = session.transaction(TransactionType::Write)?; //Open a transaction to write
    //         let timestamp = Utc::now().format("%Y-%m-%d-%H-%M-%S").to_string();
    //         let filename = format!("{}{}{}", "logs/", timestamp, ".log");
    //         let typeql_insert_query = format!("insert $f isa file, has path '{}';", filename);
    //         let _ = tx.query().insert(&typeql_insert_query)?; //Inserting file
    //         println!("Inserted file: {}", filename);
    //         let typeql_insert_query = format!(
    //             "match 
    //         $f isa file, has path '{}';
    //         $vav isa action, has name 'view_file';
    //         insert 
    //         ($vav, $f) isa access;",
    //             filename
    //         );
    //         let _ = tx.query().insert(&typeql_insert_query)?; //The second query in the same transaction
    //         println!("Added view access to the file.");
    //         return tx.commit().resolve();
    //     }
    // }
    Ok({})
}

fn unwrap_string(concept: Concept) -> String {
    match concept {
        Concept::Attribute(Attribute {
            value: Value::String(value),
            ..
        }) => value,
        _ => unreachable!(),
    }
}

fn unwrap_value_long(value: Value) -> i64 {
    match value {
        Value::Long(value) => value,
        _ => unreachable!(),
    }
}
