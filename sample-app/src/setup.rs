use std::{error::Error, fs};

use typedb_driver::{
    concept::Value, Connection, DatabaseManager, Error as TypeDBError, Promise, Session, SessionType, TransactionType,
};

fn create_new_database(driver: &Connection, db_name: String) -> Result<(), TypeDBError> {
    let databases = DatabaseManager::new(driver.to_owned());
    if databases.contains(&db_name)? {
        print!("Found an existing database. Replacing...");
        databases.get(&db_name)?.delete()?;
    } else {
        print!("Creating new database...")
    }
    let result = databases.create(&db_name);
    assert!(databases.contains(&db_name)?, "Error creating a database.");
    println!("OK");
    return result;
}

fn db_schema_setup(schema_session: &Session, schema_file: String) -> Result<(), TypeDBError> {
    let tx = schema_session.transaction(TransactionType::Write)?;
    let data = fs::read_to_string(schema_file)?; // "iam-schema.tql"
    print!("Defining schema...");
    let response = tx.query().define(&data).resolve();
    tx.commit().resolve()?;
    println!("OK");
    return response;
}

fn db_dataset_setup(data_session: &Session, data_file: String) -> Result<(), Box<dyn Error>> {
    let tx = data_session.transaction(TransactionType::Write)?;
    let data = fs::read_to_string(data_file)?; // "iam-data-single-query.tql"
    print!("Loading data...");
    let response = tx.query().insert(&data)?;
    let result = response.collect::<Vec<_>>();
    tx.commit().resolve()?;
    println!("OK");
    Ok({
        drop(result);
    })
}

fn test_initial_database(data_session: &Session) -> Result<bool, Box<dyn Error>> {
    let tx = data_session.transaction(TransactionType::Write)?;
    let test_query = "match $u isa user; get $u; count;";
    print!("Testing the database...");
    let response = tx.query().get_aggregate(test_query).resolve();
    let result = match response?.ok_or("Error: unexpected test query response.")? {
        Value::Long(value) => value,
        _ => unreachable!(),
    };
    //assert_eq!(result, 3, "Unexpected number of users: {}", result);
    if result == 3 {
        println!("OK");
        Ok(true)
    } else {
        Err(Box::new(TypeDBError::Other("Test failed. Terminating...".to_string())))
    }
}

pub fn db_setup(driver: Connection, db_name: String) -> Result<(), Box<dyn Error>> {
    println!("Setting up the database: {}", &db_name);
    let _ = create_new_database(&driver, db_name.clone());
    {
        let databases = DatabaseManager::new(driver.clone());
        {
            let schema_session = Session::new(databases.get(&db_name)?, SessionType::Schema)?;
            db_schema_setup(&schema_session, "iam-schema.tql".to_string())?;
        }
        {
            let data_session = Session::new(databases.get(&db_name)?, SessionType::Data)?;
            db_dataset_setup(&data_session, "iam-data-single-query.tql".to_string())?;
            if test_initial_database(&data_session)? == true {
                Ok(())
            } else {
                Err(Box::new(TypeDBError::Other("Test failed. Terminating...".to_string())))
            }
        }
        // let result = match test_initial_database(&data_session) {
        //     Ok(true) => Ok(()),
        //     Ok(false) => unreachable!(),
        //     Err(_) => Err(Box::new(TypeDBError::Other("Test failed. Terminating...".to_string()))),
        // };
    }
}
