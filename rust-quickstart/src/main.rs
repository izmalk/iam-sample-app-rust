use typedb_driver::{
    Connection, DatabaseManager, Error, Promise, Session, SessionType, TransactionType,
};

fn main() -> Result<(), Error> {
    const DB_NAME: &str = "access-management-db";
    const SERVER_ADDR: &str = "127.0.0.1:1729";
    let driver = Connection::new_core(SERVER_ADDR)?;
    let databases = DatabaseManager::new(driver);
    if databases.contains(DB_NAME)? {
        databases.get(DB_NAME)?.delete()?;
    }
    databases.create(DB_NAME)?;
    {
        let session = Session::new(databases.get(DB_NAME)?, SessionType::Schema)?;
        let tx = session.transaction(TransactionType::Write)?;
        tx.query().define("define person sub entity;").resolve()?;
        tx.query().define("define name sub attribute, value string; person owns name;").resolve()?;
        tx.commit().resolve()?;
    }
    {
        let session = Session::new(databases.get(DB_NAME)?, SessionType::Data)?;
        {
            let tx = session.transaction(TransactionType::Write)?;
            let _ = tx.query().insert("insert $p isa person, has name 'Alice';")?;
            let _ = tx.query().insert("insert $p isa person, has name 'Bob';")?;
            tx.commit().resolve()?;
        }
        {
            let tx = session.transaction(TransactionType::Read)?;
            let res = tx.query().fetch("match $p isa person; fetch $p: name;").unwrap();
            for item in res {
                println!("{}", &item.unwrap().to_string());
            }
            Ok(())
        }
    }
}