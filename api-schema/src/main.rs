use typedb_driver::{
    concept::{Transitivity, ValueType}, transaction::concept::api::{EntityTypeAPI, ThingTypeAPI}, Connection, DatabaseManager, Error, Promise, Session, SessionType, TransactionType
};

fn main() -> Result<(), Error> {
    //const DB_NAME: &str = "schema-api-db";
    //const SERVER_ADDR: &str = "127.0.0.1:1729";
    let driver = Connection::new_core("127.0.0.1:1729")?;
    let databases = DatabaseManager::new(driver);
/*     if databases.contains(DB_NAME)? {
        databases.get(DB_NAME)?.delete()?;
    }
    databases.create(DB_NAME)?; */
    
    Ok({
        let session = Session::new(databases.get("schema-api-db")?, SessionType::Schema)?;
        let tx = session.transaction(TransactionType::Write)?;
        //let user = tx.concept().put_entity_type("user".to_owned()).resolve()?;
        //let mut admin = tx.concept().put_entity_type("admin".to_owned()).resolve()?;
        //admin.set_supertype(&tx, user).resolve()?;
        let tag = tx.concept().put_attribute_type("tag".to_owned(),ValueType::String).resolve()?;
        let entities = tx.concept().get_entity_type("entity".to_owned()).resolve().unwrap().unwrap().get_subtypes(&tx, Transitivity::Explicit).unwrap();
        for res in entities {
            //println!("{}", entity.unwrap().label)
            let mut entity = res.unwrap();
            if !{entity.is_abstract()} {
                _ = entity.set_owns(&tx, tag.clone(),None,vec![]).resolve();
            }
        }
        tx.commit().resolve()?;
    })
    
}

//let entities = tx.concept().get_entity_type("entity").resolve()?;
