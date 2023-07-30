use mongodb::bson::doc;
use mongodb::{Client, options::ClientOptions};

use serde::{Deserialize, Serialize};

// struct representing shared state of the mapreduce system
#[derive(Clone, Debug, Deserialize, Serialize)]
struct MasterState {
    name: String,
    n_map: i64,
    n_reduce: i64,
    map_tasks_left: i64,
}

// creates a mongodb collection
pub async fn create_collection(client: &Client, db_name: &str, coll_name: &str) {
    let db = client.database(db_name);
    for collection_name in db.list_collection_names(None).await.unwrap() {
        if collection_name == coll_name {
            eprintln!("Collection {} already exists. Exiting.", collection_name);
            return
        };
    }
    db.create_collection(coll_name, None).await.unwrap();
}

// drops a mongodb collection
pub async fn drop_collection(client: &Client, db_name: &str, coll_name: &str) {
    let db = client.database(db_name);
    let coll = db.collection::<mongodb::bson::Document>(coll_name);
    coll.drop(None);
}

// initializes master state
pub async fn init_state(client: &Client, db_name: &str, coll_name: &str, record_name: &str, n_map: i64, n_reduce: i64) {
    let db = client.database(db_name);
    let coll = db.collection(coll_name);

    coll.insert_one(
        MasterState { 
            name: record_name.to_string(),
            n_map: n_map,
            n_reduce: n_reduce,
            map_tasks_left: n_map
        },
        None
    ).await.unwrap();
}

// gets value of some field of the current state
pub async fn get_val(client: &Client, db_name: &str, coll_name: &str, record_name: &str, field: &str) -> Option<i64> {
    let db = client.database(db_name);
    let coll = db.collection::<mongodb::bson::Document>(coll_name);

    let filter = doc! {"name": record_name.to_string()};
    let res = coll.find_one(Some(filter), None).await.unwrap();

    match res {
        Some(state) => {
            state.get(field.to_string()).unwrap().as_i64()
        },
        None => None
    }
}

// updates some integer count in the current state
pub async fn update_count(client: &Client, db_name: &str, coll_name: &str, record_name: &str, field: &str, new_val: i64) {
    let db = client.database(db_name);
    let coll = db.collection::<mongodb::bson::Document>(coll_name);

    let filter = doc! {"name": record_name.to_string()};
    let update = doc! {"$set": {field.to_string(): new_val}};
    coll.update_one(filter, update, None).await.unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_initialization() {
        let addr = "[::1]:50051";
        let client_options = ClientOptions::parse("mongodb://localhost:27017").await.unwrap();
        let client = Client::with_options(client_options).unwrap();
        let db_name = "test_db";
        let coll_name = "test_coll";

        create_collection(&client, db_name, coll_name).await;
        init_state(&client, db_name, coll_name, "test_state", 10, 11).await;
        assert_eq!(get_val(&client, db_name, coll_name, "test_state", "n_map").await.unwrap(), 10);
        assert_eq!(get_val(&client, db_name, coll_name, "test_state", "n_reduce").await.unwrap(), 11);

        drop_collection(&client, db_name, coll_name).await;
    }

    #[tokio::test]
    async fn test_update_count() {
        let addr = "[::1]:50051";
        let client_options = ClientOptions::parse("mongodb://localhost:27017").await.unwrap();
        let client = Client::with_options(client_options).unwrap();
        let db_name = "test_db";
        let coll_name = "test_coll";

        create_collection(&client, db_name, coll_name).await;
        init_state(&client, db_name, coll_name, "test_update", 15, 1).await;
        update_count(&client, db_name, coll_name, "test_update", "n_map", 34).await;
        assert_eq!(get_val(&client, db_name, coll_name, "test_update", "n_map").await.unwrap(), 34);

        drop_collection(&client, db_name, coll_name).await;
    }
}
