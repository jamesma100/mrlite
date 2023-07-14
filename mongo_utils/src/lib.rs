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
async fn create_collection(client: &Client, db_name: &str, coll_name: &str) {
    let db = client.database(db_name);
    db.create_collection(coll_name, None).await.unwrap();
}

// initializes master state
async fn init_state(client: &Client, db_name: &str, coll_name: &str, name: &str, n_map: i64, n_reduce: i64) {
    let db = client.database(db_name);
    let coll = db.collection(coll_name);

    coll.insert_one(
        MasterState { 
            name: name.to_string(),
            n_map: n_map,
            n_reduce: n_reduce,
            map_tasks_left: n_map
        },
        None
    ).await.unwrap();
}

// gets value of some field of the current state
async fn get_state(client: &Client, db_name: &str, coll_name: &str, name: &str, field: &str) -> Option<i64> {
    let db = client.database(db_name);
    let coll = db.collection::<mongodb::bson::Document>(coll_name);

    let filter = doc! {"name": name.to_string()};
    let res = coll.find_one(Some(filter), None).await.unwrap();

    match res {
        Some(state) => {
            state.get(field.to_string()).unwrap().as_i64()
        },
        None => None
    }
}

// updates some integer count in the current state
async fn update_count(client: &Client, db_name: &str, coll_name: &str, name: &str, field: &str, new_val: i64) {
    let db = client.database(db_name);
    let coll = db.collection::<mongodb::bson::Document>(coll_name);

    let filter = doc! {"name": name.to_string()};
    let update = doc! {"$set": {field.to_string(): new_val}};
    coll.update_one(filter, update, None).await.unwrap();
}
