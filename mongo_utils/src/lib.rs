use mongodb::bson::doc;
use mongodb::Client;
use std::collections::HashMap;

use serde::{Deserialize, Serialize};

// Struct representing shared state of the mapreduce system
#[derive(Clone, Debug, Deserialize, Serialize)]
struct MasterState {
    name: String,
    n_map: i64,
    n_reduce: i64,
    map_tasks_left: i64,
}

// Creates a mongodb collection
pub async fn create_collection(client: &Client, db_name: &str, coll_name: &str) {
    let db = client.database(db_name);
    for collection_name in db.list_collection_names(None).await.unwrap() {
        if collection_name == coll_name {
            eprintln!("Collection {} already exists. Exiting.", collection_name);
            return;
        };
    }
    db.create_collection(coll_name, None).await.unwrap();
}

// Drops a mongodb collection
pub async fn drop_collection(client: &Client, db_name: &str, coll_name: &str) {
    let db = client.database(db_name);
    let coll = db.collection::<mongodb::bson::Document>(coll_name);
    coll.drop(None).await.expect("Could not drop collection.");
}

// Initializes master state
pub async fn init_master_state(
    client: &Client,
    db_name: &str,
    coll_name: &str,
    record_name: &str,
    n_map: i64,
    n_reduce: i64,
) {
    let db = client.database(db_name);
    let coll = db.collection(coll_name);

    coll.insert_one(
        MasterState {
            name: record_name.to_string(),
            n_map: n_map,
            n_reduce: n_reduce,
            map_tasks_left: n_map,
        },
        None,
    )
    .await
    .unwrap();
}

// Initializes map and reduce tasks state (requires one call for each)
pub async fn init_tasks(
    client: &Client,
    db_name: &str,
    coll_name: &str,
    tasks: &HashMap<String, (bool, bool)>,
) {
    let db = client.database(db_name);
    let coll = db.collection::<mongodb::bson::Document>(coll_name);

    let mut vec = Vec::new();
    let mut i = 0;
    for (task_name, task_state) in tasks {
        let tasknum = if task_state.1 { i } else { -1 };
        vec.push(doc! {
            "name": task_name.to_string(),
            "is_assigned": task_state.0,
            "is_map": task_state.1,
            "tasknum": tasknum,
            "done": false,
        });
        i += 1;
    }
    coll.insert_many(vec, None).await.unwrap();
}

// Gets value of some integer field of the current (master) state
pub async fn get_val(
    client: &Client,
    db_name: &str,
    coll_name: &str,
    record_name: &str,
    field: &str,
) -> Option<i64> {
    let db = client.database(db_name);
    let coll = db.collection::<mongodb::bson::Document>(coll_name);

    let filter = doc! {"name": record_name.to_string()};
    let res = coll.find_one(Some(filter), None).await.unwrap();

    match res {
        Some(state) => state.get(field.to_string()).unwrap().as_i64(),
        None => None,
    }
}

// Returns a task tuple given a task name
pub async fn get_task(
    client: &Client,
    db_name: &str,
    coll_name: &str,
    task_name: &str,
) -> (
    Option<String>,
    Option<bool>,
    Option<bool>,
    Option<i32>,
    Option<bool>,
) {
    let db = client.database(db_name);
    let coll = db.collection::<mongodb::bson::Document>(coll_name);

    let filter = doc! {"name": task_name.to_string()};
    let res = coll.find_one(Some(filter), None).await.unwrap();

    match res {
        Some(state) => (
            Some(task_name.to_string()),
            state.get("is_assigned".to_string()).unwrap().as_bool(),
            state.get("is_map".to_string()).unwrap().as_bool(),
            state.get("tasknum".to_string()).unwrap().as_i32(),
            state.get("done".to_string()).unwrap().as_bool(),
        ),
        None => (None, None, None, None, None),
    }
}

// Updates some integer count in the current state
pub async fn update_count(
    client: &Client,
    db_name: &str,
    coll_name: &str,
    record_name: &str,
    field: &str,
    new_val: i64,
) {
    let db = client.database(db_name);
    let coll = db.collection::<mongodb::bson::Document>(coll_name);

    let filter = doc! {"name": record_name.to_string()};
    let update = doc! {"$set": {field.to_string(): new_val}};
    coll.update_one(filter, update, None).await.unwrap();
}

// Updates the assigned value in some task
pub async fn update_assigned(
    client: &Client,
    db_name: &str,
    coll_name: &str,
    task_name: &str,
    new_val: bool,
) {
    let db = client.database(db_name);
    let coll = db.collection::<mongodb::bson::Document>(coll_name);

    let filter = doc! {"name": task_name.to_string()};
    let update = doc! {"$set": {"is_assigned".to_string(): new_val}};
    coll.update_one(filter, update, None).await.unwrap();
}

// Updates done value of some task
pub async fn update_done(
    client: &Client,
    db_name: &str,
    coll_name: &str,
    task_name: &str,
    new_val: bool,
) {
    let db = client.database(db_name);
    let coll = db.collection::<mongodb::bson::Document>(coll_name);

    let filter = doc! {"name": task_name.to_string()};
    let update = doc! {"$set": {"done".to_string(): new_val}};
    coll.update_one(filter, update, None).await.unwrap();
}

// TODO: make these tests atomic
#[cfg(test)]
mod tests {
    use super::*;

    // #[tokio::test]
    // async fn test_initialization() {
    //     let addr = "[::1]:50051";
    //     let client_options = ClientOptions::parse("mongodb://localhost:27017").await.unwrap();
    //     let client = Client::with_options(client_options).unwrap();
    //     let db_name = "test_db";
    //     let coll_name = "test_coll";

    //     create_collection(&client, db_name, coll_name).await;
    //     init_master_state(&client, db_name, coll_name, "test_state", 10, 11).await;
    //     assert_eq!(get_val(&client, db_name, coll_name, "test_state", "n_map").await.unwrap(), 10);
    //     assert_eq!(get_val(&client, db_name, coll_name, "test_state", "n_reduce").await.unwrap(), 11);

    //     drop_collection(&client, db_name, coll_name).await;
    // }

    #[tokio::test]
    async fn test_update_count() {
        let addr = "[::1]:50051";
        let client_options = ClientOptions::parse("mongodb://localhost:27017")
            .await
            .unwrap();
        let client = Client::with_options(client_options).unwrap();
        let db_name = "test_db";
        let coll_name = "test_coll";

        create_collection(&client, db_name, coll_name).await;
        let n_map: i64 = 15;
        let n_reduce: i64 = 1;
        let new_n_map: i64 = 34;
        init_master_state(&client, db_name, coll_name, "test_update", n_map, n_reduce).await;
        update_count(
            &client,
            db_name,
            coll_name,
            "test_update",
            "n_map",
            new_n_map,
        )
        .await;
        assert_eq!(
            get_val(&client, db_name, coll_name, "test_update", "n_map")
                .await
                .unwrap(),
            new_n_map
        );

        drop_collection(&client, db_name, coll_name).await;
    }
}
