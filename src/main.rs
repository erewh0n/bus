extern crate actix;
extern crate actix_web;
extern crate serde;
extern crate serde_json;
#[macro_use] extern crate serde_derive;
use actix_web::{server, App, Json, http};

#[derive(Deserialize)]
struct Topic {
    name: String,
    body: String,
}

fn index(topic: Json<Topic>) -> String {
    format!("topic name is {}!, topic body is {}", topic.name, topic.body)
}

fn main() {
    let sys = actix::System::new("example");

    server::new(
        || App::new()
            .resource("/", |r| r.method(http::Method::POST).with(index)))
        .bind("127.0.0.1:8088").unwrap()
        .start();

    println!("Started http server: 127.0.0.1:8088");
    let _ = sys.run();
}
