mod queue;
extern crate actix;
extern crate actix_web;
extern crate serde;
extern crate serde_json;
extern crate env_logger;
extern crate tokio;
extern crate futures;
#[macro_use] extern crate serde_derive;

use actix::Addr;
use actix_web::AsyncResponder;
use futures::future::{Future};
use actix::Actor;
use actix_web::{server, Json, App, State, HttpResponse, FutureResponse};
use actix_web::middleware::Logger;


#[derive(Deserialize)]
struct QueueMessage {
    body: String,
}

#[derive(Serialize)]
struct QueueResponse {
    result: String,
}

struct AppState {
    queue: Addr<queue::Queue>,
}

fn new_app(q: Addr<queue::Queue>) -> App<AppState> {
    let state = AppState {
        queue: q,
    };

    App::with_state(state)
        .middleware(Logger::default())
}

fn post_queue((message, state): (Json<QueueMessage>, State<AppState>)) -> FutureResponse<HttpResponse> {
    state.queue.send(queue::QueueMessage::Write(message.body.clone()))
        .from_err()
        .and_then(move |res| {
            Ok(HttpResponse::Created().json(QueueResponse{result: res.unwrap()}))
        }).responder()
}

fn get_queue(state: State<AppState>) -> FutureResponse<HttpResponse> {
    state.queue.send(queue::QueueMessage::Read)
        .from_err()
        .and_then(move |res| {
            if res.is_ok() {
                let result = res.unwrap();
                if result == "" {
                    Ok(HttpResponse::NoContent().finish())
                }
                else {
                    Ok(HttpResponse::Ok().json(QueueResponse{result: result}))
                }
            }
            else {
                Ok(HttpResponse::BadRequest().json(QueueResponse{result: res.unwrap_err().to_string()}))
            }
        }).responder()
}

fn main() {
    std::env::set_var("RUST_LOG", "actix_web=info");
    env_logger::init();

    let sys = actix::System::new("Bus");
    let q = queue::Queue::new().start();

    server::new(
        move || new_app(q.clone())
            .resource("/q", |r| {
                r.post().with(post_queue);
                r.get().with(get_queue);
            }))
        .bind("127.0.0.1:8088")
        .unwrap()
        .start();

    println!("Started http server: 127.0.0.1:8088");
    let _ = sys.run();
}
