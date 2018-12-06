
use actix::Handler;
use std::io;
use actix::Message;
use actix::Actor;
use actix::Context;


pub struct Queue {
    messages: Vec<String>,
}

impl Queue {
    pub fn new() -> Queue {
        Queue {
            messages: Vec::new(),
        }
    }
}

pub enum QueueMessage {
    Write(String),
    Read,
}

impl Message for QueueMessage {
    type Result = Result<String, io::Error>;
}

impl Actor for Queue {
    type Context = Context<Self>;
}

impl Handler<QueueMessage> for Queue {
    type Result = Result<String, io::Error>;

    fn handle(&mut self, message: QueueMessage, _ctx: &mut Context<Self>) -> Self::Result {
        match message {
            QueueMessage::Write(body) => {
                self.messages.push(body);
                Ok("OK".to_string())
            },
            QueueMessage::Read => {
                let t = self.messages.pop();
                if t.is_none() {
                    Ok("".to_string())
                }
                else {
                    Ok(t.unwrap_or_default())
                }
            },
        }
    }
}
