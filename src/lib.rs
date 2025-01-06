use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender}, oneshot};
use tracing::debug;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Ballot {
    pub counter: u64,
    pub id: u64,
}
impl Ballot {
    const ZERO: Ballot = Ballot { counter: 0, id: 0 };
    fn inc(&mut self) -> Ballot {
        self.counter += 1;
        *self
    }
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct State(pub u16);


pub struct Proposer {
    tx: UnboundedSender<ProposerCommand>,
}
pub struct MyProposer {
    ballot:    Ballot,
    acceptors: Vec<Acceptor>,
}
#[derive(Debug)]
pub enum ProposerCommand {
    Cas {
        out: oneshot::Sender<CasResult>,
        cur: State,
        next: State,
    },
    Read { 
        out: oneshot::Sender<Stamp>,
    },
}
type CasResult = Result<(), Stamp>;

static PROPOSER_ID: AtomicU64 = AtomicU64::new(1);
impl MyProposer {
    pub async fn run(mut self, mut rx: UnboundedReceiver<ProposerCommand>) {
        debug!(?self.ballot.id, "starting proposer");
        while let Some(cmd) = rx.recv().await {
            debug!(?cmd, "recv");
            let b = self.ballot.inc();
            match cmd {
                ProposerCommand::Cas { out, cur: expected, next: target } => {
                     // TODO: could be implemented as `transform { x => x == cur ? next : throw x }`

                     // TODO: do in parallel, tolerate failures
                     let mut prepares = Vec::with_capacity(self.acceptors.len());
                     for a in &self.acceptors {
                        prepares.push(a.propose(b).await.expect("TODO: handle failed nodes").expect("TODO: handle conflicts"));
                     }
                     let cur = prepares.into_iter().max_by_key(|r| r.ballot).expect("at least one acceptor");
                     debug_assert!(b > cur.ballot);
                     let resp = if cur.state != expected {
                        Err(cur)
                     } else {
                        for a in &self.acceptors {
                            a.accept(b, target.clone()).await.expect("TODO: handle failed nodes").expect("TODO: handle conflicts");
                        }
                        Ok(())
                    };
                    let _ = out.send(resp);
                }
                ProposerCommand::Read { out }=> {
                    // TODO: could be implemented as `transform { x => x }`
                     let mut prepares = Vec::with_capacity(self.acceptors.len());
                     for a in &self.acceptors {
                        prepares.push(a.propose(b).await.expect("TODO: handle failed nodes").expect("TODO: handle conflicts"));
                     }
                     let cur = prepares.into_iter().max_by_key(|r| r.ballot).expect("at least one acceptor");
                     let _ = out.send(cur);
                }
            }
        }
    }
    pub fn start(acceptors: Vec<Acceptor>) -> Proposer {
        let (tx, rx) = unbounded_channel();
        let actor = MyProposer {
            ballot: Ballot { id: PROPOSER_ID.fetch_add(1, Ordering::SeqCst), counter: 0 },
            acceptors,
        };
        tokio::spawn(actor.run(rx));
        Proposer { tx }
    }
}
impl Proposer {
    pub async fn cas(&self, cur: State, next: State) -> anyhow::Result<CasResult> {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(ProposerCommand::Cas { cur, next, out: tx })?;
        Ok(rx.await?)
    }
    pub async fn read(&self) -> anyhow::Result<Stamp> {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(ProposerCommand::Read { out: tx })?;
        Ok(rx.await?)
    }
}

pub struct Acceptor {
    tx: UnboundedSender<AcceptorCommand>
}
pub struct MyAcceptor {
    pub state: State,
    pub accepted: Ballot,
    pub promise: Option<Ballot>, // invariant: if self.promise is Some, it must be > self.accepted
}

#[derive(Debug)]
pub enum AcceptorCommand {
    Propose {
        out: oneshot::Sender<PrepareResult>,
        ballot: Ballot,
    },
    Accept {
        out: oneshot::Sender<AcceptResult>,
        ballot: Ballot,
        state: State,
    },
}
impl MyAcceptor {
    pub async fn run(mut self, mut rx: UnboundedReceiver<AcceptorCommand>) {
        debug!("starting acceptor");
        while let Some(cmd) = rx.recv().await {
            debug!(?cmd, "recv");
            match cmd {
                AcceptorCommand::Propose { out, ballot } => {
                    let b = self.promise.unwrap_or(self.accepted);
                    let resp = if b >= ballot {
                        Err(b)
                    } else {
                        self.promise = Some(ballot);
                        Ok(Stamp { state: self.state.clone(), ballot: self.accepted })
                    };
                    let _ = out.send(resp);
                }
                AcceptorCommand::Accept { out, ballot, state } => {
                    let resp = if self.promise != Some(ballot) {
                        Err(self.promise.unwrap_or(self.accepted))
                    } else {
                        self.promise = None;
                        self.accepted = ballot;
                        self.state = state;
                        Ok(())
                    };
                    let _ = out.send(resp);
                }
            };
        }
    }
    pub fn start(state: State) -> Acceptor {
        let (tx, rx) = unbounded_channel();
        let actor = MyAcceptor {
            state,
            accepted: Ballot::ZERO,
            promise: None,
        };
        tokio::spawn(actor.run(rx));
        Acceptor { tx }
    }
}
impl Acceptor {
    pub async fn propose(&self, ballot: Ballot) -> anyhow::Result<PrepareResult> {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(AcceptorCommand::Propose { out: tx, ballot });
        Ok(rx.await?)
    }
    pub async fn accept(&self, ballot: Ballot, state: State) -> anyhow::Result<AcceptResult> {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(AcceptorCommand::Accept { out: tx, ballot, state });
        Ok(rx.await?)
    }
}
#[derive(Debug, PartialEq, Eq)]
pub struct Stamp {
    state: State,
    ballot: Ballot,
}
pub type PrepareResult = Result<Stamp, Ballot>;
pub type AcceptResult = Result<(), Ballot>;


#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn it_works() -> anyhow::Result<()> {
        let a1 = MyAcceptor::start(State(42));
        let a2 = MyAcceptor::start(State(42));
        let a3 = MyAcceptor::start(State(42));
        let p = MyProposer::start(vec![a1, a2, a3]);

        assert_eq!(p.read().await?.state, State(42));
        assert_eq!(p.cas(State(42), State(1984)).await?, Ok(()));
        assert_eq!(p.read().await?.state, State(1984));
        assert_eq!(p.cas(State(42), State(1984)).await?, Err(Stamp { state: State(1984), ballot: Ballot::ZERO }));
        Ok(())
    }
}
