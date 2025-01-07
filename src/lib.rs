use std::sync::atomic::{AtomicU64, Ordering};

use thiserror::Error;
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    oneshot,
};
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

#[derive(Clone)]
pub struct Proposer {
    tx: UnboundedSender<ProposerCommand>,
}
pub struct MyProposer {
    ballot: Ballot,
    acceptors: Vec<Acceptor>,
}
#[derive(Debug)]
pub enum ProposerCommand {
    Cas {
        out: oneshot::Sender<anyhow::Result<()>>,
        cur: State,
        next: State,
    },
    Read {
        out: oneshot::Sender<anyhow::Result<Stamp>>,
    },
}

static PROPOSER_ID: AtomicU64 = AtomicU64::new(1);
impl MyProposer {
    pub fn start(acceptors: Vec<Acceptor>) -> Proposer {
        let (tx, rx) = unbounded_channel();
        let actor = MyProposer {
            ballot: Ballot {
                id: PROPOSER_ID.fetch_add(1, Ordering::SeqCst),
                counter: 0,
            },
            acceptors,
        };
        tokio::spawn(actor.run(rx));
        Proposer { tx }
    }
    async fn run(mut self, mut rx: UnboundedReceiver<ProposerCommand>) {
        debug!(?self.ballot.id, "starting proposer");
        while let Some(cmd) = rx.recv().await {
            debug!(?cmd, "recv");
            match cmd {
                ProposerCommand::Cas {
                    out,
                    cur: expected,
                    next: target,
                } => {
                    let resp = self
                        .process(|stamp| {
                            if stamp.state == expected {
                                Ok(target)
                            } else {
                                Err(ProtocolError::CasMismatch { found: stamp })
                            }
                        })
                        .await;
                    let _ = out.send(resp.map(|_| ()));
                }
                ProposerCommand::Read { out } => {
                    let resp = self.process(|stamp| Ok(stamp.state)).await;
                    let _ = out.send(resp);
                }
            };
        }
    }
    async fn process(
        &mut self,
        f: impl FnOnce(Stamp) -> ProtocolResult<State>,
    ) -> anyhow::Result<Stamp> {
        let b = self.ballot.inc();
        let mut prepares = Vec::with_capacity(self.acceptors.len());
        // TODO: do in parallel, tolerate failures
        for a in &self.acceptors {
            match a.propose(b).await? {
                Ok(v) => prepares.push(v),
                Err(e) => return Err(ProtocolError::PrepareConflict { ballot: e }.into()),
            };
        }
        let cur = prepares
            .into_iter()
            .max_by_key(|r| r.ballot)
            .expect("at least one acceptor");
        debug_assert!(b > cur.ballot);
        let next = f(cur)?;
        // TODO: do in parallel, tolerate failures
        for a in &self.acceptors {
            if let Err(e) = a.accept(b, next.clone()).await? {
                return Err(ProtocolError::AcceptConflict { ballot: e }.into());
            }
        }
        Ok(Stamp {
            state: next,
            ballot: b,
        })
    }
}
#[derive(Debug, Error, PartialEq, Eq)]
enum ProtocolError {
    #[error("CAS mismatch, ({found:?})")]
    CasMismatch { found: Stamp },
    #[error("Prepare conflict, ({ballot:?})")]
    PrepareConflict { ballot: Ballot },
    #[error("Accept conflict, ({ballot:?})")]
    AcceptConflict { ballot: Ballot },
}
type ProtocolResult<T> = Result<T, ProtocolError>;
impl Proposer {
    pub async fn cas(&self, cur: State, next: State) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(ProposerCommand::Cas { cur, next, out: tx })?;
        rx.await?
    }
    pub async fn read(&self) -> anyhow::Result<Stamp> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(ProposerCommand::Read { out: tx })?;
        rx.await?
    }
}

#[derive(Clone)]
pub struct Acceptor {
    tx: UnboundedSender<AcceptorCommand>,
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
                        Ok(Stamp {
                            state: self.state.clone(),
                            ballot: self.accepted,
                        })
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
        let _ = self.tx.send(AcceptorCommand::Accept {
            out: tx,
            ballot,
            state,
        });
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

        // We can read
        let r = p.read().await?;
        assert_eq!(r.state, State(42));
        // We can write
        p.cas(State(42), State(1984)).await?;
        // We can read our writes
        let r = p.read().await?;
        assert_eq!(r.state, State(1984));
        // Writes with a mismatched ETag are rejected
        let err = p
            .cas(State(42), State(1984))
            .await
            .expect_err("cas mismatch");
        assert_eq!(
            err.downcast::<ProtocolError>().unwrap(),
            ProtocolError::CasMismatch { found: r }
        );
        Ok(())
    }

    #[tokio::test]
    async fn proposers_handle_conflicts() -> anyhow::Result<()> {
        let a1 = MyAcceptor::start(State(1));
        let a2 = MyAcceptor::start(State(1));
        let a3 = MyAcceptor::start(State(1));
        let p1 = MyProposer::start(vec![a1.clone(), a2.clone(), a3.clone()]);
        let p2 = MyProposer::start(vec![a1.clone(), a2.clone(), a3.clone()]);

        // Perform two writes from p1
        p1.cas(State(1), State(2)).await?;
        p1.cas(State(2), State(3)).await?;

        // Perform a write from p2
        let err = p2
            .cas(State(3), State(4))
            .await
            .expect_err("expecting a prepare conflict");
        assert!(matches!(
            err.downcast::<ProtocolError>().unwrap(),
            ProtocolError::PrepareConflict { .. }
        ));

        Ok(())
    }
}
