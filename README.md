# PGMint - Postgres 🗄️ + Tendermint 🌿

PGMint is a consistent, decentralized, relational database built on top of Tendermint's networking and consensus protocols. PGMint is implemented as a built-in Tendermint application and interacts with lower layers through Application Blockchain Interface APIs. 

To run a local node, perform the following:
- First install Go. Then navigate to your `$GOPATH`. Clone the repository in your `$GOPATH`. 
- Navigate to the src directory. Here we will initialize our Postgres instance. By default, the host and port that this instance will bind to are localhost and port 5432. We run the two commands in succession:
  - `initdb -D pgdata`
  - `pg_ctl -D pgdata -o “-p <port number>” -l logfile start`
- Then we build the source for the ABCI application by running `go build`. This may initially fail due to missing dependencies. Install all required dependencies (can be found in `go.mod`). 
  

To interact with the node directly, please follow the Tendermint RPC reference. For convenience, I have written a shell that behaves very similar to `psql` that abstracts away the underlying `broadcast_tx_*` and `abci_*` JSON-RPC API calls. Here, you can type native Postgres queries and have them propagated to the Tendermint node for validation and execution. To run this shell, run the following command: 
- `python shell.py <src> <n_nodes> <consistency_level>`. In this command, `src` refers to the relative path to your ABCI application source directory, `n_nodes` refers to the number of nodes currently running, and `consistency_level` refers to the tunable read consistency level (one of `{“strong” | “eventual”}`). 
  

To create local networks for testing purposes, I have developed a automated testnet generator that proceeds through the following workflow:
- Cleans up any existing Postgres processes and removes data directories. Does the same for Tendermint processes.
- Copies the source directory `n_nodes` times, building the ABCI application source within each. 
- Initializes the validator nodes. Updates config files such that each node is aware of its persistent peers. Updates genesis files such that all nodes share the same information.
- Assigns non-interfering ports for each node. 
- Spawns an ABCI application process for each node.
This script can be run through the following command: 
- `python testnet.py {start | destroy} <src> <n_nodes>`
  

Additionally, to conduct tests with asynchronous RPC calls (e.g. making batches of queries to the database asynchronously), I have written `batch_sql.py`, which reads in a `.sql` file and adds all transactions within it to the Tendermint mempool in order. It then repeatedly queries the node to see whether every transaction was included in the chain, returning only when all transactions have persisted. 

