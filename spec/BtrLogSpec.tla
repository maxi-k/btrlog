---------------------------- MODULE BtrLogSpec ---------------------------
EXTENDS Integers, FiniteSets
,TLC
\*,IOUtils
\*
\*\* Usable in simulation mode to show coverage information (Does not work in model checking mode)
\*\* See https://github.com/microsoft/CCF/issues/6537#issuecomment-2652300191
\*PrintStats ==
\*    Serialize(<<TLCGet("stats")>>, "simulation_stats.ndjson", [format |-> "NDJSON", charset |-> "UTF-8", openOptions |-> <<"WRITE", "CREATE", "APPEND">>])
\*
\*\* Usable in model checking and simulation mode to show coverage information
\*PrintCoverage ==
\*    Serialize(<<TLCGet("spec")>>, "coverage.json", [format |-> "NDJSON", charset |-> "UTF-8", openOptions |-> <<"WRITE", "CREATE", "APPEND">>])

CONSTANTS
\* Parameters for simulation/model checking
    APPEND_COUNT_MAX,     \* Maximum number of appends each client can perform
    FAULTS_COUNT,         \* Limits state space, how many server faults to inject
\* IDs for symmetry sets
    writer_ids,           \* Set of writer IDs, e.g., {w1, w2}
    reader_ids,           \* Set of reader IDs, e.g., {r1}
    server_ids,           \* Set of server IDs, e.g., {s1, s2, s3}
\* Protocol parameters, fixed/defined per log
    APPENDS_INFLIGHT_MAX \* Limits the number of uncommitted entries and gaps

\* requires TLC module
symmetry_set == Permutations(writer_ids) \union Permutations(reader_ids) \union Permutations(server_ids)

\* Helper state to limit state space
VARIABLES
    faults_injected

\* Blob store/metadata state.
VARIABLES 
    blob_store,
    meta_epoch

\* State of servers.
VARIABLES
    servers

\* State of clients.
VARIABLES
    writers,
    readers

\* Group all variables in one set for simplified usage (e.g., the spec statement).
vars == << writers, readers, servers, blob_store, faults_injected, meta_epoch>>

\* Status enumerations.
server_status == {"failed", "active", "uninitialized"}
writer_status == {"inactive", "fencing", "writing", "repairing", "done", "fenced"}
reader_status == {"inactive", "reading"}

\* Quorum definitions.
SERVER_COUNT == Cardinality(server_ids)
ceiling_divide(a, b) == (a + b - 1) \div b
writer_quorum == (SERVER_COUNT \div 2) + 1
reader_quorum == ceiling_divide(SERVER_COUNT, 2)

\* Creates a record for a log entry.
create_append(lsn, epoch, client_id, last_add_confirmed) == [
    lsn               |-> lsn,
    epoch             |-> epoch,
    client_id         |-> client_id,
    last_add_confirmed|-> last_add_confirmed
]

\* Helper to check shape/domain for an append.
is_append(e) ==
    /\ e
       \in [ lsn             : Nat,
             epoch           : Nat,
             client_id       : writer_ids,
             last_add_confirmed : Nat ]

\* Defines the initial state of a server.
init_server_state(server_id, status) == [
    server_id    |-> server_id,
    \* epoch must be initialized from the control plane
    epoch        |-> meta_epoch,
    lsn_committed|-> 0,
    lsn_flushed  |-> 0,
    status       |-> status,
    ledger       |-> {}
]

\* Defines the initial state of a writer.
init_writer_state(client_id) == [
    client_id     |-> client_id,
    epoch         |-> 0,
    lsn_current   |-> 0,
    lsn_committed |-> 0,
    appends       |-> {},
    status        |-> "inactive",
    batch_lsn     |-> 0
]

\* Defines the initial state of a reader.
init_reader_state(client_id) == [
    client_id |-> client_id,
    lsn_read  |-> 0,
    status    |-> "inactive"
]

\* Initializes the system with no active writers.
\* The model checker will activate different writers at various points in time and simulate fencing.
Init ==     
    /\ faults_injected = 0
    /\ blob_store = [server_id \in server_ids |-> {}]
    /\ meta_epoch = 0
    /\ servers = [server_id \in server_ids |-> init_server_state(server_id, "uninitialized")]
    /\ writers = [writer_id \in writer_ids |-> init_writer_state(writer_id)]
    /\ readers = [reader_id \in reader_ids |-> init_reader_state(reader_id)]

\* Helper functions for set maxima/minima and cardinalities.
max(S) ==
    IF S = {}
    THEN 0
    ELSE CHOOSE x \in S : \A y \in S : y <= x

min(S) ==
    IF S = {}
    THEN 0
    ELSE CHOOSE x \in S : \A y \in S : y >= x

server_count_with_entry(lsn, client_id) ==
    Cardinality({ s \in server_ids : \E entry \in servers[s].ledger : entry.lsn = lsn /\ entry.client_id = client_id })

blob_store_count_with_entry(lsn, client_id) ==
    Cardinality({ s \in server_ids : \E entry \in blob_store[s] : entry.lsn = lsn /\ entry.client_id = client_id })

server_count_with_lsn(lsn) ==
    Cardinality({ s \in server_ids : \E entry \in servers[s].ledger : entry.lsn = lsn })

blob_store_count_with_lsn(lsn) ==
    Cardinality({ s \in server_ids : \E entry \in blob_store[s] : entry.lsn = lsn })

\*****************************************************************
\* Invariants
\*****************************************************************

\* Extended type check for all major fields in servers/writers/readers/blob_store.
type_check_ok ==
    /\ meta_epoch \in Nat
    /\ \A s \in server_ids:
        /\ servers[s].server_id \in server_ids
        /\ servers[s].epoch \in Nat
        /\ servers[s].lsn_committed \in Nat
        /\ servers[s].lsn_flushed \in Nat
        /\ servers[s].status \in server_status
        \* Check ledger entries
        /\ \A entry \in servers[s].ledger: is_append(entry)

    /\ \A w \in writer_ids:
        /\ writers[w].client_id \in writer_ids
        /\ writers[w].epoch \in Nat
        /\ writers[w].lsn_current \in Nat
        /\ writers[w].lsn_committed \in Nat
        /\ writers[w].batch_lsn \in Nat
        /\ writers[w].status \in writer_status
        /\ \A n \in writers[w].appends: n \in Nat  \* appended LSNs are natural numbers

    /\ \A r \in reader_ids:
        /\ readers[r].client_id \in reader_ids
        /\ readers[r].lsn_read \in Nat
        /\ readers[r].status \in reader_status

    \* blob_store must contain only valid appends
    /\ \A s \in server_ids:
        \A entry \in blob_store[s]: is_append(entry)

\* If entry n is committed, all entries < n are committed (guaranteed on client)
\* In case of a server failure this invariant does not hold
\* I.e. a server might fail and committed entries will only be stored on one server
\* They would still be readable (see next invariant), hence no data is lost
commits_in_order == 
    /\ faults_injected = 0
    /\ \A c \in writer_ids:
        \/ writers[c].lsn_committed = 0 \* initial state
        \/ /\ writers[c].lsn_committed > 0 \* all LNSs up to lsn_committed must be committed 
           /\ \A n \in 1..writers[c].lsn_committed:
                \* Normal case: a committed LSN is stored on at least on server
                \/ server_count_with_lsn(n) >= 1
                \* Special case: finding the LSN in the blob_store allows us to commit it (e.g., as done during fencing/repair)
                \/ blob_store_count_with_lsn(n) >= 1

\* No data loss: If an entry has been committed, it must be readable && if an entry is read once, it must always be readable
entries_committed_readable ==
    \A c \in writer_ids:
        \/ writers[c].lsn_committed = 0 \* initial state
        \/ /\ writers[c].lsn_committed > 0 \* all LNSs up to lsn_committed must be committed 
           /\ \A n \in 1..writers[c].lsn_committed:
                \* Normal case: Committed entries can bre read from a single server (since we model LAC reads)
                \* Note: Only uncommitted entries must be read from a reader quorum (as done during fencing/repair)
                \/ server_count_with_lsn(n) >= 1
                \* Special case: finding the LSN in the blob_store allows us to read it
                \/ blob_store_count_with_lsn(n) >= 1

no_reads_uncommitted ==
    ~ \E c \in reader_ids:
        \A s \in server_ids:
            readers[c].lsn_read > servers[s].lsn_committed

\*****************************************************************
\* Actions
\* Appends happen batch-wise in reverse order to simulate out-of-order/delayed message delivery
\* Our model assumes the worst-case, ie., appends always arrive out-of-order and some appends are lost
\* A batch represents the maximum number of appends allowed to be inflight
\* Note: The MAX_INFLIGHT_APPENDS is a protocol parameter and is a property that must be fixed per log
\*****************************************************************

\* Acquire a write token (i.e., fence token) for a client.
\* This is part of the open the ledger for writing logic
client_acquire_write_token(client_id) ==
    LET w == writers[client_id]
        next_epoch == meta_epoch + 1
    IN
    /\ w.status = "inactive"
    \* In the spec, we increment the epoch atomically, in the implementation, this should be a compare-and-swap operation
    /\ meta_epoch' = next_epoch
    /\ writers' = [writers EXCEPT ![client_id] = [@ EXCEPT !.epoch = next_epoch, !.status = "fencing"]]
    /\ UNCHANGED <<servers, readers, blob_store, faults_injected>>

\* Send the acquired write token to the servers (E.g., as part of an open_ledger_for_write call).
\* A higher write token will fence clients with smaller write tokens.
\* Servers will respond with their current LAC
client_set_write_token_on_servers(client_id) ==
    LET w == writers[client_id]
    IN
    /\ w.status = "fencing"
    \* models:
    \* 1) some msg/requests are always lost
    \* 2) remaining servers will accept the append
    \* 3) either response or reply could be lost, but at the end min a write quorum of round trips will succeed
    \*    -> this implies: a client will retry as long as it receives a write quorum of successful retries
    /\ \E quorum \in { S \in SUBSET server_ids : Cardinality(S) = writer_quorum } :
        \* Models that clients can be fenced if a server's epoch has been updated
        \* We use < instead of +1 since a server might lag behind or be restarted
        \* Early abort during repair: we keep checking that we have not been fenced
        /\ \A sid \in quorum: servers[sid].epoch < w.epoch
        \* failed servers cannot be part of the quorum
        /\ \A sid \in quorum: servers[sid].status /= "failed"
        /\ servers' = [sid \in server_ids |-> IF /\ sid \in quorum
                                           THEN [servers[sid] EXCEPT
                                                    !.epoch = w.epoch]
                                           ELSE servers[sid]]
        /\ writers' = [writers EXCEPT ![client_id] =
                            [@ EXCEPT
                                \* A client uses the highest LAC returned from servers
                                \* TODO: Only active servers should be used to determine the LAC
                                !.lsn_committed = max({ servers[sid].lsn_committed : sid \in quorum }),
                                !.status = "repairing"]]
    /\ UNCHANGED <<readers, blob_store, faults_injected, meta_epoch>>

\* Try to read as far as possible to find the highest LSN that was written.
\* Entries could be found on servers or in the blob_store.
\* Find the truncation point (i.e., the highest consecutive LSN)
client_repair_ledger(client_id) ==
    LET w == writers[client_id]
    IN
    /\ w.status = "repairing"
    \* Repair reads can go to a reader_quorum
    /\ \E quorum \in { S \in SUBSET server_ids : Cardinality(S) = reader_quorum } :
        \* Models that clients can be fenced if a server's epoch has been updated
        \* We use =< instead of +1 since a server might lag behind or be restarted
        \* Early abort during repair: we keep checking that we have not been fenced
        /\ \A sid \in quorum: servers[sid].epoch =< w.epoch
        \* Uninitialized and failed servers cannot be part of the read quorum for repairs
        \* E.g., newly deployed servers have an empty state and do not know if a LSN exists or not
        \* Exception if all servers are uninitialized as in the beginning
        /\ \/ \A id \in server_ids: servers[id].status = "uninitialized"
           \/ \A sid \in quorum: servers[sid].status = "active"
        \* Find highest consecutive lsn
        /\ LET lsns_servers    == UNION { { entry.lsn : entry \in servers[sid].ledger } : sid \in quorum } 
               lsns_blob_store == UNION { { entry.lsn : entry \in blob_store[sid] } : sid \in server_ids }
               lsns_durable    == lsns_servers \union lsns_blob_store
               max_consecutive == max({n \in 1..max(lsns_durable) : 1..n \subseteq lsns_durable})
               \* Model out of order appends by performing reverse appends (i.e., count down)
               lsn_next        == max_consecutive + APPENDS_INFLIGHT_MAX
               \* Find the next multiple of APPENDS_INFLIGHT_MAX
               lsn_batch       == ( (lsn_next - 1) \div APPENDS_INFLIGHT_MAX + 1 ) * APPENDS_INFLIGHT_MAX
           IN
            writers' = [writers EXCEPT ![client_id] = [@ EXCEPT 
                                                        !.lsn_committed = max_consecutive,
                                                        !.lsn_current = lsn_next,
                                                        !.batch_lsn   = lsn_batch,
                                                        !.status = "writing"]]
    /\ UNCHANGED <<readers, servers, blob_store, faults_injected, meta_epoch>>

\* Start a new batch, increment LSN by APPENDS_INFLIGHT_MAX.
client_send_append_batch(client_id) ==
    LET w == writers[client_id]
    IN
    \* Limit state space by bounding how much appends each client is allowed to do
    /\ Cardinality(w.appends) < APPEND_COUNT_MAX
    \* Client must be in writing state
    /\ w.status = "writing"
    \* A client is only allowed to have MAX_INFLIGHT_APPENDS outstanding/uncommitted
    /\ (w.lsn_current - w.lsn_committed) <= APPENDS_INFLIGHT_MAX
    /\ w.lsn_current % APPENDS_INFLIGHT_MAX = 0
    /\ LET 
        next_lsn       == w.batch_lsn
        request_record == create_append(next_lsn, w.epoch, w.client_id, w.lsn_committed)
       IN 
        \* models:
        \* 1) some msg/requests are always lost
        \* 2) remaining servers will accept the append
        \* 3) either response or reply could be lost, but at the end min a write quorum of round trips will succeed
        \*    -> this implies: a client will retry as long as it receives a write quorum of successful retries
        /\ \E quorum \in { S \in SUBSET server_ids : Cardinality(S) = writer_quorum } :
            \* Models that clients can be fenced if a server's epoch has been updated
            \* We use =< instead of +1 since a server might lag behind or be restarted
            /\ \A sid \in quorum: servers[sid].epoch =< w.epoch
            \* failed servers cannot be part of the quorum
            /\ \A sid \in quorum: servers[sid].status /= "failed"
            /\ servers' = [sid \in server_ids |-> IF /\ sid \in quorum
                                               THEN [servers[sid] EXCEPT 
                                                        \* Since appends can arrive out-of-order, an old client can write an entry with lsn=2 in epoch 1
                                                        \* but get fenced immediately afterward. The new client will then write lsn=2 in epoch 2,
                                                        \* leading to two entries (with the same lsn but a different epoch) in a server's ledger.
                                                        \* Similarly, due to out-of-order appends, a server could theoretically store higher LSNs written in an earlier epoch.
                                                        \* E.g., LSN 3 written in epoch 1, and LSN 1-2 written in epoch 2.
                                                        \* In this case, we can deduce that LSN 3 could not have been committed, and we can truncate (i.e., delete) it.
                                                        !.ledger        = (@ \ { e \in @ : e.lsn >= request_record.lsn /\ e.epoch < request_record.epoch }) \union {request_record},
                                                        \* A server might have missed the epoch update during client failover
                                                        \* Once a server observes a higher epoch, it will adopt it
                                                        !.epoch         = request_record.epoch,
                                                        !.lsn_committed = request_record.last_add_confirmed]
                                               ELSE servers[sid]]
            /\ writers' = [writers EXCEPT ![client_id] = 
                                [@ EXCEPT 
                                    !.lsn_current = w.batch_lsn - 1,
                                    !.batch_lsn   = w.batch_lsn + APPENDS_INFLIGHT_MAX,
                                    !.appends     = @ \union {next_lsn}]]
    /\ UNCHANGED <<readers, blob_store, faults_injected, meta_epoch>>

\* Append within a batch, decrement LSN to simulate out-of-order.
client_send_append_decrement(client_id) ==
    LET w == writers[client_id]
    IN
    \* Limit state space by bounding how much appends each client is allowed to do
    /\ Cardinality(w.appends) < APPEND_COUNT_MAX
    \* Client must be in writing state
    /\ w.status = "writing"
    \* A client is only allowed to have MAX_INFLIGHT_APPENDS outstanding/uncommitted
    /\ (w.lsn_current - w.lsn_committed) <= APPENDS_INFLIGHT_MAX
    \* Limit AppendDecrement to batch boundaries
    /\ w.lsn_current % APPENDS_INFLIGHT_MAX /= 0
    /\ LET
        next_lsn       == w.lsn_current
        request_record == create_append(next_lsn, w.epoch, w.client_id, w.lsn_committed)
       IN
        \* models:
        \* 1) some msg/requests are always lost
        \* 2) remaining servers will accept the append
        \* 3) either response or reply could be lost, but at the end min a write quorum of round trips will succeed
        \*    -> this implies: a client will retry as long as it receives a write quorum of successful retries
        /\ \E quorum \in { S \in SUBSET server_ids : Cardinality(S) = writer_quorum } :
            \* Models that clients can be fenced if a server's epoch has been updated
            \* We use =< instead of +1 since a server might lag behind or be restarted
            /\ \A sid \in quorum: servers[sid].epoch =< w.epoch
            \* failed servers cannot be part of the quorum
            /\ \A sid \in quorum: servers[sid].status /= "failed"
            /\ servers' = [sid \in server_ids |-> IF /\ sid \in quorum
                                               THEN [servers[sid] EXCEPT 
                                                        \* Since appends can arrive out-of-order, an old client can write an entry with lsn=2 in epoch 1
                                                        \* but get fenced immediately afterward. The new client will then write lsn=2 in epoch 2,
                                                        \* leading to two entries (with the same lsn but a different epoch) in a server's ledger.
                                                        \* Similarly, due to out-of-order appends, a server could theoretically store higher LSNs written in an earlier epoch.
                                                        \* E.g., LSN 3 written in epoch 1, and LSN 1-2 written in epoch 2.
                                                        \* In this case, we can deduce that LSN 3 could not have been committed, and we can truncate (i.e., delete) it.
                                                        !.ledger        = (@ \ { e \in @ : e.lsn >= request_record.lsn /\ e.epoch < request_record.epoch }) \union {request_record},
                                                        \* A server might have missed the epoch update during client failover
                                                        \* Once a server observes a higher epoch, it will adopt it
                                                        !.epoch         = request_record.epoch,
                                                        !.lsn_committed = request_record.last_add_confirmed]
                                               ELSE servers[sid]]
            /\ writers' = [writers EXCEPT ![client_id] = 
                                [@ EXCEPT 
                                    !.lsn_current = w.lsn_current - 1,
                                    !.appends     = @ \union {next_lsn}]]
    /\ UNCHANGED <<readers, blob_store, faults_injected, meta_epoch>>

\* Advance lsn_committed.
\* The main "commit requirement" is "durability", which holds if:
\* 1) A majority of servers have the entry (multiple copies) OR
\* 2) The entry can be found in the blob_store (one copy is sufficient)
client_commit_lsn(client_id) ==
    LET w == writers[client_id]
    IN
    /\ w.status = "writing"
    /\ \E n \in w.appends:
        /\ n = w.lsn_committed + 1
           \* Normal case: a LSN is stored on the majority
        /\ \/ server_count_with_entry(n, w.client_id) >= writer_quorum
           \* Special case: finding the LSN in the blob_store allows us to commit it (e.g., as done during fencing/repair)
           \/ blob_store_count_with_entry(n, w.client_id) >= 1
        \* Only update the local state; the LAC is sent to the servers with the next append. This models that the LAC on the servers lag behind
        /\ writers' = [writers EXCEPT ![client_id] = [@ EXCEPT !.lsn_committed = n]]
    /\ UNCHANGED <<servers, readers, blob_store, faults_injected, meta_epoch>>

\* Mark the client as fenced.
client_mark_fenced(client_id) ==
    LET w == writers[client_id]
    IN
    /\ \/ w.status = "writing"
       \/ w.status = "fencing"
       \/ w.status = "repairing"
    /\ \E quorum \in { S \in SUBSET server_ids : Cardinality(S) = writer_quorum } :
        /\ \A sid \in quorum: w.epoch < servers[sid].epoch
        /\ \A sid \in quorum: servers[sid].status = "active"
    /\ writers' = [writers EXCEPT ![client_id] = [@ EXCEPT !.status = "fenced"]]
    /\ UNCHANGED <<servers, readers, blob_store, faults_injected, meta_epoch>>

\* Mark the client as done.
client_mark_done(client_id) ==
    LET w == writers[client_id]
    IN
    /\ Cardinality(w.appends) = APPEND_COUNT_MAX
    /\ w.lsn_committed >= APPEND_COUNT_MAX
    /\ w.status = "writing"
    /\ writers' = [writers EXCEPT ![client_id] = [@ EXCEPT !.status = "done"]]
    /\ UNCHANGED <<servers, readers, blob_store, faults_injected, meta_epoch>>

\* Open the ledger for reading, then read from server or blob_store.
client_open_ledger_for_read(client_id) ==
    LET r == readers[client_id]
    IN
    /\ r.status = "inactive"
    \* Contact a ReadQuorum of nodes to determine the current LAC
    /\ \E quorum \in { S \in SUBSET server_ids : Cardinality(S) = reader_quorum } :
        /\ LET lac == max({ servers[s].lsn_committed : s \in quorum })
           IN
            /\ readers' = [readers EXCEPT ![client_id] = 
                                    [@ EXCEPT !.lsn_read = lac, !.status = "reading"]]
            /\ UNCHANGED <<servers, writers, blob_store, faults_injected, meta_epoch>>

\* Read the next committed LSN from any server.
client_read_next_lsn_servers(client_id) ==
    LET r == readers[client_id]
    IN
    /\ r.status = "reading"
    \* LAC reads can be sent to any single server
    /\ \E s \in server_ids:
        /\ servers[s].lsn_committed > r.lsn_read
        /\ \E entry \in servers[s].ledger:
            /\ entry.lsn = r.lsn_read + 1
            /\ readers' = [readers EXCEPT ![client_id] = [@ EXCEPT !.lsn_read = @ + 1]]
            /\ UNCHANGED <<servers, writers, blob_store, faults_injected, meta_epoch>>

\* Read the next committed LSN from the blob_store.
\* This can happen if the server has flushed already to BlobStore
\* Implementation: The client should send the read request to multiple servers to avoid the high BlobStore latency
client_read_next_lsn_after_flush(client_id) ==
    LET r == readers[client_id]
    IN
    /\ r.status = "reading"
    /\ \E s \in server_ids:
        /\ servers[s].lsn_committed > r.lsn_read
        /\ servers[s].lsn_flushed >= r.lsn_read
        /\ \E entry \in blob_store[s]:
            /\ entry.lsn = r.lsn_read + 1
            /\ readers' = [readers EXCEPT ![client_id] = [@ EXCEPT !.lsn_read = @ + 1]]
            /\ UNCHANGED <<servers, writers, blob_store, faults_injected, meta_epoch>>

\* Model that clients could read from S3 directly
\* This can be relevant if servers are down or unreachable
\* Note: This is not a requirement of the protocol, but an optimization
client_read_blob_store_only(client_id) ==
    LET r == readers[client_id]
        lac_blob_store == UNION { { entry.last_add_confirmed : entry \in blob_store[sid] } : sid \in server_ids }
        lac == max(lac_blob_store)
    IN
    /\ r.status = "reading"
    /\ lac > r.lsn_read
    /\ \E s \in server_ids:
        /\ \E entry \in blob_store[s]:
            /\ entry.lsn <= lac
            /\ entry.lsn = r.lsn_read + 1
            /\ readers' = [readers EXCEPT ![client_id] = [@ EXCEPT !.lsn_read = entry.lsn]]
            /\ UNCHANGED <<servers, writers, blob_store, faults_injected, meta_epoch>>

\* The server can autonomously advance lsn_committed based on MAX_INFLIGHT_APPENDS
\* I.e., using the invariant: LSNs followed by MAX_INFLIGHT_APPENDS+1 are committed
\* Note: The +1 is important since up to (and including) MAX_INFLIGHT_APPENDS can be inflight
\* Example: If MAX_INFLIGHT_APPENDS = 2 and a server got LSNs {1,2,3,4}, it can only mark 2 as committed once it sees 5
server_advance_lsn_invariant(server_id) == 
    LET server == servers[server_id]
        maximum_lsn == max({ e.lsn : e \in server.ledger })
    IN
    /\ server.status = "active"
    /\ \E n \in server.lsn_committed..maximum_lsn:
       /\ n > server.lsn_committed
       /\ \A x \in n+1..n+APPENDS_INFLIGHT_MAX+1: \E entry \in server.ledger: entry.lsn = x
       /\ servers' = [servers EXCEPT ![server_id] = [@ EXCEPT !.lsn_committed = n]]
    /\ UNCHANGED <<writers, readers, blob_store, faults_injected, meta_epoch>>

\* Flush data to blob_store.
server_flush_to_blob_store(server_id) ==
    LET server == servers[server_id]
        maximum_lsn == max({ e.lsn : e \in server.ledger })
    IN
    /\ server.ledger /= {} \* Enabling condition
    \* Perform the flush
    /\ blob_store' = [blob_store EXCEPT ![server_id] = @ \union server.ledger]
    \* Update lsn_flushed
    /\ servers' = [servers EXCEPT ![server_id] = [@ EXCEPT !.lsn_flushed = maximum_lsn, !.ledger = {}]]
    /\ UNCHANGED <<writers, readers, faults_injected, meta_epoch>>

\* Ingest a failure into a server (making it unusable for writes, e.g., crash or network partition)
server_mark_failed(server_id) ==
    LET server == servers[server_id]
    IN
    \* Limit state space, stop injecting faults after a while
    /\ faults_injected < FAULTS_COUNT
    \* Worst case modelling to limit state space: Only inject faults when a server has data (interesting cases)
    /\ server.ledger /= {}
    \* Limit the number of failures as assumed by our fault model (otherwise no progress will be made)
    /\ Cardinality({sid \in server_ids : servers[sid].status = "active"}) > writer_quorum
    /\ server.status = "active"
    /\ servers' = [sid \in server_ids |-> IF sid = server_id
                                            THEN [servers[sid] EXCEPT
                                                    !.status = "failed"]
                                            ELSE servers[sid]
                  ]
    \* Do not flush to blob_store to simulate async flush during which a server could fail
    /\ faults_injected' = faults_injected + 1
    /\ UNCHANGED <<writers, readers, meta_epoch, blob_store>>

\* Other servers detect failure and flush to blob_store
\* The passed parameter is the server that will detect the failure
\* This models that servers might detect a failed peer at different times
server_detect_failure_and_flush(server_id) ==
    LET server == servers[server_id]
    IN
    \* Only healthy server can detect a failure
    /\ server.status /= "failed"
    \* Model that other servers detect failure immediately and flush to blob_store
    \* Flush to blob_store is atomic (ok, since servers will keep data until flush is complete)
    /\ blob_store' = [sid \in server_ids |-> IF sid = server_id
                                                THEN blob_store[sid] \union servers[sid].ledger
                                                ELSE blob_store[sid]
                     ]
    /\ servers' = [sid \in server_ids |-> IF sid = server_id
                                            THEN [servers[sid] EXCEPT
                                                    !.lsn_flushed = max({ e.lsn : e \in servers[sid].ledger }),
                                                    !.ledger = {}]
                                            ELSE servers[sid] 
                  ]
    /\ UNCHANGED <<writers, readers, meta_epoch, faults_injected>>

\* Replace a failed server by a new one
server_replace(server_id) ==
    LET server == servers[server_id]
    IN
    /\ server.status = "failed"
    \* Servers are replaced, hence all data and state is lost (worst-case)
    /\ servers' = [servers EXCEPT ![server_id] = [@ EXCEPT !.status        = "active",
    \* IMPORTANT: A server must bootstrap its epoch from the control plane, otherwise fenced clients could reappear/be able to write
                                                           !.epoch         = meta_epoch,
                                                           !.lsn_committed = 0,
                                                           !.lsn_flushed   = 0,
                                                           !.ledger        = {}]]
    /\ UNCHANGED <<writers, readers, blob_store, faults_injected, meta_epoch>>

all_fenced_or_done ==
    \A c \in writer_ids : writers[c].status \in {"fenced", "done"}

end_state_reached == 
    /\ all_fenced_or_done
    /\ UNCHANGED <<writers, servers, readers, blob_store, faults_injected, meta_epoch>>

Next ==
    \/ end_state_reached
    \/ \E c \in writer_ids: client_acquire_write_token(c)
    \/ \E c \in writer_ids: client_set_write_token_on_servers(c)
    \/ \E c \in writer_ids: client_repair_ledger(c)
    \/ \E c \in writer_ids: client_send_append_batch(c)
    \/ \E c \in writer_ids: client_send_append_decrement(c)
    \/ \E c \in writer_ids: client_mark_fenced(c)
    \/ \E c \in writer_ids: client_mark_done(c)
    \/ \E c \in writer_ids: client_commit_lsn(c) \* Important to make progress otherwise since the client has bound on MAX_INFLIGHT_APPENDS
    \* server actions
    \* Models that a server could fail anytime
    \* (increases state space to 10ths of millions distinct state)
    \/ \E s \in server_ids: \/ server_mark_failed(s)
    \/ \E s \in server_ids: \/ server_detect_failure_and_flush(s)
    \/ \E s \in server_ids: \/ server_replace(s)
    \* Models that servers could flush anytime; this is the most challenging setting for correctness
    \* (increases state space to 100ths of millions distinct state)
    \/ \E s \in server_ids: \/ server_flush_to_blob_store(s)
    \* reader actions
    \* Will further increase state space tremendously
    \/ \E c \in reader_ids: client_open_ledger_for_read(c)
    \* \/ \E c \in reader_ids: client_read_next_lsn_servers(c)
    \* \/ \E c \in reader_ids: client_read_next_lsn_after_flush(c)
    \/ \E c \in reader_ids: client_read_blob_store_only(c)

Spec ==
    /\ Init
    /\ [][Next]_vars \* Stutter-invariance: If Next is possible, it will eventually happen

=============================================================================
