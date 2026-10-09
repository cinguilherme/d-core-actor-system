# D-Core Actor System

A lightweight, data-driven **Virtual Actor System** for Clojure and Duct, built on **Java 21 Virtual Threads** and message fabrics.

---

## Why Actors in D-Core?

In typical web architectures, high-contention resources (e.g. auctions, bank accounts, inventory counters, live gaming lobbies, active payment sessions) suffer from database contention:
- Row-level lock contention (`SELECT ... FOR UPDATE`)
- Optimistic locking retries (`409 Conflict`)
- Latency and database connection pool exhaustion

`d-core-actor-system` solves this by routing requests for the same entity to a **single in-memory Virtual Actor**:
1. **Serialized In-Memory State**: Only one thread processes an entity's messages at any time. Zero database locks.
2. **Sub-millisecond Latency**: Mutations happen in RAM via pure transition functions.
3. **Virtual Actor Lifecycle**: Entities automatically **hydrate** from storage on their first message and **passivate** (persist snapshot and unload from RAM) when idle.
4. **Flexible Topology**: Run collocated inside your existing Duct monolith, or scale horizontally across worker pods using distributed transports like NATS.

---

## Architectural Components

```
                   ┌───────────────────────────────────┐
                   │           Duct Monolith           │
                   │   (HTTP Handlers / Services)      │
                   └─────────────────┬─────────────────┘
                                     │
                                     │ (tell! or ask!)
                                     ▼
═════════════════════════════════════════════════════════════════════════════
         Actor Transport (In-Memory for Monolith, or NATS for Cluster)
═════════════════════════════════════════════════════════════════════════════
                                     │
                                     ▼
                   ┌───────────────────────────────────┐
                   │        Actor Runtime Host         │
                   │                                   │
                   │  ┌─────────────────────────────┐  │
                   │  │ Virtual Thread Actor Loop   │  │
                   │  │ (LinkedTransferQueue)       │  │
                   │  │ In-memory State Atom        │  │
                   │  └──────────────┬──────────────┘  │
                   └─────────────────┼─────────────────┘
                                     │ (Hydrate on first msg / Passivate on idle)
                                     ▼
                   ┌───────────────────────────────────┐
                   │    d-core StorageProtocol / DB    │
                   └───────────────────────────────────┘
```

---

## Core Principles

- **Pure Step Functions**: State machines are pure functions `(step-fn state message context) -> [next-state reply effects]`. Easy to unit test without mocks or network.
- **Java 21 Virtual Threads**: Millions of actors can be active simultaneously with negligible JVM memory overhead.
- **D-Core Protocol Standards**: Stable protocols (`ActorClient`, `ActorSnapshotStore`, `ActorTransport`, `ActorRuntime`) with options maps as the last argument.
- **Integrant / Duct Native**: Declarative configuration via Integrant keys.

---

## Defining an Entity Behavior

An entity behavior is defined with pure functions:

```clojure
(ns my-app.actors.auction)

(defn init-state [auction-id]
  {:id auction-id
   :status :open
   :highest-bid 0
   :highest-bidder nil})

(defn handle-message [state msg ctx]
  (case (:op msg)
    :bid
    (let [{:keys [bidder amount]} msg]
      (if (and (= :open (:status state))
               (> amount (:highest-bid state)))
        (let [next-state (assoc state :highest-bid amount :highest-bidder bidder)]
          [next-state {:ok true :highest-bid amount :bidder bidder}])
        [state {:ok false :error "Bid too low or auction closed"}]))

    :close
    [(assoc state :status :closed) {:ok true :status :closed :winning-bid (:highest-bid state)}]

    :status
    [state {:ok true :state state}]

    [state {:ok false :error "Unknown operation"}]))

(def auction-spec
  {:init-fn init-state
   :step-fn handle-message
   :idle-timeout-ms 300000        ;; 5 minutes idle -> passivate to storage
   :snapshot-every-n 50})         ;; Snapshot every 50 mutations
```

---

## Integrant / Duct Configuration (`duct.edn`)

### Monolith / Local Mode (In-Memory Transport)

```edn
{:system
 {;; Storage snapshot persistence (In-memory or d-core StorageProtocol)
  :d-core.actor-system/storage
  {:type :storage-protocol
   :backend #ig/ref :d-core.core.storage/local-disk}

  ;; In-memory local transport
  :d-core.actor-system/transport
  {:type :in-memory}

  ;; Client to tell/ask actors
  :d-core.actor-system/client
  {:transport #ig/ref :d-core.actor-system/transport}

  ;; Runtime host managing active virtual threads
  :d-core.actor-system/runtime
  {:transport #ig/ref :d-core.actor-system/transport
   :storage #ig/ref :d-core.actor-system/storage
   :entities {:auction #ig/ref :my-app.actors.auction/spec}
   :options {:watchdog-interval-ms 5000
             :default-idle-timeout-ms 300000}}}}
```

### Distributed Mode (NATS Transport)

```edn
{:system
 {;; NATS connection
  :d-core.actor-system/transport
  {:type :nats
   :nats-connection #ig/ref :d-core.core.clients/nats
   :options {:subject-prefix "actors"
             :queue-group "actor-workers"}}

  :d-core.actor-system/client
  {:transport #ig/ref :d-core.actor-system/transport}

  :d-core.actor-system/runtime
  {:transport #ig/ref :d-core.actor-system/transport
   :storage #ig/ref :d-core.actor-system/storage
   :entities {:auction #ig/ref :my-app.actors.auction/spec}}}}
```

---

## Client Usage

```clojure
(require '[d-core.actor-system.core :as actors])

;; Fire-and-forget (tell)
(actors/tell! client :auction "art-101" {:op :bid :bidder "Alice" :amount 150})

;; Request-reply async (returns a Future)
(let [fut (actors/ask! client :auction "art-101" {:op :status} {:timeout-ms 2000})]
  (println "Status:" @fut))

;; Request-reply sync (convenience blocking helper)
(let [reply (actors/ask-sync! client :auction "art-101" {:op :close})]
  (println "Closed:" reply))
```

---

## Development & Testing

```bash
make tests    # Run all unit and integration tests
make lint     # Run clj-kondo linter
make build    # Build JAR artifact
make install  # Install to local ~/.m2
```
