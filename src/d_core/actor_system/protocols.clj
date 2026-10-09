(ns d-core.actor-system.protocols)

(defprotocol ActorClient
  "Client interface for interacting with actors from any service or Duct monolith."
  (tell! [this entity-type entity-id message opts]
    "Asynchronously sends a fire-and-forget message to an entity actor.
     Returns nil or a delivery confirmation.")
  (ask! [this entity-type entity-id message opts]
    "Sends a request message to an entity actor and returns a promise/deferred
     resolving with the reply, or timing out based on :timeout-ms in opts."))

(defprotocol ActorSnapshotStore
  "Pluggable snapshot persistence for virtual actor hydration and passivation."
  (load-snapshot [this entity-type entity-id opts]
    "Loads the latest saved state snapshot map for the entity, or nil if none exists.")
  (save-snapshot! [this entity-type entity-id state opts]
    "Persists a state snapshot map for an entity."))

(defprotocol ActorTransport
  "Pluggable messaging transport between clients and actor runtime hosts."
  (send-message! [this entity-type entity-id envelope opts]
    "Dispatches a fire-and-forget message envelope to the entity mailbox via the transport.")
  (request-message! [this entity-type entity-id envelope opts]
    "Dispatches a request envelope and awaits a response envelope with :timeout-ms.")
  (subscribe-entities! [this entity-type handler-fn opts]
    "Subscribes the runtime to incoming entity messages. Returns a subscription handle.")
  (unsubscribe! [this subscription opts]
    "Cancels the subscription."))

(defprotocol ActorRuntime
  "Lifecycle and state manager for virtual actors running on this host."
  (active-actors [this opts]
    "Returns a summary map or collection of currently active entity actors in memory.")
  (passivate-entity! [this entity-type entity-id opts]
    "Explicitly passivates an active entity actor, saving its snapshot and freeing memory.")
  (get-actor-state [this entity-type entity-id opts]
    "Returns the current in-memory state of an active local entity actor, or nil if inactive."))
