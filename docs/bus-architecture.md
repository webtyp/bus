# Bus Architecture: webtyp/bus

## Overview

The `webtyp/bus` package provides a unified Pub/Sub hub for inter-module communication. It is designed to work identically across all WebTyp targets: standard Go servers, TinyGo-compiled WASI modules, and browser-based WebAssembly.

To minimize binary size and runtime overhead in TinyGo environments, this implementation uses **slices** instead of maps for topic storage and subscriber management.

## Key Design Decisions

1. **Single Implementation (No Platform Split)**: Unlike other webtyp libraries that split behavior between `_backend.go` and `_wasm.go`, the bus uses a single `bus.go` file. The core logic (slices + `sync.RWMutex`) is efficient enough for both platforms and reduces maintenance complexity.
2. **Slices over Maps**: TinyGo's map implementation is significantly heavier than slice operations for small collections. Since most applications use a limited number of topics, O(n) slice scans offer the best trade-off between performance and binary size.
3. **Thread Safety**: Uses `sync.RWMutex` to ensure safe concurrent access. TinyGo supports mutexes and cooperative goroutines, making this pattern safe for browser and WASI targets.
4. **Non-blocking Dispatch**: `Publish` dispatches handlers in separate goroutines (`go handler(msg)`). This ensures that a slow or panicking subscriber does not block the entire bus or other subscribers.

## API Design

```go
package bus

import "webtyp.com/binary"

type Bus interface {
    // Subscribe registers a handler for a topic.
    // Returns a Subscription handle to cancel the registration.
    Subscribe(topic string, handler func(msg binary.Message)) Subscription

    // Publish sends a message to all subscribers of a topic.
    Publish(topic string, msg binary.Message) error

    // Topics returns a sorted list of all active topics.
    Topics() []string

    // Close shuts down the bus and clears all registrations.
    Close() error
}

type Subscription interface {
    Topic() string
    Cancel()
}
```

## Internal Structure

```go
type topicEntry struct {
    topic string
    subs  []subscriber
}

type subscriber struct {
    id      uint32
    handler func(msg binary.Message)
}

type bus struct {
    mu     sync.RWMutex
    topics []topicEntry
    nextID uint32
}
```

## WASI Module Interaction

WASI modules (e.g., `modules/*/wasm/main.go`) do **NOT** import the `bus` package directly. Instead, they interact with the host-side bus through host functions provided by the server:

- `hostPublish(topic, message)`
- `hostSubscribe(topic)`

This maintains strict isolation and allows the server to manage security and routing logic centrally.

## Build Tag Semantics

For documentation and library development within the WebTyp ecosystem, we follow these build tag conventions:

| Build Tag | Runtime Environment | Description |
|-----------|---------------------|-------------|
| `//go:build !wasm` | Standard Go Server | Code that runs on the host (e.g., wazero host, HTTP server). |
| `//go:build wasm` | Browser (TinyGo) | Code compiled for the browser target. |
| *No tag* | Both Platforms | Shared logic, interfaces, and types. |

**Note on WASI**: Modules compiled for WASI (`modules/*/wasm/`) are conceptually backend code but are compiled with TinyGo. They use `!wasm` tagged libraries when appropriate but interact with the `bus` exclusively via host functions.

## Broker vs Bus

- **Bus (`webtyp/bus`)**: Immediate inter-module event routing. Publish triggers callbacks instantly.
- **Broker (`webtyp/broker`)**: Batched event delivery. Enqueues items to be flushed after a time window or reaching a limit.

They are complementary: a `broker` can be placed on top of a `bus` to debounce and batch frequent events for UI updates or persistent storage.
