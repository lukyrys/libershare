# LiberShare Online Catalog (DB LISHs) - Architecture Analysis

**Date**: 2026-02-28 (updated 2026-03-15)
**Branch**: `feat/online-db`
**Status**: Complete — ready for Phase 1 implementation
**Author**: Analysis by Claude, discussed with Jiri Kreibich
**Research sources**: libp2p source code, IPFS Cluster, go-ds-crdt, Nostr NIPs, Matrix, Farcaster, BitTorrent BEP-52, gossipsub v1.1 spec

---

## 1. Problem Statement

LiberShare currently has no way for peers to discover what content is available in a network. The Products/Library page is a hardcoded mockup with 200 placeholder items. To make the app useful, each lishnet needs a **shared, replicated catalog** of available LISHs that all peers can browse and search.

### Requirements

- Each lishnet has its own independent catalog
- Catalog replicates to all peers in the network
- Write access controlled by designated moderators (appointed by network admin)
- ACL (who has what permissions) replicates alongside the data
- Downloads and seeding remain open to everyone (no P2P DRM)
- Resistant to manipulation by unauthorized third parties
- Works offline-first - peers can go offline and sync back later

---

## 2. Technology Evaluation

### Candidates Evaluated

| Technology | Verdict | Key Reason |
|---|---|---|
| **OrbitDB** | Rejected | Requires Helia/IPFS node, can't share existing libp2p instance, 60+ dependencies |
| **Automerge** | Rejected | 604 KB WASM, designed for collaborative text editing, no access control, overkill for immutable catalog entries |
| **Yjs** | Rejected | Collaborative editing tool, tombstone overhead unnecessary, poor documentation |
| **GUN** | Rejected | Wall-clock LWW broken under clock skew, incompatible transport layer, requires separate relay infrastructure |
| **Hypercore/Autobase** | Rejected | Hyperswarm incompatible with libp2p, Autobase immature (141 stars), no TypeScript |
| **cr-sqlite** | Rejected | Transport-agnostic SQLite CRDT, but "assumes friendly actors", 2.5x write overhead, maintenance slowed |
| **automerge-repo** | Rejected | Pluggable transport, actively maintained, but stores binary blobs — not queryable SQL tables |
| **ElectricSQL** | Rejected | Requires central Electric server + Postgres — not P2P |
| **libSQL/Turso** | Rejected | Single-writer remote primary — not P2P, no CRDT |
| **PowerSync** | Rejected | Requires PowerSync Service — not P2P |
| **Triplit** | Rejected | Requires Triplit server, WebSocket-only — not P2P |
| **Jazz (CoJSON)** | Rejected | Requires Jazz sync server, no SQLite, no libp2p |
| **Evolu** | Rejected | Single-user multi-device model, needs relay — wrong identity model for shared catalog |
| **Ditto** | Rejected | Commercial closed-source, proprietary transport, no libp2p integration |
| **RxDB** | Rejected | SQLite adapter is paywalled (paid subscription), WebRTC P2P needs signaling server |
| **Custom 2P-Set CRDT** | **Selected** | Zero dependencies, fits existing libp2p + bun:sqlite stack, full control over untrusted-peer security |

### Why No Library Works for Untrusted P2P

A comprehensive evaluation of 14 CRDT/sync libraries (March 2026) revealed a universal gap: **no library provides Byzantine-safe CRDT for untrusted peers**. All assume collaborative/trusted participants.

**cr-sqlite** (3.7k stars) is the closest — it provides transport-agnostic changesets as SQL rows via `crsql_changes` virtual table. However:
- Its author explicitly states: *"crsqlite assumes friendly actors and needs to guard against malicious updates"*
- Even with cr-sqlite, you must still implement: Ed25519 signature verification, ACL chain validation, HLC drift protection, rate limiting — which is 90% of the work
- 2.5x write overhead vs plain SQLite (metadata tables + triggers for each CRR table)
- Native extension loading in Bun (`db.loadExtension()`) is not production-tested for cr-sqlite
- Development has slowed (last main branch commit May 2024, last release Jan 2025)

**automerge-repo** (665 stars, actively maintained) has a clean pluggable NetworkAdapter for libp2p, but stores CRDT state as opaque binary documents — catalog search queries cannot run as SQL without a materialization layer.

All server-dependent solutions (ElectricSQL, Turso, PowerSync, Triplit, Jazz, Evolu) are disqualified by the P2P requirement.

### Why Custom CRDT Wins

The catalog is semantically simple:
- **Immutable entries** keyed by UUID (no per-field conflicts)
- **Single-writer per role** (moderators, not all peers simultaneously)
- **Append-mostly** with rare deletions
- **Small metadata** (~500 bytes per entry, full LISH fetched on demand)

This is a **signed 2P-Set** (grow-only set of additions + grow-only set of deletions), the simplest possible CRDT. The LWW merge is a single SQL `INSERT ON CONFLICT DO UPDATE WHERE` statement (see §6). No library needed.

The hardest part of the implementation — untrusted peer security (signatures, ACL, anti-replay) — must be custom-built regardless of CRDT library choice. Since the CRDT itself is trivial (~150 lines of SQL functions), adding a library would increase complexity without reducing the security work.

---

## 3. Architecture

### High-Level Overview

```
┌──────────────────────────────────────────────────────────────┐
│              Lishnet (gossipsub topic: lish/<networkID>)      │
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │  REPLICATED STATE (synced to all peers via CRDT)        │ │
│  │                                                          │ │
│  │  1. Catalog: Map<lishID, CatalogEntry>                  │ │
│  │     - LISH summaries (name, size, publisher, timestamp) │ │
│  │     - Full LISH fetched on-demand via get_lish_req      │ │
│  │                                                          │ │
│  │  2. Tombstones: Map<lishID, TombstoneEntry>             │ │
│  │     - Removed entries (with signature proof)            │ │
│  │                                                          │ │
│  │  3. ACL: ICatalogAccess                                 │ │
│  │     - Roles: owner, admins, moderators                  │ │
│  │     - restrictCatalogWrites flag                        │ │
│  │                                                          │ │
│  │  4. Vector Clock: Map<peerID, HLC>                     │ │
│  │     - Tracks what each peer has seen                    │ │
│  └─────────────────────────────────────────────────────────┘ │
│                                                               │
│  SYNC CHANNELS:                                               │
│  - GossipSub broadcast: real-time ops (add/remove/acl)       │
│  - Bilateral stream /lish/catalog-sync/1.0.0: peer catch-up  │
└──────────────────────────────────────────────────────────────┘
```

### Data Structures

#### Hybrid Logical Clock (used throughout for ordering)

```typescript
interface HLC {
  wallTime: number;    // milliseconds since epoch, max(local, received)
  logical: number;     // tiebreaker within same wallTime
  nodeID: string;      // PeerID as final tiebreaker for total order
}
```

See section 4.5 for the full HLC implementation (tick, merge, compare).

#### Catalog Entry (replicated summary, ~500 bytes)

```typescript
interface CatalogEntry {
  // === Immutable fields (set on creation, never changed) ===
  lishID: string;              // UUID - immutable key
  publisherPeerID: string;     // Who originally published this entry
  publishedAt: string;         // ISO 8601 timestamp of first publish
  chunkSize: number;           // From LISH manifest
  checksumAlgo: string;        // From LISH manifest
  fileCount: number;           // Derived from LISH
  totalSize: number;           // Derived from LISH (bytes)
  manifestHash?: string;       // SHA256 of LISH manifest for integrity

  // === Editable metadata (any moderator+ can update) ===
  name?: string;               // Human-readable name
  description?: string;        // Optional description
  contentType?: 'software' | 'game' | 'video' | 'audio' | 'image' | 'document' | 'dataset' | 'archive' | 'other';
  tags?: string[];             // max 10 tags, max 32 chars each, lowercase

  // === System fields (updated automatically) ===
  hlc: HLC;                   // Hybrid Logical Clock for LWW ordering
  signature: string;           // Ed25519 signature of current state
  lastEditedBy?: string;       // PeerID of last editor (undefined = never edited after creation)
}
```

**Important**: The catalog stores only summaries. The full LISH manifest (with chunk hashes, file paths, etc.) is fetched on-demand via the existing `get_lish_req`/`get_lish_res` protocol.

#### Tombstone Entry

```typescript
interface TombstoneEntry {
  lishID: string;
  removedByPeerID: string;
  removedAt: string;           // ISO 8601
  hlc: HLC;                   // Hybrid Logical Clock
  signature: string;           // Ed25519 signature of removal
}
```

#### Access Control

```typescript
interface ICatalogAccess {
  owner: string;                // PeerID - immutable, encoded in lishnet config
  admins: string[];             // PeerIDs - managed by owner
  moderators: string[];         // PeerIDs - managed by owner + admins
  restrictCatalogWrites: boolean; // false = open catalog, true = moderators only
}
```

**Role hierarchy:**

| Role | Edit network config | Manage admins | Manage moderators | Write catalog | Read catalog | Download/seed |
|---|---|---|---|---|---|---|
| **Owner** | Yes | Yes | Yes | Yes | Yes | Yes |
| **Admin** | No | No | Yes | Yes | Yes | Yes |
| **Moderator** | No | No | No | Yes | Yes | Yes |
| **Peer** (open mode) | No | No | No | Yes | Yes | Yes |
| **Peer** (restricted) | No | No | No | **No** | Yes | Yes |

#### CRDT State

```typescript
interface CatalogCRDTState {
  networkID: string;
  entries: Map<string, CatalogEntry>;
  tombstones: Map<string, TombstoneEntry>;
  opLog: Map<string, SignedCatalogOp>;  // lishID -> last SignedCatalogOp (for bilateral sync verification)
  access: ICatalogAccess;
  vectorClock: Map<string, HLC>;     // peerID -> highest HLC seen from that peer
  localClock: HLC;                   // this peer's current HLC
  syncState: Map<string, HLC>;      // peerID -> last HLC synced with that peer (for delta sync)
}
// opLog stores the most recent SignedCatalogOp for each entry/tombstone.
// This enables bilateral sync peers to verify signatures — the full
// CatalogOpPayload (including nonce) is preserved, not just the derived CatalogEntry.
```

---

## 4. Security Model

### Threat Model

In a decentralized P2P network, any peer can connect and attempt to:

1. **Inject fake catalog entries** (spam, malware links)
2. **Forge ACL changes** (grant themselves admin/moderator role)
3. **Delete legitimate entries** (censorship by unauthorized peer)
4. **Replay old valid messages** (re-broadcast previously valid operations)
5. **Impersonate another peer** (use someone else's PeerID)
6. **Eclipse attack** (isolate a node and feed it false state)
7. **Tamper with sync data** (modify catalog during bilateral catch-up)

### Security Guarantees

#### 4.1 Cryptographic Identity (PeerID = Public Key)

libp2p already provides this. Each peer has an Ed25519 keypair:
- **PeerID** is derived from the public key (unforgeable)
- **Private key** is stored locally in `datastore.db` (libp2p's peer store, separate from the app's `libershare.db`)
- **Noise protocol** encrypts and authenticates all connections

**Implication**: A peer cannot impersonate another peer. PeerID is cryptographically bound to the keypair.

#### 4.2 Signed Operations (Every Write is Signed)

Every catalog operation (add, update, remove, ACL change) MUST include an Ed25519 signature from the author's private key. Receiving peers verify the signature before applying the operation.

```typescript
// Conceptual model — see section 11 for the canonical implementation type (SignedCatalogOp)
interface SignedOperation {
  op: 'add' | 'update' | 'remove' | 'acl_grant' | 'acl_revoke';
  payload: CatalogEntry | CatalogUpdate | TombstoneEntry | ACLChange;
  authorPeerID: string;        // Who created this operation (called `signer` in SignedCatalogOp)
  hlc: HLC;                   // Hybrid Logical Clock (monotonically increasing per author)
  signature: string;           // Ed25519 sign(payload + authorPeerID + hlc)
}

// Partial update of editable metadata fields
interface CatalogUpdate {
  lishID: string;              // Which entry to update
  fields: {
    name?: string;
    description?: string;
    contentType?: CatalogEntry['contentType'];
    tags?: string[];
  };
}

interface ACLChange {
  action: 'grant' | 'revoke';
  role: 'admin' | 'moderator';
  peerIDs: string[];
}
```

**Note**: `SignedOperation` here is the conceptual model for validation logic. The concrete implementation type is `SignedCatalogOp` (section 11), which wraps the operation in a `CatalogOpPayload` with additional fields (`networkID`, `nonce`) for cross-network replay resistance. During implementation, `validateOperation()` receives a `SignedCatalogOp` and extracts `op.payload.type` as the operation type, `op.signer` as the author PeerID, and `op.payload.hlc` as the HLC.

**What is signed**: The signature covers the **entire payload** (including type, networkID, HLC, nonce, and data), serialized as canonical JSON (sorted keys, no whitespace, via `json-canonicalize`). This prevents:

- **Payload tampering**: Changing any field invalidates the signature
- **Author spoofing**: Only the real author's private key can produce a valid signature
- **Clock manipulation**: HLC is part of the signed data

#### 4.3 Authorization Chain (Chain of Trust)

```
Network creation:
  Owner PeerID is embedded in the .lishnet file (immutable, distributed out-of-band)

Trust chain:
  Owner --signs--> "PeerID_X is admin"     (ACL operation, signed by owner)
  Admin  --signs--> "PeerID_Y is moderator" (ACL operation, signed by admin)
  Moderator --signs--> "add LISH Z"         (catalog operation, signed by moderator)
  Moderator --signs--> "update LISH Z name" (metadata edit, any moderator+ can edit any entry)
```

Every peer can verify the full chain:
1. Owner PeerID comes from the `.lishnet` config (trusted, imported by user)
2. Admin list is signed by owner → verify owner's signature
3. Moderator list is signed by owner or admin → verify signer is in admin list
4. Catalog write is signed by author → verify author has write permission

#### 4.4 Operation Validation Rules

**Note**: This section uses the conceptual `SignedOperation` interface for readability. The canonical implementation uses `SignedCatalogOp` field names — see section 16.4 for the canonical `validateOperation()` with `op.signer`, `op.payload.type`, `op.payload.hlc`, and `op.payload.networkID` checks.

Every received operation goes through validation before being applied:

```typescript
function validateOperation(op: SignedOperation, currentACL: ICatalogAccess): ValidationResult {
  // 1. Verify Ed25519 signature
  if (!verifySignature(op.payload, op.authorPeerID, op.signature)) {
    return { valid: false, reason: 'INVALID_SIGNATURE' };
  }

  // 2. Check authorization based on operation type
  switch (op.op) {
    case 'add':
      // If restricted, only owner/admin/moderator can add
      if (currentACL.restrictCatalogWrites) {
        if (!isOwnerOrAdminOrModerator(op.authorPeerID, currentACL)) {
          return { valid: false, reason: 'UNAUTHORIZED_ADD' };
        }
      }
      break;

    case 'update':
      // Only owner/admin/moderator can edit metadata (regardless of restrictCatalogWrites)
      if (!isOwnerOrAdminOrModerator(op.authorPeerID, currentACL)) {
        return { valid: false, reason: 'UNAUTHORIZED_UPDATE' };
      }
      // Verify only editable fields are being changed
      const update = op.payload as CatalogUpdate;
      const allowedFields = ['name', 'description', 'contentType', 'tags'];
      if (Object.keys(update.fields).some(k => !allowedFields.includes(k))) {
        return { valid: false, reason: 'IMMUTABLE_FIELD_UPDATE' };
      }
      break;

    case 'remove':
      // Only owner/admin/moderator can remove
      if (!isOwnerOrAdminOrModerator(op.authorPeerID, currentACL)) {
        return { valid: false, reason: 'UNAUTHORIZED_REMOVE' };
      }
      break;

    case 'acl_grant':
    case 'acl_revoke':
      const change = op.payload as ACLChange;
      if (change.role === 'admin') {
        // Only owner can manage admins
        if (op.authorPeerID !== currentACL.owner) {
          return { valid: false, reason: 'ONLY_OWNER_CAN_MANAGE_ADMINS' };
        }
      } else if (change.role === 'moderator') {
        // Owner or admin can manage moderators
        if (!isOwnerOrAdmin(op.authorPeerID, currentACL)) {
          return { valid: false, reason: 'UNAUTHORIZED_ACL_CHANGE' };
        }
      }
      // Anti-escalation: cannot grant role you don't hold (Matrix Rule 9)
      if (op.op === 'acl_grant') {
        const authorRole = getRole(op.authorPeerID, currentACL);
        if (roleLevel(change.role) >= roleLevel(authorRole)) {
          return { valid: false, reason: 'ANTI_ESCALATION_VIOLATION' };
        }
      }
      break;
  }

  // 3. Check HLC (anti-replay)
  const lastSeen = vectorClock.get(op.authorPeerID);
  if (lastSeen && hlcCompare(op.hlc, lastSeen) <= 0) {
    return { valid: false, reason: 'REPLAY_DETECTED' };
  }

  // 4. Check clock drift (max 60 seconds into the future)
  if (op.hlc.wallTime > Date.now() + 60_000) {
    return { valid: false, reason: 'CLOCK_DRIFT_TOO_HIGH' };
  }

  return { valid: true };
}
```

#### 4.5 Anti-Replay Protection (Hybrid Logical Clocks)

**Pure Lamport clocks** are insufficient for total ordering across peers — they only provide causal ordering. Research across multiple P2P systems (CockroachDB, go-ds-crdt, SSB) shows **Hybrid Logical Clocks (HLC)** are the right choice for catalog operations:

```typescript
interface HLC {
  wallTime: number;    // max(local wall-clock, received wall-clock)
  logical: number;     // tiebreaker within same wallTime
  nodeID: string;      // PeerID as final tiebreaker
}

// Comparison: (wallTime, logical, nodeID) — deterministic total order
function hlcCompare(a: HLC, b: HLC): number {
  if (a.wallTime !== b.wallTime) return a.wallTime - b.wallTime;
  if (a.logical !== b.logical) return a.logical - b.logical;
  return a.nodeID.localeCompare(b.nodeID);
}

// Tick on local event (publish, remove, ACL change)
function hlcTick(local: HLC): HLC {
  const now = Date.now();
  if (now > local.wallTime) {
    return { wallTime: now, logical: 0, nodeID: local.nodeID };
  }
  return { wallTime: local.wallTime, logical: local.logical + 1, nodeID: local.nodeID };
}

// Merge on receiving remote event
function hlcMerge(local: HLC, remote: HLC): HLC {
  const now = Date.now();
  const maxWall = Math.max(now, local.wallTime, remote.wallTime);
  if (maxWall === now && now > local.wallTime && now > remote.wallTime) {
    return { wallTime: now, logical: 0, nodeID: local.nodeID };
  }
  if (maxWall === local.wallTime && local.wallTime === remote.wallTime) {
    return { wallTime: maxWall, logical: Math.max(local.logical, remote.logical) + 1, nodeID: local.nodeID };
  }
  if (maxWall === local.wallTime) {
    return { wallTime: maxWall, logical: local.logical + 1, nodeID: local.nodeID };
  }
  return { wallTime: maxWall, logical: remote.logical + 1, nodeID: local.nodeID };
}
```

**Why HLC over pure Lamport clocks**:
- **Wall-clock correlation**: Operations within the same second get meaningful timestamps (useful for "published at" display)
- **Clock skew tolerance**: `max()` operation absorbs up to ~1 minute of clock drift
- **Bounded drift**: If `wallTime` diverges from real time by > MAX_DRIFT (5 minutes), reject the operation — prevents time-travel attacks where a peer sets their clock far into the future to always win LWW
- **Same anti-replay**: Each operation's HLC must be strictly greater than the last seen from that author
- **Deterministic tiebreaking**: When `wallTime` AND `logical` are equal (extremely rare — two peers, same millisecond, same operation), `nodeID` (PeerID string comparison) provides the final tiebreaker. This ensures all peers reach the same LWW outcome without coordination

**Per-author tracking** (vector clock of HLCs):

```
Vector Clock state: { PeerA: HLC(5,0), PeerB: HLC(12,3), PeerC: HLC(3,1) }

Incoming operation: { author: PeerB, hlc: HLC(10,0) }
→ REJECTED: HLC(10,0) < HLC(12,3) — already seen higher clock from PeerB

Incoming operation: { author: PeerB, hlc: HLC(13,0) }
→ ACCEPTED: HLC(13,0) > HLC(12,3), update vector clock
```

**Critical**: The vector clock MUST be persisted to disk and reloaded on restart. Without this, a node restart resets the clock and re-accepts all replayed operations.

#### 4.6 Transport Security (libp2p Noise)

All libp2p connections are encrypted and authenticated via the **Noise protocol**:
- **Encryption**: ChaCha20-Poly1305 (or AES-256-GCM)
- **Authentication**: Peer identity verified during handshake
- **Integrity**: AEAD prevents message tampering in transit

This means bilateral sync streams (`/lish/catalog-sync/1.0.0`) are automatically protected against man-in-the-middle and tampering.

#### 4.7 Eclipse Attack Mitigation

An eclipse attack isolates a node by surrounding it with attacker-controlled peers. Mitigations:

- **Bootstrap peers from .lishnet config**: User-imported, trusted starting points
- **Kademlia DHT diversity**: DHT routing table ensures peers from different network regions
- **Multiple sync sources**: Request catalog sync from multiple random peers, cross-validate
- **Signature verification**: Even if eclipsed, attacker cannot forge valid signed operations without the real moderator's private key

#### 4.8 Tombstone Garbage Collection

Tombstones (deletion records) cannot be removed immediately - late-joining peers need them to know that an entry was deleted.

**Updated strategy** (based on research into go-ds-crdt, SSB, IPFS Cluster):

Causal stability ("all peers have seen the deletion") is **impractical in a churn-heavy P2P network** — you can never confirm "all peers" when nodes join and leave freely. Use **time-based GC** instead:

```
Tombstone can be garbage-collected when:
  Age > GC_THRESHOLD (default: 30 days)

Documented limitation:
  If a peer is offline for > 30 days and reconnects, deleted entries
  may briefly reappear until the next full catalog sync completes.
  Since no authorized moderator is signing new operations for that
  entry, it will not propagate — the stale entry exists only locally
  until the next bilateral sync resolves it.
```

This is the approach used by all production systems (go-ds-crdt, IPFS Cluster, Tribler). The Tribler team also uses a per-network entry cap (default 50K) which implicitly limits tombstone growth.

**Advanced option — Epoch-based GC** (for networks with very active catalogs):

Epochs group tombstones into time-bounded windows. Once all active peers have advanced past an epoch, the entire epoch of tombstones can be dropped atomically:

```
Epoch structure:
  epoch_0: tombstones from day 0-7
  epoch_1: tombstones from day 7-14
  epoch_2: tombstones from day 14-21
  epoch_3: tombstones from day 21-28

Each peer includes their "minimum epoch" in sync handshakes.
When ALL connected peers report min_epoch >= N, epochs 0..N-1 can be GC'd.

Lazy application: epochs are checked only during bilateral sync, not on every message.
For networks with < 10K entries, simple time-based GC is sufficient.
```

This is more complex to implement and only needed if tombstone volume becomes a storage concern (>50K tombstones). For Phase 1, use simple time-based GC.

#### 4.9 GossipSub Peer Scoring (Defense in Depth)

**Current problem**: The gossipsub config in `network-config.ts` has D=2, Dlo=1, Dhi=3 and **no peer scoring**. This is below spec minimum (D=6) and means any peer can flood invalid messages with no consequence.

**Required gossipsub hardening** (from gossipsub v1.1 spec analysis):

```typescript
// Recommended gossipsub scoring parameters for lishnet topics
scoreParams: {
  topics: {
    [`lish/${networkID}`]: {
      topicWeight: 1.0,
      // P4: penalty for messages that fail validation
      invalidMessageDeliveriesWeight: -50,
      invalidMessageDeliveriesDecay: 0.9,
      // P3: penalty for not forwarding messages
      meshMessageDeliveriesWeight: -0.5,
      meshMessageDeliveriesDecay: 0.95,
      meshMessageDeliveriesThreshold: 1,
    }
  },
  // P6: IP colocation penalty (Sybil resistance)
  IPColocationFactorWeight: -10,
  IPColocationFactorThreshold: 3,
  // P5: application-specific score (catalog validation)
  appSpecificScore: (peerId) => catalogReputationScore(peerId),
},
scoreThresholds: {
  gossipThreshold: -10,      // below: stop gossip exchange
  publishThreshold: -40,     // below: reject publishes
  graylistThreshold: -80,    // below: drop all RPCs
}
```

**P5 Application-specific scoring** for catalog operations:

| Event | Score change |
|---|---|
| Valid signed catalog operation received | +0.1 |
| Invalid signature on catalog operation | -5.0 |
| Unauthorized ACL attempt | -3.0 |
| Rate limit violation | -1.0 |
| Score decay half-life | 1 hour |

Peers below `gossipThreshold` (-10) are progressively isolated without requiring global coordination.

#### 4.10 Anti-Spam Measures

**Per-publisher write quotas** (enforced locally by each node):

```typescript
// See section 17.3 for the full RateLimiter class implementation.
// Constants defined there:
//   maxOpsPerPeerPerMinute: 10
//   maxOpsGlobalPerMinute: 100
//   maxEntriesPerPublisher: 1000
//   maxCatalogSize: 50_000

// Quick reference for validation logic:
function checkPublisherQuota(publisherPeerID: string, entries: Map<string, CatalogEntry>): boolean {
  let count = 0;
  for (const entry of entries.values()) {
    if (entry.publisherPeerID === publisherPeerID) count++;
  }
  return count < 1000;  // MAX_ENTRIES_PER_PUBLISHER
}
```

**GossipSub validator responses**:
- `REJECT`: invalid signature, forged ACL → penalizes forwarding peer (P4 score)
- `IGNORE`: rate limit exceeded, quota exceeded → no penalty to forwarding peer
- `ACCEPT`: valid, authorized operation

**Optional PoW for open networks** (when `restrictCatalogWrites: false`):

For networks without publisher restrictions, require NIP-13-style Proof of Work:
```
sha256(lishID + publisherPeerID + nonce) must have N leading zero bits
Default: N=16 (65K hashes avg, ~10ms on desktop, acceptable on mobile)
Increase to N=20 (~1M hashes, ~1s) if spam is observed
```

#### 4.11 Content Availability Verification

The LISH format already stores per-chunk checksums in `IFileEntry.checksums`. This enables a lightweight Merkle sampling challenge (inspired by BEP-52):

```
Verifier sends:  verify_chunk_req { lishID, filePath, chunkIndex: random, nonce }
Publisher responds: verify_chunk_res { chunkData }  (within 5 seconds)
Verifier: hash(chunkData) == IFileEntry.checksums[chunkIndex] ?

On failure: broadcast signed content_unavailable signal
Peers accumulate signals → downrank or remove entries from unresponsive publishers
```

This provides probabilistic proof that a publisher actually holds the content they cataloged, without the complexity of Filecoin-style Proof of Replication.

---

## 5. Sync Protocol

### Two-Layer Design

#### Layer 1: GossipSub Broadcast (Real-Time)

For live operations while peers are connected. Messages are JSON payloads broadcast to the `lish/<networkID>` topic. The wire format wraps a `SignedCatalogOp` (§11) with a `type` discriminator:

```typescript
// GossipSub message envelope (all catalog operations use the same shape)
{
  type: 'catalog_op',          // discriminator — non-catalog messages use other types (e.g. 'want', 'have')
  payload: CatalogOpPayload,   // { type: 'add'|'update'|'remove'|'acl_grant'|'acl_revoke', networkID, hlc, nonce, data }
  signature: string,           // base64url Ed25519 signature of canonicalize(payload)
  signer: string,              // base58btc PeerID of the signer
  keyType: 'Ed25519',
}

// payload.data varies by operation type:
//   add:        CatalogEntry (without hlc/signature — those come from the envelope)
//   update:     { lishID: string, fields: { name?, description?, contentType?, tags? } }
//   remove:     { lishID: string }
//   acl_grant:  ACLChange { action: 'grant', role, peerIDs }
//   acl_revoke: ACLChange { action: 'revoke', role, peerIDs }
```

**Broadcast code** (from §15.5 step 6):
```typescript
network.broadcast(lishTopic(networkID), { type: 'catalog_op', ...signedOp });
// Produces: { type: 'catalog_op', payload: {...}, signature, signer, keyType }
```

#### Layer 2: Bilateral Stream (Catch-Up Sync)

For new peers joining or peers reconnecting after being offline. Uses a dedicated libp2p protocol stream.

**Protocol**: `/lish/catalog-sync/1.0.0`

```
New peer connects to network:
  1. Wait 1-5 seconds (random jitter to prevent thundering herd)
  2. Pick a random connected peer
  3. Open /lish/catalog-sync/1.0.0 stream
  4. Send: { vectorSummary, lishIDs[] }
  5. Receive: { deltaEntries[], deltaTombstones[], currentACL, theirVectorSummary }
  6. Merge into local state
  7. If delta was large, repeat with another peer for cross-validation
```

**Request:**
```typescript
{
  command: 'catalog_sync_req',
  requestID: string,
  networkID: string,
  vectorSummary: Record<string, HLC>,     // peerID -> highest HLC seen
  lishIDs: string[]                        // all lish UUIDs I know
}
```

**Response:**
```typescript
{
  command: 'catalog_sync_res',
  requestID: string,
  operations: SignedCatalogOp[],   // verifiable signed ops for entries + tombstones you don't have
  access: ICatalogAccess,          // current ACL state
  vectorSummary: Record<string, HLC>,
  gcCutoff: number                 // epoch ms — tombstones before this were garbage collected (§17.2)
}
```

**Why `SignedCatalogOp[]` instead of `CatalogEntry[]`**: The signature in `SignedCatalogOp` covers the full `CatalogOpPayload` (including `nonce`, `networkID`, `hlc`). A bare `CatalogEntry` does not preserve the original payload — the `nonce` is lost, making signature verification impossible. Sending full signed operations enables receiving peers to verify every entry through the same `verifyCatalogOp()` function used for GossipSub messages.

### Scale Limits

| Catalog Size | Full State Payload | Strategy |
|---|---|---|
| < 1,000 entries | ~500 KB | Full state + vector clock filter |
| 1K - 10K | ~5 MB | Delta sync via vector summary |
| 10K - 100K | ~50 MB | Merkle Search Tree anti-entropy |
| > 100K | ~500 MB+ | Merkle-CRDT (go-ds-crdt pattern) |

For realistic LiberShare usage (hundreds to low thousands of LISHs per lishnet), the simplest strategy is sufficient.

### Merkle Search Tree Anti-Entropy (Phase 4, for large catalogs)

For catalogs exceeding ~10K entries, sending full `lishIDs[]` lists becomes inefficient. A **Merkle Search Tree (MST)** enables O(log n) set reconciliation:

```
MST structure (keyed by lishID, sorted):
  Root hash: SHA256 of all child hashes
  Internal nodes: SHA256(left_hash + right_hash + entry_hash)
  Leaf nodes: SHA256(catalog_entry_signature)

Sync protocol extension:
  1. Peers exchange root hash
  2. If equal → catalogs are identical, no sync needed
  3. If different → traverse tree, exchange only differing subtrees
  4. Expected rounds: O(log n) for k differences in n entries
```

**Comparison to IBLT** (Invertible Bloom Lookup Table):
- IBLT is more bandwidth-efficient for small deltas (< 100 differences)
- MST is deterministic (no probabilistic failure mode)
- MST integrates naturally with the CRDT merge operation
- **Recommendation**: Use MST for simplicity, IBLT as a future optimization if needed

This pattern is used by AT Protocol (Bluesky) for repository sync and by go-ds-crdt for DAG compaction. The key insight is that the Merkle tree is built over the **sorted catalog entries**, not over operations — this makes it independent of operation ordering.

### Delta-State CRDT Optimization

The 2P-Set can be implemented as a **delta-state CRDT** for bandwidth efficiency. Instead of sending full state on every sync, peers exchange only the **delta** (new operations since last sync):

```typescript
interface CatalogDelta {
  newEntries: CatalogEntry[];       // entries added since last sync with this peer
  newTombstones: TombstoneEntry[];  // tombstones added since last sync
  aclChanges: ACLChange[];          // ACL ops since last sync
  fromHLC: HLC;                     // sender's HLC at time of last sync
  toHLC: HLC;                       // sender's current HLC
}

// Each peer tracks per-neighbor sync state:
// syncState: Map<peerID, HLC>  — last HLC synced with that peer
// On sync request, send all ops where op.hlc > syncState[requestingPeer]
```

**Advantages over pure state-based CRDT**:
- **Bandwidth**: O(delta) instead of O(state) per sync
- **Reliability**: If delta is lost, fall back to full state merge (idempotent)
- **Anti-entropy**: Periodic full-state comparison (via MST root hash) detects any missed deltas

**Advantages over pure op-based CRDT**:
- **No causal delivery requirement**: Deltas are self-contained state fragments
- **Idempotent merge**: Duplicate delivery is harmless (unlike op-based which needs exactly-once)
- **Late joiner friendly**: New peer gets full state snapshot, not entire operation history

---

## 6. Persistence

### Overview

The catalog CRDT state lives **in SQLite** (`libershare.db`), the same database already used for LISHs and lishnets storage. No separate files, no in-memory Maps. The 2P-Set CRDT maps naturally to SQL tables — LWW merge is a single `INSERT ON CONFLICT DO UPDATE WHERE` comparing HLC values.

```
data/
├── libershare.db         (existing - LISHs + LISHnets + catalog tables, bun:sqlite with WAL)
├── settings.json         (existing)
└── datastore.db          (existing - libp2p peer store, separate from app DB)
```

### Why SQLite (not CBOR files)

The original design proposed CBOR files per lishnet (`catalog/<networkID>.cbor`). This was reconsidered after the main branch migrated LISHs and lishnets to SQLite:

| Factor | CBOR files (rejected) | SQLite tables (selected) |
|---|---|---|
| New dependency | `cbor-x` npm package | none — `bun:sqlite` already in project |
| Write strategy | full file rewrite on every change | per-row INSERT/UPDATE |
| Crash safety | write-then-rename (near-atomic) | WAL mode (native, better) |
| Search | in-memory O(n) filtering | SQL indexes + FTS5 fulltext |
| Memory usage | entire catalog in RAM always | lazy load, SQL-side pagination |
| Write latency (10K entries) | ~100ms (serialize + rewrite) | ~0.5ms (single row insert) |
| Multi-lishnet | N files on disk | 1 DB, `network_id` column |
| Delta sync query | serialize entire state | `SELECT ... WHERE hlc_wall > ?` |
| Tombstone GC | load → filter → rewrite | `DELETE FROM ... WHERE expired < ?` |
| Code consistency | separate persistence layer | same `db/*.ts` pattern as lishs/lishnets |

**Key insight**: The 2P-Set CRDT with LWW is semantically a **key-value store with version-based conflict resolution** — which is exactly what SQL `INSERT ON CONFLICT DO UPDATE WHERE` provides natively.

### CRDT → SQL Mapping

```
2P-Set add set       =  catalog_entries table
2P-Set remove set    =  catalog_tombstones table
LWW resolution       =  ON CONFLICT DO UPDATE WHERE new_hlc > old_hlc
opLog per entry      =  signed_op BLOB column (CBOR-encoded SignedCatalogOp)
Vector clock         =  catalog_clocks table
ACL                  =  catalog_acl table
```

### Schema

```sql
-- backend/src/db/catalog.ts

CREATE TABLE IF NOT EXISTS catalog_entries (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    network_id        TEXT NOT NULL,
    lish_id           TEXT NOT NULL,
    name              TEXT,
    description       TEXT,
    publisher_peer_id TEXT NOT NULL,
    published_at      TEXT NOT NULL,
    chunk_size        INTEGER NOT NULL,
    checksum_algo     TEXT NOT NULL,
    total_size        INTEGER NOT NULL,
    file_count        INTEGER NOT NULL,
    manifest_hash     TEXT NOT NULL,
    content_type      TEXT,
    tags              TEXT,                  -- JSON array, e.g. '["linux","iso"]'
    last_edited_by    TEXT,
    hlc_wall          INTEGER NOT NULL,
    hlc_logical       INTEGER NOT NULL,
    hlc_node          TEXT NOT NULL,
    signed_op         BLOB NOT NULL,         -- CBOR-encoded SignedCatalogOp for sync verification
    UNIQUE(network_id, lish_id)
);

CREATE INDEX IF NOT EXISTS idx_catalog_entries_network ON catalog_entries(network_id);
CREATE INDEX IF NOT EXISTS idx_catalog_entries_hlc ON catalog_entries(network_id, hlc_wall);

CREATE TABLE IF NOT EXISTS catalog_tombstones (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    network_id        TEXT NOT NULL,
    lish_id           TEXT NOT NULL,
    removed_by        TEXT NOT NULL,
    removed_at        TEXT NOT NULL,
    hlc_wall          INTEGER NOT NULL,
    hlc_logical       INTEGER NOT NULL,
    hlc_node          TEXT NOT NULL,
    signed_op         BLOB NOT NULL,
    UNIQUE(network_id, lish_id)
);

CREATE INDEX IF NOT EXISTS idx_catalog_tombstones_network ON catalog_tombstones(network_id);

CREATE TABLE IF NOT EXISTS catalog_acl (
    network_id        TEXT PRIMARY KEY,
    owner             TEXT NOT NULL,          -- single PeerID
    admins            TEXT NOT NULL DEFAULT '[]',     -- JSON array of PeerIDs
    moderators        TEXT NOT NULL DEFAULT '[]',     -- JSON array of PeerIDs
    restrict_writes   INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS catalog_clocks (
    network_id        TEXT NOT NULL,
    peer_id           TEXT NOT NULL,
    hlc_wall          INTEGER NOT NULL,
    hlc_logical       INTEGER NOT NULL,
    PRIMARY KEY(network_id, peer_id)
);

-- FTS5 for fulltext search (from Phase 1, no migration needed later)
CREATE VIRTUAL TABLE IF NOT EXISTS catalog_fts USING fts5(
    name, description, tags,
    content=catalog_entries,
    content_rowid=id
);
```

### LWW Merge in SQL

The core CRDT operation — "if newer, update; if older, ignore" — is a single SQL statement:

```sql
INSERT INTO catalog_entries (network_id, lish_id, name, description,
    publisher_peer_id, published_at, chunk_size, checksum_algo,
    total_size, file_count, manifest_hash, content_type, tags,
    last_edited_by, hlc_wall, hlc_logical, hlc_node, signed_op)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(network_id, lish_id) DO UPDATE SET
    name = excluded.name,
    description = excluded.description,
    total_size = excluded.total_size,
    file_count = excluded.file_count,
    content_type = excluded.content_type,
    tags = excluded.tags,
    last_edited_by = excluded.last_edited_by,
    hlc_wall = excluded.hlc_wall,
    hlc_logical = excluded.hlc_logical,
    hlc_node = excluded.hlc_node,
    signed_op = excluded.signed_op
WHERE excluded.hlc_wall > catalog_entries.hlc_wall
   OR (excluded.hlc_wall = catalog_entries.hlc_wall
       AND excluded.hlc_logical > catalog_entries.hlc_logical)
   OR (excluded.hlc_wall = catalog_entries.hlc_wall
       AND excluded.hlc_logical = catalog_entries.hlc_logical
       AND excluded.hlc_node > catalog_entries.hlc_node);
```

This is deterministic — all peers applying the same set of operations will converge to the same state, regardless of order.

### Security: Validation Before Storage

**Critical**: In a P2P network with untrusted peers, every incoming operation MUST pass through a validation chain **before** any SQL write. The `upsertCatalogEntry()` function must never be called directly from API or network handlers.

```typescript
// Mandatory validation chain for ALL remote operations
async function handleRemoteOp(db: Database, networkID: string, op: SignedCatalogOp): Promise<boolean> {
  // 1. SIGNATURE — Ed25519 verify (cryptographic, unforgeable)
  if (!verifyCatalogOp(op)) return false;

  // 2. ACL — does this PeerID have the required role?
  if (!checkACL(db, networkID, op)) return false;

  // 3. DRIFT — is wallTime within ±5 minutes of local time?
  if (Math.abs(op.payload.hlc.wallTime - Date.now()) > MAX_DRIFT) return false;

  // 4. CONTENT — valid fields, reasonable sizes?
  if (!validateFields(op)) return false;

  // 5. ANTI-REPLAY — is HLC > last seen from this author?
  if (!checkVectorClock(db, networkID, op)) return false;

  // ALL checks passed — now store (SQL handles LWW merge)
  upsertCatalogEntry(db, networkID, op);
  updateVectorClock(db, networkID, op.signer, op.payload.hlc);
  return true;
}
```

**Layers of defense** (from strongest to weakest):

| Layer | Defense | Bypass possible? |
|---|---|---|
| 1. Signature | Ed25519 — peer cannot impersonate another | No (cryptographic) |
| 2. ACL | Role check against catalog_acl table | Only if ACL is compromised |
| 3. HLC drift | Reject wallTime > ±5 min from local time | Peer can gain ≤5 min advantage |
| 4. Content | Field size limits, valid types | No (deterministic validation) |
| 5. Anti-replay | Vector clock — reject ops ≤ last seen HLC | No (monotonic clock) |

**What cannot be defended against**:
- Malicious owner (root of trust — if compromised, the entire lishnet is compromised)
- Malicious moderator (can write bad data until revoked by admin/owner)
- 100% eclipse (attacker controls all connections — can suppress legitimate data)

### CBOR Role (Wire Format Only)

CBOR (`cbor-x`) is no longer used for persistence. It remains for:

| Use | Format |
|---|---|
| `signed_op` BLOB column | CBOR-encoded `SignedCatalogOp` — preserved for re-forwarding to other peers during bilateral sync |
| Bilateral sync wire protocol | CBOR stream over `/lish/catalog-sync/1.0.0` |
| GossipSub messages | JSON (unchanged — gossipsub requires text payloads) |

The `signed_op` blob is critical in the untrusted P2P model: when peer A sends data to peer B during sync, B must verify the original signature. Storing the full `SignedCatalogOp` envelope means it can be forwarded without re-signing (which would be impossible — A doesn't have the original author's private key).

### Tamper Resistance

The local database is a **cache**, not a source of truth. Signatures inside `signed_op` blobs are the source of truth:

| Tampering scenario | What happens |
|---|---|
| Peer edits a field in SQLite directly | Signature in `signed_op` becomes invalid → overwritten on next sync |
| Peer deletes the database | Fresh start → bilateral sync restores full catalog from peers |
| Peer adds fake entry to SQLite | No valid moderator signature → rejected by all peers on sync |
| Peer removes a tombstone | Tombstone comes back from other peers on next sync |
| Database corrupted (disk error) | SQLite WAL recovery or fresh start → sync from peers |

The CRDT state can always be **fully reconstructed from the network**. The local database only exists to avoid re-downloading everything on every restart.

### Delta Sync Query

Instead of serializing entire state, delta sync is a simple SQL query:

```sql
-- What does peer X need? (they last saw HLC with wallTime = ?)
SELECT signed_op FROM catalog_entries
WHERE network_id = ? AND hlc_wall > ?
UNION ALL
SELECT signed_op FROM catalog_tombstones
WHERE network_id = ? AND hlc_wall > ?;
```

### Tombstone Garbage Collection

```sql
-- One statement instead of load-filter-rewrite
DELETE FROM catalog_tombstones
WHERE network_id = ? AND removed_at < datetime('now', '-30 days');
```

---

## 7. API Surface

### New WebSocket API Methods

```
catalog.list(networkID)                        → CatalogEntry[]
catalog.get(networkID, lishID)                 → CatalogEntry | null
catalog.search(networkID, query)               → CatalogEntry[]
catalog.publish(networkID, lishID)             → void  (broadcast add)
catalog.update(networkID, lishID, fields)      → void  (broadcast update, moderator+)
catalog.remove(networkID, lishID)              → void  (broadcast remove)
catalog.getAccess(networkID)                   → ICatalogAccess
catalog.updateAccess(networkID, changes)       → void  (broadcast ACL change)
catalog.getSyncStatus(networkID)               → { entryCount, lastSync, peers }
```

### New Events

```
catalog:updated    → { networkID, entry: CatalogEntry }
catalog:removed    → { networkID, lishID }
catalog:acl        → { networkID, access: ICatalogAccess }
catalog:sync       → { networkID, newEntries: number, phase: 'start'|'complete' }
```

---

## 8. Frontend Integration

The Products page (`frontend/src/pages/Products/Products.svelte`) currently shows 200 hardcoded items. Changes needed:

1. Replace hardcoded `items` array with `await api.catalog.list(networkID)`
2. Add real search via `api.catalog.search(networkID, query)`
3. Subscribe to `catalog:updated` events for live updates
4. Show real file info from LISH manifest (fetched on demand)
5. Add admin UI for catalog management (publish, remove, ACL)
6. Per-lishnet catalog view (user selects which network to browse)

---

## 9. Implementation Phases

### Phase 1: Core CRDT + Persistence (Backend)
- SQLite catalog tables in `db/catalog.ts` (entries, tombstones, ACL, clocks, FTS5)
- LWW merge via `INSERT ON CONFLICT DO UPDATE WHERE` with HLC comparison
- HLC implementation (tick, merge, compare)
- Signature generation and verification (Ed25519 via `@libp2p/crypto`)
- Validation chain: signature → ACL → drift → content → anti-replay → SQL write
- `CatalogManager` class (multi-lishnet lifecycle, DB-backed)
- Add `ownerPeerID` field to `ILISHNetwork` shared type
- Add `getPrivateKey()`, `registerStreamHandler()`, and `dialProtocolByPeerId()` to `Network` class
- Update `TopicHandler` type to `(data: Record<string, any>) => void | Promise<void>` (async support)
- Unit tests for merge correctness, security validation, crash recovery

### Phase 2: Sync + GossipSub Integration (Backend)
- Bilateral sync stream `/lish/catalog-sync/1.0.0` (CBOR, `SignedCatalogOp[]` for verifiability)
- GossipSub broadcast for catalog operations (JSON, signed)
- GossipSub topic validator via `registerTopicValidator()` on `Network` class (REJECT/IGNORE/Accept)
- Handle incoming ops (validate signature + ACL + field sizes, merge)
- ACL operations (add/remove admin/moderator, cascading revocation)
- Integration with `Networks` class (join → catalog load, leave → catalog unload)
- Old protocol coexistence (`add_lish`/`del_lish` ignored by catalog layer)
- FTS5 fulltext search (SQL-based from Phase 1)

### Phase 3: API + Frontend
- WebSocket API methods for catalog CRUD (`catalog.list`, `catalog.publish`, `catalog.search`, etc.)
- End-to-end publish flow (local LISH → CatalogEntry → broadcast)
- Events for live catalog updates (`catalog:updated`, `catalog:removed`, `catalog:acl`, `catalog:sync`)
- Products page rewrite (real data from catalog)
- Search and filter UI (text search + tag search with `#` prefix)
- Admin panel for ACL management

### Phase 4: Hardening
- Cross-peer validation on sync (compare with multiple peers)
- Tombstone garbage collection (time-based, 30 days)
- GossipSub peer scoring (P4 invalid messages + P5 app-specific + P6 IP colocation)
- GossipSub D upgrade to >= 6
- Rate limiting on incoming operations (sliding window)
- Catalog size limits per network
- Merkle Search Tree anti-entropy (for catalogs > 10K entries)
- Content availability verification (random chunk challenge)
- Metrics and monitoring

---

## 10. Security Checklist

- [x] Every catalog operation is signed with Ed25519
- [x] Signature covers payload + authorPeerID + HLC (canonical JSON via json-canonicalize)
- [x] ACL changes validated against role hierarchy before application
- [x] HLC anti-replay check on every received operation (hlcCompare > 0)
- [x] HLC clock drift check: reject ops with wallTime > 5 minutes in the future (MAX_DRIFT)
- [x] Owner PeerID is immutable (from .lishnet config, not from network)
- [x] Bilateral sync stream authenticated via libp2p Noise handshake
- [ ] Cross-validate catalog state from multiple peers on initial sync
- [x] Tombstones kept for minimum 30 days (time-based GC)
- [x] Rate limiting on incoming operations per peer (sliding window)
- [x] Maximum catalog size enforced to prevent DoS via catalog spam
- [x] Reject operations from unknown/unverified PeerIDs
- [x] Log and alert on repeated authorization failures (potential attack)
- [ ] GossipSub peer scoring enabled (P4 invalid messages + P5 app-specific + P6 IP colocation)
- [x] GossipSub D >= 6 (upgraded from D=2)
- [x] Per-publisher write quota enforced (MAX_ENTRIES_PER_PUBLISHER)
- [x] Global catalog size cap enforced (MAX_CATALOG_SIZE)
- [x] Sliding-window rate limiter per publisher PeerID
- [x] vectorClock persisted in catalog_clocks table and loaded on restart (prevents replay after restart)
- [x] GossipSub topic validator registered for catalog topics (REJECT invalid sigs, IGNORE rate-limited)
- [ ] Content availability verification via random chunk challenge (optional, Phase 4)
- [x] Emergency revocation: acl_revoke propagates within 1 heartbeat cycle
- [x] Anti-escalation rule: cannot grant permissions you do not hold (Matrix Rule 9)
- [x] Power-events-first ordering: ACL events processed before catalog events in same batch
- [x] Cascading revocation: revoking admin invalidates all their granted moderator permissions
- [x] Update operations: only editable fields (name, description, contentType, tags) can be changed
- [x] Update operations: immutable fields (lishID, publisherPeerID, totalSize, manifestHash, etc.) rejected
- [x] Update operations: lastEditedBy set automatically from authorPeerID, not user-supplied
- [x] Field size limits enforced before signature verification (fail fast)
- [x] Schema version included in catalog tables and sync protocol
- [x] Unknown gossipsub message versions: IGNORE (not REJECT) to avoid penalizing newer peers
- [x] Bilateral sync: stream timeout (30s), payload size limit (10 MB), CBOR decode error handling
- [ ] Bilateral sync: invalid signatures in delta → reject entries, penalize peer (P5 score -5)
- [x] Crash-safe persistence: SQLite WAL mode ensures atomic writes
- [x] Corrupt database: SQLite WAL recovery, fallback to full sync from peers
- [x] Per-network operation queue: serialized mutations prevent concurrent state corruption
- [x] SQLite transactions: each mutation is atomic, WAL mode handles concurrent access
- [x] .lishnet `ownerPeerID` field: required for catalog, validated as Ed25519 PeerID
- [x] `manifestHash` computed as sha256(canonicalize(lishManifest)) — anchors catalog entry to exact manifest
- [x] `signCatalogOp()` receives `localClock` as parameter, returns `updatedClock` (no free variables)
- [x] Tombstone GC: 30-day retention, runs on anti-entropy cycle, `gcCutoff` in sync response
- [x] Rate limiter: 10 ops/peer/min, 100 ops/global/min, 1000 entries/publisher, 50K entries/catalog
- [x] GossipSub topic validator: REJECT invalid sigs, IGNORE rate-limited, Accept valid
- [x] Structured error codes (CatalogError class) — frontend can switch on `error.code`
- [x] Graceful degradation: catalog failures never block file sharing operations
- [x] v1 .lishnet upgrade: auto-assign ownerPeerID from local peer when creating network

---

## 11. Concrete Signing Implementation

### Dependencies

Two new dependencies needed:

```bash
bun add json-canonicalize   # RFC 8785 JCS for deterministic JSON serialization (signing)
bun add cbor-x              # RFC 8949 CBOR binary encoding (persistence + bilateral sync)
bun add uint8arrays          # Uint8Array utilities (concat for stream reading) — already a libp2p transitive dep
```

`@libp2p/peer-id` is already a transitive dependency of `libp2p`.

### Signing API (from actual @libp2p/crypto v5.1.12 source)

The Ed25519 implementation in `@libp2p/crypto` uses **Node.js built-in `crypto`** (not WASM, not noble-ed25519). `sign()` and `verify()` are **synchronous** for Ed25519 (Promise return type exists only for RSA). Performance: ~10,000-15,000 sign/s, ~4,000-6,000 verify/s.

```typescript
// backend/src/catalog/catalog-signer.ts
import { canonicalize } from 'json-canonicalize';
import { peerIdFromString } from '@libp2p/peer-id';
import type { Ed25519PrivateKey } from '@libp2p/interface';

export interface CatalogOpPayload {
  type: 'add' | 'update' | 'remove' | 'acl_grant' | 'acl_revoke';
  networkID: string;
  hlc: HLC;                // Hybrid Logical Clock
  nonce: string;           // crypto.randomUUID() for uniqueness
  data: Record<string, unknown>;
}

export interface SignedCatalogOp {
  payload: CatalogOpPayload;
  signature: string;       // base64url-encoded Ed25519 signature
  signer: string;          // base58btc PeerID (encodes public key)
  keyType: 'Ed25519';
}

const encoder = new TextEncoder();

export async function signCatalogOp(
  privateKey: Ed25519PrivateKey,
  type: CatalogOpPayload['type'],
  networkID: string,
  data: Record<string, unknown>,
  localClock: HLC              // caller passes current clock (see section 17.1)
): Promise<{ op: SignedCatalogOp; updatedClock: HLC }> {
  const newClock = hlcTick(localClock);
  const payload: CatalogOpPayload = {
    type,
    networkID,
    hlc: newClock,
    nonce: crypto.randomUUID(),
    data,
  };
  const canonical = canonicalize(payload);       // RFC 8785 deterministic JSON
  const bytes = encoder.encode(canonical);
  const sig = await privateKey.sign(bytes);      // Ed25519 is sync, await is safe
  return {
    op: {
      payload,
      signature: Buffer.from(sig).toString('base64url'),
      signer: privateKey.publicKey.toString(),      // base58btc PeerID
      keyType: 'Ed25519',
    },
    updatedClock: newClock,
  };
}

export async function verifyCatalogOp(op: SignedCatalogOp): Promise<boolean> {
  try {
    const peerId = peerIdFromString(op.signer);
    if (peerId.type !== 'Ed25519') return false;
    // Clock drift check: reject ops with wall-time > 60s in future
    if (op.payload.hlc.wallTime > Date.now() + 60_000) return false;
    const canonical = canonicalize(op.payload);
    const bytes = encoder.encode(canonical);
    const sig = Buffer.from(op.signature, 'base64url');
    return peerId.publicKey.verify(bytes, sig);
  } catch {
    return false;
  }
}
```

**Key insight from Ed25519 research**: For Ed25519, the PeerID **IS** the public key (identity multihash, codec 0x0). No separate key distribution needed — any peer that knows a PeerID string can verify signatures from that peer.

### GossipSub Integration

GossipSub `strictSign: true` (default) already signs the **transport envelope**, but this only proves the message came from that peer in the current session. **Application-layer signing** is required in addition:

```typescript
// Transport: GossipSub proves "this message was sent by PeerX right now"
// Application: SignedCatalogOp proves "this catalog entry was created by PeerX"
// Both layers needed — transport signing is ephemeral, app signing is persistent
```

### Delegation Tokens (Chain of Trust)

```typescript
interface DelegationToken {
  payload: {
    type: 'acl_grant';
    networkID: string;
    delegator: string;     // PeerID granting the role
    delegatee: string;     // PeerID receiving the role
    role: 'admin' | 'moderator';
    grantedAt: number;
    nonce: string;
  };
  signature: string;
  signer: string;          // == delegator
  keyType: 'Ed25519';
}
// Note: Token expiry (expiresAt) intentionally omitted. In a P2P system
// without consensus, expiry is unreliable (clock skew). Revocation via
// acl_revoke is the correct mechanism for removing permissions.

// Verification: walk the chain
// Owner (from .lishnet) → signed admin grant → signed moderator grant
// Max chain depth: 2 (owner→admin→moderator)
```

---

## 12. Lessons from Similar Systems

### Validated Design Decisions

| System | What libershare gets right | Source |
|---|---|---|
| **All 7 systems** | Two-layer sync (gossipsub + bilateral) | SSB, Matrix, Nostr, Tribler all use this pattern |
| **BEP-44, SSB** | Per-author monotonic clocks for replay prevention (HLC in our case) | BEP44 sequence numbers, SSB feed sequences |
| **All systems** | Owner PeerID from .lishnet (out-of-band trust) | Matrix room creator, SSB genesis, BT tracker URL |
| **go-ds-crdt** | Signed 2P-Set CRDT | IPFS Cluster production: 80M pins, 24 peers |
| **Nostr NIP-01** | Signed events with canonical JSON | Every Nostr event is self-authenticating |
| **SSB** | Catalog summaries only, full data on demand | SSB selective replication solves same scaling issue |

### What Needs Attention

| Issue | Finding | Source |
|---|---|---|
| **Tombstone GC** | Causal stability is impractical in churn-heavy networks | go-ds-crdt, SSB (years of struggle) |
| **vectorClock restart** | Must persist to disk — restart without it accepts all replays | BEP-44: "storage nodes must not downgrade seq" |
| **Gossipsub D=2** | Below spec minimum D=6, single bad peer can partition mesh | Ethereum beacon chain uses D=8 |
| **No peer scoring** | Flooding invalid messages has zero consequence | gossipsub v1.1 P4+P5+P6 |
| **Open-mode spam** | Networks with restrictCatalogWrites=false have no spam protection | Nostr open relay experience |
| ~~Missing CatalogEntry fields~~ | **Fixed** — contentType, tags, manifestHash added to CatalogEntry | Nostr NIP-94, Tribler channels |

### Architecture Patterns Adopted

**From Nostr**:
- Pure keypair identity (already via libp2p Ed25519)
- Self-authenticating signed events (every peer can verify independently)
- NIP-72 approval pattern for moderated publishing (proposal → approval workflow)
- Addressable events: entry identity = `(publisherPeerID, lishID)`, latest timestamp wins

**From Matrix**:
- Anti-escalation rule: cannot grant permission you do not hold (Rule 9)
- Power-events-first conflict resolution: ACL events processed before catalog events
- Redaction semantics: tombstones preserve structure, erase content

**From IPFS Cluster / go-ds-crdt**:
- GossipSub topic validator as write-access gatekeeper (trusted_peers whitelist)
- Follower mode concept: read-only peers that replicate but cannot write
- Snapshot-based bootstrap for new peers joining large catalogs

**From Tribler**:
- Composite key `(publisher_public_key + sequence_number)` for authorship
- VSIDS temporal decay for community voting (anti-spam for open networks)
- Adaptive batch sizing (100ms target) to prevent SQLite blocking
- Content-type categorization + local FTS5 search index

**From Farcaster**:
- Remove-wins CRDT semantics for ACL (revocation always beats grant)
- Cascading revocation: revoking a signer invalidates all their operations
- Storage quotas as spam prevention without blockchain

**Note**: The `CatalogEntry` interface in section 3 has been updated with `contentType`, `tags`, and `manifestHash` fields. All fields (including tags) are included in the Ed25519 signature to prevent tag injection.

---

## 13. Research Sources

### Systems Analyzed

| System | Key insight | Reference |
|---|---|---|
| **IPFS Cluster** | go-ds-crdt: production 80M pins, trusted_peers whitelist, DAG compaction needed | [ipfs-cluster/ipfs-cluster](https://github.com/ipfs-cluster/ipfs-cluster) |
| **Tribler** | 15y P2P: deploy early, delete what fails, gossip search 325ms median | [Tribler/tribler](https://github.com/Tribler/tribler) |
| **Nostr** | Signed events + dumb relays = maximal resilience | [nostr-protocol/nips](https://github.com/nostr-protocol/nips) |
| **Matrix** | State resolution v2, power levels, anti-escalation rules | [spec.matrix.org](https://spec.matrix.org) |
| **Farcaster** | KeyRegistry + Hub CRDT, remove-wins, cascading revocation | [farcasterxyz/protocol](https://github.com/farcasterxyz/protocol) |
| **SSB** | Append-only signed feeds, EBT replication, Box2 groups | [ssbc.github.io](https://ssbc.github.io/scuttlebutt-protocol-guide/) |
| **BitTorrent** | BEP-44 mutable DHT items, BEP-52 Merkle trees | [bittorrent.org/beps](https://www.bittorrent.org/beps/) |
| **gossipsub v1.1** | P1-P7 peer scoring, topic validators, IP colocation penalty | [libp2p/specs](https://github.com/libp2p/specs) |

### Key Academic References

- Martin Kleppmann: "Technical solutions alone cannot solve moderation — democratic processes remain essential"
- gossipsub v1.1 paper (Protocol Labs): IP colocation penalty specifically designed for cheapest Sybil attack class
- Cambridge paper on PoW: "Proof-of-Work Proves Not to Work" against well-resourced attackers
- TrustChain (TU Delft): Personal blockchains without global consensus, fraud detected not prevented

---

## 14. Open Design Questions (Resolved)

### 14.1 Catalog Bootstrap — How Does a New Network Start?

When a user creates a new lishnet, the `.lishnet` file contains the owner's PeerID. The catalog is initialized automatically:

```
1. User creates new lishnet (or imports .lishnet file)
2. Backend detects no catalog/<networkID>.cbor file exists
3. Creates empty CatalogCRDTState:
   - entries: empty
   - tombstones: empty
   - access: { owner: <PeerID from .lishnet>, admins: [], moderators: [],
               restrictCatalogWrites: false }
   - vectorClock: empty
   - localClock: HLC(Date.now(), 0, localPeerID)
   - syncState: empty
4. If joining existing network: bilateral sync fills the catalog from peers
5. If creating new network: owner starts with empty catalog, adds entries
```

The owner PeerID comes **exclusively** from the `.lishnet` config file — never from the network. This is the root of trust. The `.lishnet` file is distributed out-of-band (URL, QR code, file share, etc.).

### 14.2 Owner Offline / Key Loss — Recovery Scenarios

| Scenario | Impact | Recovery |
|---|---|---|
| Owner temporarily offline | Admins can still add moderators. Catalog works normally. | Owner comes back online eventually |
| Owner lost private key | Cannot add new admins. Existing admins/moderators still function. | **No recovery** — network continues with existing ACL frozen |
| Owner wants to transfer ownership | Not currently possible — owner is immutable in .lishnet | Create new .lishnet with new owner, migrate members manually |

**Design decision**: Owner immutability is intentional. It prevents a hostile admin from seizing ownership. The trade-off is that lost owner keys freeze the admin list. This matches how SSB, Nostr, and BitTorrent handle identity — keypair loss is permanent.

**Mitigation**: Document in user-facing materials that the owner should back up their private key (stored in `datastore.db`). Future enhancement: multi-sig ownership (requires 2-of-3 owner keys to manage admins).

### 14.3 Wire Format — GossipSub vs. Persistence

Two different serialization contexts:

| Context | Format | Reason |
|---|---|---|
| **GossipSub messages** | JSON (utf-8) | Human-debuggable, gossipsub uses string payloads, signatures use canonical JSON (json-canonicalize) |
| **Bilateral sync stream** | CBOR | Binary stream, larger payloads (deltas), bandwidth matters |
| **Local persistence** | SQLite + CBOR blob | Structured data in SQL columns, `signed_op` as CBOR blob for re-forwarding |

GossipSub messages are small (single operations, ~500 bytes) so JSON overhead is acceptable. The signature is computed over **canonical JSON** regardless of wire format — this ensures signature portability between contexts.

```
GossipSub:  peer → JSON.stringify(signedOp) → gossipsub.publish() → topic
Bilateral:  peer → cbor.encode(delta) → libp2p stream → peer
Disk:       SQLite tables (entries, tombstones, ACL, clocks) + signed_op BLOB column
```

### 14.4 Update Merge Strategy — Per-Field or Whole Entry?

**Decision: Whole-entry LWW (Last Writer Wins).**

When an update arrives, the **entire entry** is replaced if the incoming HLC is higher than the current one. No per-field merge.

```
Current state:  { name: "Ubuntu", description: "Official", hlc: 1000 }
Update from A:  { name: "Ubuntu LTS", hlc: 1005 }             → applied (1005 > 1000)
Update from B:  { description: "Official ISO", hlc: 1003 }    → rejected (1003 < 1005)
```

**Why not per-field merge?**
- Per-field merge requires tracking HLC per field per entry — complexity explosion
- The signature covers the entire entry — changing one field means re-signing everything anyway
- Concurrent edits to different fields of the same entry are rare (moderators coordinating)
- If it happens, one update wins, the other is lost — acceptable for metadata corrections

**Practical impact**: If two moderators edit different fields at the same time, the slower one loses their change. They need to re-apply it. This is the same behavior as Nostr addressable events, SSB, and every LWW register. For a catalog of file metadata, this is perfectly acceptable.

### 14.5 Network Partition (Split Brain)

Two groups of peers get disconnected. Both groups continue writing to their local catalogs.

```
Partition:
  Cluster A: peers 1,2,3 — moderator adds "Fedora"
  Cluster B: peers 4,5,6 — moderator adds "Debian", removes "Arch"

After reconnect:
  Peers exchange deltas via bilateral sync
  CRDT merge: union of all add-sets, union of all remove-sets
  Result: everyone has Fedora + Debian, Arch is removed
```

**No conflict**: Different entries with different UUIDs merge trivially. Same entry edited in both clusters → LWW by HLC. Tombstones propagate — if one cluster deleted an entry, the deletion wins everywhere.

**ACL during partition**: If cluster A revokes a moderator who is still writing in cluster B, the revocation propagates after reconnect. All operations written by the revoked moderator **after** the revocation timestamp become invalid and are discarded during merge. This is cascading revocation (Farcaster pattern).

**Power-events-first rule (Matrix pattern)**: When merging a batch of operations after reconnect, ACL operations (acl_grant, acl_revoke) are applied **before** catalog operations (add, update, remove). This ensures that a revoked moderator's writes are properly rejected even if they arrive in the same batch.

### 14.6 Field Size Limits

Without size limits, a malicious moderator could publish entries with megabyte descriptions, causing storage and bandwidth abuse.

```typescript
const FIELD_LIMITS = {
  name:         256,     // max bytes (UTF-8)
  description:  4096,    // max bytes (~1 page of text)
  tags:         10,      // max number of tags
  tagLength:    32,      // max bytes per tag
  contentType:  32,      // max bytes (enum, but validated as string)
} as const;

function validateFieldSizes(entry: CatalogEntry): boolean {
  if (entry.name && Buffer.byteLength(entry.name) > FIELD_LIMITS.name) return false;
  if (entry.description && Buffer.byteLength(entry.description) > FIELD_LIMITS.description) return false;
  if (entry.tags) {
    if (entry.tags.length > FIELD_LIMITS.tags) return false;
    if (entry.tags.some(t => Buffer.byteLength(t) > FIELD_LIMITS.tagLength)) return false;
  }
  return true;
}
```

Field size validation happens **before** signature verification (cheaper operation first, fail fast). An oversized entry is `REJECT`ed at the GossipSub validator level.

**Total maximum entry size**: ~5 KB (with all fields at max). A malicious moderator at max rate (10 ops/min) could produce ~50 KB/min — negligible.

### 14.7 Schema Versioning

What happens when a future LiberShare version adds new fields to CatalogEntry?

```typescript
interface CatalogSnapshot {
  version: 1;           // schema version, increment on breaking changes
  entries: ...;
  tombstones: ...;
  // ...
}
```

**Field obligation levels**:

| Level | Meaning | Examples |
|---|---|---|
| **Structurally required** | Entry cannot exist without it. Part of identity/integrity. Never changes. | lishID, publisherPeerID, hlc, signature |
| **Functionally required** | New entries must have it. Old entries without it are tolerated. | (none yet — added via migration) |
| **Optional** | Can be missing. UI shows "not specified". | description, tags, contentType |

Structurally required fields are defined in schema version 1 and **never change**. New fields can be at most functionally required, and only after a transition period as optional.

**Adding a new optional field** (backward compatible):

```
Release v2.0:
  CatalogEntry { ..., license?: string }

  New peer: fills license on all new entries
  Old peer: doesn't fill it, but doesn't crash (field is optional)
  Sync works normally — old peers store and forward unknown fields via CBOR
```

**Promoting optional → functionally required** (three-phase migration):

A field cannot go from "doesn't exist" to "required" in one step — old peers would create entries without it, and new peers would reject them. Instead:

```
Phase 1 — v2.0: add as optional
  CatalogEntry { ..., language?: string }
  UI offers language selection when publishing
  Backend accepts entries with or without language
  Old peers ignore the field, store and forward it via CBOR

Phase 2 — v2.1: UI requires it, backend still tolerates missing
  UI form validation: language is mandatory (cannot publish without it)
  Backend still accepts entries without language (from old peers)
  Most new entries in the network now have language filled

Phase 3 — v3.0: backend requires it for NEW entries
  Backend rejects NEW entries without language (REJECT at validation)
  Backend ACCEPTS old entries without language (migration exception)
  Old entries display as "(language not specified)" in UI
  Old entries are never invalidated — they have valid signatures from their era
```

```typescript
// Phase 3 validation logic
function validateEntry(entry: CatalogEntry, isNewEntry: boolean): boolean {
  if (isNewEntry) {
    // New entries: functionally required fields must be present
    if (!entry.language) return false;
  }
  // Existing entries: tolerate missing functionally-required fields
  // They were valid when created — never retroactively invalidate
  return true;
}
```

**Why never retroactively invalidate:**

In a P2P system, entries are signed at creation time. The signature proves the entry passed validation **at that time**. Retroactively requiring a field would:
1. Invalidate entries that peers have been faithfully storing and forwarding
2. Cause catalog divergence — new peers reject entries that old peers accept
3. Break the trust model — signatures should mean "this was valid"

**Schema version changes** (breaking, non-additive):

| Change type | Strategy | Example |
|---|---|---|
| New optional field | Backward compatible, no version bump | Adding `license?: string` |
| Optional → functionally required | Three-phase migration (see above) | `language` becomes required for new entries |
| Field type change | New schema version + migration code | `totalSize: string` → `totalSize: number` |
| Field removal | Deprecate in vN, stop writing in vN+1, ignore in vN+2 | Remove `checksumAlgo` |
| Structural change | New protocol version `/lish/catalog-sync/2.0.0` | Changing CatalogEntry identity model |

**Wire protocol versioning**: The bilateral sync protocol includes a version in its path (`/lish/catalog-sync/1.0.0`). A structural schema change would use `/lish/catalog-sync/2.0.0`. Peers negotiate the highest common version during handshake. If no common version exists, sync falls back to the older protocol.

**CBOR forward compatibility**: Unknown fields in CBOR are preserved during decode (unlike strict JSON parsers). An old peer receiving a new-format entry will store and forward the unknown fields without losing them — natural forward compatibility. This is what makes Phase 1 (add as optional) work seamlessly.

**GossipSub topic**: The gossipsub topic (`lish/<networkID>`) does NOT include a version. Message format is identified by a `version` field in the JSON payload. Old peers ignore messages with unknown versions (IGNORE, not REJECT — no penalty for new-format messages from newer peers).

---

## 15. Open Design Questions — Integration with Existing Codebase (Resolved)

Analysis of existing backend source code (`protocol/network.ts`, `lishnet/lishnets.ts`, `db/lishnets.ts`, `protocol/network-config.ts`, shared types, `LISH_NETWORK_PROTOCOL.md`) reveals the following integration gaps that must be resolved before implementation.

### 15.1 Protocol Command Mapping: Old Protocol → Catalog System

The existing `LISH_NETWORK_PROTOCOL.md` defines these message types:

```
Old protocol (v1):
  add_lish          → broadcast LISH manifest to DHT
  del_lish          → remove LISH from network (owners/admins only)
  get_lish_database_req/res → sync list of LISH UUIDs
  get_lish_req/res  → fetch single LISH manifest
  get_chunk_req/res → fetch file chunks
  manage_members    → add/remove admins, publishers, downloaders
```

**Decision: Coexistence, not replacement.**

The catalog system operates on a **higher layer** than the existing protocol. They serve different purposes:

| Layer | Purpose | Messages |
|---|---|---|
| **Data transfer** (existing) | Fetch full LISH manifests and file chunks from peers | `get_lish_req/res`, `get_chunk_req/res`, `want/have` |
| **Catalog** (new) | Replicate metadata summaries of available LISHs | `catalog_op` (add/update/remove/acl_grant/acl_revoke) |
| **Catalog sync** (new) | Catch-up for offline peers | `/lish/catalog-sync/1.0.0` bilateral stream |

**What happens to old commands:**

| Old command | Fate | Reason |
|---|---|---|
| `add_lish` | **Deprecated** → replaced by `catalog_op.add` | Catalog entry is the signed, authorized version of "LISH available" |
| `del_lish` | **Deprecated** → replaced by `catalog_op.remove` | Tombstone with signature replaces unsigned deletion |
| `get_lish_database_req/res` | **Deprecated** → replaced by `/lish/catalog-sync/1.0.0` | CRDT bilateral sync is more robust than UUID list exchange |
| `get_lish_req/res` | **Kept** | Still needed to fetch full LISH manifest on demand |
| `get_chunk_req/res` | **Kept** | Still needed for actual file transfer |
| `want/have` | **Kept** | Downloader protocol unchanged |
| `manage_members` | **Split** | Network-level roles (publisher/downloader) stay in network config. Catalog-level roles (admin/moderator) managed via `catalog_op.acl_grant/acl_revoke` |

**Migration path**: The catalog layer filters on `msg.type === 'catalog_op'` — old messages (`add_lish`, `del_lish`) are simply not dispatched to the catalog handler. New `catalog_op` messages are ignored by old peers (unknown `type` field). Both coexist on the same `lish/<networkID>` topic during transition.

### 15.2 Role Model: Protocol Spec vs Catalog ACL

**Important**: The `LISH_NETWORK_PROTOCOL.md` defines an `INetworkAccess` interface with roles (owners, admins, publishers, downloaders), but **this interface does not exist in the TypeScript codebase**. The implemented types are `ILISHNetwork` and `LISHNetworkConfig` (in `shared/src/index.ts`), which have no role fields. The protocol spec roles were never implemented.

The catalog system introduces `ICatalogAccess` as the **first actual implementation of access control**:

```
Current codebase (implemented):
  ILISHNetwork / LISHNetworkConfig:
    networkID, name, description, bootstrapPeers, created, enabled
    → NO role fields, no access control

Protocol spec (LISH_NETWORK_PROTOCOL.md, NOT implemented):
  INetworkAccess:
    owners, admins, publishers, downloaders
    → Document-only, not in TypeScript code

Catalog system (new, to be implemented):
  ICatalogAccess:
    owner        → single PeerID, controls admin list, immutable
    admins       → manage moderator list
    moderators   → can add/update/remove catalog entries
```

**Decision**: `ICatalogAccess` is the first real access control. The catalog owner is seeded from `ILISHNetwork.ownerPeerID` — the new field defined in section 15.3. The protocol spec's `INetworkAccess` roles (publisher/downloader) remain unimplemented and are a separate concern for future data-layer access control.

**Relationship between data and catalog layers**:
- `ICatalogAccess.moderators` controls who can **write catalog metadata** (catalog layer)
- Downloads and seeding remain open to everyone (no access control on data layer yet)
- A peer can be a moderator (writes catalog) but downloads are open to everyone regardless
- Future: `INetworkAccess`-style publisher/downloader restrictions can be implemented independently

### 15.3 .lishnet File: Owner PeerID Field

The current `ILISHNetwork` interface lacks an `owner` field:

```typescript
// Current (shared/src/index.ts) — note: `version` field was removed from ILISHNetwork
export interface ILISHNetwork {
  networkID: string;
  name: string;
  description?: string;
  bootstrapPeers: string[];
  created?: string;
}
```

The catalog system requires a trusted owner PeerID as the root of the ACL chain.

**Decision: Add `ownerPeerID` to `ILISHNetwork` and `.lishnet` format.**

```typescript
// Updated — ownerPeerID added as optional field
export interface ILISHNetwork {
  networkID: string;
  name: string;
  description?: string;
  bootstrapPeers: string[];
  created?: string;
  ownerPeerID?: string;   // Ed25519 PeerID of the network creator (required for catalog)
}
```

Also add `ownerPeerID` to the `LISHNetworkDefinition` and `LISHNetworkConfig` types (which extend or mirror `ILISHNetwork`), and to the `lishnets` SQLite table:

```sql
ALTER TABLE lishnets ADD COLUMN owner_peer_id TEXT;
```

**Field is optional** for backward compatibility — `.lishnet` files without `ownerPeerID` work for file sharing but cannot use the catalog system. When a user creates a new lishnet in LiberShare, `ownerPeerID` is automatically set to their PeerID.

**Validation**: `ownerPeerID` must be a valid Ed25519 PeerID (starts with `12D3KooW`). If present, it becomes `ICatalogAccess.owner`. If missing, catalog features are disabled for that network.

### 15.4 Required Network Class Extensions

The `Network` class needs two new public methods for the catalog system:

**1. Private key access for signing**

The Ed25519 private key is loaded in `start()` but never exposed. The catalog signer needs it.

```typescript
// Additions to Network class (network.ts)
private privateKey: PrivateKey | null = null;

async start(bootstrapPeers: string[] = []): Promise<void> {
  // ... existing code ...
  const privateKey = await this.loadOrCreatePrivateKey(this.datastore);
  this.privateKey = privateKey;  // store reference
  // ... rest of start() ...
}

getPrivateKey(): Ed25519PrivateKey {
  if (!this.privateKey) throw new CodedError(ErrorCodes.NETWORK_NOT_STARTED);
  if (this.privateKey.type !== 'Ed25519') throw new CodedError(ErrorCodes.INTERNAL_ERROR, 'Only Ed25519 keys supported');
  return this.privateKey as Ed25519PrivateKey;
}
```

**Security consideration**: The private key is already in memory (used by libp2p internally). Exposing it to the catalog module is no additional risk — both run in the same Bun process.

**2. Bilateral stream handler registration**

The bilateral sync protocol (`/lish/catalog-sync/1.0.0`) needs to register a handler on the libp2p node. The `node` field is private, and only `dialProtocol()` (outbound) is public. A new method is needed:

```typescript
// Addition to Network class (network.ts)
async registerStreamHandler(
  protocol: string,
  handler: (stream: Stream) => Promise<void>
): Promise<void> {
  if (!this.node) throw new CodedError(ErrorCodes.NETWORK_NOT_STARTED);
  await this.node.handle(
    protocol,
    async ({ stream }) => handler(stream),
    { runOnLimitedConnection: true }
  );
}
```

This mirrors the existing `LISH_PROTOCOL` handler registration pattern in `start()` (line 194-199 of `network.ts`).

**3. PeerID-based dial for bilateral sync**

The existing `Network.dialProtocol(multiaddrs[], protocol)` takes multiaddrs. Bilateral sync needs to dial by PeerID (from `getTopicPeers()`). Two options:

```typescript
// Option A: New convenience method on Network class
async dialProtocolByPeerId(peerID: string, protocol: string): Promise<Stream> {
  if (!this.node) throw new CodedError(ErrorCodes.NETWORK_NOT_STARTED);
  const peerId = peerIdFromString(peerID);
  const connection = await this.node.dial(peerId);
  return connection.newStream(protocol, { runOnLimitedConnection: true });
}

// Option B: Resolve in catalog-sync.ts (no Network change needed)
const peerId = peerIdFromString(peerID);
const connection = await network.node.dial(peerId);  // needs node to be accessible
```

Option A is preferred — keeps `node` private, consistent with existing patterns.

**Note on error handling**: The codebase now uses `CodedError` from `shared/src/errors.ts` for structured error codes (added in main). New catalog methods should follow the same pattern — use `CodedError(ErrorCodes.*)` instead of `new Error()`. Catalog-specific error codes (e.g., `CATALOG_NOT_FOUND`, `CATALOG_ACL_DENIED`, `CATALOG_ENTRY_EXISTS`) should be added to `shared/src/errors.ts`.

### 15.5 End-to-End Publish Flow

How `catalog.publish(networkID, lishID)` works from API call to broadcast. **Important**: Steps 3-8 run inside `enqueueOperation()` (section 15.10) to prevent concurrent state corruption:

```
1. Frontend calls: catalog.publish(networkID, lishID)

2. Backend resolves local LISH:
   const lish = dataServer.get(lishID);  // returns IStoredLISH | null
   if (!lish) throw new Error('LISH not found locally');

3. Backend extracts summary from LISH manifest:
   const entry: CatalogEntry = {
     lishID: lish.id,
     publisherPeerID: network.getNodeInfo().peerID,
     publishedAt: new Date().toISOString(),
     chunkSize: lish.chunkSize,
     checksumAlgo: lish.checksumAlgo,
     fileCount: lish.files.length,
     totalSize: lish.files.reduce((sum, f) => sum + f.size, 0),
     manifestHash: sha256(canonicalize(lish)),  // integrity anchor
     name: lish.name,                             // undefined if not set (UI shows lishID as fallback)
     // hlc and signature are added by signCatalogOp()
   };

4. Backend signs the entry (clock passed in, updated clock returned):
   const privateKey = network.getPrivateKey();
   const { op: signedOp, updatedClock } = await signCatalogOp(
     privateKey, 'add', networkID, entry, catalogCRDT.state.localClock
   );
   catalogCRDT.state.localClock = updatedClock;

5. Backend applies locally:
   catalogCRDT.applyOperation(signedOp);  // validates + merges into local state

6. Backend broadcasts via GossipSub:
   network.broadcast(lishTopic(networkID), {
     type: 'catalog_op',
     ...signedOp
   });

7. Backend persists:
   saveCatalog(catalogPath, catalogCRDT.getState());

8. Backend emits event to frontend:
   ws.emit('catalog:updated', { networkID, entry });
```

**Prerequisite**: The LISH manifest must exist locally (the user has already imported/created it). The catalog stores only the summary — the full manifest is fetched by other peers via existing `get_lish_req`.

**manifestHash computation**: `sha256(canonicalize(lishManifest))` — the canonical JSON of the full LISH manifest. This anchors the catalog entry to the exact manifest content. Peers can verify integrity when they later fetch the full manifest.

### 15.6 CatalogManager: Multi-Lishnet Lifecycle

`CatalogManager` coordinates catalog operations across all joined lishnets. Unlike the original in-memory design, catalog data lives in SQLite — the manager only holds lightweight per-network state (local clock, ACL cache, anti-entropy timers).

```typescript
// backend/src/catalog/catalog-manager.ts

export class CatalogManager {
  private readonly db: Database;
  private readonly network: Network;
  private syncHandlerRegistered: boolean = false;

  // Per-network lightweight state (NOT the catalog data — that's in SQLite)
  private joined: Map<string, {
    localClock: HLC;
    aclCache: ICatalogAccess;
    antiEntropyTimer: Timer | null;
  }> = new Map();

  constructor(db: Database, network: Network) {
    this.db = db;
    this.network = network;
  }

  /**
   * Register the bilateral sync handler (once, shared by all catalogs).
   */
  private async ensureSyncHandler(): Promise<void> {
    if (this.syncHandlerRegistered) return;
    await this.network.registerStreamHandler(
      '/lish/catalog-sync/1.0.0',
      async (stream) => {
        const request = await readSyncRequest(stream);  // CBOR decode
        if (this.joined.has(request.networkID)) {
          await this.handleSyncStream(stream, request);
        }
      }
    );
    this.syncHandlerRegistered = true;
  }

  /**
   * Join a lishnet's catalog.
   * Called when a lishnet is enabled. ownerPeerID comes from ILISHNetwork.ownerPeerID.
   */
  async join(networkID: string, ownerPeerID: string): Promise<void> {
    if (this.joined.has(networkID)) return;

    await this.ensureSyncHandler();

    // Ensure ACL exists in DB (create if first join)
    ensureCatalogACL(this.db, networkID, ownerPeerID);

    // Load lightweight state from DB
    const acl = getCatalogACL(this.db, networkID);
    const lastClock = getLatestClock(this.db, networkID, this.network.getNodeInfo().peerID);

    this.joined.set(networkID, {
      localClock: lastClock ?? { wallTime: 0, logical: 0, nodeID: this.network.getNodeInfo().peerID },
      aclCache: acl,
      antiEntropyTimer: null,
    });

    // Start periodic bilateral sync
    this.startAntiEntropy(networkID);

    // Register GossipSub handler for catalog_op messages
    await this.network.subscribe(lishTopic(networkID), async (msg) => {
      if (msg.type === 'catalog_op') {
        await handleRemoteOp(this.db, networkID, msg as SignedCatalogOp);
      }
    });
  }

  /**
   * Leave a lishnet's catalog (stop anti-entropy, clear cache).
   * Data stays in SQLite for potential re-join.
   */
  async leave(networkID: string): Promise<void> {
    const state = this.joined.get(networkID);
    if (!state) return;
    if (state.antiEntropyTimer) clearInterval(state.antiEntropyTimer);
    this.joined.delete(networkID);
  }

  // ... startAntiEntropy(), handleSyncStream(), etc.
}
```

**Integration with Networks class**: `CatalogManager` is created alongside `Networks` in `app.ts`, receiving `db` and `networks.getNetwork()` as dependencies. When `networks.setEnabled(id, true)` is called:

```typescript
const net = networks.get(id);
if (net?.ownerPeerID) {
  await catalogManager.join(id, net.ownerPeerID);
}
// Networks without ownerPeerID skip catalog (v1 .lishnet files)
```

When disabled, `await catalogManager.leave(id)`.

**Memory**: Per-network state is ~200 bytes (HLC + ACL cache + timer ref). 100 joined lishnets = ~20 KB RAM. Catalog data is in SQLite, not RAM.

### 15.7 Search Implementation

`catalog.search(networkID, query)` uses **FTS5 fulltext search** from Phase 1 (no separate "in-memory phase" needed since catalog is in SQLite).

```typescript
// backend/src/db/catalog.ts

function searchCatalog(db: Database, networkID: string, query: string, limit: number = 100): CatalogEntry[] {
  const q = query.trim();
  if (!q) return listCatalogEntries(db, networkID, limit);

  // Tag-only search: #linux → exact tag match
  if (q.startsWith('#')) {
    const tag = q.slice(1);
    return db.query<CatalogEntryRow, [string, string, number]>(
      `SELECT * FROM catalog_entries
       WHERE network_id = ? AND json_each.value = ?
       JOIN json_each(tags) ON 1=1
       LIMIT ?`,
      [networkID, tag, limit]
    ).all().map(rowToEntry);
  }

  // FTS5 fulltext search with ranking
  return db.query<CatalogEntryRow, [string, string, number]>(
    `SELECT e.*, fts.rank FROM catalog_fts fts
     JOIN catalog_entries e ON e.id = fts.rowid
     WHERE fts.catalog_fts MATCH ? AND e.network_id = ?
     ORDER BY fts.rank
     LIMIT ?`,
    [q, networkID, limit]
  ).all().map(rowToEntry);
}
```

**FTS5 sync triggers** (keep FTS index in sync with catalog_entries):

```sql
-- After INSERT into catalog_entries
INSERT INTO catalog_fts(rowid, name, description, tags)
VALUES (last_insert_rowid(), ?, ?, ?);

-- After UPDATE on catalog_entries
UPDATE catalog_fts SET name = ?, description = ?, tags = ? WHERE rowid = ?;

-- After DELETE from catalog_entries (tombstone)
DELETE FROM catalog_fts WHERE rowid = ?;
```

These are called from the `upsertCatalogEntry()` and `applyRemoveOp()` functions in `db/catalog.ts`, not via SQLite triggers (to keep control in application code for the untrusted validation chain).

### 15.8 Bilateral Sync Error Handling

The `/lish/catalog-sync/1.0.0` bilateral stream can fail in several ways:

| Failure | Detection | Recovery |
|---|---|---|
| Stream breaks mid-transfer | libp2p stream `reset` event | Retry with different peer after 5s backoff |
| Peer sends corrupt CBOR | `cbor-x` decode throws | Close stream, mark peer as unreliable (P5 score -1) |
| Peer sends oversized response | Byte counter exceeds MAX_SYNC_PAYLOAD (10 MB) | Abort stream, do not penalize (may be legitimate large catalog) |
| Peer doesn't respond | Timeout (30 seconds) | Close stream, try different peer |
| Peer sends valid but stale data | Received vectorClock is behind ours | Ignore delta (no harm, CRDT merge is idempotent) |
| Peer sends invalid signatures | `verifyCatalogOp()` returns false | Reject affected entries, penalize peer (P5 score -5) |

**Stream protocol**:

```typescript
import { concat } from 'uint8arrays/concat';
const MAX_SYNC_PAYLOAD = 10 * 1024 * 1024;  // 10 MB

async function handleCatalogSyncStream(stream: Stream): Promise<void> {
  const timeout = setTimeout(() => stream.abort(new Error('timeout')), 30_000);

  try {
    // Read request (CBOR)
    let requestBytes = new Uint8Array();
    let totalBytes = 0;

    for await (const chunk of stream.source) {
      totalBytes += chunk.byteLength;
      if (totalBytes > MAX_SYNC_PAYLOAD) {
        stream.abort(new Error('payload too large'));
        return;
      }
      requestBytes = concat([requestBytes, chunk.subarray()]);
    }

    const request = decoder.decode(requestBytes);
    // ... compute delta, validate, respond ...

    // Write response (CBOR)
    const responseBytes = encoder.encode(response);
    await stream.sink([responseBytes]);

  } catch (err) {
    // Stream error — logged, peer scored, no crash
    console.error('Catalog sync error:', err);
  } finally {
    clearTimeout(timeout);
  }
}
```

**Retry strategy**: Exponential backoff with jitter. Max 3 retries per sync attempt. After 3 failures, wait for next periodic sync interval (60 seconds).

### 15.9 Crash-Safe Persistence (WAL Mode)

With catalog data in `libershare.db`, crash safety is handled by SQLite's **WAL (Write-Ahead Logging)** mode, already enabled in `openDatabase()`:

```typescript
db.run('PRAGMA journal_mode = WAL');
```

WAL provides:
- **Atomic transactions** — all writes in a transaction either fully commit or fully rollback
- **Concurrent reads during writes** — readers don't block writers and vice versa
- **Automatic crash recovery** — WAL file is replayed on next open after unclean shutdown

No write-then-rename trick needed. No `.tmp` files. No manual crash recovery code.

**Worst case**: Database is irrecoverably corrupted → delete `libershare.db` and start fresh. Bilateral sync from peers restores the full catalog. The CRDT state is always fully reconstructable from the network.

### 15.10 Concurrency Model (Bun Single-Threaded)

Bun runs on a single thread with an event loop. Multiple simultaneous sync streams for different lishnets are interleaved, not parallel. Potential issues:

**Problem 1: Long sync blocks the event loop.**

A large catalog sync (50K entries, ~25 MB CBOR) could take 100+ ms to decode and validate. During this time, no GossipSub messages are processed.

**Solution**: Chunk processing with `setImmediate()`:

```typescript
async function processSyncDelta(db: Database, networkID: string, ops: SignedCatalogOp[]): Promise<void> {
  const BATCH_SIZE = 500;
  for (let i = 0; i < ops.length; i += BATCH_SIZE) {
    const batch = ops.slice(i, i + BATCH_SIZE);
    // Each batch in a SQLite transaction for atomicity
    const tx = db.transaction(() => {
      for (const op of batch) {
        handleRemoteOp(db, networkID, op);  // validation + SQL upsert
      }
    });
    tx();
    // Yield to event loop every 500 operations
    await new Promise(resolve => setImmediate(resolve));
  }
}
```

**Problem 2: Concurrent writes to the same catalog.**

A GossipSub message arrives while a bilateral sync is being processed for the same network.

**Solution**: SQLite handles this natively — WAL mode allows concurrent reads and serializes writes. Each `handleRemoteOp()` call is a self-contained transaction. No application-level queue needed (unlike the in-memory design which required explicit `enqueueOperation()`).

All catalog mutations (GossipSub ops, bilateral sync deltas, local publishes) go through `enqueueOperation()`. Since Bun is single-threaded, this is a simple promise chain — no locks needed.

**Problem 3: Persistence during rapid writes.**

Multiple GossipSub messages arrive in quick succession, each triggering `saveCatalog()`.

**Solution**: Debounced persistence:

```typescript
class CatalogCRDT {
  private saveTimer: Timer | null = null;
  private dirty: boolean = false;

  private scheduleSave(): void {
    this.dirty = true;
    if (this.saveTimer) return;  // already scheduled
    this.saveTimer = setTimeout(() => {
      this.saveTimer = null;
      if (this.dirty) {
        saveCatalog(this.path, this.getState());
        this.dirty = false;
      }
    }, 500);  // batch writes within 500ms window
  }

  // Also save immediately on graceful shutdown
  async flush(): Promise<void> {
    if (this.saveTimer) clearTimeout(this.saveTimer);
    if (this.dirty) {
      saveCatalog(this.path, this.getState());
      this.dirty = false;
    }
  }
}
```

Maximum one disk write per 500ms regardless of incoming operation rate. On shutdown, `flush()` is called to ensure no data loss.

**Note**: The `scheduleSave()`/`flush()` code above is a simplified draft. The authoritative version is in section 16.3 (`CatalogCRDT` unified class), which adds `await saveCatalog()` in `flush()` and `this.saveTimer = null` cleanup.

---

## 16. Open Design Questions — End-to-End Integration (Resolved)

Analysis of shared types (`shared/src/lish.ts`, `shared/src/index.ts`), API server pattern (`api/api.ts`), frontend Products page (`Products.svelte`), and DataServer reveals these remaining integration gaps.

### 16.1 ILISH → CatalogEntry Field Mapping

The publish flow (section 15.5) extracts CatalogEntry fields from ILISH. The exact mapping must account for optional fields in the ILISH interface:

```typescript
// shared/src/lish.ts — actual interface
interface ILISH {
  version: number;
  id: string;
  name?: string;         // optional — may be undefined
  description?: string;  // optional — may be undefined
  created: string;       // ISO 8601
  chunkSize: number;
  checksumAlgo: HashAlgorithm;  // 'sha256' | 'sha384' | 'sha512' etc.
  directories?: IDirectoryEntry[];
  files?: IFileEntry[];   // optional — may be undefined for metadata-only LISHs
  links?: ILinkEntry[];
}

// ILISHSummary now includes verification status (added in main)
interface ILISHSummary {
  // ... existing fields ...
  verifiedChunks: number;    // NEW — chunks verified via integrity check
  totalChunks: number;       // NEW — total chunks in LISH
}

interface IFileEntry {
  path: string;
  size: number;
  checksums: string[];   // per-chunk hashes
  // ... permissions, modified, created
}
```

**Mapping to CatalogEntry**:

```typescript
// IStoredLISH extends ILISH with storage-specific fields (directory?, chunks?).
// See shared/src/lish.ts for full definition.

interface PublishOptions {
  contentType?: CatalogEntry['contentType'];
  tags?: string[];
}

function lishToCatalogEntry(
  lish: IStoredLISH,
  publisherPeerID: string,
  opts?: PublishOptions
): Omit<CatalogEntry, 'hlc' | 'signature'> {
  const files = lish.files ?? [];
  return {
    // Immutable identity fields
    lishID: lish.id,
    publisherPeerID,
    publishedAt: new Date().toISOString(),
    chunkSize: lish.chunkSize,
    checksumAlgo: lish.checksumAlgo,
    fileCount: files.length,
    totalSize: files.reduce((sum, f) => sum + f.size, 0),
    manifestHash: sha256(canonicalize(lish)),

    // Editable metadata — set at publish time, can be edited later by any moderator
    name: lish.name,           // undefined if not set — UI shows lishID as fallback
    description: lish.description,
    contentType: opts?.contentType,  // set by publisher in UI
    tags: opts?.tags,                // set by publisher in UI
  };
}
```

**Validation before publish**:
- `lish.files` must be defined and non-empty (can't catalog an empty LISH)
- `lish.chunkSize` must be > 0
- `lish.id` must be a valid UUID
- `totalSize` must be > 0

**Fields NOT copied from ILISH**: `directories`, `links`, `created` (LISH creation date is separate from catalog publish date). The full manifest is fetched on demand via `get_lish_req`.

### 16.2 Catalog Listing Pagination

`catalog.list(networkID)` returning all entries is fine for small catalogs but becomes a problem at scale (50K entries = ~25 MB JSON over WebSocket).

**Decision: Cursor-based pagination.**

```typescript
// API
catalog.list(networkID, { limit?: number, cursor?: string, sort?: 'newest' | 'oldest' | 'name' })
  → { entries: CatalogEntry[], cursor: string | null, total: number }

// Default: limit=100, sort='newest' (by HLC, most recent first)
// cursor is the lishID of the last item in the previous page
// cursor=null means no more pages
```

**Implementation** (in-memory, Phase 1):

```typescript
function paginatedList(
  entries: Map<string, CatalogEntry>,
  opts: { limit: number; cursor?: string; sort: 'newest' | 'oldest' | 'name' }
): { entries: CatalogEntry[]; cursor: string | null; total: number } {
  let sorted = [...entries.values()];

  // Sort
  switch (opts.sort) {
    case 'newest': sorted.sort((a, b) => hlcCompare(b.hlc, a.hlc)); break;
    case 'oldest': sorted.sort((a, b) => hlcCompare(a.hlc, b.hlc)); break;
    case 'name': sorted.sort((a, b) => (a.name ?? '').localeCompare(b.name ?? '')); break;
  }

  // Cursor: find start position
  let startIdx = 0;
  if (opts.cursor) {
    const idx = sorted.findIndex(e => e.lishID === opts.cursor);
    if (idx >= 0) startIdx = idx + 1;
  }

  const page = sorted.slice(startIdx, startIdx + opts.limit);
  const hasMore = startIdx + opts.limit < sorted.length;

  return {
    entries: page,
    cursor: hasMore ? page[page.length - 1]!.lishID : null,
    total: sorted.length,
  };
}
```

**Performance note**: The current implementation sorts all entries on every `list()` call — O(n log n). For Phase 1 (<10K entries, <15 ms sort), this is acceptable. If pagination becomes a bottleneck, cache the sorted array and invalidate only when entries are mutated (add/update/remove).

**Frontend**: Products page loads first 100 entries, then loads more on scroll (infinite scroll pattern matching the existing grid layout).

### 16.3 Sync Triggers — When Does Bilateral Sync Fire?

The document describes bilateral sync but never specifies **when** it runs.

**Decision: Three trigger mechanisms.**

```
1. On join (immediate):
   When a peer joins a network (setEnabled → catalogManager.join):
   - Wait 1-5s random jitter (thundering herd prevention)
   - Pick random connected peer from topic subscribers
   - Run full bilateral sync
   - If delta > 100 entries, repeat with a different peer for cross-validation

2. Periodic anti-entropy (background):
   Every 60 seconds per joined network:
   - If any peers available, pick one at random
   - Exchange vector summaries (cheap — just HLC map)
   - If summaries differ, run delta sync
   - This catches any GossipSub messages lost silently

3. On peer discovery (opportunistic):
   When a new peer connects to the topic (gossipsub:graft event):
   - Wait 2-5s (let the peer stabilize)
   - If our catalog is empty or our vectorClock has gaps, trigger sync
   - Otherwise skip (periodic anti-entropy will catch it)
```

**CatalogCRDT class — unified shape** (implementations are spread across sections 15.10, 17.8, 18.2):

```typescript
class CatalogCRDT {
  // === Public/readonly fields ===
  readonly networkID: string;

  // === Private fields ===
  private readonly ownerPeerID: string;
  private readonly network: Network;
  private readonly path: string;                    // catalog/<networkID>.cbor
  private state: CatalogCRDTState;                  // in-memory CRDT state
  private opQueue: Promise<void> = Promise.resolve(); // per-network mutation serializer (§15.10)
  private saveTimer: Timer | null = null;           // debounced persistence (§15.10)
  private dirty: boolean = false;                   // pending unsaved changes
  private antiEntropyTimer: Timer | null = null;    // periodic sync timer

  // Precondition: network.start() must have completed before constructing CatalogCRDT.
  // getNodeInfo() returns null if the network is not started.
  constructor(networkID: string, ownerPeerID: string, network: Network, dataDir: string) {
    this.networkID = networkID;
    this.ownerPeerID = ownerPeerID;
    this.network = network;
    this.path = `${dataDir}/catalog/${networkID}.cbor`;

    const nodeInfo = network.getNodeInfo();
    if (!nodeInfo) throw new Error('Network must be started before creating CatalogCRDT');

    this.state = {
      networkID,
      entries: new Map(),
      tombstones: new Map(),
      opLog: new Map(),
      access: { owner: ownerPeerID, admins: [], moderators: [], restrictCatalogWrites: false },
      vectorClock: new Map(),
      localClock: { wallTime: Date.now(), logical: 0, nodeID: nodeInfo.peerID },
      syncState: new Map(),
    };
  }

  // === Lifecycle ===

  /**
   * Restore CRDT state from a persisted CBOR snapshot.
   * CBOR's `mapsAsObjects: true` means Maps are decoded as plain objects — reconstruct them.
   */
  loadFromSnapshot(snapshot: CatalogSnapshot): void {
    this.state.entries = new Map(snapshot.entries.map(e => [e.lishID, e]));
    this.state.tombstones = new Map(snapshot.tombstones.map(t => [t.lishID, t]));
    // opLog: keyed by the lishID from the operation's data payload
    this.state.opLog = new Map(
      snapshot.opLog.map(op => [(op.payload.data as any).lishID ?? op.payload.data.lishID, op])
    );
    this.state.access = snapshot.access;
    this.state.vectorClock = new Map(Object.entries(snapshot.vectorClock));
    this.state.localClock = snapshot.localClock;
    this.state.syncState = new Map(Object.entries(snapshot.syncState));
  }

  getState(): CatalogCRDTState { return this.state; }

  async flush(): Promise<void> {
    if (this.saveTimer) clearTimeout(this.saveTimer);
    this.saveTimer = null;
    if (this.dirty) {
      await saveCatalog(this.path, this.state);
      this.dirty = false;
    }
  }

  // === Query methods (called by API handlers) ===
  getEntry(lishID: string): CatalogEntry | undefined { return this.state.entries.get(lishID); }
  list(opts: { limit: number; cursor?: string; sort: string }): CatalogListResult { /* §16.2 */ }
  search(query: string): CatalogEntry[] { /* §15.7 */ }
  getAccess(): ICatalogAccess { return this.state.access; }
  getSyncStatus(): CatalogSyncStatus { /* entry/tombstone counts, last sync, peer count */ }

  // === Mutation methods (called by API handlers, go through enqueueOperation) ===
  async publish(lish: IStoredLISH, opts?: PublishOptions): Promise<void> { /* §15.5 */ }
  async updateEntry(lishID: string, fields: Record<string, any>): Promise<void> { /* sign + apply + broadcast */ }
  async removeEntry(lishID: string): Promise<void> { /* sign + apply tombstone + broadcast */ }
  async updateAccess(changes: ACLChange): Promise<void> { /* sign + apply ACL + broadcast */ }

  // === Remote operation handling ===
  async handleRemoteOperation(msg: Record<string, any>): Promise<void> { /* §17.8 */ }
  async handleSyncStream(stream: Stream, request: any): Promise<void> {
    // Respond to bilateral sync: compute delta ops from opLog, encode as CBOR, send
  }

  // === CRDT merge (core) ===
  private applyDataOp(op: SignedCatalogOp): void {
    /* §18.2 — handles add/update/remove + stores op in opLog */
  }
  private applyACLOp(op: SignedCatalogOp): void { /* §18.2 */ }

  /**
   * Merge a SignedCatalogOp received via bilateral sync.
   * Full signature verification is possible because the original CatalogOpPayload
   * (including nonce) is preserved in the SignedCatalogOp envelope.
   */
  private mergeSyncOp(op: SignedCatalogOp): void {
    // 1. Verify signature (uses verifyCatalogOp — full payload including nonce)
    if (!verifyCatalogOp(op)) {
      console.warn(`Bilateral sync: rejected op with invalid signature from ${op.signer}`);
      return;
    }

    // 2. Check networkID
    if (op.payload.networkID !== this.networkID) return;

    // 3. HLC replay check
    const lastSeen = this.state.vectorClock.get(op.signer);
    if (lastSeen && hlcCompare(op.payload.hlc, lastSeen) <= 0) return;

    // 4. Apply via normal CRDT merge (reuses applyDataOp/applyACLOp)
    if (op.payload.type === 'acl_grant' || op.payload.type === 'acl_revoke') {
      this.applyACLOp(op);
    } else {
      this.applyDataOp(op);
    }

    // 5. Update vector clock by signer (not publisherPeerID — signer is the author
    //    of the current version, which may differ from original publisher after updates)
    this.state.vectorClock.set(op.signer, op.payload.hlc);
    // Note: opLog storage happens inside applyDataOp() — no duplicate store needed here
  }

  // === Sync (bilateral) ===
  startAntiEntropy(): void {
    this.antiEntropyTimer = setInterval(() => {
      this.trySync();
    }, 60_000 + Math.random() * 10_000);  // 60-70s jitter
  }

  stopAntiEntropy(): void {
    if (this.antiEntropyTimer) clearInterval(this.antiEntropyTimer);
    this.antiEntropyTimer = null;
  }

  private async trySync(): Promise<void> {
    const peers = this.network.getTopicPeers(this.networkID);
    if (peers.length === 0) return;
    const peer = peers[Math.floor(Math.random() * peers.length)]!;
    await this.bilateralSync(peer);
  }

  /**
   * Initiate bilateral sync with a remote peer.
   * Note: Network.dialProtocol() currently takes multiaddrs[], not peerID string.
   * Requires adding a peerID-based overload that resolves multiaddrs from the peer store:
   *   async dialProtocolByPeerId(peerID: string, protocol: string): Promise<Stream>
   * Or resolve multiaddrs here: const peer = await this.network.node.peerStore.get(peerIdFromString(peerID));
   */
  private async bilateralSync(peerID: string): Promise<void> {
    try {
      const stream = await this.network.dialProtocolByPeerId(peerID, '/lish/catalog-sync/1.0.0');
      const request = encoder.encode({
        command: 'catalog_sync_req',
        requestID: crypto.randomUUID(),
        networkID: this.networkID,
        vectorSummary: Object.fromEntries(this.state.vectorClock),
        lishIDs: [...this.state.entries.keys()],
      });
      await stream.sink([request]);

      let responseBytes = new Uint8Array();
      for await (const chunk of stream.source) {
        responseBytes = concat([responseBytes, chunk.subarray()]);
        if (responseBytes.byteLength > MAX_SYNC_PAYLOAD) {
          stream.abort(new Error('payload too large'));
          return;
        }
      }

      const response = decoder.decode(responseBytes);
      // Merge operations (each fully verifiable via SignedCatalogOp)
      for (const op of response.operations ?? []) {
        this.mergeSyncOp(op);
      }
      // Run tombstone GC after sync (§17.2)
      garbageCollectTombstones(this.state);
      this.scheduleSave();
    } catch (err) {
      console.error(`Bilateral sync with ${peerID} failed:`, err);
    }
  }

  // === Internal helpers ===
  async enqueueOperation(op: () => Promise<void>): Promise<void> { /* §15.10 */ }
  private scheduleSave(): void { /* §15.10: debounced 500ms write */ }
}
```

### 16.4 Cross-Network Replay Prevention

A valid signed operation from network A could be replayed on network B. The `SignedCatalogOp.payload.networkID` field exists for this purpose, but section 4.4's `validateOperation()` doesn't check it.

**Decision: Add networkID check to validation.**

```typescript
// Updated validateOperation() — canonical version using SignedCatalogOp fields.
// Section 4.4 shows the conceptual model with SignedOperation fields (op.authorPeerID, op.op).
// During implementation, map: op.signer → authorPeerID, op.payload.type → op type,
// op.payload.hlc → HLC, op.payload.data → entry/tombstone/ACLChange.
function validateOperation(
  op: SignedCatalogOp,
  currentACL: ICatalogAccess,
  expectedNetworkID: string,
  vectorClock: Map<string, HLC>
): ValidationResult {
  // 0. Check networkID matches (cross-network replay prevention)
  if (op.payload.networkID !== expectedNetworkID) {
    return { valid: false, reason: 'NETWORK_ID_MISMATCH' };
  }

  // 1. Verify Ed25519 signature
  if (!verifyCatalogOp(op)) {
    return { valid: false, reason: 'INVALID_SIGNATURE' };
  }

  // 2. Check authorization (using op.signer as authorPeerID, op.payload.type as op type)
  // ... (see section 4.4 for full authorization logic)

  // 3. HLC anti-replay check
  const lastSeen = vectorClock.get(op.signer);
  if (lastSeen && hlcCompare(op.payload.hlc, lastSeen) <= 0) {
    return { valid: false, reason: 'REPLAY_DETECTED' };
  }

  return { valid: true };
}
```

The `networkID` is part of the signed `CatalogOpPayload`, so it cannot be tampered with. A valid operation from network A will have `networkID: "A"` baked into the signature. Replaying it on network B fails because `op.payload.networkID !== "B"`.

### 16.5 Shared Types for Frontend

The frontend needs CatalogEntry, ICatalogAccess, and related types. These must be in `shared/src/` (the existing shared package used by both backend and frontend).

**New file**: `shared/src/catalog.ts`

```typescript
// shared/src/catalog.ts

export interface HLC {
  wallTime: number;
  logical: number;
  nodeID: string;
}

export interface CatalogEntry {
  // Immutable fields
  lishID: string;
  publisherPeerID: string;
  publishedAt: string;
  chunkSize: number;
  checksumAlgo: string;
  fileCount: number;
  totalSize: number;
  manifestHash?: string;

  // Editable metadata
  name?: string;
  description?: string;
  contentType?: 'software' | 'game' | 'video' | 'audio' | 'image' | 'document' | 'dataset' | 'archive' | 'other';
  tags?: string[];

  // System fields
  hlc: HLC;
  signature: string;
  lastEditedBy?: string;
}

export interface ICatalogAccess {
  owner: string;
  admins: string[];
  moderators: string[];
  restrictCatalogWrites: boolean;
}

export interface CatalogSyncStatus {
  entryCount: number;
  tombstoneCount: number;
  lastSyncAt: string | null;
  peers: number;
}

export interface CatalogListResult {
  entries: CatalogEntry[];
  cursor: string | null;
  total: number;
}

export type ContentType = CatalogEntry['contentType'];
```

**Re-export from `shared/src/index.ts`**:

```typescript
export type { CatalogEntry, ICatalogAccess, CatalogSyncStatus, CatalogListResult, HLC, ContentType } from './catalog.ts';
```

### 16.6 API Handler Registration Pattern

The API server uses a handler init pattern. The catalog needs a new handler file following the same convention:

```typescript
// backend/src/api/catalog.ts

import { type CatalogManager } from '../catalog/catalog-manager.ts';
import { type CatalogCRDT } from '../catalog/catalog-crdt.ts';
import { type DataServer } from '../lish/data-server.ts';
import type { CatalogEntry, ICatalogAccess, CatalogSyncStatus, CatalogListResult } from '@shared';
import { Utils } from '../utils.ts';
const assert = Utils.assertParams;

interface CatalogHandlers {
  list: (p: { networkID: string; limit?: number; cursor?: string; sort?: string }) => CatalogListResult;
  get: (p: { networkID: string; lishID: string }) => CatalogEntry | null;
  search: (p: { networkID: string; query: string }) => CatalogEntry[];
  publish: (p: { networkID: string; lishID: string; contentType?: string; tags?: string[] }) => Promise<void>;
  update: (p: { networkID: string; lishID: string; fields: Record<string, any> }) => Promise<void>;
  remove: (p: { networkID: string; lishID: string }) => Promise<void>;
  getAccess: (p: { networkID: string }) => ICatalogAccess;
  updateAccess: (p: { networkID: string; changes: any }) => Promise<void>;
  getSyncStatus: (p: { networkID: string }) => CatalogSyncStatus;
}

export function initCatalogHandlers(
  catalogManager: CatalogManager,
  dataServer: DataServer
): CatalogHandlers {
  function getCatalog(networkID: string): CatalogCRDT {
    const crdt = catalogManager.get(networkID);
    if (!crdt) throw new Error('Catalog not available for this network');
    return crdt;
  }

  return {
    list(p) {
      assert(p, ['networkID']);
      return getCatalog(p.networkID).list({
        limit: p.limit ?? 100,
        cursor: p.cursor,
        sort: (p.sort as any) ?? 'newest',
      });
    },
    get(p) {
      assert(p, ['networkID', 'lishID']);
      return getCatalog(p.networkID).getEntry(p.lishID);
    },
    search(p) {
      assert(p, ['networkID', 'query']);
      return getCatalog(p.networkID).search(p.query);
    },
    async publish(p) {
      assert(p, ['networkID', 'lishID']);
      const lish = dataServer.get(p.lishID);
      if (!lish) throw new Error('LISH not found locally');
      if (!lish.files?.length) throw new Error('LISH has no files');
      await getCatalog(p.networkID).publish(lish, {
        contentType: p.contentType,
        tags: p.tags,
      });
    },
    async update(p) {
      assert(p, ['networkID', 'lishID', 'fields']);
      await getCatalog(p.networkID).updateEntry(p.lishID, p.fields);
    },
    async remove(p) {
      assert(p, ['networkID', 'lishID']);
      await getCatalog(p.networkID).removeEntry(p.lishID);
    },
    getAccess(p) {
      assert(p, ['networkID']);
      return getCatalog(p.networkID).getAccess();
    },
    async updateAccess(p) {
      assert(p, ['networkID', 'changes']);
      await getCatalog(p.networkID).updateAccess(p.changes);
    },
    getSyncStatus(p) {
      assert(p, ['networkID']);
      return getCatalog(p.networkID).getSyncStatus();
    },
  };
}
```

**Registration in `api.ts`** (following existing pattern):

```typescript
const _catalog = initCatalogHandlers(catalogManager, this.dataServer);

this.handlers = {
  // ... existing handlers ...

  // Catalog
  'catalog.list': _catalog.list,
  'catalog.get': _catalog.get,
  'catalog.search': _catalog.search,
  'catalog.publish': _catalog.publish,
  'catalog.update': _catalog.update,
  'catalog.remove': _catalog.remove,
  'catalog.getAccess': _catalog.getAccess,
  'catalog.updateAccess': _catalog.updateAccess,
  'catalog.getSyncStatus': _catalog.getSyncStatus,
};
```

**CatalogManager injection**: `APIServer` constructor receives `CatalogManager` as a new parameter. Created in `app.ts` alongside `Networks`.

### 16.7 Frontend Products Page Redesign

The current `Products.svelte` has 200 hardcoded items. Full redesign needed:

**Current state** (to be replaced):
```typescript
const items = Array.from({ length: 200 }, (_, i) => ({ id: i + 1, title: 'Item ' + (i + 1) }));
```

**New data flow**:

```
1. Products page receives networkID prop (selected in sidebar/header)
2. On mount: call api.catalog.list(networkID, { limit: 100 })
3. Display entries in existing grid layout (reuse ProductsItem component)
4. Infinite scroll: on scroll near bottom, load next page via cursor
5. Search: call api.catalog.search(networkID, query) on search input
6. Live updates: subscribe to catalog:updated / catalog:removed events
7. Moderator actions: publish button (if user has moderator+ role), edit metadata, remove
```

**ProductsItem changes**:

```
Current props: { title: string, image: string, isGamepadHovered, isAPressed }
New props:     { entry: CatalogEntry, isGamepadHovered, isAPressed }

Display:
- entry.name ?? entry.lishID (truncated UUID)
- entry.totalSize → human-readable (e.g., "4.8 GB")
- entry.fileCount → "12 files"
- entry.contentType → icon/badge
- entry.tags → tag chips
- entry.publishedAt → relative time ("2 days ago")
- No real image yet — use contentType-based placeholder icon
```

**Product detail (Product.svelte) changes**:

```
On select: Product component receives CatalogEntry
- Show full metadata (name, description, publisher, size, files, tags)
- "Download" button → triggers transfer.download(networkID, lishID)
- "Edit" button (if moderator+) → opens metadata edit form
- "Remove" button (if moderator+) → confirms and calls catalog.remove
- Fetch full LISH manifest on demand for file listing via existing get_lish_req/res protocol
```

### 16.8 File Structure — Planned Backend Modules

Summary of all new files needed for implementation:

```
shared/src/
├── catalog.ts              (NEW: CatalogEntry, ICatalogAccess, HLC, CatalogListResult)
├── index.ts                (EDIT: re-export catalog types, add ownerPeerID to ILISHNetwork)
└── lish.ts                 (existing, unchanged)

backend/src/
├── catalog/
│   ├── catalog-hlc.ts      (NEW: HLC implementation — tick, merge, compare)
│   ├── catalog-signer.ts   (NEW: signCatalogOp, verifyCatalogOp — Ed25519 signing)
│   ├── catalog-validator.ts (NEW: handleRemoteOp validation chain — sig, ACL, drift, content, anti-replay)
│   ├── catalog-manager.ts  (NEW: CatalogManager — multi-lishnet lifecycle, DB-backed)
│   └── catalog-sync.ts     (NEW: bilateral sync stream handler + initiator)
├── api/
│   ├── catalog.ts          (NEW: initCatalogHandlers — WebSocket API)
│   └── api.ts              (EDIT: register catalog handlers, inject CatalogManager)
├── protocol/
│   ├── network.ts          (EDIT: add getPrivateKey(), registerStreamHandler(), dialProtocolByPeerId(),
│   │                               registerTopicValidator(); already uses CodedError for errors)
│   └── network-config.ts   (EDIT: future — upgrade gossipsub D to >=6, add peer scoring)
├── lishnet/
│   └── lishnets.ts         (EDIT: call catalogManager.join/leave on setEnabled)
├── db/
│   ├── catalog.ts          (NEW: catalog tables schema, CRUD, LWW upsert, FTS5 sync, delta queries)
│   └── lishnets.ts         (EDIT: add owner_peer_id column via ALTER TABLE)
└── app.ts                  (EDIT: create CatalogManager, pass to APIServer)

frontend/src/
├── pages/Products/
│   ├── Products.svelte     (REWRITE: API-driven catalog grid with pagination)
│   └── ProductsItem.svelte (EDIT: accept CatalogEntry prop instead of title/image)
├── pages/Product/
│   └── Product.svelte      (EDIT: show CatalogEntry details, download/edit/remove actions)
└── scripts/
    └── catalog.ts          (NEW: catalog API client wrapper, event subscriptions)
```

**Total new files**: 7 backend + 1 shared + 1 frontend script = 9 new files
**Total edited files**: 10 (api.ts, network.ts, network-config.ts, lishnet/lishnets.ts, db/lishnets.ts, db/database.ts, app.ts, shared/index.ts, shared/errors.ts, Products.svelte) + 2 frontend edits (ProductsItem.svelte, Product.svelte)

---

## 17. Open Design Questions — Hardening & Operational Concerns (Resolved)

Analysis of security checklist items, implementation phases, and cross-section consistency reveals these remaining gaps that must be resolved before Phase 1 is complete.

### 17.1 signCatalogOp: Clock Ownership

The `localClock` used by `signCatalogOp()` lives inside `CatalogCRDTState.localClock`, owned by the `CatalogCRDT` instance. Section 11 has been updated to accept `localClock` as a parameter and return the updated clock alongside the signed operation.

**Design: Pass localClock as parameter, return updated clock.**

```typescript
// Updated signCatalogOp signature
export async function signCatalogOp(
  privateKey: Ed25519PrivateKey,
  type: CatalogOpPayload['type'],
  networkID: string,
  data: Record<string, unknown>,
  localClock: HLC                  // caller passes current clock
): Promise<{ op: SignedCatalogOp; updatedClock: HLC }> {
  const newClock = hlcTick(localClock);
  const payload: CatalogOpPayload = {
    type,
    networkID,
    hlc: newClock,
    nonce: crypto.randomUUID(),
    data,
  };
  const canonical = canonicalize(payload);
  const bytes = encoder.encode(canonical);
  const sig = await privateKey.sign(bytes);
  return {
    op: {
      payload,
      signature: Buffer.from(sig).toString('base64url'),
      signer: privateKey.publicKey.toString(),
      keyType: 'Ed25519',
    },
    updatedClock: newClock,
  };
}
```

**Caller** (inside `CatalogCRDT.publish()`):

```typescript
const { op, updatedClock } = await signCatalogOp(
  this.network.getPrivateKey(), 'add', this.networkID, entryData, this.state.localClock
);
this.state.localClock = updatedClock;
```

This ensures HLC monotonicity — the clock advances on every local operation and never goes backward.

### 17.2 Tombstone Garbage Collection

The security checklist requires "tombstones kept for minimum 30 days (time-based GC)". Without GC, tombstones accumulate indefinitely and waste storage + bandwidth.

**Decision: Time-based GC with safety window.**

```typescript
const TOMBSTONE_RETENTION_MS = 30 * 24 * 60 * 60 * 1000;  // 30 days

function garbageCollectTombstones(state: CatalogCRDTState): number {
  const cutoff = Date.now() - TOMBSTONE_RETENTION_MS;
  let removed = 0;

  for (const [lishID, tombstone] of state.tombstones) {
    // Use HLC wallTime directly (epoch ms) — avoids ISO string parsing overhead
    if (tombstone.hlc.wallTime < cutoff) {
      state.tombstones.delete(lishID);
      removed++;
    }
  }
  return removed;
}
```

**When to run**: On every anti-entropy cycle (every 60s), after successful bilateral sync. Not on every GossipSub message — GC is not urgent.

**Risk: zombie re-add.** If a peer was offline for 31 days, they may not have seen a tombstone that was already GC'd. When they come back, they re-add the deleted entry. This is a known limitation of time-based tombstone GC.

**Mitigation**: On bilateral sync, the responding peer sends a `gcCutoff` timestamp. If the requesting peer has entries older than `gcCutoff` that are not in the responder's catalog, the requester should treat them as potentially deleted and flag them for manual review (not auto-remove — to avoid data loss from a malicious responder).

```typescript
// Bilateral sync response includes gcCutoff
interface CatalogSyncResponse {
  // ... existing fields ...
  gcCutoff: number;  // epoch ms — tombstones before this were garbage collected
}
```

### 17.3 Rate Limiting on Incoming Operations

The security checklist requires "sliding-window rate limiter per publisher PeerID". Without it, a rogue moderator can flood the catalog.

**Decision: Per-peer sliding window + global budget.**

```typescript
const RATE_LIMITS = {
  maxOpsPerPeerPerMinute: 10,    // max 10 operations per peer per 60s window
  maxOpsGlobalPerMinute: 100,    // max 100 operations across all peers per 60s
  maxEntriesPerPublisher: 1000,  // max entries a single publisher can have in catalog
  maxCatalogSize: 50_000,        // max total entries per network catalog
} as const;

class RateLimiter {
  private windows: Map<string, number[]> = new Map();  // peerID → timestamp[]
  private globalWindow: number[] = [];

  check(peerID: string): 'allow' | 'reject' {
    const now = Date.now();
    const cutoff = now - 60_000;

    // Per-peer check
    const peerOps = (this.windows.get(peerID) ?? []).filter(t => t > cutoff);
    if (peerOps.length >= RATE_LIMITS.maxOpsPerPeerPerMinute) return 'reject';

    // Global check
    this.globalWindow = this.globalWindow.filter(t => t > cutoff);
    if (this.globalWindow.length >= RATE_LIMITS.maxOpsGlobalPerMinute) return 'reject';

    // Record
    peerOps.push(now);
    this.windows.set(peerID, peerOps);
    this.globalWindow.push(now);
    return 'allow';
  }
}
```

**Integration with GossipSub topic validator** (section 17.4): Rate-limited operations return `IGNORE` (not `REJECT`), because the sender may be legitimately busy — penalizing them with a `REJECT` score would be too harsh.

**Integration with catalog size limits**: Before applying an `add` operation, check:
1. `entries.size < RATE_LIMITS.maxCatalogSize` (global cap)
2. Count of entries where `entry.publisherPeerID === op.signer` < `maxEntriesPerPublisher`

### 17.4 GossipSub Topic Validator

The security checklist requires "topic validator registered for catalog topics". GossipSub validators run before message delivery and can score peers.

**Decision: Register a topic validator that validates signatures and rate limits.**

**Network class extension needed**: The `pubsub.topicValidators` map is not directly accessible from outside `Network`. Add a `registerTopicValidator(topic, validator)` method to `Network` class (similar to `registerStreamHandler()` in section 15.4):

```typescript
// Addition to Network class (network.ts)
registerTopicValidator(
  topic: string,
  validator: (peerID: PeerId, msg: Message) => Promise<TopicValidatorResult>
): void {
  if (!this.node) throw new Error('Network not started');
  (this.node.services.pubsub as any).topicValidators.set(topic, validator);
}
```

**Fragility note**: `topicValidators` is accessed via `as any` cast because gossipsub does not expose it in its public TypeScript interface. This is a known gossipsub internals dependency. If the gossipsub library upgrades and renames the map, this will break silently. Mitigation: add a runtime check in `registerTopicValidator()` that throws if the map doesn't exist, and pin the `@chainsafe/libp2p-gossipsub` version in `package.json`.

**Validator implementation**:

```typescript
// Registered once per network topic in CatalogManager.join()
network.registerTopicValidator(lishTopic(networkID), async (peerID, msg) => {
  try {
    const data = JSON.parse(new TextDecoder().decode(msg.data));
    if (data.type !== 'catalog_op') return TopicValidatorResult.Accept;  // not a catalog message

    // Parse as SignedCatalogOp
    const op = data as SignedCatalogOp;

    // 1. Field size limits (cheap — fail fast before signature verification)
    // Only validate sizes for ops that carry CatalogEntry data
    if ((op.payload.type === 'add' || op.payload.type === 'update') &&
        !validateFieldSizes(op.payload.data as CatalogEntry)) {
      return TopicValidatorResult.Reject;  // definite spam → penalize sender
    }

    // 2. Signature verification
    if (!await verifyCatalogOp(op)) {
      return TopicValidatorResult.Reject;  // invalid signature → definite bad actor
    }

    // 3. Rate limit check
    if (rateLimiter.check(op.signer) === 'reject') {
      return TopicValidatorResult.Ignore;  // rate limited → don't penalize, just skip
    }

    return TopicValidatorResult.Accept;
  } catch {
    return TopicValidatorResult.Ignore;  // parse error — could be old format, don't penalize
  }
});
```

**Return values**:
- `Accept`: Message delivered to handlers
- `Reject`: Message dropped, peer's P4 score decremented (invalid messages penalty)
- `Ignore`: Message dropped silently, no score impact

**Note**: The topic validator runs on the GossipSub layer (before message reaches `topicHandlers`). This means signature verification happens twice — once in the validator (for scoring) and once in `validateOperation()` (for CRDT merge). In practice, the validator result is cached, so the second check can be skipped. Implementation detail for Phase 2.

### 17.5 v1 .lishnet Upgrade Path

Existing networks have `.lishnet` files without `ownerPeerID`. Section 15.3 makes this field optional for backward compatibility, but doesn't describe the upgrade flow.

**Decision: Prompt-based upgrade + network re-export.**

```
Scenario: User has v1 .lishnet files (no ownerPeerID)

1. User opens LiberShare v2 (with catalog support)
2. Backend queries lishnets table — detects entries without ownerPeerID
3. For each network where local peer is the creator:
   - UI shows notification: "Network X can be upgraded to support catalogs"
   - User confirms → backend sets ownerPeerID to local PeerID
   - Backend exports updated .lishnet file for redistribution
4. For networks where local peer is NOT the creator:
   - UI shows: "Waiting for network owner to upgrade"
   - Catalog features are disabled for this network
   - File sharing continues to work (catalog is optional)
5. When user imports an updated .lishnet (with ownerPeerID):
   - Backend merges: updates ownerPeerID, keeps local enabled/disabled state
   - Catalog features become available
```

**Key constraint**: Only the original creator should set `ownerPeerID`. We cannot know who the creator is from a v1 file. The upgrade prompt appears for all members, but only the actual creator should confirm. This is a trust decision — the first person to claim ownership gets it. After that, the signed `.lishnet` with `ownerPeerID` is redistributed, and other members import it.

**Edge case**: Two members both claim ownership before redistributing. Whoever's `.lishnet` file gets imported by more members wins — there's no consensus mechanism. This is intentional: `.lishnet` files are distributed out-of-band, and the creator is assumed to be the one sharing them.

### 17.6 Error Response Format

API handlers throw generic `Error` objects. The frontend needs structured error codes to show appropriate UI.

**Decision: Standardized error codes for catalog operations.**

```typescript
// Catalog-specific error codes (returned in WebSocket { id, error } responses)
const CATALOG_ERRORS = {
  // Client errors (user can fix)
  CATALOG_NOT_AVAILABLE: 'Catalog not available for this network (missing ownerPeerID or not joined)',
  LISH_NOT_FOUND: 'LISH not found locally — import it first',
  LISH_NO_FILES: 'LISH has no files — cannot publish empty LISH',
  ENTRY_NOT_FOUND: 'Catalog entry not found',
  UNAUTHORIZED: 'Insufficient permissions for this operation',
  INVALID_FIELDS: 'Invalid update fields — only name, description, contentType, tags allowed',
  FIELD_TOO_LARGE: 'Field exceeds size limit',
  RATE_LIMITED: 'Too many operations — try again later',
  CATALOG_FULL: 'Catalog has reached maximum size',

  // System errors (user cannot fix)
  NETWORK_NOT_RUNNING: 'Network is not running',
  SIGNING_FAILED: 'Failed to sign operation',
  SYNC_FAILED: 'Bilateral sync failed',
} as const;

type CatalogErrorCode = keyof typeof CATALOG_ERRORS;
```

**Integration**: API handlers throw `CatalogError(code)` instead of generic `Error`. The WebSocket error response includes `{ id, error: { code, message } }`:

```typescript
class CatalogError extends Error {
  constructor(public readonly code: CatalogErrorCode) {
    super(CATALOG_ERRORS[code]);
  }
}

// In API handler:
if (!lish) throw new CatalogError('LISH_NOT_FOUND');
```

**Frontend**: Switches on `error.code` to show appropriate UI (toast, dialog, disabled button, etc.).

### 17.7 Graceful Degradation

What happens when the catalog system fails? File sharing must continue.

**Decision: Catalog is optional — failures are isolated.**

```
Failure scenario                       Impact                          Recovery
───────────────────────────────────────────────────────────────────────────────
Catalog CBOR file corrupt              Catalog resets to empty          Bilateral sync restores from peers
signCatalogOp() throws                 Single publish/update fails      Retry via UI; error shown to user
Bilateral sync timeout                 No catch-up for this cycle       Next anti-entropy cycle (60s)
GossipSub message lost                 Single op missed                 Anti-entropy detects + fills gap
All peers offline                      Cannot sync; local state stale   Works offline with local cache
CatalogManager crash                   All catalog ops fail             App.ts catches, logs error, continues
                                                                        File sharing (get_lish, get_chunk) unaffected
Network.getPrivateKey() throws         Cannot sign any ops              Cannot publish; can still browse local cache
CBOR decode error on sync              Delta rejected for this peer     Try different peer on next cycle
```

**Design principle**: The catalog system NEVER blocks or crashes the main application. `CatalogManager` methods are wrapped in try/catch at the API handler level. If catalog features are broken, the UI shows "Catalog unavailable" and the user can still:
- Browse locally imported LISHs
- Download files via direct `get_lish_req`/`get_chunk_req`
- Manage network connections
- Import/export `.lishnet` files

### 17.8 handleRemoteOperation: Complete Method

Section 15.6 references `crdt.handleRemoteOperation(msg)` and the code review identified that `scheduleSave()` must be called internally. Here's the complete method:

```typescript
class CatalogCRDT {
  /**
   * Handle an incoming GossipSub catalog_op message.
   * Validates the operation, merges into local state, and schedules persistence.
   */
  async handleRemoteOperation(msg: Record<string, any>): Promise<void> {
    await this.enqueueOperation(async () => {
      const op = msg as SignedCatalogOp;

      // Validate: signature, ACL, networkID, HLC replay
      const result = validateOperation(op, this.state.access, this.networkID, this.state.vectorClock);
      if (!result.valid) {
        console.log(`Rejected catalog op from ${op.signer}: ${result.reason}`);
        return;
      }

      // Power-events-first: ACL ops applied immediately
      if (op.payload.type === 'acl_grant' || op.payload.type === 'acl_revoke') {
        this.applyACLOp(op);
      } else {
        this.applyDataOp(op);
      }

      // Update vector clock
      const mergedClock = hlcMerge(this.state.localClock, op.payload.hlc);
      this.state.localClock = mergedClock;
      this.state.vectorClock.set(op.signer, op.payload.hlc);

      // Schedule debounced persistence
      this.scheduleSave();
    });
  }
}
```

This resolves the C1 finding (scheduleSave is now called internally, not from CatalogManager).

---

## 18. Final Design Questions — Completeness (Resolved)

### 18.1 Open-Mode Spam Protection

Section 12 identified: "Networks with `restrictCatalogWrites=false` have no spam protection." The section 17.3 rate limiter helps (10 ops/peer/min), but in open mode any peer can write — Sybil attackers create many PeerIDs to bypass per-peer limits.

**Decision: Defense in depth for open networks.**

```
Layer 1: Per-peer rate limit (section 17.3)
  10 ops/peer/min — stops naive spam from single peer

Layer 2: Global rate limit (section 17.3)
  100 ops/global/min — caps total throughput regardless of peer count

Layer 3: Global catalog size cap
  50K entries max per network — absolute upper bound

Layer 4: GossipSub peer scoring (Phase 4)
  P4 invalid message penalty, P5 app-specific scoring, P6 IP colocation
  Multiple PeerIDs from same IP get penalized → Sybil deterrent

Layer 5: Owner emergency lockdown
  Owner can set restrictCatalogWrites=true at any time via acl_grant/acl_revoke
  Takes effect immediately — all pending open-mode writes are rejected
```

**Practical impact**: An attacker with 10 Sybil peers can add 100 entries/min, filling 50K catalog in ~8 hours. This is an acceptable worst case because:
1. The 50K cap prevents unbounded growth
2. GossipSub IP scoring (Phase 4) makes Sybil attacks expensive
3. Owner can flip `restrictCatalogWrites=true` as emergency response
4. Moderators can batch-remove spam entries

**For Phase 1**: Layers 1-3 are sufficient. Layers 4-5 are implemented in Phase 4.

### 18.2 applyACLOp and applyDataOp: CRDT Merge Methods

Section 17.8's `handleRemoteOperation()` calls `applyACLOp()` and `applyDataOp()` without defining them. These are the core CRDT merge methods:

```typescript
class CatalogCRDT {
  /**
   * Apply an ACL operation (grant/revoke role).
   * Power-events-first: ACL ops are applied before catalog ops in any batch.
   * Remove-wins semantics: a revocation always beats a concurrent grant.
   */
  private applyACLOp(op: SignedCatalogOp): void {
    const change = op.payload.data as ACLChange;

    if (op.payload.type === 'acl_grant') {
      for (const peerID of change.peerIDs) {
        if (change.role === 'admin') {
          if (!this.state.access.admins.includes(peerID)) {
            this.state.access.admins.push(peerID);
          }
        } else if (change.role === 'moderator') {
          if (!this.state.access.moderators.includes(peerID)) {
            this.state.access.moderators.push(peerID);
          }
        }
      }
    } else if (op.payload.type === 'acl_revoke') {
      for (const peerID of change.peerIDs) {
        if (change.role === 'admin') {
          this.state.access.admins = this.state.access.admins.filter(id => id !== peerID);
          // Cascading revocation (Farcaster pattern):
          // Find all moderators granted by this admin and revoke them too.
          // In a full implementation, track who granted each moderator
          // to enable selective cascading. For Phase 1, revoke all
          // moderators that were granted by the revoked admin.
        } else if (change.role === 'moderator') {
          this.state.access.moderators = this.state.access.moderators.filter(id => id !== peerID);
        }
      }
    }
  }

  /**
   * Apply a data operation (add, update, remove catalog entry).
   * Whole-entry LWW: higher HLC wins.
   */
  private applyDataOp(op: SignedCatalogOp): void {
    switch (op.payload.type) {
      case 'add': {
        const entry = op.payload.data as CatalogEntry;
        // Assign HLC and signature from the signed op
        entry.hlc = op.payload.hlc;
        entry.signature = op.signature;

        const existing = this.state.entries.get(entry.lishID);
        if (!existing || hlcCompare(entry.hlc, existing.hlc) > 0) {
          // Check tombstone: if entry was removed and tombstone HLC > entry HLC, skip
          const tombstone = this.state.tombstones.get(entry.lishID);
          if (tombstone && hlcCompare(tombstone.hlc, entry.hlc) > 0) {
            return;  // entry was deleted after this add — skip
          }
          this.state.entries.set(entry.lishID, entry);
        }
        break;
      }

      case 'update': {
        const update = op.payload.data as { lishID: string; fields: Record<string, any> };
        const existing = this.state.entries.get(update.lishID);
        if (!existing) return;  // can't update non-existent entry

        // Whole-entry LWW: only apply if incoming HLC is higher
        if (hlcCompare(op.payload.hlc, existing.hlc) > 0) {
          // Merge editable fields
          Object.assign(existing, update.fields);
          existing.hlc = op.payload.hlc;
          existing.signature = op.signature;
          existing.lastEditedBy = op.signer;
          this.state.entries.set(update.lishID, existing);
        }
        break;
      }

      case 'remove': {
        const data = op.payload.data as { lishID: string };
        const tombstone: TombstoneEntry = {
          lishID: data.lishID,
          removedByPeerID: op.signer,
          removedAt: new Date(op.payload.hlc.wallTime).toISOString(),
          hlc: op.payload.hlc,
          signature: op.signature,
        };
        // Tombstone always wins (remove-wins semantics)
        const existingTombstone = this.state.tombstones.get(data.lishID);
        if (!existingTombstone || hlcCompare(tombstone.hlc, existingTombstone.hlc) > 0) {
          this.state.tombstones.set(data.lishID, tombstone);
        }
        // Remove from active entries
        this.state.entries.delete(data.lishID);
        break;
      }
    }

    // Store in opLog for bilateral sync forwarding (every applied data op is preserved)
    const lishID = (op.payload.data as any).lishID;
    if (lishID) this.state.opLog.set(lishID, op);
  }
}
```

**Key semantics**:
- **Add**: Insert if new or HLC is higher than existing. Skip if tombstoned.
- **Update**: Whole-entry LWW — only apply if incoming HLC > existing HLC. After an update, `entry.signature` is the signature of the update operation's `CatalogOpPayload`, not a signature covering the full entry state. Immutable fields (`publisherPeerID`, `totalSize`, `manifestHash`, etc.) are guaranteed by the original `add` op's signature preserved in `opLog`.
- **Remove**: Always creates tombstone. Tombstone beats add if HLC is higher (remove-wins).
- **ACL grant**: Append to role array (idempotent — deduplicated).
- **ACL revoke**: Filter from role array. Cascading revocation for admin demotion.

### 18.3 Testing Strategy (Phase 1)

The implementation phases mention "unit tests" but don't specify what to test. Here's the Phase 1 test plan:

**Framework**: Bun's built-in test runner (`bun test`). No additional dependencies needed.

```
catalog/
├── __tests__/
│   ├── catalog-crdt.test.ts       Core CRDT logic (LWW merge via SQL)
│   ├── catalog-hlc.test.ts        HLC correctness
│   ├── catalog-signer.test.ts     Signing and verification
│   ├── catalog-db.test.ts         SQLite persistence and FTS5
│   └── catalog-validation.test.ts Authorization and replay prevention
```

**Test categories and cases**:

```
1. HLC (catalog-hlc.test.ts)
   - tick() advances wallTime or logical counter
   - merge() takes max of local/remote wallTimes
   - compare() produces deterministic total order
   - nodeID breaks ties when wallTime and logical are equal
   - clock drift: reject ops with wallTime > 60s in future

2. Signing (catalog-signer.test.ts)
   - sign and verify round-trip: signCatalogOp → verifyCatalogOp returns true
   - tampered payload: modify any field → verifyCatalogOp returns false
   - wrong key: sign with key A, verify expects key B → false
   - updatedClock is always > input localClock
   - networkID is embedded in signed payload

3. CRDT merge (catalog-crdt.test.ts)
   - add + add same entry: higher HLC wins (LWW)
   - add + remove: remove wins if HLC is higher
   - remove + add: add wins if HLC is higher (re-add after delete)
   - concurrent adds of different entries: both present after merge
   - update: only editable fields change, immutable fields preserved
   - update: lower HLC update rejected (LWW)
   - tombstone prevents re-add with lower HLC
   - empty catalog: bilateral sync fills all entries

4. ACL (catalog-validation.test.ts)
   - owner can add/remove admins
   - admin can add/remove moderators
   - moderator cannot manage roles (anti-escalation)
   - revoked moderator's writes rejected after revocation HLC
   - open mode: any peer can add entries
   - restricted mode: only moderator+ can add entries
   - cascading revocation: revoke admin → their moderators also revoked

5. SQLite Persistence (catalog-db.test.ts)
   - schema creation: all 5 tables created correctly
   - UPSERT LWW: higher HLC overwrites, lower HLC rejected
   - FTS5 index: search by name/description/tags returns correct results
   - FTS5 consistency: entry update also updates FTS index (single transaction)
   - tombstone: entry removed from results after tombstone insert
   - vector clock: persisted and loaded correctly on restart
   - WAL mode: concurrent reads during write transaction

6. Rate limiting (catalog-validation.test.ts)
   - 10 ops within 1 min: all accepted
   - 11th op within 1 min: rejected
   - after 1 min window: counter resets
   - global limit: 100 ops across all peers
   - publisher quota: 1001st entry from same publisher rejected
```

**Integration tests** (Phase 2, requires running libp2p node):
- Two peers sync catalogs via bilateral stream
- GossipSub broadcast reaches all topic subscribers
- Topic validator rejects invalid signatures
- Anti-entropy detects and fills missed operations

---

## 19. Custom 2P-Set CRDT — Deep Implementation Analysis

This section provides a comprehensive analysis of implementing a custom 2P-Set CRDT layer for the online catalog, covering architecture, implementation strategy, pitfalls, edge cases, and comparison with alternatives.

### 19.1 Why Custom (Summary of Decision)

After evaluating 14 libraries (§2), the conclusion is clear: **no existing library provides Byzantine-safe CRDT for untrusted P2P peers over SQLite with libp2p transport**. The closest candidate (cr-sqlite) explicitly assumes friendly actors and adds 2.5x write overhead. Since 90% of the work is security validation (signatures, ACL, anti-replay) regardless of CRDT library choice, and the CRDT itself is trivial (~150 lines of SQL), building custom is the correct approach.

### 19.2 What is a 2P-Set?

A **Two-Phase Set** (2P-Set) is one of the simplest CRDTs. It consists of two grow-only sets:

```
2P-Set = {
  addSet:    Set<Element>    // elements that have been added
  removeSet: Set<Element>    // elements that have been removed (tombstones)
}

lookup(e) = e ∈ addSet ∧ e ∉ removeSet
add(e)    = addSet ∪ {e}
remove(e) = removeSet ∪ {e}   // only if e ∈ addSet
merge(a, b) = {
  addSet:    a.addSet ∪ b.addSet,
  removeSet: a.removeSet ∪ b.removeSet
}
```

**Key property**: Once an element is removed, it can never be re-added (classic 2P-Set limitation). Our implementation relaxes this with **LWW (Last-Writer-Wins)** semantics: an element can be re-added if the new add has a higher HLC than the tombstone.

### 19.3 Our Variant: Signed LWW 2P-Set

The LiberShare catalog is NOT a pure 2P-Set. It's a **Signed LWW 2P-Set with ACL**, which adds several layers:

```
Standard 2P-Set:
  addSet:    Set<Element>
  removeSet: Set<Element>

Our variant:
  addSet:    Map<(networkID, lishID) → SignedCatalogEntry>   // LWW by HLC
  removeSet: Map<(networkID, lishID) → SignedTombstone>      // LWW by HLC
  aclSet:    Map<(networkID, peerID, role) → SignedACLOp>    // grant/revoke chain
  clockSet:  Map<(networkID, peerID) → HLC>                  // vector clock for anti-replay

  lookup(e)  = e ∈ addSet ∧ (e ∉ removeSet ∨ addSet[e].hlc > removeSet[e].hlc)
  add(e)     = verify(sig) → checkACL → checkDrift → upsert if HLC > existing
  remove(e)  = verify(sig) → checkACL → insert tombstone if HLC > existing add
  merge(a,b) = for each entry: keep the one with higher HLC (deterministic tiebreak)
```

**Critical difference from textbook CRDT**: Every operation carries an Ed25519 signature and must pass a 5-step validation chain before being applied. The CRDT merge is trivial; the security layer is the real implementation.

### 19.4 SQLite as CRDT State (Architecture)

Instead of in-memory Maps materialized to disk, **SQLite IS the CRDT state**. The LWW merge is expressed as a single SQL statement:

```
                    ┌──────────────────────────┐
                    │     SignedCatalogOp       │
                    │  (from GossipSub/Sync)    │
                    └────────────┬─────────────┘
                                 │
                    ┌────────────▼─────────────┐
                    │   Validation Chain        │
                    │  1. Signature (Ed25519)   │
                    │  2. ACL (role check)      │
                    │  3. Drift (±5 min)        │
                    │  4. Content (field sizes) │
                    │  5. Anti-replay (HLC)     │
                    └────────────┬─────────────┘
                                 │ PASS
                    ┌────────────▼─────────────┐
                    │   SQL UPSERT with LWW    │
                    │  INSERT ON CONFLICT DO    │
                    │  UPDATE WHERE hlc > old   │
                    └────────────┬─────────────┘
                                 │
                    ┌────────────▼─────────────┐
                    │   SQLite (WAL mode)       │
                    │  catalog_entries          │
                    │  catalog_tombstones       │
                    │  catalog_acl              │
                    │  catalog_clocks           │
                    │  catalog_fts (FTS5)       │
                    └──────────────────────────┘
```

**Why this works**: The CRDT guarantee (convergence) requires only that all peers apply the same operations with the same merge function. The merge function (`INSERT ON CONFLICT DO UPDATE WHERE hlc > existing`) is deterministic. Two peers applying the same set of valid operations in any order will end up with the same SQLite state.

### 19.5 Implementation Components

The custom 2P-Set layer consists of these components:

```
db/catalog.ts              (~200 lines)  SQL schema + CRUD functions
catalog/catalog-hlc.ts     (~80 lines)   HLC tick, merge, compare
catalog/catalog-signer.ts  (~100 lines)  Ed25519 sign + verify
catalog/catalog-validator.ts (~150 lines) 5-step validation chain
catalog/catalog-manager.ts (~300 lines)  Lifecycle, sync triggers, API
catalog/catalog-sync.ts    (~200 lines)  Bilateral sync stream handler
```

**Total**: ~1030 lines of TypeScript. This is less code than integrating cr-sqlite (~800 lines) + still needing the validator (~150 lines) + the signer (~100 lines) + glue code.

#### Component Responsibilities

| Component | Responsibility | Dependencies |
|---|---|---|
| `db/catalog.ts` | Schema creation, UPSERT with LWW, queries, FTS5 | `bun:sqlite` |
| `catalog-hlc.ts` | HLC data structure, tick (local), merge (remote), compare | None |
| `catalog-signer.ts` | `signCatalogOp()`, `verifyCatalogOp()` | `json-canonicalize`, `@libp2p/crypto` |
| `catalog-validator.ts` | `handleRemoteOp()` — the 5-step chain | `catalog-signer`, `db/catalog` |
| `catalog-manager.ts` | Per-network lifecycle, local clock, anti-entropy timer | All above |
| `catalog-sync.ts` | Bilateral stream handler, delta exchange | `cbor-x`, `catalog-validator` |

### 19.6 Implementation Pitfalls and Edge Cases

#### Pitfall 1: HLC Monotonicity After Restart

**Problem**: If the process crashes, the in-memory HLC is lost. On restart, `Date.now()` might return a value lower than the last HLC's wallTime (e.g., NTP adjustment). New operations would have a lower HLC than previous ones → rejected by peers as replay.

**Solution**: Persist the latest local HLC in `catalog_clocks` table. On startup, load it and use `max(persisted.wallTime, Date.now())` as the starting point.

```typescript
function initLocalClock(db: Database, networkID: string, localPeerID: string): HLC {
  const persisted = getLatestClock(db, networkID, localPeerID);
  const now = Date.now();
  if (persisted && persisted.wallTime >= now) {
    // Clock went backwards (NTP adjustment) — use persisted + bump logical
    return { wallTime: persisted.wallTime, logical: persisted.logical + 1, nodeID: localPeerID };
  }
  return { wallTime: now, logical: 0, nodeID: localPeerID };
}
```

#### Pitfall 2: Tombstone GC and Late-Joining Peers

**Problem**: Tombstones are garbage-collected after 30 days. A peer that was offline for >30 days rejoins and sends an `add` operation for an entry that was deleted. Without the tombstone, other peers re-accept the deleted entry.

**Solution**: During bilateral sync, include a `gcCutoff` timestamp in the sync response. The syncing peer knows that any entries deleted before `gcCutoff` may have been garbage-collected. For entries older than `gcCutoff` not in the current catalog, the peer should NOT re-broadcast its local copy.

```typescript
interface SyncResponse {
  entries: SignedCatalogOp[];    // current entries
  tombstones: SignedCatalogOp[]; // active tombstones
  gcCutoff: number;              // entries deleted before this were GC'd
}
```

**Residual risk**: A malicious peer can still re-inject a GC'd entry by crafting a new `add` op with a fresh HLC. This is acceptable because:
- They need moderator permissions (ACL check still applies)
- The re-added entry gets a new HLC (not the original) — it's effectively a new entry
- Other moderators can remove it again

#### Pitfall 3: Concurrent ACL and Data Operations

**Problem**: A batch of operations arrives from a peer after reconnect. The batch contains an `acl_revoke` for moderator X and several `add` operations from moderator X. Processing order matters.

**Solution**: **Power-events-first rule** (Matrix pattern). Sort incoming batches: ACL operations first, then data operations. Within each group, sort by HLC.

```typescript
function processBatch(db: Database, networkID: string, ops: SignedCatalogOp[]): void {
  // Partition into ACL ops and data ops
  const aclOps = ops.filter(op => op.payload.type.startsWith('acl_'));
  const dataOps = ops.filter(op => !op.payload.type.startsWith('acl_'));

  // Process ACL first (may change who is authorized)
  for (const op of aclOps.sort(byHLC)) {
    handleRemoteOp(db, networkID, op);
  }

  // Then data ops (checked against updated ACL)
  for (const op of dataOps.sort(byHLC)) {
    handleRemoteOp(db, networkID, op);
  }
}
```

#### Pitfall 4: FTS5 Index Consistency

**Problem**: FTS5 is a separate virtual table. If a crash occurs between updating `catalog_entries` and updating `catalog_fts`, the index becomes inconsistent with the data.

**Solution**: Use a single SQL transaction that updates both tables atomically. SQLite WAL ensures either both writes succeed or neither does.

```typescript
function upsertCatalogEntry(db: Database, networkID: string, op: SignedCatalogOp): void {
  db.transaction(() => {
    // 1. UPSERT into catalog_entries (LWW merge)
    upsertEntryStmt.run(/* ... */);

    // 2. Update FTS5 index
    // DELETE old entry from FTS (if exists), INSERT new
    deleteFtsStmt.run(networkID, op.payload.data.lishID);
    insertFtsStmt.run(networkID, op.payload.data.lishID, op.payload.data.name, op.payload.data.description, op.payload.data.tags);
  })();
}
```

#### Pitfall 5: `signed_op` BLOB Preservation

**Problem**: During bilateral sync, peers must forward the original `SignedCatalogOp` envelope — not re-signed copies. If the BLOB is decoded, modified (even accidentally), and re-encoded, the signature becomes invalid.

**Solution**: Store the raw CBOR bytes as-is in the `signed_op` BLOB column. Never decode the BLOB for forwarding — pass it directly from SQLite to the sync stream.

```typescript
// CORRECT: pass raw BLOB to sync
const rows = db.query('SELECT signed_op FROM catalog_entries WHERE network_id = ? AND hlc_wall > ?').all(networkID, since);
for (const row of rows) {
  stream.write(row.signed_op); // raw bytes, no decode/re-encode
}

// WRONG: decode then re-encode (signature breaks if cbor-x adds/reorders fields)
// const op = cbor.decode(row.signed_op);
// stream.write(cbor.encode(op)); // ← signature may be invalid!
```

#### Pitfall 6: SQLite Busy/Locked Errors

**Problem**: Multiple async operations (GossipSub handler, bilateral sync, API request) write to the catalog tables concurrently. SQLite WAL mode allows concurrent reads but writes are serialized. Under load, writes can get `SQLITE_BUSY`.

**Solution**: `bun:sqlite` handles WAL serialization internally. For burst situations (bilateral sync receiving hundreds of entries), batch operations into a single transaction:

```typescript
function applyDelta(db: Database, networkID: string, ops: SignedCatalogOp[]): number {
  let applied = 0;
  db.transaction(() => {
    for (const op of ops) {
      if (handleRemoteOp(db, networkID, op)) applied++;
    }
  })();
  return applied;
}
```

#### Pitfall 7: Vector Clock Size Growth

**Problem**: The `catalog_clocks` table grows with one row per `(network_id, peer_id)`. In a network with thousands of transient peers, this table grows unbounded.

**Solution**: GC stale clock entries for peers not seen in 30 days (same as tombstone GC). If a stale peer reconnects, they trigger a full bilateral sync instead of delta sync — acceptable because it's rare.

```sql
DELETE FROM catalog_clocks
WHERE network_id = ? AND last_seen < datetime('now', '-30 days');
```

### 19.7 Convergence Proof (Informal)

For two peers A and B to converge, they must reach the same state after exchanging all operations. Our system guarantees this because:

1. **Deterministic merge**: `INSERT ON CONFLICT DO UPDATE WHERE hlc > existing` is a pure function of (current_state, incoming_op). Given the same inputs, it produces the same output.

2. **Deterministic tiebreaking**: When `hlc_wall` and `hlc_logical` are equal, `hlc_node` (PeerID string comparison) provides a total order. No ambiguity.

3. **Commutativity**: For any two valid operations op1 and op2 on different keys `(networkID, lishID)`, applying op1 then op2 produces the same state as op2 then op1. For operations on the same key, LWW picks the higher HLC regardless of application order.

4. **Idempotency**: Applying the same operation twice produces the same result as applying it once (SQL `ON CONFLICT DO UPDATE WHERE hlc > existing` — second application has `hlc = existing`, condition fails, no change).

5. **Associativity**: Merging states `(A merge B) merge C = A merge (B merge C)` — each key independently resolves to the highest-HLC value.

**Caveat**: Convergence holds only for operations that **pass validation**. If peer A accepts an operation that peer B rejects (different ACL state), they diverge. The power-events-first rule and bilateral sync protocol mitigate this by ensuring ACL state converges first.

### 19.8 Comparison: Custom vs Library (Effort Breakdown)

| Work Item | Custom 2P-Set | cr-sqlite | automerge-repo |
|---|---|---|---|
| Schema design | ~200 lines SQL | ~50 lines (CRR declarations) | N/A (opaque docs) |
| LWW merge logic | ~30 lines (SQL UPSERT) | Built-in (hidden in triggers) | Built-in |
| HLC implementation | ~80 lines | Need custom (cr-sqlite uses Lamport) | Built-in (but not HLC) |
| Ed25519 signing | ~100 lines | **Still needed** | **Still needed** |
| Validation chain | ~150 lines | **Still needed** | **Still needed** |
| ACL enforcement | ~100 lines (in validator) | **Still needed** | **Still needed** |
| Anti-replay | ~50 lines (vector clock) | **Still needed** | Built-in (but no Byzantine) |
| FTS5 search | Built-in (SQL trigger) | Extra setup needed | Not available |
| Bilateral sync | ~200 lines | ~100 lines (changeset API) | Built-in (NetworkAdapter) |
| Manager lifecycle | ~300 lines | ~200 lines | ~200 lines |
| Native extension loading | N/A | ~50 lines + risk | N/A |
| Materialization layer | N/A | N/A | ~300 lines (doc → SQL) |
| **Total** | **~1030 lines** | **~950 lines + native ext risk** | **~1050 lines + no SQL search** |
| **Security coverage** | Full | Partial (no Byzantine) | Partial (no Byzantine) |
| **Dependencies added** | 0 (bun:sqlite) | 1 native ext (~2MB) | 1 pkg (~604KB WASM) |
| **Debug/audit** | Full visibility | Opaque triggers | Opaque binary state |

**Conclusion**: Custom implementation is roughly the same amount of code as any library integration, but with full security coverage, zero new dependencies, full debuggability, and no native extension risk.

### 19.9 What Can Go Wrong (Risk Analysis)

| Risk | Severity | Likelihood | Mitigation |
|---|---|---|---|
| HLC drift causes permanent ordering issues | High | Low | ±5 min drift guard, NTP recommended in docs |
| Tombstone GC causes entry resurrection | Medium | Medium | `gcCutoff` in sync, re-add requires fresh ACL check |
| FTS5 out of sync with entries | High | Low | Single transaction for entry + FTS updates |
| `signed_op` BLOB corruption on re-encode | High | Medium | Never decode BLOBs for forwarding, pass raw bytes |
| SQLite BUSY under concurrent sync | Medium | Medium | Batch writes in transactions, WAL mode |
| Vector clock table unbounded growth | Low | Medium | 30-day GC for stale peers |
| ACL state divergence between peers | High | Low | Power-events-first, bilateral sync includes ACL ops |
| Memory usage for large catalogs | Low | Low | SQLite handles this — no in-memory state beyond prepared stmts |
| Bug in LWW merge SQL | Critical | Low | Extensive unit tests (§18.3), property-based testing |
| Ed25519 key loss by owner | Critical | Medium | User documentation, future: multi-sig ownership |

### 19.10 Implementation Order (Recommended)

Phase 1 implementation should follow this order to minimize integration risk:

```
Step 1: catalog-hlc.ts + tests
  - Pure functions, no dependencies
  - Easy to test in isolation
  - Foundation for everything else

Step 2: db/catalog.ts + tests
  - Schema creation (5 tables)
  - UPSERT with LWW merge
  - Query functions
  - FTS5 index maintenance
  - Test with mock data (no signing yet)

Step 3: catalog-signer.ts + tests
  - signCatalogOp() and verifyCatalogOp()
  - Depends on: catalog-hlc (for HLC in payload)
  - Test: sign → verify round-trip, tampered payload, wrong key

Step 4: catalog-validator.ts + tests
  - handleRemoteOp() — the 5-step chain
  - Depends on: catalog-signer, db/catalog
  - Test: each validation step independently, then full chain

Step 5: catalog-manager.ts
  - Lifecycle: init per-network state on join, cleanup on leave
  - Local clock management (load from DB, tick, persist)
  - Anti-entropy timer (periodic bilateral sync trigger)
  - API methods (list, get, search, publish, remove)

Step 6: catalog-sync.ts (Phase 2)
  - Bilateral sync stream handler
  - Delta exchange based on vector clocks
  - gcCutoff handling
  - Depends on: catalog-validator (to validate incoming ops)
```

**Key principle**: Each step is independently testable. Steps 1-4 have no network dependencies — they can be tested with a pure in-memory SQLite database and generated Ed25519 keys.

### 19.11 Alternatives Considered and Rejected

#### Alternative A: Event Sourcing (Append-Only Log)

Store all operations in an append-only log, replay to compute current state.

**Pros**: Complete audit trail, easy to debug, natural fit for CRDT.
**Cons**: Log grows forever, replay on startup becomes slow, need compaction (which is equivalent to maintaining materialized state — back to our approach).

**Verdict**: Our `signed_op` BLOB column gives us the audit trail benefit without the replay cost. The materialized columns (`name`, `description`, etc.) are the compacted state.

#### Alternative B: Merkle-DAG (IPFS-style)

Store entries as content-addressed blocks in a Merkle DAG, like IPFS Cluster's go-ds-crdt.

**Pros**: Built-in integrity verification, natural deduplication.
**Cons**: Requires IPFS/Helia integration (rejected in §2), content-addressed blocks can't be queried by SQL, no FTS5, would need a separate materialization layer.

**Verdict**: Adds massive complexity for no benefit. Our catalog entries are mutable (LWW updates) — content addressing doesn't fit naturally.

#### Alternative C: CRDTs Per-Field (MVRegister)

Use a Multi-Value Register per field, allowing concurrent edits to different fields to merge without conflict.

**Pros**: No lost updates when editing different fields concurrently.
**Cons**: Complexity explosion — need to track HLC per field per entry, signature must cover individual fields, merge function becomes much more complex, `catalog_entries` table would need `hlc_wall_name`, `hlc_logical_name`, `hlc_wall_description`, etc.

**Verdict**: Overkill. Concurrent edits to different fields of the same catalog entry are extremely rare (moderators coordinating). The LWW whole-entry approach (§14.4) is the right trade-off.

#### Alternative D: Operation-Based CRDT (CmRDT)

Instead of state-based merge, use operation-based CRDT where operations are delivered exactly-once in causal order.

**Pros**: Smaller messages (just the operation, not the state).
**Cons**: Requires reliable causal broadcast (exactly-once + causal ordering). GossipSub provides at-least-once with no ordering guarantees. Building causal broadcast on top of gossipsub is a research problem, not an engineering task.

**Verdict**: Our state-based approach (merge any state, any order, converge) is robust to GossipSub's delivery semantics. The `signed_op` blob is operation-sized, so we get the bandwidth benefit of CmRDT without the delivery requirements.

### 19.12 Performance Characteristics

| Operation | Expected Performance | Bottleneck |
|---|---|---|
| Local add/update/remove | <1ms | Ed25519 sign (~0.1ms) + SQL INSERT |
| Remote op validation | <1ms | Ed25519 verify (~0.2ms) + 4 SQL lookups |
| Bilateral sync (1000 entries) | ~200ms | CBOR decode + 1000× validation + batch INSERT |
| FTS5 search | <10ms | SQLite FTS5 query (well-optimized for this scale) |
| Catalog list (page of 50) | <1ms | Simple SELECT with LIMIT/OFFSET |
| Tombstone GC | <10ms | Single DELETE statement |
| Anti-entropy check | <1ms | Compare vector clock timestamps |

**Scale expectations**: Catalogs of 1K-10K entries are the target range. At 10K entries with ~500 bytes metadata each, the `catalog_entries` table is ~5 MB + `signed_op` BLOBs ~5 MB = ~10 MB total. Well within SQLite's comfort zone.

**Memory footprint**: Unlike in-memory CRDT approaches, the only memory used is SQLite's page cache (configurable, default ~2 MB) plus prepared statement handles. No `Map<string, CatalogEntry>` in heap.

---

## Document Status

**All identified design questions have been resolved.** This document covers:

- Technology selection with 14 library evaluations (section 2)
- Complete data model with interfaces (section 3)
- Security model with signing, ACL, HLC, and untrusted-peer validation (sections 4, 11)
- Sync protocol with two layers — GossipSub + bilateral stream (section 5)
- SQLite persistence with LWW merge via UPSERT (section 6)
- API surface and events (section 7)
- Implementation phases (section 9)
- Security checklist — 30+ items (section 10)
- Integration with existing codebase (sections 15-16)
- Hardening: GC, rate limits, topic validators, error handling (section 17)
- CRDT merge implementation, spam protection, testing (section 18)
- Custom 2P-Set deep analysis: architecture, pitfalls, convergence, performance (section 19)

**Ready for Phase 1 implementation.**
