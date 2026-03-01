# LiberShare Online Catalog (DB LISHs) - Architecture Analysis

**Date**: 2026-02-28 (updated 2026-02-28)
**Branch**: `feat/online-db`
**Status**: Design phase - Research iteration 5 (integration design, protocol mapping, crash safety, concurrency)
**Author**: Analysis by Claude, discussed with Jiri Kreibich
**Research sources**: libp2p source code, IPFS Cluster, go-ds-crdt, Nostr NIPs, Matrix, Farcaster, BitTorrent BEP-52, gossipsub v1.1 spec

---

## 1. Problem Statement

LiberShare currently has no way for peers to discover what content is available in a network. The Products/Library page is a hardcoded mockup with 200 fake items. To make the app useful, each lishnet needs a **shared, replicated catalog** of available LISHs that all peers can browse and search.

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
| **cr-sqlite** | Future option | Full SQL queries, but native binary distribution complexity, Bun extension loading untested |
| **Custom 2P-Set CRDT** | **Selected** | Zero dependencies, fits existing libp2p stack perfectly, ~100 lines of code, full control over security |

### Why Custom CRDT Wins

The catalog is semantically simple:
- **Immutable entries** keyed by UUID (no per-field conflicts)
- **Single-writer per role** (moderators, not all peers simultaneously)
- **Append-mostly** with rare deletions
- **Small metadata** (~500 bytes per entry, full LISH fetched on demand)

This is a **signed 2P-Set** (grow-only set of additions + grow-only set of deletions), the simplest possible CRDT. No library needed.

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
  contentType?: 'software' | 'media' | 'document' | 'dataset' | 'archive' | 'other';
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
  access: ICatalogAccess;
  vectorClock: Map<string, HLC>;     // peerID -> highest HLC seen from that peer
  localClock: HLC;                   // this peer's current HLC
  syncState: Map<string, HLC>;      // peerID -> last HLC synced with that peer (for delta sync)
}
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
- **Private key** is stored locally in the SQLite datastore
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
- **Bounded drift**: If `wallTime` diverges from real time by > MAX_DRIFT (60s), reject the operation — prevents time-travel attacks
- **Same anti-replay**: Each operation's HLC must be strictly greater than the last seen from that author

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
const MAX_ENTRIES_PER_PUBLISHER = 10_000;  // configurable per network
const MAX_CATALOG_SIZE = 100_000;          // global soft cap
const RATE_LIMIT_WINDOW = 60_000;          // 1 minute
const RATE_LIMIT_MAX_OPS = 10;             // max 10 ops per window per publisher

// Sliding window rate limiter per publisher PeerID
const rateLimiter = new Map<string, number[]>();

function checkRateLimit(publisherPeerID: string): boolean {
  const now = Date.now();
  const timestamps = rateLimiter.get(publisherPeerID) ?? [];
  const recent = timestamps.filter(t => now - t < RATE_LIMIT_WINDOW);
  if (recent.length >= RATE_LIMIT_MAX_OPS) return false; // IGNORE, not REJECT
  recent.push(now);
  rateLimiter.set(publisherPeerID, recent);
  return true;
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

For live operations while peers are connected. Messages are small JSON payloads broadcast to the `lish/<networkID>` topic.

```typescript
// Add a LISH to catalog
{
  type: 'catalog_op',
  op: 'add',
  entry: CatalogEntry,
  authorPeerID: string,
  hlc: HLC,
  signature: string
}

// Update metadata of existing LISH (any moderator+ can edit any entry)
{
  type: 'catalog_op',
  op: 'update',
  lishID: string,
  fields: { name?, description?, contentType?, tags? },
  authorPeerID: string,
  hlc: HLC,
  signature: string
}

// Remove a LISH from catalog
{
  type: 'catalog_op',
  op: 'remove',
  lishID: string,
  removedByPeerID: string,
  hlc: HLC,
  signature: string
}

// Grant or revoke ACL role
{
  type: 'catalog_op',
  op: 'acl_grant' | 'acl_revoke',
  change: ACLChange,
  authorPeerID: string,
  hlc: HLC,
  signature: string
}
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
  entries: CatalogEntry[],        // entries you don't have
  tombstones: TombstoneEntry[],   // deletions you don't have
  access: ICatalogAccess,         // current ACL state
  vectorSummary: Record<string, HLC>
}
```

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

The CRDT state lives **in memory** during runtime. Persistence is a snapshot written to disk on every change and reloaded on startup. One file per lishnet:

```
data/
├── lishs.json            (existing - local LISH manifests)
├── lishnets.json         (existing - network configs)
├── settings.json         (existing)
├── catalog/
│   ├── <networkID>.cbor  (catalog entries + tombstones + ACL)
│   └── <networkID>.cbor
└── datastore.db          (existing - libp2p peer store)
```

### Format Evaluation

| | JSON | MessagePack | **CBOR** | SQLite |
|---|---|---|---|---|
| File size (10K entries) | 5 MB | 3.2 MB | **3 MB** | ~4 MB |
| Signatures stored as | base64 string (33% overhead) | base64 string (33% overhead) | **native bytes (0% overhead)** | BLOB (0% overhead) |
| Write strategy | full file rewrite | full file rewrite | **full file rewrite** | per-row INSERT/UPDATE |
| Parse speed (10K) | ~50 ms | ~20 ms | **~15 ms** | ~5 ms (indexed query) |
| Search | filter in memory | filter in memory | **filter in memory** | SQL + FTS5 fulltext |
| Human readable | yes (text editor) | no | **no** | SQLite browser |
| New dependency | none | @msgpack/msgpack | **cbor-x** | none (bun:sqlite) |
| Binary data support | no (base64 workaround) | limited | **native (Uint8Array, Buffer)** | native (BLOB) |
| Standards | RFC 8259 | msgpack.org spec | **RFC 8949 (IETF standard)** | — |
| Used by libp2p internally | no | no | **yes (dag-cbor)** | no |

### Decision: CBOR (RFC 8949)

**Selected**: `cbor-x` package for encoding/decoding.

**Why CBOR over JSON:**

1. **Native binary data** — Ed25519 signatures (64 bytes), public keys, and manifest hashes are binary. JSON requires base64 encoding (+33% size, encode/decode overhead on every operation). CBOR stores `Uint8Array` directly
2. **~40% smaller files** — no repeated key names in quotes, no base64 bloat, compact integer encoding. A 5 MB JSON catalog becomes ~3 MB CBOR
3. **~3x faster parsing** — `cbor-x` is one of the fastest serializers for Node/Bun, binary format skips text parsing entirely
4. **IETF standard** — RFC 8949, widely adopted (WebAuthn, COSE signatures, IPFS dag-cbor, IoT). Not a niche format
5. **libp2p ecosystem alignment** — IPFS and libp2p use dag-cbor internally for content-addressed data. Same conceptual model

**Why CBOR over MessagePack:**

- MessagePack has no native `Uint8Array` type — binary data needs explicit `Ext` type wrapping
- CBOR is an IETF standard (RFC 8949), MessagePack is a community spec
- CBOR has COSE (RFC 9052) for signed structures — potential future use for standardized signature envelopes
- Performance difference is negligible (`cbor-x` and `@msgpack/msgpack` are within 5% of each other)

**Why CBOR over SQLite (for now):**

- SQLite solves a different problem (partial writes, indexed queries) that we don't need at <10K entries
- CRDT merge is simpler with full-state serialization than with SQL INSERT/UPDATE reconciliation
- Full file rewrite is fine up to ~25 MB (~50K entries, ~100 ms write time)
- SQLite would require mapping CRDT semantics to relational schema — added complexity for no gain at current scale
- **Migration path**: If catalogs grow beyond 50K entries, SQLite becomes the right choice. The persistence layer is isolated from CRDT logic, so migration is a clean module swap

**When to reconsider SQLite:**

| Signal | Action |
|---|---|
| Catalog write time exceeds 100 ms | Migrate to SQLite |
| Users request fulltext search across catalogs | Add SQLite with FTS5 |
| Single catalog exceeds 50K entries | SQLite partial writes become essential |
| Need to query across multiple lishnets | SQLite with shared DB file |

### Implementation

```typescript
// backend/src/catalog/catalog-persistence.ts
import { Encoder, Decoder } from 'cbor-x';
import { renameSync, existsSync, unlinkSync } from 'fs';

const encoder = new Encoder({ mapsAsObjects: true, useRecords: false });
const decoder = new Decoder({ mapsAsObjects: true });

interface CatalogSnapshot {
  version: 1;                           // schema version for forward compatibility
  entries: CatalogEntry[];
  tombstones: TombstoneEntry[];
  access: ICatalogAccess;
  vectorClock: Record<string, HLC>;
  localClock: HLC;
  syncState: Record<string, HLC>;
}

export async function saveCatalog(path: string, state: CatalogCRDTState): Promise<void> {
  const snapshot: CatalogSnapshot = {
    version: 1,
    entries: [...state.entries.values()],
    tombstones: [...state.tombstones.values()],
    access: state.access,
    vectorClock: Object.fromEntries(state.vectorClock),
    localClock: state.localClock,
    syncState: Object.fromEntries(state.syncState),
  };
  const bytes = encoder.encode(snapshot);

  // Crash-safe: write to temp file, then rename (atomic on POSIX, near-atomic on NTFS)
  const tmpPath = path + '.tmp';
  await Bun.write(tmpPath, bytes);
  renameSync(tmpPath, path);
}

export async function loadCatalog(path: string): Promise<CatalogSnapshot | null> {
  // Try main file first, then temp file (crash recovery)
  for (const candidate of [path, path + '.tmp']) {
    try {
      if (!existsSync(candidate)) continue;
      const buf = await Bun.file(candidate).arrayBuffer();
      const bytes = new Uint8Array(buf);
      const snapshot = decoder.decode(bytes) as CatalogSnapshot;
      // Clean up stale temp file if main file loaded successfully
      if (candidate === path && existsSync(path + '.tmp')) {
        try { unlinkSync(path + '.tmp'); } catch {}
      }
      return snapshot;
    } catch {
      continue;  // corrupt file, try next candidate
    }
  }
  return null;  // all files missing or corrupt → start fresh, sync from peers
}
```

**Signature storage comparison** (per entry):

```
JSON:    "signature": "MEUCIQC7x2nQ3Kp..."   → 92 bytes (base64 of 64-byte Ed25519 sig)
CBOR:    signature: <64 raw bytes>             → 66 bytes (2-byte CBOR header + 64 bytes)

Per 10K entries: JSON wastes ~260 KB on base64 encoding alone.
```

### Tamper Resistance

The local file is a **cache**, not a source of truth. Signatures are the source of truth:

| Tampering scenario | What happens |
|---|---|
| Peer edits a field in the file | Signature becomes invalid → overwritten on next sync |
| Peer deletes the file | Fresh start → bilateral sync restores full catalog from peers |
| Peer adds fake entry | No valid moderator signature → rejected by all peers on sync |
| Peer removes a tombstone | Tombstone comes back from other peers on next sync |
| File corrupted (disk error) | CBOR decode fails → treated as missing → sync from peers |

The CRDT state can always be **fully reconstructed from the network**. The local file only exists to avoid re-downloading everything on every restart.

### Example: Logical structure (shown as JSON for readability)

The actual file is binary CBOR, but the logical structure is:

```json
{
  "entries": [
    {
      "lishID": "34aacabb-...",
      "name": "Ubuntu 24.04 LTS",
      "publisherPeerID": "12D3KooW...",
      "publishedAt": "2026-02-28T22:00:00Z",
      "fileCount": 1,
      "totalSize": 4800000000,
      "hlc": { "wallTime": 1709164800000, "logical": 0, "nodeID": "12D3KooWJdc..." },
      "signature": "<64 bytes binary, not base64>"
    }
  ],
  "tombstones": [],
  "access": {
    "owner": "12D3KooWJdctGgbEdbUTCvpCoW73E67mHF92v4dD7yvxAkVztnCy",
    "admins": [],
    "moderators": ["12D3KooWAbc..."],
    "restrictCatalogWrites": true
  },
  "vectorClock": {
    "12D3KooWJdc...": { "wallTime": 1709164800000, "logical": 0, "nodeID": "12D3KooWJdc..." },
    "12D3KooWAbc...": { "wallTime": 1709164700000, "logical": 2, "nodeID": "12D3KooWAbc..." }
  },
  "localClock": { "wallTime": 1709164800000, "logical": 0, "nodeID": "12D3KooWJdc..." },
  "syncState": {
    "12D3KooWAbc...": { "wallTime": 1709164650000, "logical": 0, "nodeID": "12D3KooWJdc..." }
  }
}
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
- `CatalogCRDT` class (entries, tombstones, merge, LWW, operation queue)
- HLC implementation (tick, merge, compare)
- Signature generation and verification (Ed25519 via `@libp2p/crypto`)
- Crash-safe CBOR persistence (write-then-rename, debounced saves)
- `CatalogManager` class (multi-lishnet lifecycle, load/unload catalogs)
- Add `ownerPeerID` field to `ILISHNetwork` shared type
- Add `getPrivateKey()` and `registerStreamHandler()` to `Network` class
- Unit tests for merge correctness, security validation, crash recovery

### Phase 2: Sync + GossipSub Integration (Backend)
- Bilateral sync stream `/lish/catalog-sync/1.0.0` (CBOR, with error handling and timeouts)
- GossipSub broadcast for catalog operations (JSON, signed)
- Handle incoming ops (validate signature + ACL + field sizes, merge)
- ACL operations (add/remove admin/moderator, cascading revocation)
- Integration with `Networks` class (join → catalog load, leave → catalog unload)
- Old protocol coexistence (`add_lish`/`del_lish` ignored by catalog layer)
- In-memory search (AND-logic term matching)

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
- SQLite migration path (when catalogs exceed 50K entries)
- Metrics and monitoring

---

## 10. Security Checklist

- [ ] Every catalog operation is signed with Ed25519
- [ ] Signature covers payload + authorPeerID + HLC (canonical JSON via json-canonicalize)
- [ ] ACL changes validated against role hierarchy before application
- [ ] HLC anti-replay check on every received operation (hlcCompare > 0)
- [ ] HLC clock drift check: reject ops with wallTime > 60s in the future
- [ ] Owner PeerID is immutable (from .lishnet config, not from network)
- [ ] Bilateral sync stream authenticated via libp2p Noise handshake
- [ ] Cross-validate catalog state from multiple peers on initial sync
- [ ] Tombstones kept for minimum 30 days (time-based GC)
- [ ] Rate limiting on incoming operations per peer (sliding window)
- [ ] Maximum catalog size enforced to prevent DoS via catalog spam
- [ ] Reject operations from unknown/unverified PeerIDs
- [ ] Log and alert on repeated authorization failures (potential attack)
- [ ] GossipSub peer scoring enabled (P4 invalid messages + P5 app-specific + P6 IP colocation)
- [ ] GossipSub D >= 6 (current D=2 is below spec minimum)
- [ ] Per-publisher write quota enforced (MAX_ENTRIES_PER_PUBLISHER)
- [ ] Global catalog size cap enforced (MAX_CATALOG_SIZE)
- [ ] Sliding-window rate limiter per publisher PeerID
- [ ] vectorClock (HLC map) persisted to disk and reloaded on restart (prevents replay after restart)
- [ ] GossipSub topic validator registered for catalog topics (REJECT invalid sigs, IGNORE rate-limited)
- [ ] Content availability verification via random chunk challenge (optional, Phase 4)
- [ ] Emergency revocation: acl_revoke propagates within 1 heartbeat cycle
- [ ] Anti-escalation rule: cannot grant permissions you do not hold (Matrix Rule 9)
- [ ] Power-events-first ordering: ACL events processed before catalog events in same batch
- [ ] Cascading revocation: revoking admin invalidates all their granted moderator permissions
- [ ] Update operations: only editable fields (name, description, contentType, tags) can be changed
- [ ] Update operations: immutable fields (lishID, publisherPeerID, totalSize, manifestHash, etc.) rejected
- [ ] Update operations: lastEditedBy set automatically from authorPeerID, not user-supplied
- [ ] Field size limits enforced before signature verification (fail fast)
- [ ] Schema version included in CBOR snapshot and sync protocol
- [ ] Unknown gossipsub message versions: IGNORE (not REJECT) to avoid penalizing newer peers
- [ ] Bilateral sync: stream timeout (30s), payload size limit (10 MB), CBOR decode error handling
- [ ] Bilateral sync: invalid signatures in delta → reject entries, penalize peer (P5 score -5)
- [ ] Crash-safe persistence: write-then-rename (atomic write via .tmp file)
- [ ] Corrupt catalog file: fallback to .tmp, then full sync from peers
- [ ] Per-network operation queue: serialized mutations prevent concurrent state corruption
- [ ] Debounced persistence: max 1 disk write per 500ms, flush on shutdown
- [ ] .lishnet `ownerPeerID` field: required for catalog, validated as Ed25519 PeerID
- [ ] `manifestHash` computed as sha256(canonicalize(lishManifest)) — anchors catalog entry to exact manifest

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
// backend/src/protocol/catalog-signer.ts
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
  data: Record<string, unknown>
): Promise<SignedCatalogOp> {
  const payload: CatalogOpPayload = {
    type,
    networkID,
    hlc: hlcTick(localClock),  // advance local HLC
    nonce: crypto.randomUUID(),
    data,
  };
  const canonical = canonicalize(payload);       // RFC 8785 deterministic JSON
  const bytes = encoder.encode(canonical);
  const sig = await privateKey.sign(bytes);      // Ed25519 is sync, await is safe
  return {
    payload,
    signature: Buffer.from(sig).toString('base64url'),
    signer: privateKey.publicKey.toString(),      // base58btc PeerID
    keyType: 'Ed25519',
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
| **Local persistence** | CBOR | Disk efficiency, native binary signatures |

GossipSub messages are small (single operations, ~500 bytes) so JSON overhead is acceptable. The signature is computed over **canonical JSON** regardless of wire format — this ensures signature portability between contexts.

```
GossipSub:  peer → JSON.stringify(signedOp) → gossipsub.publish() → topic
Bilateral:  peer → cbor.encode(delta) → libp2p stream → peer
Disk:       cbor.encode(fullState) → Bun.write(file)
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

Analysis of existing backend source code (`network.ts`, `networks.ts`, `lishnetStorage.ts`, `network-config.ts`, shared types, `LISH_NETWORK_PROTOCOL.md`) reveals the following integration gaps that must be resolved before implementation.

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
    networkID, name, description, bootstrapPeers, enabled
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
// Current (shared/src/index.ts)
export interface ILISHNetwork {
  version: number;
  networkID: string;
  name: string;
  description?: string;
  bootstrapPeers: string[];
  created?: string;
}
```

The catalog system requires a trusted owner PeerID as the root of the ACL chain.

**Decision: Add `ownerPeerID` to `.lishnet` format.**

```typescript
// Updated
export interface ILISHNetwork {
  version: number;        // bump to 2
  networkID: string;
  name: string;
  description?: string;
  bootstrapPeers: string[];
  created?: string;
  ownerPeerID?: string;   // Ed25519 PeerID of the network creator (required for catalog)
}
```

**Field is optional** for backward compatibility — version 1 `.lishnet` files without `ownerPeerID` work for file sharing but cannot use the catalog system. When a user creates a new lishnet in LiberShare, `ownerPeerID` is automatically set to their PeerID.

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

getPrivateKey(): PrivateKey {
  if (!this.privateKey) throw new Error('Network not started');
  return this.privateKey;
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
  if (!this.node) throw new Error('Network not started');
  await this.node.handle(
    protocol,
    async ({ stream }) => handler(stream),
    { runOnLimitedConnection: true }
  );
}
```

This mirrors the existing `LISH_PROTOCOL` handler registration pattern in `start()` (line 194-199 of `network.ts`).

### 15.5 End-to-End Publish Flow

How `catalog.publish(networkID, lishID)` works from API call to broadcast:

```
1. Frontend calls: catalog.publish(networkID, lishID)

2. Backend resolves local LISH:
   const lish = dataServer.get(lishID);
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
     hlc: hlcTick(localClock),
     signature: '',  // filled below
   };

4. Backend signs the entry:
   const privateKey = network.getPrivateKey();
   const signedOp = await signCatalogOp(privateKey, 'add', networkID, entry);

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

Each joined lishnet needs its own `CatalogCRDT` instance. A `CatalogManager` coordinates lifecycle.

```typescript
// backend/src/catalog/catalog-manager.ts

export class CatalogManager {
  private catalogs: Map<string, CatalogCRDT> = new Map();
  private readonly dataDir: string;
  private readonly network: Network;
  private syncHandlerRegistered: boolean = false;

  constructor(dataDir: string, network: Network) {
    this.dataDir = dataDir;
    this.network = network;
  }

  /**
   * Register the bilateral sync handler (once, shared by all catalogs).
   * libp2p allows only one handler per protocol path, so CatalogManager
   * owns the handler and dispatches by networkID from the request body.
   */
  private async ensureSyncHandler(): Promise<void> {
    if (this.syncHandlerRegistered) return;
    await this.network.registerStreamHandler(
      '/lish/catalog-sync/1.0.0',
      async (stream) => {
        const request = await readSyncRequest(stream);  // CBOR decode
        const crdt = this.catalogs.get(request.networkID);
        if (crdt) {
          await crdt.handleSyncStream(stream, request);
        }
        // Unknown networkID → close stream silently
      }
    );
    this.syncHandlerRegistered = true;
  }

  /**
   * Load or create a catalog for a lishnet.
   * Called when a lishnet is joined (enabled).
   * ownerPeerID comes from ILISHNetwork.ownerPeerID — if absent, catalog is not created.
   */
  async join(networkID: string, ownerPeerID: string): Promise<void> {
    if (this.catalogs.has(networkID)) return;

    // Ensure bilateral sync handler is registered (once)
    await this.ensureSyncHandler();

    // Load from disk or create fresh
    const path = `${this.dataDir}/catalog/${networkID}.cbor`;
    const snapshot = await loadCatalog(path);

    const crdt = new CatalogCRDT(networkID, ownerPeerID, this.network);
    if (snapshot) {
      crdt.loadFromSnapshot(snapshot);
    }

    this.catalogs.set(networkID, crdt);

    // Register GossipSub handler for catalog_op messages on this network's topic
    await this.network.subscribe(lishTopic(networkID), (msg) => {
      if (msg.type === 'catalog_op') {
        crdt.handleRemoteOperation(msg);
        crdt.scheduleSave();  // debounced persistence
      }
    });
  }

  /**
   * Unload a catalog when leaving a lishnet.
   */
  async leave(networkID: string): Promise<void> {
    const crdt = this.catalogs.get(networkID);
    if (!crdt) return;
    await crdt.flush();  // persist any pending changes
    this.catalogs.delete(networkID);
  }

  /**
   * Get catalog for a specific network.
   */
  get(networkID: string): CatalogCRDT | undefined {
    return this.catalogs.get(networkID);
  }

  /**
   * Flush all catalogs (called on graceful shutdown).
   */
  async flushAll(): Promise<void> {
    for (const crdt of this.catalogs.values()) {
      await crdt.flush();
    }
  }
}
```

**Integration with Networks class**: `CatalogManager` is created alongside `Networks` in `app.ts`, receiving `networks.getNetwork()` as its `Network` dependency. When `networks.setEnabled(id, true)` is called, it also calls:

```typescript
const net = networks.get(id);
if (net?.ownerPeerID) {
  await catalogManager.join(id, net.ownerPeerID);
}
// Networks without ownerPeerID skip catalog (v1 .lishnet files)
```

When disabled, `await catalogManager.leave(id)`. On shutdown, `await catalogManager.flushAll()`.

**Memory**: Each `CatalogCRDT` holds its catalog in memory. For a typical lishnet with 1K entries (~500 KB), having 10 joined lishnets costs ~5 MB RAM. Acceptable.

### 15.7 Search Implementation

`catalog.search(networkID, query)` needs a strategy.

**Phase 1: In-memory filtering** (simple, sufficient for < 10K entries):

```typescript
function searchCatalog(entries: CatalogEntry[], query: string): CatalogEntry[] {
  const q = query.toLowerCase().trim();
  if (!q) return entries;

  const terms = q.split(/\s+/);

  return entries
    .filter(entry => {
      const searchable = [
        entry.name ?? '',
        entry.description ?? '',
        entry.contentType ?? '',
        ...(entry.tags ?? []),
      ].join(' ').toLowerCase();

      // All terms must match (AND logic)
      return terms.every(term => searchable.includes(term));
    })
    .sort((a, b) => {
      // Relevance scoring: name match (2) > tag match (1) > description-only match (0)
      const score = (e: CatalogEntry): number => {
        const name = (e.name ?? '').toLowerCase();
        const tags = (e.tags ?? []).join(' ').toLowerCase();
        let s = 0;
        for (const term of terms) {
          if (name.includes(term)) s += 2;
          else if (tags.includes(term)) s += 1;
        }
        return s;
      };
      const diff = score(b) - score(a);
      if (diff !== 0) return diff;
      // Tiebreaker: newest first (by HLC)
      return hlcCompare(b.hlc, a.hlc);
    });
}
```

**Phase 2: SQLite FTS5** (when catalogs exceed 10K entries or users request fulltext):

```sql
CREATE VIRTUAL TABLE catalog_fts USING fts5(
  name, description, tags, content_type,
  content='catalog_entries', content_rowid='rowid'
);

-- Triggered on catalog merge (insert/update)
INSERT INTO catalog_fts(rowid, name, description, tags, content_type)
VALUES (?, ?, ?, ?, ?);

-- Search with ranking
SELECT *, rank FROM catalog_fts WHERE catalog_fts MATCH ? ORDER BY rank;
```

This aligns with the SQLite migration path defined in section 6. FTS5 would be added when SQLite replaces CBOR for persistence.

**Tag-only search**: When query starts with `#`, search only in tags:

```typescript
if (q.startsWith('#')) {
  const tag = q.slice(1).toLowerCase();
  return entries.filter(e => e.tags?.some(t => t === tag));
}
```

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
    stream.sink([responseBytes]);

  } catch (err) {
    // Stream error — logged, peer scored, no crash
    console.error('Catalog sync error:', err);
  } finally {
    clearTimeout(timeout);
  }
}
```

**Retry strategy**: Exponential backoff with jitter. Max 3 retries per sync attempt. After 3 failures, wait for next periodic sync interval (60 seconds).

### 15.9 Crash-Safe Persistence (Atomic Writes)

`Bun.write(path, bytes)` is **not guaranteed atomic** on all filesystems. A crash during write can corrupt the file.

**Decision: Rename trick (write-then-rename).** Implemented in `saveCatalog()` and `loadCatalog()` in section 6:

- `saveCatalog()` writes to `.tmp` file first, then `renameSync()` atomically replaces the main file
- `loadCatalog()` tries main file first, falls back to `.tmp` if corrupt (crash recovery)
- Both functions are `async` (using `await Bun.file().arrayBuffer()`)

**Worst case**: Both files are corrupt → treated as fresh start, bilateral sync from peers restores the full catalog. The CRDT state is always fully reconstructable from the network.

### 15.10 Concurrency Model (Bun Single-Threaded)

Bun runs on a single thread with an event loop. Multiple simultaneous sync streams for different lishnets are interleaved, not parallel. Potential issues:

**Problem 1: Long sync blocks the event loop.**

A large catalog sync (50K entries, ~25 MB CBOR) could take 100+ ms to decode. During this time, no GossipSub messages are processed.

**Solution**: Chunk processing with `setImmediate()`:

```typescript
async function processSyncDelta(entries: CatalogEntry[], crdt: CatalogCRDT): Promise<void> {
  const BATCH_SIZE = 500;
  for (let i = 0; i < entries.length; i += BATCH_SIZE) {
    const batch = entries.slice(i, i + BATCH_SIZE);
    for (const entry of batch) {
      crdt.mergeEntry(entry);  // signature verification + merge
    }
    // Yield to event loop every 500 entries
    await new Promise(resolve => setImmediate(resolve));
  }
}
```

**Problem 2: Concurrent writes to the same catalog.**

A GossipSub message arrives while a bilateral sync is being processed for the same network.

**Solution**: Per-network operation queue:

```typescript
class CatalogCRDT {
  private opQueue: Promise<void> = Promise.resolve();

  async enqueueOperation(op: () => Promise<void>): Promise<void> {
    this.opQueue = this.opQueue.then(op).catch(err => {
      console.error('Catalog operation failed:', err);
    });
    return this.opQueue;
  }
}
```

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
