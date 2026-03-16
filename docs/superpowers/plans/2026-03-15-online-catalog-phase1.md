# Online Catalog Phase 1 — Core CRDT + Persistence

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the core CRDT layer for the online catalog: HLC, SQLite schema, Ed25519 signing, and validation chain — all with tests.

**Architecture:** Custom signed 2P-Set CRDT backed by SQLite. Each catalog operation is signed with Ed25519, validated through a 5-step chain (signature → ACL → drift → content → anti-replay), and merged via SQL UPSERT with HLC-based LWW. No in-memory state beyond prepared statements.

**Tech Stack:** Bun + bun:sqlite + @libp2p/crypto (Ed25519) + json-canonicalize + cbor-x

---

## File Structure

```
backend/src/
├── catalog/
│   ├── catalog-hlc.ts          HLC data structure: tick, merge, compare
│   ├── catalog-signer.ts       Ed25519 sign/verify for catalog operations
│   ├── catalog-validator.ts    5-step validation chain (handleRemoteOp)
│   ├── catalog-manager.ts      Multi-lishnet lifecycle, API methods
│   └── __tests__/
│       ├── catalog-hlc.test.ts
│       ├── catalog-signer.test.ts
│       ├── catalog-validator.test.ts
│       └── catalog-db.test.ts
├── db/
│   ├── catalog.ts              SQL schema, CRUD, LWW upsert, FTS5, delta queries
│   └── database.ts             (EDIT: add initCatalogTables call)
```

## Chunk 1: HLC Implementation

### Task 1: HLC — Types and Core Functions

**Files:**
- Create: `backend/src/catalog/catalog-hlc.ts`
- Create: `backend/src/catalog/__tests__/catalog-hlc.test.ts`

- [ ] **Step 1: Write the HLC test file**

```typescript
// backend/src/catalog/__tests__/catalog-hlc.test.ts
import { describe, test, expect } from 'bun:test';
import { hlcTick, hlcMerge, hlcCompare, type HLC } from '../catalog-hlc.ts';

describe('hlcCompare', () => {
  test('higher wallTime wins', () => {
    const a: HLC = { wallTime: 100, logical: 0, nodeID: 'A' };
    const b: HLC = { wallTime: 200, logical: 0, nodeID: 'A' };
    expect(hlcCompare(a, b)).toBeLessThan(0);
    expect(hlcCompare(b, a)).toBeGreaterThan(0);
  });

  test('same wallTime — higher logical wins', () => {
    const a: HLC = { wallTime: 100, logical: 1, nodeID: 'A' };
    const b: HLC = { wallTime: 100, logical: 2, nodeID: 'A' };
    expect(hlcCompare(a, b)).toBeLessThan(0);
  });

  test('same wallTime and logical — nodeID breaks tie', () => {
    const a: HLC = { wallTime: 100, logical: 0, nodeID: 'A' };
    const b: HLC = { wallTime: 100, logical: 0, nodeID: 'B' };
    expect(hlcCompare(a, b)).toBeLessThan(0);
    expect(hlcCompare(b, a)).toBeGreaterThan(0);
  });

  test('identical clocks compare as equal', () => {
    const a: HLC = { wallTime: 100, logical: 0, nodeID: 'A' };
    expect(hlcCompare(a, { ...a })).toBe(0);
  });
});

describe('hlcTick', () => {
  test('advances wallTime when Date.now() > local', () => {
    const local: HLC = { wallTime: 0, logical: 5, nodeID: 'peer1' };
    const result = hlcTick(local);
    expect(result.wallTime).toBeGreaterThan(0);
    expect(result.logical).toBe(0);
    expect(result.nodeID).toBe('peer1');
  });

  test('increments logical when wallTime unchanged', () => {
    const now = Date.now();
    const local: HLC = { wallTime: now + 100_000, logical: 3, nodeID: 'peer1' };
    const result = hlcTick(local);
    expect(result.wallTime).toBe(now + 100_000);
    expect(result.logical).toBe(4);
  });

  test('tick is always strictly greater than input', () => {
    const local: HLC = { wallTime: Date.now(), logical: 0, nodeID: 'peer1' };
    const result = hlcTick(local);
    expect(hlcCompare(result, local)).toBeGreaterThan(0);
  });
});

describe('hlcMerge', () => {
  test('takes max wallTime from local, remote, and now', () => {
    const local: HLC = { wallTime: 100, logical: 0, nodeID: 'A' };
    const remote: HLC = { wallTime: 200, logical: 0, nodeID: 'B' };
    const result = hlcMerge(local, remote);
    // now > 200, so wallTime = now, logical = 0
    expect(result.wallTime).toBeGreaterThanOrEqual(200);
    expect(result.nodeID).toBe('A');
  });

  test('same wallTime — increments logical', () => {
    const futureTime = Date.now() + 100_000;
    const local: HLC = { wallTime: futureTime, logical: 5, nodeID: 'A' };
    const remote: HLC = { wallTime: futureTime, logical: 3, nodeID: 'B' };
    const result = hlcMerge(local, remote);
    expect(result.wallTime).toBe(futureTime);
    expect(result.logical).toBe(6); // max(5,3) + 1
  });

  test('merge result is always > local', () => {
    const local: HLC = { wallTime: Date.now(), logical: 0, nodeID: 'A' };
    const remote: HLC = { wallTime: Date.now() - 1000, logical: 0, nodeID: 'B' };
    const result = hlcMerge(local, remote);
    expect(hlcCompare(result, local)).toBeGreaterThan(0);
  });

  test('preserves local nodeID', () => {
    const local: HLC = { wallTime: 100, logical: 0, nodeID: 'LOCAL' };
    const remote: HLC = { wallTime: 200, logical: 0, nodeID: 'REMOTE' };
    const result = hlcMerge(local, remote);
    expect(result.nodeID).toBe('LOCAL');
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /c/work/sources/libershare/.claude/worktrees/feat-online-db && bun test backend/src/catalog/__tests__/catalog-hlc.test.ts`
Expected: FAIL — module not found

- [ ] **Step 3: Write HLC implementation**

```typescript
// backend/src/catalog/catalog-hlc.ts

export interface HLC {
  wallTime: number;
  logical: number;
  nodeID: string;
}

export function hlcCompare(a: HLC, b: HLC): number {
  if (a.wallTime !== b.wallTime) return a.wallTime - b.wallTime;
  if (a.logical !== b.logical) return a.logical - b.logical;
  return a.nodeID.localeCompare(b.nodeID);
}

export function hlcTick(local: HLC): HLC {
  const now = Date.now();
  if (now > local.wallTime) {
    return { wallTime: now, logical: 0, nodeID: local.nodeID };
  }
  return { wallTime: local.wallTime, logical: local.logical + 1, nodeID: local.nodeID };
}

export function hlcMerge(local: HLC, remote: HLC): HLC {
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

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /c/work/sources/libershare/.claude/worktrees/feat-online-db && bun test backend/src/catalog/__tests__/catalog-hlc.test.ts`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
cd /c/work/sources/libershare/.claude/worktrees/feat-online-db
git add backend/src/catalog/catalog-hlc.ts backend/src/catalog/__tests__/catalog-hlc.test.ts
git commit -m "feat: add HLC implementation with tests"
```

---

## Chunk 2: SQLite Catalog Schema

### Task 2: Catalog Database Schema and CRUD

**Files:**
- Create: `backend/src/db/catalog.ts`
- Modify: `backend/src/db/database.ts`
- Create: `backend/src/catalog/__tests__/catalog-db.test.ts`

- [ ] **Step 1: Write catalog-db tests**

```typescript
// backend/src/catalog/__tests__/catalog-db.test.ts
import { describe, test, expect, beforeEach } from 'bun:test';
import { Database } from 'bun:sqlite';
import {
  initCatalogTables,
  upsertCatalogEntry,
  getCatalogEntry,
  listCatalogEntries,
  upsertTombstone,
  isTombstoned,
  getCatalogACL,
  ensureCatalogACL,
  updateCatalogACL,
  getVectorClock,
  updateVectorClock,
  searchCatalog,
  deleteTombstonesOlderThan,
  getDeltaEntries,
} from '../../db/catalog.ts';

let db: Database;

beforeEach(() => {
  db = new Database(':memory:');
  db.run('PRAGMA journal_mode = WAL');
  db.run('PRAGMA foreign_keys = ON');
  initCatalogTables(db);
});

describe('schema', () => {
  test('creates all 5 tables', () => {
    const tables = db.query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").all() as { name: string }[];
    const names = tables.map(t => t.name);
    expect(names).toContain('catalog_entries');
    expect(names).toContain('catalog_tombstones');
    expect(names).toContain('catalog_acl');
    expect(names).toContain('catalog_clocks');
  });
});

describe('upsertCatalogEntry — LWW merge', () => {
  const baseEntry = {
    network_id: 'net1',
    lish_id: 'lish1',
    name: 'Test',
    description: 'A test',
    publisher_peer_id: 'peer1',
    published_at: '2026-01-01T00:00:00Z',
    chunk_size: 1024,
    checksum_algo: 'sha256',
    total_size: 5000,
    file_count: 3,
    manifest_hash: 'abc123',
    content_type: 'software',
    tags: '["linux"]',
    last_edited_by: null,
    hlc_wall: 1000,
    hlc_logical: 0,
    hlc_node: 'peer1',
    signed_op: Buffer.from('op1'),
  };

  test('inserts new entry', () => {
    upsertCatalogEntry(db, baseEntry);
    const entry = getCatalogEntry(db, 'net1', 'lish1');
    expect(entry).not.toBeNull();
    expect(entry!.name).toBe('Test');
  });

  test('higher HLC overwrites existing', () => {
    upsertCatalogEntry(db, baseEntry);
    upsertCatalogEntry(db, { ...baseEntry, name: 'Updated', hlc_wall: 2000, signed_op: Buffer.from('op2') });
    const entry = getCatalogEntry(db, 'net1', 'lish1');
    expect(entry!.name).toBe('Updated');
  });

  test('lower HLC is rejected (no overwrite)', () => {
    upsertCatalogEntry(db, { ...baseEntry, hlc_wall: 2000 });
    upsertCatalogEntry(db, { ...baseEntry, name: 'Old', hlc_wall: 500, signed_op: Buffer.from('op2') });
    const entry = getCatalogEntry(db, 'net1', 'lish1');
    expect(entry!.name).toBe('Test'); // unchanged
  });

  test('same wallTime — higher logical wins', () => {
    upsertCatalogEntry(db, { ...baseEntry, hlc_logical: 1 });
    upsertCatalogEntry(db, { ...baseEntry, name: 'Logical', hlc_logical: 2, signed_op: Buffer.from('op2') });
    const entry = getCatalogEntry(db, 'net1', 'lish1');
    expect(entry!.name).toBe('Logical');
  });

  test('same wallTime and logical — nodeID tiebreak', () => {
    upsertCatalogEntry(db, { ...baseEntry, hlc_node: 'A' });
    upsertCatalogEntry(db, { ...baseEntry, name: 'NodeB', hlc_node: 'B', signed_op: Buffer.from('op2') });
    const entry = getCatalogEntry(db, 'net1', 'lish1');
    expect(entry!.name).toBe('NodeB'); // 'B' > 'A'
  });
});

describe('tombstones', () => {
  test('insert and check tombstone', () => {
    upsertTombstone(db, {
      network_id: 'net1', lish_id: 'lish1', removed_by: 'peer1',
      removed_at: '2026-01-01T00:00:00Z',
      hlc_wall: 1000, hlc_logical: 0, hlc_node: 'peer1',
      signed_op: Buffer.from('tomb1'),
    });
    expect(isTombstoned(db, 'net1', 'lish1')).toBe(true);
    expect(isTombstoned(db, 'net1', 'lish2')).toBe(false);
  });

  test('GC deletes old tombstones', () => {
    upsertTombstone(db, {
      network_id: 'net1', lish_id: 'lish1', removed_by: 'peer1',
      removed_at: '2025-01-01T00:00:00Z', // old
      hlc_wall: 1000, hlc_logical: 0, hlc_node: 'peer1',
      signed_op: Buffer.from('tomb1'),
    });
    const deleted = deleteTombstonesOlderThan(db, 'net1', 30);
    expect(deleted).toBe(1);
    expect(isTombstoned(db, 'net1', 'lish1')).toBe(false);
  });
});

describe('ACL', () => {
  test('ensures default ACL on first call', () => {
    ensureCatalogACL(db, 'net1', 'ownerPeer');
    const acl = getCatalogACL(db, 'net1');
    expect(acl).not.toBeNull();
    expect(acl!.owner).toBe('ownerPeer');
    expect(acl!.admins).toEqual([]);
    expect(acl!.moderators).toEqual([]);
    expect(acl!.restrict_writes).toBe(1);
  });

  test('update ACL admins', () => {
    ensureCatalogACL(db, 'net1', 'ownerPeer');
    updateCatalogACL(db, 'net1', { admins: ['admin1', 'admin2'] });
    const acl = getCatalogACL(db, 'net1');
    expect(acl!.admins).toEqual(['admin1', 'admin2']);
  });
});

describe('vector clocks', () => {
  test('get/set clock', () => {
    updateVectorClock(db, 'net1', 'peer1', 1000, 5);
    const clock = getVectorClock(db, 'net1', 'peer1');
    expect(clock).not.toBeNull();
    expect(clock!.hlc_wall).toBe(1000);
    expect(clock!.hlc_logical).toBe(5);
  });

  test('update replaces older clock', () => {
    updateVectorClock(db, 'net1', 'peer1', 1000, 5);
    updateVectorClock(db, 'net1', 'peer1', 2000, 0);
    const clock = getVectorClock(db, 'net1', 'peer1');
    expect(clock!.hlc_wall).toBe(2000);
  });
});

describe('FTS5 search', () => {
  test('finds entry by name', () => {
    const entry = {
      network_id: 'net1', lish_id: 'lish1', name: 'Ubuntu ISO',
      description: 'Official Ubuntu desktop image', publisher_peer_id: 'p1',
      published_at: '2026-01-01T00:00:00Z', chunk_size: 1024, checksum_algo: 'sha256',
      total_size: 5000, file_count: 1, manifest_hash: 'h1', content_type: 'software',
      tags: '["linux","ubuntu"]', last_edited_by: null,
      hlc_wall: 1000, hlc_logical: 0, hlc_node: 'p1',
      signed_op: Buffer.from('op1'),
    };
    upsertCatalogEntry(db, entry);
    const results = searchCatalog(db, 'net1', 'Ubuntu');
    expect(results.length).toBe(1);
    expect(results[0]!.name).toBe('Ubuntu ISO');
  });

  test('finds entry by description', () => {
    const entry = {
      network_id: 'net1', lish_id: 'lish1', name: 'Fedora',
      description: 'Workstation edition with GNOME',
      publisher_peer_id: 'p1', published_at: '2026-01-01T00:00:00Z',
      chunk_size: 1024, checksum_algo: 'sha256', total_size: 5000,
      file_count: 1, manifest_hash: 'h1', content_type: 'software',
      tags: '["linux"]', last_edited_by: null,
      hlc_wall: 1000, hlc_logical: 0, hlc_node: 'p1',
      signed_op: Buffer.from('op1'),
    };
    upsertCatalogEntry(db, entry);
    const results = searchCatalog(db, 'net1', 'GNOME');
    expect(results.length).toBe(1);
  });

  test('tag search with # prefix', () => {
    const entry = {
      network_id: 'net1', lish_id: 'lish1', name: 'Test',
      description: null, publisher_peer_id: 'p1',
      published_at: '2026-01-01T00:00:00Z', chunk_size: 1024,
      checksum_algo: 'sha256', total_size: 5000, file_count: 1,
      manifest_hash: 'h1', content_type: null, tags: '["linux","iso"]',
      last_edited_by: null, hlc_wall: 1000, hlc_logical: 0, hlc_node: 'p1',
      signed_op: Buffer.from('op1'),
    };
    upsertCatalogEntry(db, entry);
    const results = searchCatalog(db, 'net1', '#linux');
    expect(results.length).toBe(1);
  });
});

describe('delta sync', () => {
  test('getDeltaEntries returns entries newer than given HLC', () => {
    upsertCatalogEntry(db, {
      network_id: 'net1', lish_id: 'lish1', name: 'Old',
      description: null, publisher_peer_id: 'p1',
      published_at: '2026-01-01T00:00:00Z', chunk_size: 1024,
      checksum_algo: 'sha256', total_size: 5000, file_count: 1,
      manifest_hash: 'h1', content_type: null, tags: null,
      last_edited_by: null, hlc_wall: 500, hlc_logical: 0, hlc_node: 'p1',
      signed_op: Buffer.from('op1'),
    });
    upsertCatalogEntry(db, {
      network_id: 'net1', lish_id: 'lish2', name: 'New',
      description: null, publisher_peer_id: 'p1',
      published_at: '2026-01-01T00:00:00Z', chunk_size: 1024,
      checksum_algo: 'sha256', total_size: 3000, file_count: 1,
      manifest_hash: 'h2', content_type: null, tags: null,
      last_edited_by: null, hlc_wall: 2000, hlc_logical: 0, hlc_node: 'p1',
      signed_op: Buffer.from('op2'),
    });
    const delta = getDeltaEntries(db, 'net1', 1000);
    expect(delta.length).toBe(1);
    expect(delta[0]!.lish_id).toBe('lish2');
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /c/work/sources/libershare/.claude/worktrees/feat-online-db && bun test backend/src/catalog/__tests__/catalog-db.test.ts`
Expected: FAIL — module not found

- [ ] **Step 3: Write catalog.ts (SQLite schema + CRUD)**

Create `backend/src/db/catalog.ts` with:
- `initCatalogTables(db)` — creates 5 tables (entries, tombstones, ACL, clocks, FTS5)
- `upsertCatalogEntry(db, entry)` — LWW merge via INSERT ON CONFLICT
- `getCatalogEntry(db, networkID, lishID)` — single entry lookup
- `listCatalogEntries(db, networkID, limit?)` — list all entries for a network
- `upsertTombstone(db, tombstone)` — insert/replace tombstone
- `isTombstoned(db, networkID, lishID)` — check if entry is tombstoned
- `deleteTombstonesOlderThan(db, networkID, days)` — GC
- `ensureCatalogACL(db, networkID, ownerPeerID)` — create ACL if not exists
- `getCatalogACL(db, networkID)` — read ACL
- `updateCatalogACL(db, networkID, changes)` — update ACL fields
- `getVectorClock(db, networkID, peerID)` — read clock
- `updateVectorClock(db, networkID, peerID, hlcWall, hlcLogical)` — upsert clock
- `searchCatalog(db, networkID, query, limit?)` — FTS5 search
- `getDeltaEntries(db, networkID, sinceHlcWall)` — entries newer than timestamp

SQL schema from architecture doc §6 (5 tables: catalog_entries, catalog_tombstones, catalog_acl, catalog_clocks, catalog_fts).

- [ ] **Step 4: Modify database.ts — add initCatalogTables call**

Add `import { initCatalogTables } from './catalog.ts'` and call `initCatalogTables(db)` after existing init calls.

- [ ] **Step 5: Run tests**

Run: `cd /c/work/sources/libershare/.claude/worktrees/feat-online-db && bun test backend/src/catalog/__tests__/catalog-db.test.ts`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
cd /c/work/sources/libershare/.claude/worktrees/feat-online-db
git add backend/src/db/catalog.ts backend/src/db/database.ts backend/src/catalog/__tests__/catalog-db.test.ts
git commit -m "feat: add catalog SQLite schema with LWW upsert, FTS5, and tests"
```

---

## Chunk 3: Ed25519 Signing

### Task 3: Catalog Signer — Sign and Verify Operations

**Files:**
- Create: `backend/src/catalog/catalog-signer.ts`
- Create: `backend/src/catalog/__tests__/catalog-signer.test.ts`

**Dependencies needed first:**
```bash
cd /c/work/sources/libershare/.claude/worktrees/feat-online-db/backend
bun add json-canonicalize cbor-x
```

- [ ] **Step 1: Install dependencies**

Run: `cd /c/work/sources/libershare/.claude/worktrees/feat-online-db/backend && bun add json-canonicalize cbor-x`

- [ ] **Step 2: Write signer tests**

```typescript
// backend/src/catalog/__tests__/catalog-signer.test.ts
import { describe, test, expect } from 'bun:test';
import { generateKeyPair } from '@libp2p/crypto/keys';
import { signCatalogOp, verifyCatalogOp, type SignedCatalogOp } from '../catalog-signer.ts';
import { type HLC } from '../catalog-hlc.ts';

describe('signCatalogOp + verifyCatalogOp', () => {
  test('sign and verify round-trip', async () => {
    const key = await generateKeyPair('Ed25519');
    const clock: HLC = { wallTime: Date.now(), logical: 0, nodeID: 'test' };
    const { op, updatedClock } = await signCatalogOp(key, 'add', 'net1', { lishID: '123', name: 'Test' }, clock);
    expect(op.payload.type).toBe('add');
    expect(op.payload.networkID).toBe('net1');
    expect(op.keyType).toBe('Ed25519');
    const valid = await verifyCatalogOp(op);
    expect(valid).toBe(true);
  });

  test('tampered payload fails verification', async () => {
    const key = await generateKeyPair('Ed25519');
    const clock: HLC = { wallTime: Date.now(), logical: 0, nodeID: 'test' };
    const { op } = await signCatalogOp(key, 'add', 'net1', { lishID: '123' }, clock);
    op.payload.data = { lishID: 'TAMPERED' };
    const valid = await verifyCatalogOp(op);
    expect(valid).toBe(false);
  });

  test('wrong key fails verification', async () => {
    const key1 = await generateKeyPair('Ed25519');
    const key2 = await generateKeyPair('Ed25519');
    const clock: HLC = { wallTime: Date.now(), logical: 0, nodeID: 'test' };
    const { op } = await signCatalogOp(key1, 'add', 'net1', { lishID: '123' }, clock);
    // Replace signer with key2's PeerID
    const fakeOp: SignedCatalogOp = { ...op, signer: key2.publicKey.toString() };
    const valid = await verifyCatalogOp(fakeOp);
    expect(valid).toBe(false);
  });

  test('updatedClock is > input clock', async () => {
    const key = await generateKeyPair('Ed25519');
    const clock: HLC = { wallTime: Date.now(), logical: 0, nodeID: 'test' };
    const { updatedClock } = await signCatalogOp(key, 'add', 'net1', {}, clock);
    expect(updatedClock.wallTime).toBeGreaterThanOrEqual(clock.wallTime);
    // updatedClock should be strictly greater
    const isGreater = updatedClock.wallTime > clock.wallTime ||
      (updatedClock.wallTime === clock.wallTime && updatedClock.logical > clock.logical);
    expect(isGreater).toBe(true);
  });

  test('networkID is embedded in signed payload', async () => {
    const key = await generateKeyPair('Ed25519');
    const clock: HLC = { wallTime: Date.now(), logical: 0, nodeID: 'test' };
    const { op } = await signCatalogOp(key, 'add', 'mynet', {}, clock);
    expect(op.payload.networkID).toBe('mynet');
  });
});
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd /c/work/sources/libershare/.claude/worktrees/feat-online-db && bun test backend/src/catalog/__tests__/catalog-signer.test.ts`
Expected: FAIL — module not found

- [ ] **Step 4: Write catalog-signer.ts**

Implementation from architecture doc §11 — `signCatalogOp()` and `verifyCatalogOp()` using `json-canonicalize` for deterministic serialization and `@libp2p/crypto` for Ed25519 signing.

- [ ] **Step 5: Run tests**

Run: `cd /c/work/sources/libershare/.claude/worktrees/feat-online-db && bun test backend/src/catalog/__tests__/catalog-signer.test.ts`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
cd /c/work/sources/libershare/.claude/worktrees/feat-online-db
git add backend/src/catalog/catalog-signer.ts backend/src/catalog/__tests__/catalog-signer.test.ts backend/bun.lock backend/package.json
git commit -m "feat: add Ed25519 catalog signer with tests"
```

---

## Chunk 4: Validation Chain

### Task 4: 5-Step Validation Chain (handleRemoteOp)

**Files:**
- Create: `backend/src/catalog/catalog-validator.ts`
- Create: `backend/src/catalog/__tests__/catalog-validator.test.ts`

- [ ] **Step 1: Write validator tests**

Tests for each validation step:
1. Invalid signature → rejected
2. Unauthorized peer (not in ACL) → rejected
3. Clock drift > 5 min → rejected
4. Invalid fields (oversized name) → rejected
5. Replay (HLC ≤ last seen) → rejected
6. Valid operation → accepted and stored in DB
7. ACL operations: owner can grant admin, admin can grant moderator
8. Anti-escalation: moderator cannot grant roles

- [ ] **Step 2: Run test to verify it fails**

- [ ] **Step 3: Write catalog-validator.ts**

Implementation of `handleRemoteOp(db, networkID, op)` — the 5-step validation chain from architecture doc §6.

Functions:
- `handleRemoteOp(db, networkID, op)` — full chain
- `checkACL(db, networkID, op)` — role check
- `validateFields(op)` — size limits from §14.6
- `checkVectorClock(db, networkID, op)` — anti-replay

- [ ] **Step 4: Run tests**

Expected: ALL PASS

- [ ] **Step 5: Run ALL tests together**

Run: `cd /c/work/sources/libershare/.claude/worktrees/feat-online-db && bun test backend/src/catalog/`
Expected: ALL PASS (HLC + DB + Signer + Validator)

- [ ] **Step 6: Commit**

```bash
git add backend/src/catalog/catalog-validator.ts backend/src/catalog/__tests__/catalog-validator.test.ts
git commit -m "feat: add 5-step validation chain for catalog operations"
```

---

## Chunk 5: Integration and Full Test Suite

### Task 5: Full Integration — Run All Tests, TypeScript Check

- [ ] **Step 1: Run full test suite**

Run: `cd /c/work/sources/libershare/.claude/worktrees/feat-online-db && bun test backend/src/catalog/`
Expected: ALL PASS

- [ ] **Step 2: TypeScript strict check**

Run: `cd /c/work/sources/libershare/.claude/worktrees/feat-online-db/backend && bunx tsc --noEmit`
Expected: No errors (strict mode)

- [ ] **Step 3: Fix any TypeScript errors**

If errors found, fix them and re-run.

- [ ] **Step 4: Commit fixes if any**

```bash
git add -A && git commit -m "fix: resolve TypeScript strict mode issues in catalog"
```

- [ ] **Step 5: Run all tests one final time**

Run: `cd /c/work/sources/libershare/.claude/worktrees/feat-online-db && bun test backend/src/catalog/`
Expected: ALL PASS — Phase 1 core complete
