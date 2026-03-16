# Library Download Flow — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:executing-plans to implement this plan.

**Goal:** Wire the Download button in Product detail to actually initiate P2P download when LISH exists locally, or show clear error when not.

**Architecture:** Add `catalog.startDownload` backend endpoint that bridges catalog entries to the transfer/download system. Frontend calls this instead of `transfer.download` directly.

**Tech Stack:** Bun, TypeScript, Svelte 5, WebSocket JSON-RPC

---

## Chunk 1: Backend — catalog.startDownload endpoint

### Task 1: Add startDownload to CatalogManager

**Files:**
- Modify: `backend/src/catalog/catalog-manager.ts`
- Modify: `backend/src/api/catalog.ts`
- Modify: `backend/src/api/api.ts`

- [ ] Add `startDownload(networkID, lishID)` method to CatalogManager
- [ ] Method checks if LISH exists in local DB (via dataServer.getLISH)
- [ ] If exists: creates Downloader, starts download, returns { status: 'downloading', downloadDir }
- [ ] If not exists: returns { status: 'not_local', message: 'LISH not available locally' }
- [ ] Register handler in api.ts

### Task 2: Add shared API type

**Files:**
- Modify: `shared/src/api.ts`

- [ ] Add `startDownload` method to CatalogAPI class
- [ ] Frontend can call `api.catalog.startDownload(networkID, lishID)`

## Chunk 2: Frontend — Product detail download flow

### Task 3: Wire Download button to catalog.startDownload

**Files:**
- Modify: `frontend/src/pages/Product/Product.svelte`

- [ ] Change `startDownload()` to call `api.catalog.startDownload()` instead of `api.transfer.download()`
- [ ] Handle 'downloading' status → show success alert
- [ ] Handle 'not_local' status → show info message "LISH available in catalog but not yet on this node"

### Task 4: Add mock handler + fix E2E tests

**Files:**
- Modify: `frontend/tests/e2e/fixtures/mock-backend.ts`

- [ ] Add `catalog.startDownload` handler returning { status: 'downloading', downloadDir: '/tmp/test' }
- [ ] Update E2E tests if needed

## Chunk 3: UI Polish

### Task 5: Consistent error display

- [ ] Replace INTERNAL_ERROR with user-friendly messages
- [ ] Download button shows spinner while starting
- [ ] Success message links to Downloads page conceptually
