#!/usr/bin/env node
/**
 * v4: Runtime patch for @libp2p/kad-dht@16.1.6 pbStream memory leak.
 *
 * Fixes code review issues from v3:
 *  - Idempotent (detects already-patched state, no-op safely)
 *  - Version-gated (aborts on unsupported @libp2p/kad-dht versions)
 *  - Backs up originals to .orig before patching
 *  - Marker-based success detection (not accidental substring match)
 *  - Real error logging instead of silent catch(e){}
 *  - Well-defined exit codes: 0=applied, 2=already-patched, 3=unsupported version,
 *    4=files missing, 5=patch verification failed
 *
 * Usage:
 *   node patch-kad-dht-v4.js           # apply patch (inside container: /app/backend/node_modules)
 *   node patch-kad-dht-v4.js --prefix=/custom/path/node_modules
 *
 * Markers inserted by the patch (used for idempotence + verification):
 *   rpc/index.js:   "// v4-marker: rpc onIncomingStream unwrap"
 *   network.js:     "// v4-marker: network _writeMessage unwrap"
 *                   "// v4-marker: network _writeReadMessage unwrap"
 */

"use strict";
const fs = require("fs");
const path = require("path");

const SUPPORTED_VERSIONS = ["16.1.6"];
const MARKERS = {
  rpc: "v4-marker: rpc onIncomingStream unwrap",
  writeMsg: "v4-marker: network _writeMessage unwrap",
  writeReadMsg: "v4-marker: network _writeReadMessage unwrap",
};

function parseArgs() {
  const args = { prefix: "/app/backend/node_modules" };
  for (const a of process.argv.slice(2)) {
    if (a.startsWith("--prefix=")) args.prefix = a.slice("--prefix=".length);
  }
  return args;
}

function log(level, msg) {
  const ts = new Date().toISOString();
  console[level === "error" ? "error" : "log"](`[${ts}] [patch-kad-dht-v4] [${level}] ${msg}`);
}

function readFile(p) {
  try {
    return fs.readFileSync(p, "utf8");
  } catch (err) {
    log("error", `cannot read ${p}: ${err.message}`);
    return null;
  }
}

function backupOnce(p) {
  const orig = p + ".orig";
  if (fs.existsSync(orig)) return; // already backed up
  fs.copyFileSync(p, orig);
  log("info", `backed up ${path.basename(p)} → ${path.basename(orig)}`);
}

function applyRpcPatch(src) {
  if (src.includes(MARKERS.rpc)) return { src, changed: false, reason: "already-patched" };

  const needle = `        const messages = pbStream(stream).pb(Message);\n        while (true) {`;
  if (!src.includes(needle)) return { src, changed: false, reason: "needle-not-found" };

  const before = `        const messages = pbStream(stream).pb(Message);\n        while (true) {`;
  const after = `        const messages = pbStream(stream).pb(Message);\n        // ${MARKERS.rpc}\n        try { while (true) {`;
  src = src.replace(before, after);

  // Close try block + add finally with unwrap before the trailing method brace.
  // We anchor on the last `}` of onIncomingStream which follows the while loop's
  // last `signal.addEventListener('abort', abortListener);\n        }\n    }`.
  const closeNeedle = `signal.addEventListener('abort', abortListener);\n        }\n    }`;
  if (!src.includes(closeNeedle)) return { src, changed: false, reason: "close-needle-not-found" };
  const closeReplace =
    `signal.addEventListener('abort', abortListener);\n` +
    `        }\n` +
    `        } finally {\n` +
    `            signal.removeEventListener('abort', abortListener);\n` +
    `            try { messages.unwrap().unwrap(); } catch (e) { this.log.error('pbStream unwrap error - %e', e); }\n` +
    `        }\n` +
    `    }`;
  src = src.replace(closeNeedle, closeReplace);

  return { src, changed: true };
}

function applyNetworkPatch(src) {
  let changed = false;
  const results = [];

  // _writeMessage
  if (!src.includes(MARKERS.writeMsg)) {
    const needle = `    async _writeMessage(stream, msg, options) {\n        const pb = pbStream(stream);\n        await pb.write(msg, Message, options);\n    }`;
    if (src.includes(needle)) {
      const replace =
        `    async _writeMessage(stream, msg, options) {\n` +
        `        // ${MARKERS.writeMsg}\n` +
        `        const pb = pbStream(stream);\n` +
        `        try {\n` +
        `            await pb.write(msg, Message, options);\n` +
        `        } finally {\n` +
        `            try { pb.unwrap(); } catch (e) { this.log.error('pbStream unwrap error (writeMessage) - %e', e); }\n` +
        `        }\n` +
        `    }`;
      src = src.replace(needle, replace);
      changed = true;
      results.push("writeMsg:applied");
    } else {
      results.push("writeMsg:needle-not-found");
    }
  } else {
    results.push("writeMsg:already-patched");
  }

  // _writeReadMessage
  if (!src.includes(MARKERS.writeReadMsg)) {
    const needle = `    async _writeReadMessage(stream, msg, options) {\n        const pb = pbStream(stream);\n        await pb.write(msg, Message, options);\n        const message = await pb.read(Message, options);`;
    if (src.includes(needle)) {
      const replace =
        `    async _writeReadMessage(stream, msg, options) {\n` +
        `        // ${MARKERS.writeReadMsg}\n` +
        `        const pb = pbStream(stream);\n` +
        `        let message;\n` +
        `        try {\n` +
        `            await pb.write(msg, Message, options);\n` +
        `            message = await pb.read(Message, options);\n` +
        `        } finally {\n` +
        `            try { pb.unwrap(); } catch (e) { this.log.error('pbStream unwrap error (writeReadMessage) - %e', e); }\n` +
        `        }`;
      src = src.replace(needle, replace);
      changed = true;
      results.push("writeReadMsg:applied");
    } else {
      results.push("writeReadMsg:needle-not-found");
    }
  } else {
    results.push("writeReadMsg:already-patched");
  }

  return { src, changed, results };
}

function main() {
  const { prefix } = parseArgs();
  const kadDir = path.join(prefix, "@libp2p", "kad-dht");
  const pkgPath = path.join(kadDir, "package.json");
  const rpcPath = path.join(kadDir, "dist", "src", "rpc", "index.js");
  const netPath = path.join(kadDir, "dist", "src", "network.js");

  // --- Preflight: files exist
  for (const p of [pkgPath, rpcPath, netPath]) {
    if (!fs.existsSync(p)) {
      log("error", `file missing: ${p}`);
      log("error", "kad-dht not installed at the expected path. Nothing to patch.");
      process.exit(4);
    }
  }

  // --- Version gate
  const pkg = JSON.parse(readFile(pkgPath));
  if (!SUPPORTED_VERSIONS.includes(pkg.version)) {
    log("error", `unsupported @libp2p/kad-dht version ${pkg.version}; supported: ${SUPPORTED_VERSIONS.join(", ")}`);
    log("error", "refusing to patch — upstream may have changed. Review the fix and update this script.");
    process.exit(3);
  }
  log("info", `target: @libp2p/kad-dht@${pkg.version} at ${kadDir}`);

  let rpcSrc = readFile(rpcPath);
  let netSrc = readFile(netPath);

  // --- Idempotence check before touching anything
  const rpcAlready = rpcSrc.includes(MARKERS.rpc);
  const netAlready = netSrc.includes(MARKERS.writeMsg) && netSrc.includes(MARKERS.writeReadMsg);
  if (rpcAlready && netAlready) {
    log("info", "all 3 patch points already carry v4 markers — no changes needed");
    process.exit(2);
  }

  // --- Backup originals (only first time)
  backupOnce(rpcPath);
  backupOnce(netPath);

  // --- Apply patches
  const rpcResult = applyRpcPatch(rpcSrc);
  if (rpcResult.changed) {
    fs.writeFileSync(rpcPath, rpcResult.src);
    log("info", "rpc/index.js: applied onIncomingStream unwrap");
  } else {
    log("info", `rpc/index.js: not modified (${rpcResult.reason})`);
  }

  const netResult = applyNetworkPatch(netSrc);
  if (netResult.changed) {
    fs.writeFileSync(netPath, netResult.src);
  }
  log("info", `network.js: ${netResult.results.join(", ")}`);

  // --- Verify using markers, not loose substring counts
  const rpcFinal = readFile(rpcPath);
  const netFinal = readFile(netPath);
  const ok = {
    rpc: rpcFinal.includes(MARKERS.rpc) && rpcFinal.includes("messages.unwrap().unwrap()"),
    writeMsg: netFinal.includes(MARKERS.writeMsg) && netFinal.includes("pb.unwrap()"),
    writeReadMsg: netFinal.includes(MARKERS.writeReadMsg),
  };

  log("info", `verification: rpc=${ok.rpc} writeMsg=${ok.writeMsg} writeReadMsg=${ok.writeReadMsg}`);

  if (!(ok.rpc && ok.writeMsg && ok.writeReadMsg)) {
    log("error", "patch verification FAILED — check file contents manually");
    process.exit(5);
  }

  log("info", "all 3 patch points verified OK — pbStream unwrap cleanup in place");
  process.exit(0);
}

main();
