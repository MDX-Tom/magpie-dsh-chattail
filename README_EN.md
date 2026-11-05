<h1 align="center">🩹 Magpie DSH ChatTail</h1>

<p align="center">
  Dynamically repairs missing <code>finish_reason</code> in upstream Chat SSE streams (such as MiniMax) at the Magpie gateway layer, completely eliminating <code>Stream ended without finish_reason</code> crashes in DeepSeek Harness (DSH).
</p>

<p align="center">
  <a href="https://usemagpie.ai/"><img alt="Magpie Middleware" src="https://img.shields.io/badge/Magpie-Gateway_Middleware-111827?style=flat-square" /></a>
  <a href="https://github.com/yetone/magpie/issues/5289"><img alt="Issue Fix" src="https://img.shields.io/badge/Fix-Issue_%235289-008080?style=flat-square" /></a>
  <a href="https://github.com/deepseek-ai"><img alt="DeepSeek Harness" src="https://img.shields.io/badge/Agent-DeepSeek_Harness-0284C7?style=flat-square" /></a>
  <a href="https://nodejs.org/"><img alt="Node / Bun" src="https://img.shields.io/badge/Runtime-moejs%20%2F%20Bun-3776AB?style=flat-square" /></a>
  <a href="LICENSE"><img alt="MIT License" src="https://img.shields.io/badge/License-MIT-yellow?style=flat-square" /></a>
</p>

<p align="center">
  <a href="README.md">简体中文</a> · <strong>English</strong>
</p>

> [!IMPORTANT]
> **This plugin resolves the critical issue where connecting DeepSeek Harness (DSH) through Magpie causes `Stream ended without finish_reason`, triggering 5 consecutive retries and eventually dropping the conversation turn (corresponding to Magpie Issue [#5289](https://github.com/yetone/magpie/issues/5289)).**
> 
> DSH incorporates `pi-ai`'s strict `openai-completions` stream parser, requiring a valid `finish_reason` in SSE chunks before stream termination. Without it, DSH classifies the connection drop as an unrecoverable transport-layer failure and throws `Stream ended without finish_reason`. In heavy-context, multi-tool agentic workflows (e.g., MiniMax-M3.1-Flash with 48 tools and 360k–440k tokens), upstream Chat providers often terminate the TCP connection immediately after completing the tool call arguments JSON without emitting a finish frame or `data: [DONE]`. Furthermore, Magpie's internal `chatTail` gate (`saysItsEnd`) only whitelists Gemini and CommandCode, allowing MiniMax's socket termination to pass through unhandled. This middleware intercepts SSE events in real-time, validates tool argument JSON closure, and injects `finish_reason` on the fly, guaranteeing 100% reliable tool dispatching in DSH.

---

## Why It Is Needed

| Comparison Dimension | Stock Magpie + DeepSeek Harness | With magpie-dsh-chattail |
| --- | --- | --- |
| **Problem** | Upstream Chat stream silently drops after completing tool arguments, missing `finish_reason`; DSH throws transport failure and retries 5 times (Issue [#5289](https://github.com/yetone/magpie/issues/5289)) | Automatically injects `finish_reason` at stream tail or upon tool argument JSON closure, eliminating socket-drop errors |
| **Method** | Downgrading models, disabling tools, or waiting for upstream Magpie `saysItsEnd` refactoring | Intercepts SSE chunks in microkernel (`moejs`) in `< 1 µs`, repairing frames based on JSON syntax completeness and token stats |
| **Outcome** | Frequent `Stream ended without finish_reason` crashes during long sessions; Agent drops active turn | Tool calls dispatch seamlessly without errors; smooth, robust long-context agentic experience |
| **Overhead** | None | Zero latency impact; intermediate non-terminal chunks pass through immediately without re-serialization |
| **Configurability** | None | Full Magpie GUI options panel; customizable target model list, finish reasons, and repair modes |

---

## Installation & Setup

### Method 1: Local Installation via Magpie Desktop GUI

1. Open the **Magpie Desktop Application**, click **Plugins** in the sidebar.
2. Navigate to **Installed → Add Plugin**.
3. Select the local directory `magpie-dsh-chattail`, click Add and enable it.

### Method 2: Installation via Magpie CLI

```bash
# Direct local installation
/Applications/magpie.app/Contents/MacOS/magpie plugin add "/Users/midaxia/Documents/Projects/Codex Projects/magpie-addons/magpie-dsh-chattail"

# Or install from GitHub after publishing
magpie plugin add github:MDX-Tom/magpie-dsh-chattail
```

---

## Configuration Options

Configure options directly in the **Magpie GUI Plugins Panel** without modifying any source files.

### 1. In Magpie GUI

1. Go to **Plugins → Installed**.
2. Locate **DSH ChatTail Repair (`magpie-dsh-chattail`)**.
3. Click the **Options** button on the right.
4. Modify options in the JSON editor (changes apply immediately to subsequent requests):

```json
{
  "enabled": true,
  "applyTo": ["minimax", "minimax-cn", "MiniMax"],
  "defaultFinishReason": "auto",
  "repairOnToolComplete": true,
  "repairUsageChunk": true,
  "repairEmptyDelta": true,
  "forceNonStream": false
}
```

### 2. Via Magpie CLI

```bash
# Update target model filter
/Applications/magpie.app/Contents/MacOS/magpie plugin options magpie-dsh-chattail '{"applyTo": ["minimax", "minimax-cn", "qwen"]}'

# Temporarily disable plugin
/Applications/magpie.app/Contents/MacOS/magpie plugin off magpie-dsh-chattail

# Reset to default options
/Applications/magpie.app/Contents/MacOS/magpie plugin options magpie-dsh-chattail off
```

### Detailed Parameter Reference

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `enabled` | `boolean` | `true` | Toggle middleware on/off. When `false`, all chunks pass through untouched. |
| `applyTo` | `string[]` | `["minimax", "minimax-cn", "MiniMax"]` | Case-insensitive substring match against model ID. |
| `defaultFinishReason` | `string` | `"auto"` | Terminal finish reason. `"auto"` sets `"tool_calls"` if tool invocations were detected, `"stop"` otherwise. |
| `repairOnToolComplete` | `boolean` | `true` | **Core Strategy**: Injects `finish_reason: "tool_calls"` immediately into the chunk that closes the tool argument JSON (`}`). Even if the upstream connection drops immediately after, DSH already holds the finish flag. |
| `repairUsageChunk` | `boolean` | `true` | Repairs trailing chunks containing only `usage` and empty `choices: []` by injecting a synthetic choice with `finish_reason`. |
| `repairEmptyDelta` | `boolean` | `true` | Repairs empty delta chunks (`{ delta: {} }`) that lack `finish_reason`. |
| `forceNonStream` | `boolean` | `false` | (Fallback mode) When `true`, forces upstream request to `stream: false` to completely bypass SSE chunking. |

---

## Technical Architecture & Root Cause

### 1. DSH Transport Failure Trigger
DSH uses `pi-ai`'s OpenAI stream reader:
```typescript
const choice = Array.isArray(chunk.choices) ? chunk.choices[0] : undefined;
if (!choice) continue;
if (choice.finish_reason) { hasFinishReason = true; }
...
if ((compat.supportsFinishReason && !hasFinishReason) || output.stopReason === "pending")
    throw new Error("Stream ended without finish_reason");
```
When MiniMax abruptly terminates the connection after sending tool arguments without a terminal chunk or `data: [DONE]`, DSH reaches EOF with `hasFinishReason === false`, raising an unrecoverable network transport error.

### 2. Magpie Gateway Blind Spot
Magpie's gateway includes `sniff.tail.finish()`, but gate `saysItsEnd(p, proto, model)` only allows `geminiCompat` and `commandCodeHost`. MiniMax is excluded from `saysItsEnd`, so Magpie relays the socket close directly without appending a finish frame.

### 3. Middleware Triple-Defense Repair
1. **Immediate Tool Closure Injection (`repairOnToolComplete`)**: Accumulates tool argument buffers and tests for valid JSON parsing upon receiving closing braces `}`. When valid, sets `finish_reason: "tool_calls"` right on the closing chunk. DSH registers completion before any socket disconnection occurs.
2. **Trailing Usage Chunk Repair (`repairUsageChunk`)**: Expands `{ usage: {...}, choices: [] }` with a choice containing `finish_reason`.
3. **Empty Delta Tail Repair (`repairEmptyDelta`)**: Intercepts empty delta chunks and safely closes the stream.

---

## Testing & Verification

The plugin includes a full unit test suite covering tool completion injection, usage chunk repairs, empty delta handling, options filtering, and non-streaming responses:

```bash
cd magpie-dsh-chattail
npm test
```

Test Results:
```text
▶ magpie-dsh-chattail middleware
  ✔ Scenario 1: Tool call argument JSON completion (MiniMax cut repair)
  ✔ Scenario 2: Trailing usage chunk repair
  ✔ Scenario 3: Empty delta terminal chunk repair
  ✔ Scenario 4: Already complete stream & options bypass
  ✔ Scenario 5: onRequest and onProvider hooks
  ✔ Scenario 6: onResponse whole JSON repair
✔ magpie-dsh-chattail middleware (3.0ms)
ℹ tests 10
ℹ suites 6
ℹ pass 10
ℹ fail 0
```

---

## License

MIT License © 2026 MDX-Tom
