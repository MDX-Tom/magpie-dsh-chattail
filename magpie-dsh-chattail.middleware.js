/**
 * magpie-dsh-chattail / magpie-dsh-chattail.middleware.js
 *
 * Magpie Gateway Middleware
 * -------------------------
 * Auto-heals truncated Chat SSE streams missing finish_reason for DeepSeek Harness (DSH).
 *
 * Context & Problem:
 * DeepSeek Harness (DSH) embeds pi-ai's strict openai-completions stream reader:
 *
 *   if ((compat.supportsFinishReason && !hasFinishReason) || output.stopReason === "pending")
 *       throw new Error("Stream ended without finish_reason");
 *
 * When connecting models like minimax-cn/MiniMax-M3.1-Flash (especially with large
 * context: 48 tools, 360k–440k prompt tokens), upstream Chat servers frequently close
 * the connection immediately after finishing tool_calls arguments without emitting
 * a dedicated finish_reason frame or data: [DONE] (Magpie issue #5289).
 * DSH classifies this as a TRANSPORT error, retries up to 5 times, and fails the turn.
 *
 * Solution:
 * This gateway middleware observes Chat SSE streaming chunks in real-time:
 * 1. Tracks tool_calls argument assembly. The moment tool call arguments form a complete,
 *    syntactically valid JSON payload, it attaches finish_reason: "tool_calls" to that
 *    chunk. Even if the upstream socket abruptly cuts right after, DSH already holds
 *    hasFinishReason = true and successfully invokes the tool.
 * 2. Trailing usage chunks ({ usage: {...}, choices: [] }) emitted without finish_reason
 *    are repaired with choices: [{ delta: {}, finish_reason: "tool_calls" | "stop" }].
 * 3. Empty delta terminal chunks missing finish_reason are similarly synthesized.
 *
 * Runtime Contract:
 * - Runs in moejs inside Magpie gateway process (< 50ms budget per onEvent).
 * - Modifies event objects in place and returns them; returns undefined for fast pass-through.
 * - Never throws.
 */

const DEFAULT_APPLY_TO = ["minimax", "minimax-cn", "MiniMax"];

/**
 * Check whether a target model matches configured patterns.
 */
function isTargetModel(modelName, patterns) {
  if (typeof modelName !== "string" || !modelName.trim()) return false;
  const lower = modelName.toLowerCase().trim();
  const list = Array.isArray(patterns) && patterns.length > 0 ? patterns : DEFAULT_APPLY_TO;
  return list.some((p) => {
    if (typeof p !== "string") return false;
    const clean = p.toLowerCase().trim();
    return clean.length > 0 && lower.includes(clean);
  });
}

/**
 * onRequest hook: preprocess incoming agent request body.
 */
export function onRequest(body, ctx) {
  try {
    if (!body || typeof body !== "object") return body;
    const options = (ctx && ctx.options && typeof ctx.options === "object") ? ctx.options : {};
    if (options.enabled === false) return body;

    // Optional non-streaming fallback override if requested
    if (options.forceNonStream === true) {
      body.stream = false;
    }

    return body;
  } catch {
    return body;
  }
}

/**
 * onProvider hook: preprocess request before sending to provider try.
 */
export function onProvider(body, ctx) {
  try {
    if (!body || typeof body !== "object") return body;
    const options = (ctx && ctx.options && typeof ctx.options === "object") ? ctx.options : {};
    if (options.enabled === false) return body;

    if (options.forceNonStream === true) {
      body.stream = false;
    }

    return body;
  } catch {
    return body;
  }
}

/**
 * onEvent hook: inspect and repair each SSE event chunk.
 */
export function onEvent(event, ctx) {
  try {
    if (!event || typeof event !== "object") return;

    const options = (ctx && ctx.options && typeof ctx.options === "object") ? ctx.options : {};
    if (options.enabled === false) return;

    // Check if the current request targets a model we should heal
    const modelName = String(
      (ctx && ctx.model) ||
      (ctx && ctx.provider && ctx.provider.model) ||
      event.model ||
      ""
    );

    const patterns = Array.isArray(options.applyTo) && options.applyTo.length > 0
      ? options.applyTo
      : DEFAULT_APPLY_TO;

    if (modelName && !isTargetModel(modelName, patterns)) {
      return;
    }

    // Initialize or access per-request state (persisted across chunks by Magpie)
    const state = (ctx && ctx.state && typeof ctx.state === "object")
      ? ctx.state
      : (ctx.state = {});

    if (state.hasToolCalls === undefined) {
      state.hasToolCalls = false;
      state.hasFinishReason = false;
      state.toolArgBuffers = {};
      state.allToolsComplete = false;
      state.sawText = false;
    }

    let touched = false;
    const choices = Array.isArray(event.choices) ? event.choices : [];

    // 1. Inspect choices and deltas in this event
    for (const choice of choices) {
      if (!choice || typeof choice !== "object") continue;

      if (choice.finish_reason) {
        state.hasFinishReason = true;
      }

      const delta = choice.delta;
      if (delta && typeof delta === "object") {
        if (typeof delta.content === "string" && delta.content.length > 0) {
          state.sawText = true;
        }

        if (Array.isArray(delta.tool_calls) && delta.tool_calls.length > 0) {
          state.hasToolCalls = true;
          for (const tc of delta.tool_calls) {
            if (!tc || typeof tc !== "object") continue;
            const idx = tc.index ?? 0;
            if (!state.toolArgBuffers[idx]) {
              state.toolArgBuffers[idx] = "";
            }
            if (tc.function && typeof tc.function.arguments === "string") {
              state.toolArgBuffers[idx] += tc.function.arguments;
            }
          }

          // Check if all recorded tool call arguments currently form valid JSON
          const indices = Object.keys(state.toolArgBuffers);
          if (indices.length > 0) {
            let complete = true;
            for (const k of indices) {
              const buf = state.toolArgBuffers[k].trim();
              if (!buf || (!buf.endsWith("}") && !buf.endsWith("]"))) {
                complete = false;
                break;
              }
              try {
                JSON.parse(buf);
              } catch {
                complete = false;
                break;
              }
            }
            state.allToolsComplete = complete;
          }
        }
      }
    }

    // Determine target finish reason
    const targetFin = (typeof options.defaultFinishReason === "string" && options.defaultFinishReason !== "auto")
      ? options.defaultFinishReason
      : (state.hasToolCalls ? "tool_calls" : "stop");

    // Repair Rule 1: Tool call argument JSON completed in this chunk.
    // Upstream MiniMax frequently hangs up immediately after emitting the closing '}' of
    // tool arguments. Injecting finish_reason: "tool_calls" onto this chunk guarantees
    // DSH marks hasFinishReason = true before the socket closes.
    if (
      options.repairOnToolComplete !== false &&
      state.hasToolCalls &&
      state.allToolsComplete &&
      !state.hasFinishReason
    ) {
      if (choices.length > 0 && choices[0]) {
        choices[0].finish_reason = "tool_calls";
        state.hasFinishReason = true;
        touched = true;
      }
    }

    // Repair Rule 2: Trailing usage chunk missing finish_reason.
    // e.g. { choices: [], usage: { prompt_tokens: 120, completion_tokens: 30 } }
    if (options.repairUsageChunk !== false && event.usage && typeof event.usage === "object") {
      if (!state.hasFinishReason) {
        if (!Array.isArray(event.choices) || event.choices.length === 0) {
          event.choices = [{ index: 0, delta: {} }];
        }
        if (!event.choices[0].finish_reason) {
          event.choices[0].finish_reason = targetFin;
          state.hasFinishReason = true;
          touched = true;
        }
      }
    }

    // Repair Rule 3: Empty delta chunk ({ delta: {} }) without finish_reason.
    if (options.repairEmptyDelta !== false && !state.hasFinishReason && choices.length > 0 && choices[0]) {
      const d = choices[0].delta;
      const isEmptyDelta = d && typeof d === "object" && Object.keys(d).length === 0;
      if (isEmptyDelta && !choices[0].finish_reason) {
        choices[0].finish_reason = targetFin;
        state.hasFinishReason = true;
        touched = true;
      }
    }

    if (touched) {
      return event;
    }
    // Returning undefined tells Magpie writer to relay raw bytes unchanged (fast path)
    return undefined;
  } catch {
    // Fail-open: middleware exceptions never break client streams
    return undefined;
  }
}

/**
 * onResponse hook: inspect and repair whole non-streamed JSON response.
 */
export function onResponse(body, ctx) {
  try {
    if (!body || typeof body !== "object") return;
    const options = (ctx && ctx.options && typeof ctx.options === "object") ? ctx.options : {};
    if (options.enabled === false) return;

    if (Array.isArray(body.choices) && body.choices[0]) {
      const c = body.choices[0];
      if (!c.finish_reason) {
        const hasTools = !!(c.message && Array.isArray(c.message.tool_calls) && c.message.tool_calls.length > 0);
        const targetFin = (typeof options.defaultFinishReason === "string" && options.defaultFinishReason !== "auto")
          ? options.defaultFinishReason
          : (hasTools ? "tool_calls" : "stop");
        c.finish_reason = targetFin;
        return body;
      }
    }
  } catch {
    return;
  }
}
