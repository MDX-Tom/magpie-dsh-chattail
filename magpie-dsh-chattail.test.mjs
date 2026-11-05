import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  onRequest,
  onProvider,
  onEvent,
  onResponse,
} from "./magpie-dsh-chattail.middleware.js";

describe("magpie-dsh-chattail middleware", () => {
  describe("Scenario 1: Tool call argument JSON completion (MiniMax cut repair)", () => {
    it("attaches finish_reason: 'tool_calls' on the chunk that completes valid JSON tool arguments", () => {
      const ctx = {
        model: "minimax-cn/MiniMax-M3.1-Flash",
        options: { enabled: true },
        state: {},
      };

      // Event 1: role
      const ev1 = {
        id: "msg_1",
        choices: [{ index: 0, delta: { role: "assistant" } }],
      };
      const res1 = onEvent(ev1, ctx);
      assert.equal(res1, undefined, "ev1 should pass through unmodified");

      // Event 2: reasoning
      const ev2 = {
        id: "msg_1",
        choices: [{ index: 0, delta: { reasoning_content: "Checking directory..." } }],
      };
      const res2 = onEvent(ev2, ctx);
      assert.equal(res2, undefined, "ev2 should pass through unmodified");

      // Event 3: tool_calls init
      const ev3 = {
        id: "msg_1",
        choices: [{
          index: 0,
          delta: {
            tool_calls: [{
              index: 0,
              id: "call_abc123",
              type: "function",
              function: { name: "bash", arguments: "" },
            }],
          },
        }],
      };
      const res3 = onEvent(ev3, ctx);
      assert.equal(res3, undefined, "ev3 should pass through unmodified");

      // Event 4: tool_calls args fragment 1 (incomplete JSON)
      const ev4 = {
        id: "msg_1",
        choices: [{
          index: 0,
          delta: {
            tool_calls: [{
              index: 0,
              function: { arguments: '{"command":"' },
            }],
          },
        }],
      };
      const res4 = onEvent(ev4, ctx);
      assert.equal(res4, undefined, "ev4 incomplete JSON should pass through unmodified");

      // Event 5: tool_calls args fragment 2 (JSON closes with '}')
      const ev5 = {
        id: "msg_1",
        choices: [{
          index: 0,
          delta: {
            tool_calls: [{
              index: 0,
              function: { arguments: 'ls -la"}' },
            }],
          },
        }],
      };
      const res5 = onEvent(ev5, ctx);
      assert.notEqual(res5, undefined, "ev5 should be modified");
      assert.equal(res5.choices[0].finish_reason, "tool_calls");
      assert.equal(ctx.state.hasFinishReason, true);

      // Subsequent socket close: DSH already has finish_reason = "tool_calls",
      // preventing "Stream ended without finish_reason" error.
    });
  });

  describe("Scenario 2: Trailing usage chunk repair", () => {
    it("repairs usage chunk with choices: [] when tool_calls were emitted", () => {
      const ctx = {
        model: "minimax-cn/MiniMax-M3.1-Flash",
        options: { enabled: true, repairOnToolComplete: false },
        state: { hasToolCalls: true, hasFinishReason: false },
      };

      const usageEv = {
        id: "msg_2",
        choices: [],
        usage: { prompt_tokens: 420000, completion_tokens: 150, total_tokens: 420150 },
      };

      const res = onEvent(usageEv, ctx);
      assert.notEqual(res, undefined);
      assert.equal(res.choices.length, 1);
      assert.equal(res.choices[0].finish_reason, "tool_calls");
      assert.equal(ctx.state.hasFinishReason, true);
    });

    it("repairs usage chunk with choices: [] when text only (no tools)", () => {
      const ctx = {
        model: "minimax-cn/MiniMax-M3.1-Flash",
        options: { enabled: true },
        state: { hasToolCalls: false, hasFinishReason: false },
      };

      const usageEv = {
        id: "msg_3",
        choices: [],
        usage: { prompt_tokens: 100, completion_tokens: 50 },
      };

      const res = onEvent(usageEv, ctx);
      assert.notEqual(res, undefined);
      assert.equal(res.choices[0].finish_reason, "stop");
    });
  });

  describe("Scenario 3: Empty delta terminal chunk repair", () => {
    it("repairs { choices: [{ delta: {} }] } with no finish_reason", () => {
      const ctx = {
        model: "minimax-cn/MiniMax-M3.1-Flash",
        options: { enabled: true, repairOnToolComplete: false },
        state: { hasToolCalls: true, hasFinishReason: false },
      };

      const terminalEv = {
        id: "msg_4",
        choices: [{ index: 0, delta: {} }],
      };

      const res = onEvent(terminalEv, ctx);
      assert.notEqual(res, undefined);
      assert.equal(res.choices[0].finish_reason, "tool_calls");
    });
  });

  describe("Scenario 4: Already complete stream & options bypass", () => {
    it("does not modify chunks if finish_reason was already provided upstream", () => {
      const ctx = {
        model: "minimax-cn/MiniMax-M3.1-Flash",
        options: { enabled: true },
        state: {},
      };

      const ev = {
        id: "msg_5",
        choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
      };

      const res = onEvent(ev, ctx);
      assert.equal(res, undefined);
      assert.equal(ctx.state.hasFinishReason, true);
    });

    it("bypasses if options.enabled is false", () => {
      const ctx = {
        model: "minimax-cn/MiniMax-M3.1-Flash",
        options: { enabled: false },
        state: {},
      };

      const usageEv = {
        id: "msg_6",
        choices: [],
        usage: { prompt_tokens: 100 },
      };

      const res = onEvent(usageEv, ctx);
      assert.equal(res, undefined);
    });

    it("bypasses models that do not match applyTo patterns", () => {
      const ctx = {
        model: "anthropic/claude-3-5-sonnet",
        options: { enabled: true, applyTo: ["minimax", "minimax-cn"] },
        state: {},
      };

      const usageEv = {
        id: "msg_7",
        choices: [],
        usage: { prompt_tokens: 100 },
      };

      const res = onEvent(usageEv, ctx);
      assert.equal(res, undefined);
    });
  });

  describe("Scenario 5: onRequest and onProvider hooks", () => {
    it("forces stream: false when forceNonStream is enabled", () => {
      const body = { model: "minimax-cn/MiniMax-M3.1-Flash", stream: true };
      const ctx = { options: { forceNonStream: true } };

      const out = onRequest(body, ctx);
      assert.equal(out.stream, false);

      const provOut = onProvider({ ...body, stream: true }, ctx);
      assert.equal(provOut.stream, false);
    });

    it("leaves stream: true when forceNonStream is false", () => {
      const body = { model: "minimax-cn/MiniMax-M3.1-Flash", stream: true };
      const ctx = { options: { forceNonStream: false } };

      const out = onRequest(body, ctx);
      assert.equal(out.stream, true);
    });
  });

  describe("Scenario 6: onResponse whole JSON repair", () => {
    it("supplies finish_reason on whole JSON response when missing", () => {
      const body = {
        choices: [{
          message: {
            role: "assistant",
            tool_calls: [{ id: "call_1", type: "function", function: { name: "bash" } }],
          },
        }],
      };
      const ctx = { options: { enabled: true } };

      const out = onResponse(body, ctx);
      assert.equal(out.choices[0].finish_reason, "tool_calls");
    });
  });
});
