import { describe, expect, it } from "vitest";

import { clampHeight, computeHeight, isMultiline } from "../src/mode.js";

describe("isMultiline (the Enter-mode / line-number / history predicate)", () => {
  it("is false for empty and single-line buffers", () => {
    expect(isMultiline("")).toBe(false);
    expect(isMultiline("MATCH (n) RETURN n")).toBe(false);
  });

  it("is true as soon as the buffer contains a newline", () => {
    expect(isMultiline("MATCH (n)\nRETURN n")).toBe(true);
    expect(isMultiline("\n")).toBe(true);
    expect(isMultiline("trailing newline\n")).toBe(true);
  });

  it("ignores soft-wrap concerns: a long single line is still single-line", () => {
    expect(isMultiline("MATCH (n) WHERE n.name = 'x' ".repeat(40))).toBe(false);
  });
});

describe("clampHeight (auto-grow cap)", () => {
  it("passes content height through below the cap", () => {
    expect(clampHeight(36, 384)).toBe(36);
  });

  it("caps at maxHeightPx", () => {
    expect(clampHeight(900, 384)).toBe(384);
  });

  it("rounds fractional content heights up so the last line is not clipped", () => {
    expect(clampHeight(36.4, 384)).toBe(37);
  });

  it("is exact at the boundary", () => {
    expect(clampHeight(384, 384)).toBe(384);
  });
});

describe("computeHeight (user-resize floor + auto-grow)", () => {
  it("returns clamped content height when no user floor is set", () => {
    expect(computeHeight(36, 384, null)).toBe(36);
    expect(computeHeight(900, 384, null)).toBe(384);
  });

  it("returns the user floor when content height is below it", () => {
    expect(computeHeight(36, 384, 200)).toBe(200);
  });

  it("lets content height win when it exceeds the user floor", () => {
    expect(computeHeight(300, 384, 200)).toBe(300);
  });

  it("caps at maxHeightPx even when content exceeds the user floor", () => {
    expect(computeHeight(900, 384, 200)).toBe(384);
  });

  it("respects a user floor equal to the cap exactly", () => {
    expect(computeHeight(36, 384, 384)).toBe(384);
  });

  it("rounds fractional content heights up before comparing to the user floor", () => {
    // content 199.4 → ceil → 200; user floor 200 → max(200, 200) = 200
    expect(computeHeight(199.4, 384, 200)).toBe(200);
    // content 200.1 → ceil → 201; user floor 200 → max(200, 201) = 201
    expect(computeHeight(200.1, 384, 200)).toBe(201);
  });
});
