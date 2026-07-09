/**
 * Pure helpers behind the editor's single-line/multi-line mode switching
 * and auto-grow height handling. Kept DOM-free so they are unit-testable.
 */

/**
 * The mode predicate: a buffer is "multi-line" when it contains a hard
 * newline. Drives the `queryBarMultiline` context key (Enter runs vs.
 * inserts a newline), the line-number morph, and the history gating.
 *
 * Soft-wrapped long single lines are NOT multi-line: wrapping is a rendering
 * concern, the mode is a property of the buffer text.
 */
export function isMultiline(value: string): boolean {
  return value.includes("\n");
}

/**
 * Clamp Monaco's reported content height to the editor's growth cap.
 * Below the cap the editor grows to fit; at the cap it scrolls internally.
 */
export function clampHeight(contentHeightPx: number, maxHeightPx: number): number {
  return Math.min(Math.ceil(contentHeightPx), maxHeightPx);
}

/**
 * Compute the container height to use during layout.
 *
 * When `userSetHeightPx` is non-null (the user has dragged the resize handle),
 * the returned height is the larger of the user's floor and the auto-grow
 * value — so the editor can still grow above the user's floor when content
 * demands it, but never shrinks below it. When `userSetHeightPx` is null the
 * result is the plain clamped content height (standard auto-grow behaviour).
 */
export function computeHeight(
  contentHeightPx: number,
  maxHeightPx: number,
  userSetHeightPx: number | null,
): number {
  const auto = clampHeight(contentHeightPx, maxHeightPx);
  return userSetHeightPx !== null ? Math.max(userSetHeightPx, auto) : auto;
}
