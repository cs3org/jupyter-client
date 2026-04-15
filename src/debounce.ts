/**
 * Creates a debounced async caller that delays invocation and aborts
 * any in-flight request when a new call arrives.
 *
 * Returns a `run` function and a `cancel` function (for cleanup).
 * The callback receives an `AbortSignal` it should forward to fetch calls.
 */
export function createDebouncedFetcher(delay: number) {
  let timer: ReturnType<typeof setTimeout> | undefined;
  let controller: AbortController | undefined;

  function run(fn: (signal: AbortSignal) => Promise<void>): void {
    if (timer) clearTimeout(timer);
    timer = setTimeout(() => {
      if (controller) controller.abort();
      controller = new AbortController();
      fn(controller.signal);
    }, delay);
  }

  function cancel(): void {
    if (timer) clearTimeout(timer);
    if (controller) controller.abort();
  }

  return { run, cancel };
}
