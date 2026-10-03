/** Serialises state-mutating sections so concurrent requests cannot interleave. */
export class Mutex {
  constructor() {
    this._chain = Promise.resolve();
  }

  /** Run `fn` once the lock is free; returns fn's result. */
  runExclusive(fn) {
    const prev = this._chain;
    let release;
    const gate = new Promise((resolve) => {
      release = resolve;
    });
    this._chain = prev.then(() => gate);
    return (async () => {
      await prev;
      try {
        return await fn();
      } finally {
        release();
      }
    })();
  }
}
