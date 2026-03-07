"use client";

import {
  detectNextRuntimeSupport,
} from "@asupersync/next";

const support = detectNextRuntimeSupport("client");

export default function HomePage() {
  return (
    <main>
      <h1>Asupersync Next/Turbopack Consumer Fixture</h1>
      <pre id="next-support">
        {JSON.stringify(
          {
            support,
            diagnostic: support.message,
          },
          null,
          2,
        )}
      </pre>
    </main>
  );
}
