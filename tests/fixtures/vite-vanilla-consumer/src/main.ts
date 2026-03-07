import {
  abiFingerprint,
  abiVersion,
  detectBrowserRuntimeSupport,
} from "@asupersync/browser";

const statusElement = document.getElementById("status");
if (!statusElement) {
  throw new Error("status element missing");
}

const support = detectBrowserRuntimeSupport();
const version = abiVersion();
const fingerprint = abiFingerprint();

statusElement.textContent = JSON.stringify(
  {
    support,
    abiVersion: version,
    abiFingerprint: fingerprint,
  },
  null,
  2,
);
