// Fails CI unless every executed Autobahn case is OK / NON-STRICT /
// INFORMATIONAL / UNIMPLEMENTED. 12.* / 13.* (permessage-deflate) are
// excluded in fuzzingclient.json — compression is OFF in Phase 1.
import { readFileSync } from 'node:fs';

const index = JSON.parse(readFileSync(new URL('./reports/index.json', import.meta.url)));
const results = index.beamsocket ?? {};
const acceptable = new Set(['OK', 'NON-STRICT', 'INFORMATIONAL', 'UNIMPLEMENTED']);
const bad = Object.entries(results).filter(([, r]) => !acceptable.has(r.behavior));

console.log(`autobahn: ${Object.keys(results).length} cases`);
if (bad.length > 0) {
  for (const [caseId, r] of bad) {
    console.error(`FAIL ${caseId}: ${r.behavior} (close: ${r.behaviorClose})`);
  }
  process.exit(1);
}
console.log('autobahn: all green');
