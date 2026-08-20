# Publishing BeamSocket to npm

## What's verified (done, in the sandbox)

- The package **builds and runs**. A clean `npm install` of the packed tarball,
  then `import 'beamsocket'`, boots a real server and echoes a live client —
  proven end-to-end on linux-x64 (glibc), Node 20.
- The name `beamsocket` is **available** on npm.
- A platform guardrail (`"os": ["linux"], "cpu": ["x64"]` in the preview
  manifest) makes npm **refuse to install** on macOS/Windows/arm with a clean
  `EBADPLATFORM` error — never a runtime crash.

## The one honest blocker

The Rust engine compiles to a **separate native binary per platform**. Only the
**linux-x64** binary can be built on the Linux sandbox — a macOS binary must be
built on macOS, a Windows binary on Windows. There is no cross-compile shortcut
here (no Apple SDK, no cross-linkers, no root) and no pure-JS fallback.

So publishing a package that works on **every** platform requires building on
every platform. That's what CI runners are for.

---

## Recommended path — full release, all platforms (via CI)

The workflow already exists: `.github/workflows/prebuild.yml`. It builds all six
targets on real `ubuntu-latest`, `macos-14`, and `windows-latest` runners and
publishes on a version tag. You do three things, once:

1. **Push this repo to GitHub** (if it isn't already).
2. **Add your npm token as a GitHub secret** named `NPM_TOKEN`
   (repo → Settings → Secrets and variables → Actions → New repository secret).
   *Never paste the token into code, chat, or a commit.*
3. **Tag and push** (from `main`, after the release branch merges):
   ```bash
   git tag v0.2.0
   git push origin v0.2.0
   ```
4. **After the publish succeeds:** regenerate the lockfile against the now-
   published platform packages and commit it —
   ```bash
   npm install          # resolves beamsocket-*-0.2.0 with real integrity hashes
   ```
   (the lockfile deliberately still points at the 0.1.0-alpha.0 platform
   tarballs until then — hand-editing versions without real registry hashes
   would produce a lockfile that lies about what it can verify), then run the
   install-and-echo smoke test from a clean directory:
   ```bash
   mkdir /tmp/smoke && cd /tmp/smoke && npm init -y && npm install beamsocket@alpha
   node -e "import('beamsocket').then(async ({BeamSocket}) => { const io = new BeamSocket({}); await io.listen(0); console.log('boots'); await io.close(); })"
   ```

CI compiles the binary on each OS, publishes the six per-platform packages, then
the main `beamsocket` package. After that, `npm install beamsocket` works on
Linux, macOS, and Windows. This is the clean path — use it.

> Before the tag: run the CI once on a branch to confirm the macOS build is
> green (the darwin row was CI-gated, never run locally — see the roadmap).

---

## Fast path — linux-x64 preview today (optional)

If you want to stake the name **now** with an honest, clearly-scoped preview,
from a **linux-x64 machine** with Rust installed:

```bash
# 1. build the native binary (needs the Rust toolchain)
npm run build:native -w beamsocket

# 2. stage the linux-only manifest + assets, then publish under the `alpha` tag
bash scripts/publish-preview.sh        # packs dist/ + native/ + preview manifest

# publish (you must be `npm login`'d, or set NPM_TOKEN in your env):
npm publish beamsocket-0.2.0.tgz --tag alpha --access public
```

Notes:
- `--tag alpha`, **not** `latest`: so `npm install beamsocket` (which grabs
  `latest`) is not pointed at a linux-only build. Users opt in with
  `npm install beamsocket@alpha`.
- The preview manifest carries `"os": ["linux"], "cpu": ["x64"]`, so anyone on
  another platform gets a clean install-time rejection.
- This still needs a linux-x64 box to produce the binary. If you're on a Mac,
  use the CI path above instead.

---

## Still owed before this is a "1.0-serious" release

(a `0.2.0` alpha publish does not retire them)

- pinned-box benchmark + constant re-confirmation, the 10-minute soak
- the cluster mesh's 30-minute real-hardware soak, before any release
  *upgrades* cluster support from "alpha feature" to a headline claim

Retired on the `v0.2.0-cluster-js` branch (2026-08-20):
- ~~vendored-crypto → audited `hmac`/`sha2` swap~~ — done, KAT-regression
  green (FIPS 180-4 / RFC 4231 vectors byte-identical).
- ~~JS-side cluster activation (the addon rebuild)~~ — done; clustering is
  reachable from `new BeamSocket({ cluster: {...} })`, proven by the JS test
  suite and a by-hand 3-node `examples/cluster` run.
