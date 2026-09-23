import { createHash, createHmac } from "node:crypto";
import { readFileSync } from "node:fs";
import { createServer } from "node:http";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { startPeryx } from "../../../peryx-web/tests/frontend/server.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const port = Number(process.env.PERYX_PYPI_FRONTEND_PORT ?? 4456);
const upstreamPort = Number(process.env.PERYX_PYPI_UPSTREAM_PORT ?? 4454);
const upstreamBase = `http://127.0.0.1:${upstreamPort}`;
const wheel = readFileSync(
  join(here, "..", "fixtures", "veloxdemo-1.0.0-py3-none-any.whl"),
);
const signedFilename =
  "circleci_sign_publish_example-0.0.1.dev137-py3-none-any.whl";
const signedWheel = Buffer.from(
  readFileSync(
    join(here, "..", "fixtures", `${signedFilename}.b64`),
    "utf8",
  ).replace(/\s/g, ""),
  "base64",
);
const signedAttestation = readFileSync(
  join(here, "..", "fixtures", `${signedFilename}.publish.attestation`),
  "utf8",
);
const sigstoreTrustedRoot = readFileSync(
  join(here, "..", "fixtures", "sigstore-production-trusted-root.json"),
  "utf8",
);
const signingKey = "frontend-trusted-publisher-signing-key";
const attestationIdentity =
  "https://circleci.com/api/v2/projects/fdd9283f-e619-46af-8f9c-851f7d3e8b2b/" +
  "pipeline-definitions/8e4f8ab2-8d7c-4827-9f15-de076d6d647f";

function file(filename) {
  const digest = createHash("sha256").update(filename).digest("hex");
  return {
    filename,
    url: `${upstreamBase}/files/${digest}/${filename}`,
    hashes: { sha256: digest },
    size: filename.length,
    "upload-time": "2026-01-01T00:00:00Z",
    yanked: false,
  };
}

const largeVersions = Array.from(
  { length: 100 },
  (_, version) => `${version}.0`,
);
const simplePages = new Map([
  [
    "/simple/",
    {
      meta: { "api-version": "1.1" },
      projects: [{ name: "large-demo" }, { name: "veloxdemo" }],
    },
  ],
  [
    "/simple/veloxdemo/",
    {
      meta: { "api-version": "1.1" },
      name: "veloxdemo",
      versions: ["0.9"],
      files: [
        {
          ...file("veloxdemo-0.9-py3-none-any.whl"),
          provenance: `${upstreamBase}/files/veloxdemo-0.9-py3-none-any.whl.provenance`,
        },
      ],
    },
  ],
  [
    "/simple/large-demo/",
    {
      meta: { "api-version": "1.1" },
      name: "large-demo",
      versions: largeVersions,
      files: largeVersions.flatMap((version) =>
        Array.from({ length: 20 }, (_, build) =>
          file(
            `large_demo-${version}-${String(build).padStart(3, "0")}-py3-none-any.whl`,
          ),
        ),
      ),
    },
  ],
]);
const upstream = createServer((request, response) => {
  const path = new URL(request.url, upstreamBase).pathname;
  if (simplePages.has(path)) {
    response.writeHead(200, {
      "content-type": "application/vnd.pypi.simple.v1+json",
    });
    response.end(JSON.stringify(simplePages.get(path)));
  } else if (path.startsWith("/files/")) {
    response.writeHead(200, { "content-type": "application/octet-stream" });
    response.end(decodeURIComponent(path.split("/").at(-1)));
  } else {
    response.writeHead(404);
    response.end("not found");
  }
});
await new Promise((resolve, reject) => {
  upstream.once("error", reject);
  upstream.listen(upstreamPort, "127.0.0.1", resolve);
});

await startPeryx({
  configText: `[auth]
signing_key = "${signingKey}"
sigstore_trusted_root = '''${sigstoreTrustedRoot}'''

[[auth.trusted_publisher]]
id = "release"
issuer = "https://oidc.circleci.com"
repository = "hosted"
subject = "*"
projects = ["circleci-sign-publish-example"]
attestation_identity = "${attestationIdentity}"

[auth.trusted_publisher.attestation_claims]
"1.3.6.1.4.1.57264.1.12" = "github.com/CircleCI-Public/sign-and-publish-examples"

[[index]]
name = "pypi"
ecosystem = "pypi"

[[index.upstream]]
name = "fixture"
url = "${upstreamBase}/simple/"

[[index]]
name = "hosted"
ecosystem = "pypi"
hosted = true

[[index.access_token]]
name = "uploader"
secret = "playwright-secret"
actions = ["write", "delete"]

[[index]]
name = "internal"
ecosystem = "pypi"
hosted = true

[[index.access_token]]
name = "uploader"
secret = "playwright-secret"
actions = ["write", "delete"]

[[index.access_token]]
name = "reader"
secret = "playwright-reader"
actions = ["read"]

[[index]]
name = "limited"
ecosystem = "pypi"
hosted = true

[index.policy]
max_file_size_bytes = 512

[[index.access_token]]
name = "uploader"
secret = "playwright-secret"
actions = ["write", "delete"]

[[index]]
name = "zz-browser-upload"
ecosystem = "pypi"
hosted = true

[[index.access_token]]
name = "uploader"
secret = "playwright-secret"
actions = ["write", "delete"]

[[index]]
name = "root-pypi"
route = "root/pypi"
ecosystem = "pypi"
layers = ["hosted", "pypi"]
write_target = "hosted"
`,
  port,
  readyPort: Number(process.env.PERYX_PYPI_READY_PORT ?? 5456),
  repo: join(here, "..", "..", "..", ".."),
  close: [() => upstream.close()],
  prepare: async ({ base }) => {
    const form = new FormData();
    form.set(":action", "file_upload");
    form.set("name", "veloxdemo");
    form.set("version", "1.0.0");
    form.set("filetype", "bdist_wheel");
    form.set("content", new Blob([wheel]), "veloxdemo-1.0.0-py3-none-any.whl");
    const response = await fetch(`${base}/root/pypi/`, {
      method: "POST",
      headers: {
        authorization: `Basic ${Buffer.from("__token__:playwright-secret").toString("base64")}`,
      },
      body: form,
    });
    if (!response.ok)
      throw new Error(
        `upload rejected: ${response.status} ${await response.text()}`,
      );
    const signedForm = new FormData();
    signedForm.set(":action", "file_upload");
    signedForm.set("name", "circleci-sign-publish-example");
    signedForm.set("version", "0.0.1.dev137");
    signedForm.set("filetype", "bdist_wheel");
    signedForm.set("attestations", `[${signedAttestation}]`);
    signedForm.set("content", new Blob([signedWheel]), signedFilename);
    const signedResponse = await fetch(`${base}/root/pypi/`, {
      method: "POST",
      headers: { authorization: `Bearer ${trustedPublisherToken()}` },
      body: signedForm,
    });
    if (!signedResponse.ok)
      throw new Error(
        `signed upload rejected: ${signedResponse.status} ${await signedResponse.text()}`,
      );
    const search = await fetch(`${base}/+search?q=veloxdemo&page_size=1`);
    const body = await search.text();
    if (!search.ok || !body.includes("veloxdemo"))
      throw new Error(
        `search index did not publish the fixture: ${search.status} ${body}`,
      );
  },
});

function trustedPublisherToken() {
  const issuedAt = Math.floor(Date.now() / 1000);
  const encode = (value) =>
    Buffer.from(JSON.stringify(value)).toString("base64url");
  const content = `${encode({ alg: "HS256", typ: "JWT" })}.${encode({
    sub: "trusted-publisher:release",
    aud: "peryx",
    iat: issuedAt,
    exp: issuedAt + 300,
    jti: "frontend-trusted-token",
    purpose: "trusted-publishing",
    grants: [
      {
        resources: ["root/pypi/circleci-sign-publish-example"],
        actions: ["write"],
      },
    ],
  })}`;
  return `${content}.${createHmac("sha256", signingKey).update(content).digest("base64url")}`;
}
