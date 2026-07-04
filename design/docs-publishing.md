# Documentation Publishing

SwarmOtter's user-facing documentation is published from `docs/` with mdBook.
The `design/` directory is not published as the user guide; it remains the
home for design notes, ADRs, contributor guidance, and agent-facing project
context.

## Local build

Install the same mdBook tooling versions used by CI:

```bash
cargo install mdbook --version 0.5.0 --locked
cargo install mdbook-mermaid --version 0.17.0 --locked
```

Build the static site:

```bash
mdbook build
```

Serve it locally while editing:

```bash
mdbook serve --open
```

The generated site is written to `book/`. That directory is ignored by git and
should not be committed.

## Mermaid diagrams

Mermaid support is provided by
[`mdbook-mermaid`](https://github.com/badboy/mdbook-mermaid). The preprocessor
configuration lives in `book.toml`, and the browser runtime assets are checked
in at the repository root:

- `mermaid.min.js`
- `mermaid-init.js`

Use standard Mermaid fences in files under `docs/`:

````markdown
```mermaid
flowchart LR
    client["Client"] --> api["SwarmOtter API"]
```
````

If the mdBook Mermaid tooling is upgraded, run this after changing the pinned
version in `.github/workflows/ci.yml`:

```bash
mdbook-mermaid install .
```

Review and commit any resulting changes to `book.toml`, `mermaid.min.js`, and
`mermaid-init.js`.

## GitHub Actions flow

The `CI` workflow builds the user guide as part of the normal main and pull
request checks:

- `docs-site` installs pinned mdBook tooling and runs `mdbook build`.
- Pull requests build the book but do not publish it.
- Pushes to `main` upload the generated `book/` directory as a GitHub Pages
  artifact.
- `deploy-pages` deploys that artifact to the `github-pages` environment.

The container image publishing job remains separate. Documentation publishing
depends on the same `build-test` job as the container image, so the public
documentation is updated only after the Rust checks pass.

## GitHub Pages repository settings

After the workflow is merged to `main`, configure repository Pages once:

1. Open the repository on GitHub.
2. Go to **Settings**.
3. In **Code and automation**, open **Pages**.
4. Under **Build and deployment**, set **Source** to **GitHub Actions**.
5. Do not select a branch or `/docs` folder as the publishing source; the
   workflow publishes the built mdBook artifact.
6. Leave the deployment environment as `github-pages`.

For the default project site, the published URL is expected to be:

```text
https://sphildreth.github.io/swarmotter/
```

`book.toml` sets `site-url = "/swarmotter/"` for that default project-site
path. If a custom domain is configured later, update both the GitHub Pages
custom-domain setting and `book.toml` so generated metadata uses the deployed
path.

## Publication boundary

GitHub Pages output is public. Do not place secrets, private operational
details, unpublished credentials, or maintainer-only instructions under
`docs/`. Put maintainer-only and design documentation under `design/`.
