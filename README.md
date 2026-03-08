# Market Crawler API Docs

Mintlify documentation source for the crawler workspace.

## What this repo contains

- GMGN token API reference
- GMGN wallet API reference
- OKX trending API reference
- Padre snapshot and streaming API reference
- OpenAPI files used by Mintlify API Reference

## Local preview

```bash
npx mint dev
```

The docs site is expected on `http://localhost:3000`.

## Source of truth

- Navigation and theme: `docs.json`
- Guide pages: root `*.mdx`
- API overview pages: `api-reference/*.mdx`
- Endpoint reference: `openapi/*.openapi.json`

## Publish flow

Push changes to `main`. Mintlify should redeploy from the default branch configured in the dashboard.
