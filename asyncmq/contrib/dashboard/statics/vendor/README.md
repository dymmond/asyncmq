# Dashboard Vendor Assets

AsyncMQ vendors dashboard browser assets as package resources so the contrib
dashboard does not depend on public CDNs at runtime.

## Current Assets

| Asset | Version | Source | Runtime file |
| --- | --- | --- | --- |
| Alpine.js | 3.15.12 | npm `alpinejs` | `vendor/alpinejs/alpine-3.15.12.min.js` |
| Tailwind CSS | 4.3.2 | npm `tailwindcss` + `@tailwindcss/cli` | `css/tailwind-4.3.2.min.css` |
| Chart.js | 4.5.1 | npm `chart.js` | `vendor/chartjs/chart.umd-4.5.1.min.js` |

Checksums and npm integrity strings are recorded in `manifest.json`.
License attributions are stored in `vendor/licenses/`.

## Update Procedure

1. Query authoritative npm package metadata:
   `https://registry.npmjs.org/alpinejs/latest`,
   `https://registry.npmjs.org/tailwindcss/latest`,
   `https://registry.npmjs.org/@tailwindcss/cli/latest`, and
   `https://registry.npmjs.org/chart.js/latest`.
2. Download the exact tarballs listed in the metadata and verify their npm
   integrity values plus SHA-256 checksums.
3. Copy Alpine's `dist/cdn.min.js` to
   `vendor/alpinejs/alpine-<version>.min.js`.
4. Generate Tailwind's committed stylesheet from `css/asyncmq-tailwind.input.css`
   using the matching `@tailwindcss/cli` and `tailwindcss` versions. The output
   must be written to `css/tailwind-<version>.min.css`.
5. Copy Chart.js `dist/chart.umd.min.js` to
   `vendor/chartjs/chart.umd-<version>.min.js`.
6. Update template references, `manifest.json`, and license files.
7. Run package-content tests and build a wheel/sdist to prove every asset is
   included from an installed package.

Do not replace these assets with public CDN links.
