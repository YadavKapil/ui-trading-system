# Vendored chart libraries

These browser builds are pinned and served by Flask so authenticated dashboard
pages do not execute chart code from third-party CDNs.

| Library | Version and build | SHA-256 | Source |
| --- | --- | --- | --- |
| Chart.js | 4.4.0 `dist/chart.umd.js` | `321e3a3fa98da4aaa957d10be57cbb514de0989eed8f9d726b5d05902cd01904` | Official [`chart.js-4.4.0.tgz`](https://github.com/chartjs/Chart.js/releases/download/v4.4.0/chart.js-4.4.0.tgz) release asset |
| Chart.js | 4.5.1 `dist/chart.umd.min.js` | `48444a82d4edcb5bec0f1965faacdde18d9c17db3063d042abada2f705c9f54a` | Official [`chart.js-4.5.1.tgz`](https://github.com/chartjs/Chart.js/releases/download/v4.5.1/chart.js-4.5.1.tgz) release asset |
| TradingView Lightweight Charts | 4.1.0 `dist/lightweight-charts.standalone.production.js` | `78d2bcbd79556d4f67ae3e3f7776f74e3b46a499466615b1f99397c53cb4056f` | Published [`lightweight-charts@4.1.0`](https://www.npmjs.com/package/lightweight-charts/v/4.1.0) package distribution |

The Chart.js 4.4.0 and 4.5.1 release archives have SHA-256
`3bbaa76f87d7cb75cc917eec7d4a7960c9ad3e1825cb88010386462a871d81a1`
and `d74ffdc9e4760d8960de3fd92bfa03397f01f85497c887d580bcf72641766aad`,
respectively. Version 4.5.1 is retained for the contributors page because its
formerly unpinned jsDelivr URL resolved to that exact published build.
The Lightweight Charts JavaScript was verified byte-for-byte against both
jsDelivr and unpkg copies of the pinned npm package. Upstream license and notice
files are retained beside each build.
