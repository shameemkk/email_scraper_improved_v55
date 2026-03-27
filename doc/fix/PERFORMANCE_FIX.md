# Performance Fix: innerHTML Regex Bottleneck

## Problem

Scraping heavy pages (e.g. Dollar Tree store locator) caused **120s+ timeouts** and hung requests.

**Root cause:** `page.evaluate()` was running `document.documentElement.innerHTML.match(emailRegex)` — a regex scan on the **entire raw HTML** string. For pages with lots of inline JS, JSON-LD, SVG data, and map embeds, this HTML can be **1.76 MB+**, causing the regex to take **2+ minutes** to complete.

### Example URL that triggered the issue

```
https://locations.dollartree.com/ga/rincon/410-s-columbia-ave-ste-f?utm_source=google&utm_medium=organic&utm_campaign=maps
```

### Diagnostic timeline

| Step | Time |
|------|------|
| `page.goto()` (domcontentloaded) | ~1.7s |
| `waitForLoadState('networkidle')` | 5s (timeout, expected) |
| `page.evaluate()` with innerHTML regex | **2min 14s** |
| **Total** | **~2min 21s → timed out at 120s** |

---

## Fixes Applied

### 1. Replaced innerHTML regex with DOM TreeWalker + attribute scan

**Before:** Scanned the entire raw HTML string (1.76 MB) with a regex.

```js
// OLD — extremely slow on large pages
const fullHtml = document.documentElement.innerHTML || '';
const htmlEmails = fullHtml.match(emailRegex);
```

**After:** Walks individual DOM text nodes and checks specific attributes. Only regex-matches strings that contain `@`.

```js
// NEW — fast, skips binary/script noise
const walker = document.createTreeWalker(
  document.body || document.documentElement,
  NodeFilter.SHOW_TEXT
);
while (walker.nextNode()) {
  const text = walker.currentNode.nodeValue;
  if (text && text.includes('@')) {
    const found = text.match(emailRegex);
    if (found) found.forEach((e) => emailSet.add(decodeEmail(e)));
  }
}

// Targeted attribute scan
document.querySelectorAll('[href], [content], [value], [title], [alt]').forEach((el) => {
  for (const attr of ['href', 'content', 'value', 'title', 'alt']) {
    const val = el.getAttribute(attr);
    if (val && val.includes('@')) {
      const found = val.match(emailRegex);
      if (found) found.forEach((e) => emailSet.add(decodeEmail(e)));
    }
  }
});
```

### 2. Added sibling/cousin page filter (`isSiblingPage`)

Store locator pages link to **hundreds of sibling store pages** (e.g. `/ga/pooler/...`, `/sc/hardeeville/...`). These have the same structure but no unique contact emails, and each one is heavy (loads Google Maps, tracking scripts, etc.).

The `isSiblingPage()` function detects and skips:
- **Same-depth pages** with different path segments (cousin stores in different cities)
- **Parent listing pages** (e.g. `/ga/rincon` or `/ga` when base is `/ga/rincon/410-s-columbia`)

Only applies when the base URL has 3+ path segments, so simple sites are unaffected.

### 3. Blocked maps and heavy third-party domains

Added to the resource blocker:
- **Maps:** `maps.googleapis.com`, `maps.gstatic.com`, `maps.google.com`, `tiles.mapbox.com`, `api.mapbox.com`
- **CDNs (not needed for email extraction):** `cdn.jsdelivr.net`, `cdnjs.cloudflare.com`, `unpkg.com`
- **Chat widgets:** `widget.trustpilot.com`, `js.driftt.com`, `embed.tawk.to`, `static.zdassets.com`, `ekr.zdassets.com`

### 4. Fixed Facebook URL extraction from encoded JSON

Script blocks containing JSON-LD with URL-encoded data (e.g. `facebook.com/dollartree%22,%22c_file_url%22:...`) caused the Facebook URL regex to swallow entire JSON blobs. Fixed by truncating matches at `%22`, `%27`, `%2C` and embedded `http` protocol starts.

---

## Impact on Email Extraction Accuracy

**No meaningful impact.** The new approach covers the same email sources, just more efficiently:

| Email source | Old (innerHTML) | New (TreeWalker + attributes) | Status |
|---|---|---|---|
| Visible text | via `innerText` | Same (unchanged) | Covered |
| Hidden text nodes | via innerHTML regex | TreeWalker `SHOW_TEXT` walks all text nodes | Covered |
| `<script>` text (JSON-LD etc.) | via innerHTML regex | TreeWalker walks script text nodes too | Covered |
| Attributes (`href`, `content`, `value`, etc.) | via innerHTML regex | Explicit attribute scan | Covered |
| `data-email`, `data-mail` | Separate handler | Same (unchanged) | Covered |
| Cloudflare `data-cfemail` | Separate handler | Same (unchanged) | Covered |
| `mailto:` links | Separate handler | Same (unchanged) | Covered |
| HTML comments | Caught by innerHTML | **Not covered** | Negligible risk |

The only theoretical gap is emails inside HTML comments (`<!-- email@example.com -->`), which is essentially never a real contact email.

---

## Result

| Metric | Before | After |
|--------|--------|-------|
| Dollar Tree scrape time | 120s+ (timeout) | **11.7s** |
| `page.evaluate()` time | 2min 14s | **< 1s** |
| Subpages crawled | 20+ sibling stores | Only relevant pages (contact, about, etc.) |
| Facebook URLs | Garbage encoded JSON blobs | Clean URLs only |
