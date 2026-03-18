import 'dotenv/config';
import cluster from 'cluster';
import os from 'os';
import express from 'express';
import cors from 'cors';
import { chromium } from 'playwright';

const PORT = process.env.PORT || 3000;
const NUM_WORKERS = parseInt(process.env.NUM_WORKERS, 10) || (os.cpus().length || 10);

// Create Express app
const app = express();

// Configuration
const MAX_DEPTH = Math.max(1, parseInt(process.env.MAX_DEPTH, 10) || 2);
const parsedSubpageConcurrency = parseInt(process.env.SUBPAGE_CONCURRENCY, 10);
const SUBPAGE_CONCURRENCY = Math.max(
  1,
  Number.isFinite(parsedSubpageConcurrency) ? parsedSubpageConcurrency : 10
); // Max secondary links in parallel
const parsedPlaywrightContexts = parseInt(process.env.PLAYWRIGHT_MAX_CONTEXTS, 10);
const PLAYWRIGHT_MAX_CONTEXTS = Math.max(
  1,
  Number.isFinite(parsedPlaywrightContexts) ? parsedPlaywrightContexts : 10
);
const rawScrapeDelayMin = parseInt(process.env.SCRAPE_DELAY_MIN_MS, 10);
const rawScrapeDelayMax = parseInt(process.env.SCRAPE_DELAY_MAX_MS, 10);
const SCRAPE_DELAY_MIN_MS = Math.max(0, Number.isFinite(rawScrapeDelayMin) ? rawScrapeDelayMin : 0);
const SCRAPE_DELAY_MAX_MS = Math.max(
  SCRAPE_DELAY_MIN_MS,
  Number.isFinite(rawScrapeDelayMax) ? rawScrapeDelayMax : SCRAPE_DELAY_MIN_MS
);
const MAX_LINKS_PER_PAGE = Math.max(1, parseInt(process.env.MAX_LINKS_PER_PAGE, 10) || 50);
const PAGE_NAVIGATION_TIMEOUT_MS = Math.max(5000, parseInt(process.env.PAGE_NAVIGATION_TIMEOUT_MS, 10) || 60000);
const MAX_STORED_VISITED_URLS = Math.max(1, parseInt(process.env.MAX_STORED_VISITED_URLS, 10) || 200);
const MAX_SUBPAGE_CRAWLS = Math.max(1, parseInt(process.env.MAX_SUBPAGE_CRAWLS, 10) || 20);
const PLAYWRIGHT_BLOCKED_RESOURCE_TYPES = new Set([
  'image', 'media', 'font',
]);

const USER_AGENTS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:118.0) Gecko/20100101 Firefox/118.0'
];

const ACCEPT_LANGUAGES = [
  'en-US,en;q=0.9',
  'en-GB,en;q=0.9',
  'en-US,en;q=0.8,fr;q=0.6',
  'en-US,en;q=0.8,es;q=0.6'
];

const LOCALES = ['en-US', 'en-GB', 'en-CA', 'en-AU'];

const VIEWPORTS = [
  { width: 1920, height: 1080 },
  { width: 1536, height: 864 },
  { width: 1440, height: 900 },
  { width: 1366, height: 768 }
];

const REFERERS = [
  'https://www.google.com/',
  'https://www.bing.com/',
  'https://duckduckgo.com/',
  'https://search.yahoo.com/'
];

const proxyPool = (process.env.PROXY_URLS || '')
  .split(',')
  .map((value) => value.trim())
  .filter(Boolean);

let identityCursor = 0;
let proxyCursor = 0;

function getNextIdentity() {
  const fallbackViewport = { width: 1366, height: 768 };
  const index = identityCursor++;

  return {
    userAgent: USER_AGENTS[index % USER_AGENTS.length] || USER_AGENTS[0],
    acceptLanguage: ACCEPT_LANGUAGES[index % ACCEPT_LANGUAGES.length] || 'en-US,en;q=0.9',
    locale: LOCALES[index % LOCALES.length] || 'en-US',
    viewport: VIEWPORTS[index % VIEWPORTS.length] || fallbackViewport,
    referer: REFERERS[index % REFERERS.length] || 'https://www.google.com/'
  };
}

function getNextProxyUrl() {
  if (!proxyPool.length) {
    return null;
  }

  const proxyUrl = proxyPool[proxyCursor % proxyPool.length];
  proxyCursor = (proxyCursor + 1) % proxyPool.length;
  return proxyUrl;
}

const COMMON_PAGE_PATHS = [
  '/about/',
  '/contact/',
  '/about-us/',
  '/contact-us/',
  '/privacy/',
  '/terms',
];

// Shared Playwright browser management for faster fallbacks
const PLAYWRIGHT_LAUNCH_ARGS = [
  '--disable-dev-shm-usage',
  '--disable-gpu',
  '--no-zygote',
  '--no-sandbox',
  '--disable-extensions',
  '--disable-background-networking',
  '--disable-default-apps',
  '--disable-sync',
  '--disable-translate',
  '--hide-scrollbars',
  '--mute-audio',
  '--metrics-recording-only',
  '--disable-renderer-backgrounding',
  '--disable-component-update',
  '--disable-breakpad',
  '--disable-features=TranslateUI',
  '--disable-ipc-flooding-protection',
];

let sharedBrowserInstance = null;
let sharedBrowserPromise = null;

async function getSharedBrowser() {
  if (sharedBrowserInstance) {
    return sharedBrowserInstance;
  }

  if (!sharedBrowserPromise) {
    sharedBrowserPromise = chromium.launch({
      headless: true,
      args: PLAYWRIGHT_LAUNCH_ARGS,
    });
  }

  try {
    sharedBrowserInstance = await sharedBrowserPromise;
    sharedBrowserInstance.once('disconnected', () => {
      sharedBrowserInstance = null;
      sharedBrowserPromise = null;
    });
    return sharedBrowserInstance;
  } catch (error) {
    sharedBrowserPromise = null;
    throw error;
  }
}

async function resetSharedBrowser() {
  if (sharedBrowserInstance) {
    try {
      await sharedBrowserInstance.close();
    } catch (error) {
      console.error('[Playwright] Error closing shared browser:', error.message);
    }
  }
  sharedBrowserInstance = null;
  sharedBrowserPromise = null;
}

async function closeSharedBrowser() {
  if (sharedBrowserPromise || sharedBrowserInstance) {
    await resetSharedBrowser();
  }
}


// Middleware
app.use(cors());
app.use(express.json());

// =========================================================================
// EMAIL & URL EXTRACTION FUNCTIONS (Kept as is - DO NOT CHANGE)
// =========================================================================

// Email extraction function - works with HTML content
function extractEmails(html) {
  const emails = [];
  
  // Extract emails from mailto: href attributes
  const mailtoRegex = /href=["']mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})["']/gi;
  const mailtoMatches = html.matchAll(mailtoRegex);
  for (const match of mailtoMatches) {
    emails.push(match[1]);
  }
  
  // Extract emails wrapped in HTML tags (like font, b, span, etc.)
  const htmlEmailRegex = /<[^>]*>([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})<\/[^>]*>/gi;
  const htmlEmailMatches = html.matchAll(htmlEmailRegex);
  for (const match of htmlEmailMatches) {
    emails.push(match[1]);
  }
  
  // Extract emails from plain text (after removing HTML tags)
  const textContent = html.replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
  const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
  const textEmails = textContent.match(emailRegex) || [];
  emails.push(...textEmails);
  
  return [...new Set(emails)]; // Remove duplicates
}

// Email validation – filters false positives from regex matches
const INVALID_EMAIL_TLDS = new Set([
  'png', 'jpg', 'jpeg', 'gif', 'svg', 'webp', 'ico', 'bmp',
  'css', 'js', 'map', 'json', 'xml', 'woff', 'woff2', 'ttf', 'eot',
  'mp3', 'mp4', 'wav', 'avi', 'mov', 'pdf', 'zip', 'gz',
]);

function isValidEmail(email) {
  if (!email || typeof email !== 'string') return false;
  const e = email.toLowerCase().trim();
  if (e.length < 6 || e.length > 254) return false;

  const atIdx = e.indexOf('@');
  if (atIdx < 1 || atIdx !== e.lastIndexOf('@')) return false;

  const local = e.slice(0, atIdx);
  const domain = e.slice(atIdx + 1);
  if (!local || local.length > 64 || !domain) return false;
  if (!domain.includes('.')) return false;

  const tld = domain.split('.').pop();
  if (!tld || tld.length < 2) return false;
  if (INVALID_EMAIL_TLDS.has(tld)) return false;

  // reject leading/trailing dots or hyphens, consecutive dots
  if (/^[.\-_+]|[.\-_+]$/.test(local)) return false;
  if (/\.{2,}/.test(local) || /\.{2,}/.test(domain)) return false;

  if (!/^[a-z0-9._%+\-]+$/.test(local)) return false;
  if (!/^[a-z0-9.\-]+\.[a-z]{2,}$/.test(domain)) return false;

  return true;
}

// Facebook URL extraction function - improved to handle escaped/encoded links (including script blocks)
function extractFacebookUrls(text) {
  if (typeof text !== 'string' || text.trim().length === 0) {
    return [];
  }

  const candidates = new Set();
  const allowedShortSegments = new Set(['p', 'sharer.php', 'share.php']);

  const patterns = [
    /https?:\/\/(?:www\.)?(?:facebook\.com|fb\.com)[^\s"'<>\\)]+/gi,
    /https?:\\\/\\\/(?:www\.)?(?:facebook\.com|fb\.com)[^"'<>\\)]+/gi,
    /https?%3A%2F%2F(?:www\.)?(?:facebook\.com|fb\.com)[^"'<>\\)]+/gi
  ];

  patterns.forEach(pattern => {
    let match;
    while ((match = pattern.exec(text)) !== null) {
      candidates.add(match[0]);
    }
  });

  const barePattern = /(?:facebook\.com|fb\.com)\/[^\s"'<>\\)]+/gi;
  let bareMatch;
  while ((bareMatch = barePattern.exec(text)) !== null) {
    candidates.add(`https://${bareMatch[0]}`);
  }

  const results = new Set();
  for (const candidate of candidates) {
    const normalized = normalizeFacebookCandidate(candidate);
    if (normalized) {
      results.add(normalized);
    }
  }

  return Array.from(results);

  function normalizeFacebookCandidate(rawValue, depth = 0) {
    if (!rawValue || typeof rawValue !== 'string') {
      return null;
    }
    if (depth > 3) {
      return null;
    }

    let value = rawValue.trim();
    if (!value) {
      return null;
    }

    value = value
      .replace(/\\u0026/gi, '&')
      .replace(/\\u002F/gi, '/')
      .replace(/\\u003A/gi, ':')
      .replace(/\\x2F/gi, '/')
      .replace(/\\x3A/gi, ':')
      .replace(/\\\//g, '/')
      .replace(/\\\\/g, '\\')
      .replace(/&amp;/gi, '&')
      .replace(/^['"`]+|['"`]+$/g, '');

    if (/^https?%3A%2F%2F/i.test(value) || value.includes('%2F') || value.includes('%3A')) {
      try {
        value = decodeURIComponent(value);
      } catch {
        // Ignore malformed URI components
      }
    }

    if (!/^https?:\/\//i.test(value)) {
      if (value.startsWith('//')) {
        value = `https:${value}`;
      } else if (/^(?:www\.)?(facebook\.com|fb\.com)/i.test(value)) {
        value = `https://${value.replace(/^https?:\\\/\\\//i, '')}`;
      }
    }

    value = value.replace(/\/+$/, '');

    let urlObj;
    try {
      urlObj = new URL(value);
    } catch {
      return null;
    }

    const hostname = urlObj.hostname.toLowerCase();
    const allowedDomains = ['facebook.com', 'fb.com'];
    const isAllowedHost = allowedDomains.some(domain => hostname === domain || hostname.endsWith(`.${domain}`));
    if (!isAllowedHost) {
      return null;
    }

    if (hostname.endsWith('facebook.com') && urlObj.pathname === '/l.php') {
      const forwarded = urlObj.searchParams.get('u') || urlObj.searchParams.get('href');
      if (forwarded) {
        return normalizeFacebookCandidate(forwarded, depth + 1);
      }
    }

    const trackingParams = [
      'fbclid',
      'utm_source',
      'utm_medium',
      'utm_campaign',
      'utm_term',
      'utm_content',
      'mibextid',
      'ref',
      'refid'
    ];
    trackingParams.forEach(param => urlObj.searchParams.delete(param));
    urlObj.hash = '';
    const searchString = urlObj.searchParams.toString();
    urlObj.search = searchString ? `?${searchString}` : '';

    const firstPathSegment = urlObj.pathname.split('/').filter(Boolean)[0] || '';
    if (
      firstPathSegment.length > 0 &&
      firstPathSegment.length < 2 &&
      !allowedShortSegments.has(firstPathSegment.toLowerCase())
    ) {
      return null;
    }

    return urlObj.toString();
  }
}

// URL cleaning function to remove hash fragments
function cleanUrl(url) {
  try {
    const urlObj = new URL(url);
    urlObj.hash = ''; // Remove the hash fragment
    return urlObj.href;
  } catch (error) {
    // If URL parsing fails, try simple string replacement
    return url.split('#')[0];
  }
}

function generateCommonPageVariants(pagePath) {
  const variants = new Set();
  if (typeof pagePath !== 'string') {
    return variants;
  }

  const trimmed = pagePath.trim();
  if (!trimmed) {
    return variants;
  }

  const ensureTrailingSlash = (value) =>
    value && !value.endsWith('/') ? `${value}/` : value;

  const withoutLeadingSlash = trimmed.replace(/^\/+/, '');

  variants.add(trimmed);
  variants.add(ensureTrailingSlash(trimmed));

  if (withoutLeadingSlash) {
    variants.add(withoutLeadingSlash);
    variants.add(ensureTrailingSlash(withoutLeadingSlash));

    const withLeadingSlash = `/${withoutLeadingSlash}`;
    variants.add(withLeadingSlash);
    variants.add(ensureTrailingSlash(withLeadingSlash));
  }

  return variants;
}

// File extensions to exclude from crawling (PDFs, images, documents, etc.)
const EXCLUDED_FILE_EXTENSIONS = new Set([
  'pdf', 'jpg', 'jpeg', 'png', 'gif', 'webp', 'svg', 'ico', 'bmp', 'tiff', 'tif',
  'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx', 'zip', 'rar', 'tar', 'gz', '7z',
  'mp3', 'mp4', 'wav', 'avi', 'mov', 'webm', 'flv', 'wmv', 'm4a', 'ogg'
]);

function isNonHtmlResource(url) {
  try {
    const pathname = new URL(url).pathname.toLowerCase();
    const ext = pathname.split('.').pop()?.split(/[?#]/)[0] || '';
    return ext && EXCLUDED_FILE_EXTENSIONS.has(ext);
  } catch {
    return false;
  }
}

function createSameDomainLinkCollector(baseUrlHref) {
  const baseUrl = new URL(baseUrlHref);
  const normalizedCurrentUrl = cleanUrl(baseUrl.href);
  const prioritizedLinks = [];
  const seenLinks = new Set();
  const canAddMore = () => prioritizedLinks.length < MAX_LINKS_PER_PAGE;

  const addCandidateLink = (candidate) => {
    if (!candidate || !canAddMore()) {
      return;
    }
    if (isNonHtmlResource(candidate)) {
      return;
    }
    try {
      const linkUrl = new URL(candidate, baseUrl.href);
      if (linkUrl.origin !== baseUrl.origin) {
        return;
      }
      const finalUrl = cleanUrl(linkUrl.href);
      if (finalUrl === normalizedCurrentUrl || seenLinks.has(finalUrl)) {
        return;
      }
      seenLinks.add(finalUrl);
      prioritizedLinks.push(finalUrl);
    } catch {
      // Skip invalid URLs
    }
  };

  const addCommonPages = () => {
    if (!canAddMore()) {
      return;
    }
    for (const pagePath of COMMON_PAGE_PATHS) {
      if (!canAddMore()) {
        break;
      }
      const variants = generateCommonPageVariants(pagePath);
      for (const variant of variants) {
        if (!canAddMore()) {
          break;
        }
        addCandidateLink(variant);
      }
    }
  };

  return {
    addCandidateLink,
    addCommonPages,
    getLinks: () => prioritizedLinks,
  };
}

/**
 * Utility to run async tasks with a fixed concurrency limit.
 * @param {Array<any>} items
 * @param {number} limit
 * @param {(item: any, index: number) => Promise<void>} iteratee
 */
async function runWithConcurrency(items, limit, iteratee) {
  if (!Array.isArray(items) || items.length === 0) {
    return;
  }
  
  const normalizedLimit = Math.max(1, Number.isFinite(limit) ? limit : 1);
  let currentIndex = 0;
  
  const worker = async () => {
    while (currentIndex < items.length) {
      const index = currentIndex++;
      await iteratee(items[index], index);
    }
  };
  
  const workers = Array.from(
    { length: Math.min(normalizedLimit, items.length) },
    () => worker()
  );
  
  await Promise.all(workers);
}

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** Run a promise with a hard timeout – resolves to undefined on timeout instead of hanging. */
const withTimeout = (promise, ms) =>
  Promise.race([promise, delay(ms).then(() => undefined)]);

class AsyncSemaphore {
  constructor(limit) {
    this.limit = Math.max(1, Number.isFinite(limit) ? limit : 1);
    this.active = 0;
    this.queue = [];
  }

  async acquire() {
    if (this.active < this.limit) {
      this.active++;
      return;
    }

    await new Promise(resolve => this.queue.push(resolve));
    this.active++;
  }

  release() {
    if (this.active > 0) {
      this.active--;
    }

    if (this.queue.length > 0 && this.active < this.limit) {
      const resolve = this.queue.shift();
      resolve();
    }
  }
}

const playwrightContextSemaphore = new AsyncSemaphore(PLAYWRIGHT_MAX_CONTEXTS);


// =========================================================================
// Playwright-Only Core Scraping
// =========================================================================

/**
 * Scrapes a single URL using an already-open Playwright context.
 * Opens a page, extracts data in-browser, closes the page.
 * No full HTML is transferred back -- all extraction runs in the browser.
 */
async function scrapeUrl(url, depth, visitedUrls, context) {
  const result = { emails: [], facebookUrls: [], newUrls: [] };

  if (visitedUrls.has(url)) return result;
  visitedUrls.add(url);

  if (SCRAPE_DELAY_MAX_MS > 0) {
    const ms = SCRAPE_DELAY_MIN_MS + Math.random() * (SCRAPE_DELAY_MAX_MS - SCRAPE_DELAY_MIN_MS);
    if (ms > 0) await delay(ms);
  }

  let page;
  try {
    page = await context.newPage();
    // Cancel any downloads (e.g. when server sends Content-Disposition: attachment)
    page.on('download', (download) => download.cancel().catch(() => {}));
    await page.goto(url, { waitUntil: 'load', timeout: PAGE_NAVIGATION_TIMEOUT_MS });
    await page.waitForLoadState('networkidle', { timeout: 15000 }).catch(() => {});

    const runPageEvaluate = () => page.evaluate((candidateLimit) => {
      const toAbsolute = (href) => {
        try { return new URL(href, window.location.href).href; } catch { return null; }
      };

      const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
      const emailSet = new Set();
      const fbRaw = [];
      const candidateSet = new Set();

      const decodeEmail = (raw) => {
        try { return decodeURIComponent(raw).toLowerCase().trim(); } catch { return raw.toLowerCase().trim(); }
      };

      document.querySelectorAll('a[href]').forEach((a) => {
        let href = a.getAttribute('href') || '';
        try { href = decodeURIComponent(href); } catch {}
        if (href.toLowerCase().startsWith('mailto:')) {
          const email = decodeEmail(href.slice(7).split('?')[0]);
          if (email) emailSet.add(email);
        }
      });

      document.querySelectorAll('a[href]').forEach((a) => {
        const raw = a.getAttribute('href') || '';
        if (!raw || raw.startsWith('#') || raw.toLowerCase().startsWith('javascript:')) return;
        const abs = toAbsolute(raw);
        if (!abs) return;
        if (candidateSet.size < candidateLimit) candidateSet.add(abs);
        const lower = abs.toLowerCase();
        if (lower.includes('facebook.com') || lower.includes('fb.com/')) fbRaw.push(abs);
      });

      // Emails from visible body text
      const bodyText = document.body ? document.body.innerText || '' : '';
      if (bodyText) {
        const found = bodyText.match(emailRegex);
        if (found) found.forEach((e) => emailSet.add(decodeEmail(e)));
      }

      // Emails from full HTML (hidden elements, attributes, comments)
      const fullHtml = document.documentElement.innerHTML || '';
      const htmlEmails = fullHtml.match(emailRegex);
      if (htmlEmails) htmlEmails.forEach((e) => emailSet.add(decodeEmail(e)));

      // Emails from data-* attributes commonly used for obfuscation
      document.querySelectorAll('[data-email], [data-mail], [data-cfemail]').forEach((el) => {
        for (const attr of ['data-email', 'data-mail', 'data-cfemail']) {
          const val = el.getAttribute(attr);
          if (val) {
            const m = val.match(emailRegex);
            if (m) m.forEach((e) => emailSet.add(decodeEmail(e)));
          }
        }
      });

      // Emails from meta tags
      document.querySelectorAll('meta[content]').forEach((meta) => {
        const content = meta.getAttribute('content') || '';
        const found = content.match(emailRegex);
        if (found) found.forEach((e) => emailSet.add(decodeEmail(e)));
      });

      // Emails from structured data (JSON-LD)
      document.querySelectorAll('script[type="application/ld+json"]').forEach((s) => {
        try {
          const text = s.textContent || '';
          const found = text.match(emailRegex);
          if (found) found.forEach((e) => emailSet.add(decodeEmail(e)));
        } catch {}
      });

      // Facebook URLs from body text
      if (bodyText) {
        const fbText = bodyText.match(/https?:\/\/(?:www\.)?(?:facebook\.com|fb\.com)[^\s"'<>]+/gi);
        if (fbText) fbRaw.push(...fbText);
      }

      // Facebook URLs from script blocks
      document.querySelectorAll('script').forEach((s) => {
        const c = s.textContent;
        if (!c || !/facebook\.com|fb\.com/i.test(c)) return;
        const m = c.match(/https?(?:[:\\/]{1,5}|%3A%2F%2F)(?:www\.)?(?:facebook\.com|fb\.com)[^\s"'<>\\)]{1,500}/gi);
        if (m) fbRaw.push(...m);
      });

      return {
        emails: Array.from(emailSet),
        fbRaw,
        candidateLinks: Array.from(candidateSet).slice(0, candidateLimit),
      };
    }, MAX_LINKS_PER_PAGE);

    let evalResult = await runPageEvaluate();

    // If no emails found, the page might still be JS-rendering — short retry once
    if (!evalResult?.emails?.length) {
      await delay(1500);
      evalResult = await runPageEvaluate();
    }

    const pageEmails = (evalResult?.emails || []).filter(isValidEmail);
    const fbRaw = evalResult?.fbRaw || [];
    const candidateLinks = evalResult?.candidateLinks || [];

    const normalizedFacebook = fbRaw.length > 0 ? extractFacebookUrls(fbRaw.join('\n')) : [];

    if (pageEmails.length > 0) result.emails.push(...pageEmails);
    if (normalizedFacebook.length > 0) result.facebookUrls.push(...normalizedFacebook);

    if (depth < MAX_DEPTH && candidateLinks.length > 0) {
      const linkCollector = createSameDomainLinkCollector(url);
      linkCollector.addCommonPages();
      for (const link of candidateLinks) linkCollector.addCandidateLink(link);
      const collected = linkCollector.getLinks();
      if (collected.length > 0) result.newUrls.push(...collected);
    }

    console.log(`[Playwright] ${url} → ${pageEmails.length} emails, ${candidateLinks.length} links`);
  } catch (error) {
    console.error(`[Playwright Error] ${url}: ${error.message}`);
    throw error;
  } finally {
    if (page) try { await withTimeout(page.close(), 5000); } catch { /* ignore */ }
  }

  return result;
}


// =========================================================================
// Website-level orchestrator -- one context per website, reused for subpages
// =========================================================================
async function scrapeWebsite(url) {
  console.log(`Starting scrape for URL: ${url}`);

  const uniqueEmails = new Set();
  const uniqueFacebookUrls = new Set();
  const visitedUrls = new Set();
  let context;

  await playwrightContextSemaphore.acquire();
  try {
    const browser = await getSharedBrowser();
    const identity = getNextIdentity();
    const proxy = getNextProxyUrl();

    context = await browser.newContext({
      userAgent: identity.userAgent,
      locale: identity.locale,
      viewport: identity.viewport,
      ...(proxy ? { proxy: { server: proxy } } : {}),
    });

    await context.setExtraHTTPHeaders({
      'Accept-Language': identity.acceptLanguage,
      ...(identity.referer ? { Referer: identity.referer } : {}),
    });

    await context.route('**/*', async (route) => {
      const req = route.request();
      if (PLAYWRIGHT_BLOCKED_RESOURCE_TYPES.has(req.resourceType())) {
        return route.abort().catch(() => {});
      }
      if (isNonHtmlResource(req.url())) {
        return route.abort().catch(() => {});
      }
      return route.continue().catch(() => {});
    });

    const primaryResult = await scrapeUrl(url, 0, visitedUrls, context);
    primaryResult.emails.forEach((e) => uniqueEmails.add(e));
    primaryResult.facebookUrls.forEach((f) => uniqueFacebookUrls.add(f));

    if (MAX_DEPTH > 1) {
      const baseOrigin = new URL(url).origin;
      const subpageLimit = Math.min(MAX_SUBPAGE_CRAWLS, MAX_LINKS_PER_PAGE);
      const candidateLinks = (primaryResult.newUrls || [])
        .filter((link) => {
          try { return new URL(link).origin === baseOrigin && !visitedUrls.has(link); }
          catch { return false; }
        })
        .slice(0, subpageLimit);

      await runWithConcurrency(candidateLinks, SUBPAGE_CONCURRENCY, async (link) => {
        try {
          const sub = await scrapeUrl(link, 1, visitedUrls, context);
          sub.emails.forEach((e) => uniqueEmails.add(e));
          sub.facebookUrls.forEach((f) => uniqueFacebookUrls.add(f));
        } catch (e) {
          console.error(`Error scraping ${link}: ${e?.message || e}`);
        }
      });
    }

    const finalEmails = Array.from(uniqueEmails);
    const finalFacebookUrls = Array.from(uniqueFacebookUrls);
    console.log(`Completed scrape: ${finalEmails.length} emails, ${finalFacebookUrls.length} Facebook URLs`);

    return {
      success: true,
      emails: finalEmails,
      facebook_urls: finalFacebookUrls,
      crawled_urls: Array.from(visitedUrls).slice(0, MAX_STORED_VISITED_URLS),
      pages_crawled: visitedUrls.size,
    };
  } catch (error) {
    const browser = sharedBrowserInstance;
    if (browser && !browser.isConnected()) await resetSharedBrowser();
    // console.error(`Scrape failed for ${url}:`, error);
    throw error;
  } finally {
    if (context) try { await withTimeout(context.close(), 10000); } catch { /* ignore */ }
    playwrightContextSemaphore.release();
  }
}

// =========================================================================
// ENDPOINTS
// =========================================================================

// Direct /extract-emails endpoint - scrapes immediately and returns results
app.post('/extract-emails', async (req, res) => {
  try {
    const { url } = req.body;
    
    if (!url) {
      return res.status(400).json({ 
        error: 'URL is required',
        message: 'Please provide a URL in the request body'
      });
    }

    // Validate URL format
    try {
      new URL(url);
    } catch (error) {
      return res.status(400).json({ 
        error: 'Invalid URL format',
        message: 'Please provide a valid URL'
      });
    }

    // Reject PDF, image, and other non-HTML resource URLs
    if (isNonHtmlResource(url)) {
      return res.status(400).json({ 
        error: 'Unsupported URL type',
        message: 'PDF, image, and document URLs are not supported. Please provide a webpage URL.'
      });
    }

    // Scrape directly and return results
    const result = await scrapeWebsite(url);
    res.json(result);

  } catch (error) {
    console.error('Error scraping:', error);
    res.status(500).json({
      success: false,
      error: 'Internal server error',
      message: error.message || 'Failed to scrape the website'
    });
  }
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ 
    status: 'OK', 
    message: 'Email extraction API is running'
  });
});

// Root endpoint
app.get('/', (req, res) => {
  res.json({
    message: 'Email and Facebook URL Extraction API',
    endpoints: {
      'POST /extract-emails': 'Extract emails and Facebook URLs from a website (direct scraping)',
      'GET /health': 'Health check'
    },
    usage: {
      method: 'POST',
      url: '/extract-emails',
      body: { url: 'https://example.com' }
    },
    features: [
      'Direct scraping - returns results immediately',
      'Extract email addresses',
      'Extract Facebook URLs',
      'Crawl multiple pages within same domain',
      'Full JavaScript rendering via Playwright',
      'Playwright-only architecture - no static fallbacks',
      'Clustered architecture for high concurrency (10k+ requests)'
    ],
    clustering: {
      workers: NUM_WORKERS,
      note: 'Running in clustered mode for optimal performance'
    }
  });
});

// Initialize and start the server
async function startServer() {
  try {
    app.listen(PORT, () => {
      console.log(`[Worker ${process.pid}] Email extraction API running on port ${PORT}`);
      console.log(`[Worker ${process.pid}] Visit http://localhost:${PORT} for API documentation`);
    });
    
  } catch (error) {
    console.error(`[Worker ${process.pid}] Failed to start server:`, error);
    await closeSharedBrowser();
    process.exit(1);
  }
}

// =========================================================================
// CLUSTERING SETUP
// =========================================================================

if (cluster.isPrimary) {
  // Master process - spawn workers
  console.log(`[Master ${process.pid}] Starting ${NUM_WORKERS} workers...`);
  
  // Spawn workers
  for (let i = 0; i < NUM_WORKERS; i++) {
    const worker = cluster.fork();
    console.log(`[Master] Spawned worker ${worker.process.pid}`);
  }
  
  // Handle worker exit - restart if crashed
  cluster.on('exit', (worker, code, signal) => {
    console.log(`[Master] Worker ${worker.process.pid} died (code: ${code}, signal: ${signal}). Restarting...`);
    const newWorker = cluster.fork();
    console.log(`[Master] Spawned new worker ${newWorker.process.pid}`);
  });
  
  // Handle worker online
  cluster.on('online', (worker) => {
    console.log(`[Master] Worker ${worker.process.pid} is online`);
  });
  
  // Graceful shutdown for master
  const shutdown = async () => {
    console.log('[Master] Shutting down gracefully...');
    console.log('[Master] Closing all workers...');

    // Disconnect all workers
    for (const id in cluster.workers) {
      const worker = cluster.workers[id];
      if (worker) {
        worker.disconnect();
      }
    }

    // After 5s, kill any workers still alive, then exit
    setTimeout(() => {
      for (const id in cluster.workers) {
        const worker = cluster.workers[id];
        if (worker && !worker.isDead()) {
          console.log(`[Master] Force killing worker ${worker.process.pid}`);
          worker.process.kill('SIGKILL');
        }
      }
      setTimeout(() => {
        console.log('[Master] Force exiting...');
        process.exit(0);
      }, 1000);
    }, 5000);
  };
  
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
  
} else {
  // Worker process - start the server
  startServer();
  
  // Graceful shutdown for workers
  const shutdown = async () => {
    console.log(`[Worker ${process.pid}] Shutting down gracefully...`);
    await withTimeout(closeSharedBrowser(), 8000);
    process.exit(0);
  };
  
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
  
  // Handle disconnect from master
  process.on('disconnect', async () => {
    console.log(`[Worker ${process.pid}] Disconnected from master, closing browser...`);
    await withTimeout(closeSharedBrowser(), 8000);
    process.exit(0);
  });
}
