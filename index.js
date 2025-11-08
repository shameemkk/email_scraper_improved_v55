import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import { chromium } from 'playwright'; // Import Playwright directly
import * as cheerio from 'cheerio'; // Import Cheerio
import axios from 'axios'; // Import Axios for light HTTP requests

const app = express();
const PORT = process.env.PORT || 3000;

// Configuration
const MAX_DEPTH = Math.max(1, parseInt(process.env.MAX_DEPTH, 10) || 2);
const parsedSubpageConcurrency = parseInt(process.env.SUBPAGE_CONCURRENCY, 10);
const SUBPAGE_CONCURRENCY = Math.max(
  1,
  Number.isFinite(parsedSubpageConcurrency) ? parsedSubpageConcurrency : 4
); // Max secondary links in parallel
const parsedPlaywrightContexts = parseInt(process.env.PLAYWRIGHT_MAX_CONTEXTS, 10);
const PLAYWRIGHT_MAX_CONTEXTS = Math.max(
  1,
  Number.isFinite(parsedPlaywrightContexts) ? parsedPlaywrightContexts : 6
);
const rawScrapeDelayMin = parseInt(process.env.SCRAPE_DELAY_MIN_MS, 10);
const rawScrapeDelayMax = parseInt(process.env.SCRAPE_DELAY_MAX_MS, 10);
const SCRAPE_DELAY_MIN_MS = Math.max(0, Number.isFinite(rawScrapeDelayMin) ? rawScrapeDelayMin : 0);
const SCRAPE_DELAY_MAX_MS = Math.max(
  SCRAPE_DELAY_MIN_MS,
  Number.isFinite(rawScrapeDelayMax) ? rawScrapeDelayMax : Math.max(SCRAPE_DELAY_MIN_MS, 100)
);
const REQUEST_TIMEOUT_MS = 5000; // 5 seconds for initial HTTP request
const HTTP_MAX_RETRIES = Math.max(0, parseInt(process.env.HTTP_MAX_RETRIES, 10) || 2);
const RETRY_BACKOFF_BASE_MS = Math.max(100, parseInt(process.env.RETRY_BACKOFF_BASE_MS, 10) || 500);
const MAX_LINKS_PER_PAGE = Math.max(1, parseInt(process.env.MAX_LINKS_PER_PAGE, 10) || 50);
const MAX_STORED_VISITED_URLS = Math.max(1, parseInt(process.env.MAX_STORED_VISITED_URLS, 10) || 200);
const MAX_SUBPAGE_CRAWLS = Math.max(1, parseInt(process.env.MAX_SUBPAGE_CRAWLS, 10) || 20);
const PLAYWRIGHT_BLOCKED_RESOURCE_TYPES = new Set(['image', 'media', 'font', 'stylesheet']);

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

function buildAxiosProxyConfig(proxyUrl) {
  if (!proxyUrl) {
    return undefined;
  }

  try {
    const parsed = new URL(proxyUrl);
    const auth = parsed.username
      ? {
          username: decodeURIComponent(parsed.username),
          password: decodeURIComponent(parsed.password || '')
        }
      : undefined;

    const protocol = parsed.protocol.replace(':', '');
    const port = parsed.port
      ? parseInt(parsed.port, 10)
      : protocol === 'https' ? 443 : 80;

    return {
      protocol,
      host: parsed.hostname,
      port,
      auth
    };
  } catch (error) {
    console.warn(`[Proxy] Failed to parse proxy URL "${proxyUrl}":`, error?.message || error);
    return undefined;
  }
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
// NEW: Playwright/Cheerio Core Functions
// =========================================================================

/**
 * Scrapes a single URL using the Cheerio-first, Playwright-fallback strategy.
 * @param {string} url - The URL to scrape.
 * @param {number} depth - The current crawl depth (for recursive calls).
 * @param {Set<string>} visitedUrls - Set of URLs already visited in this job.
 * @returns {Promise<{emails: string[], facebookUrls: string[], newUrls: string[]}>}
 */
async function scrapeUrl(url, depth, visitedUrls) {
  const result = {
    emails: [],
    facebookUrls: [],
    newUrls: [],
  };

  if (visitedUrls.has(url)) {
    return result;
  }
  visitedUrls.add(url);

  // Apply minimal delay only if configured (default is 0 for speed)
  if (SCRAPE_DELAY_MAX_MS > 0) {
    const scrapeDelay = SCRAPE_DELAY_MIN_MS + Math.random() * (SCRAPE_DELAY_MAX_MS - SCRAPE_DELAY_MIN_MS);
    if (scrapeDelay > 0) {
      await delay(scrapeDelay);
    }
  }

  let htmlContent = '';
  let isJsRendered = false;
  let activeIdentity = getNextIdentity();
  let activeProxy = getNextProxyUrl();

  // --- 1. Cheerio-First (Light/Fast HTTP Request) ---
  try {
    let response = null;
    let fetchError = null;

    for (let attempt = 0; attempt <= HTTP_MAX_RETRIES; attempt++) {
      const attemptIdentity = attempt === 0 ? activeIdentity : getNextIdentity();
      const attemptProxy = attempt === 0 ? activeProxy : (getNextProxyUrl() || activeProxy);
      const axiosProxyConfig = buildAxiosProxyConfig(attemptProxy);

      try {
        response = await axios.get(url, {
          timeout: REQUEST_TIMEOUT_MS,
          validateStatus: (status) => status >= 200 && status < 400, 
          headers: {
            'User-Agent': attemptIdentity.userAgent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': attemptIdentity.acceptLanguage,
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0',
            ...(attemptIdentity.referer ? { Referer: attemptIdentity.referer } : {})
          },
          maxRedirects: 5,
          decompress: true, // Automatically decompress gzipped responses
          maxContentLength: 512 * 1024,
          maxBodyLength: 512 * 1024,
          responseType: 'text',
          transformResponse: [(data) => data],
          ...(axiosProxyConfig ? { proxy: axiosProxyConfig } : {})
        });

        activeIdentity = attemptIdentity;
        activeProxy = attemptProxy;
        fetchError = null;
        break;
      } catch (error) {
        fetchError = error;
        const status = error?.response?.status;
        const shouldRetry = status === 403 || status === 429 || error?.code === 'ECONNABORTED';

        if (!shouldRetry || attempt === HTTP_MAX_RETRIES) {
          if (shouldRetry) {
            isJsRendered = true;
          }
          break;
        }

        const backoffMs = RETRY_BACKOFF_BASE_MS * Math.pow(2, attempt);
        await delay(backoffMs);
      }
    }

    if (!response) {
      if (fetchError && !isJsRendered) {
        throw fetchError;
      }
      isJsRendered = true;
    } else {
      htmlContent = typeof response.data === 'string' ? response.data : '';
    }

    if (!htmlContent) {
      isJsRendered = true;
    } else {
      // Collect Facebook links from the static HTML pass
      const facebookLinks = extractFacebookUrls(htmlContent);
      if (facebookLinks.length > 0) {
        result.facebookUrls.push(...facebookLinks);
      }

      // Attempt extraction with Cheerio
      const emails = extractEmails(htmlContent);

      if (emails.length > 0) {
        result.emails.push(...emails);
        console.log(`[Cheerio] Found ${emails.length} emails. Job successful.`);
        
        // If emails found, gather next targets from navigation plus common pages
        const $ = cheerio.load(htmlContent);
        const linkCollector = createSameDomainLinkCollector(url);
        linkCollector.addCommonPages();

        const navLinks = $('nav a, header a, .navbar a, .nav a, .navigation a, .menu a, .main-menu a, .primary-menu a, .top-menu a, [role="navigation"] a, .site-nav a, .main-nav a')
          .map((i, el) => {
            const href = $(el).attr('href');
            if (!href || href.startsWith('#') || href.startsWith('javascript:')) return null;
            try {
              return new URL(href, url).href;
            } catch {
              return null;
            }
          })
          .get()
          .filter(Boolean);

        for (const link of navLinks) {
          linkCollector.addCandidateLink(link);
        }

        const collectedLinks = linkCollector.getLinks();
        if (collectedLinks.length > 0) {
          result.newUrls.push(...collectedLinks);
        }

        return result; // Early exit if successful with Cheerio
      }
    }

  } catch (error) {
    const message = error && error.message ? error.message : 'Unknown error';
    if (/maxcontentlength|maxbodylength/i.test(message)) {
      console.log(`[Cheerio Fallback] Response exceeded static fetch limit for ${url}. Escalating to Playwright.`);
    } else {
      console.log(`[Cheerio Fallback] HTTP request or initial Cheerio attempt failed for ${url}: ${message}`);
    }
    isJsRendered = true; // Assume JS rendering or a temporary error
  }


  // --- 2. Playwright-Fallback (Full Browser Rendering) ---
  if (isJsRendered || result.emails.length === 0) {
    console.log(`[Playwright] Falling back to browser rendering for ${url}...`);
    let browser;
    let context;
    let page;
    let semaphoreAcquired = false;
    try {
      // Reuse a shared browser instance for faster startup
      browser = await getSharedBrowser();
      await playwrightContextSemaphore.acquire();
      semaphoreAcquired = true;
      // Create explicit context and page
      const contextOptions = {
        userAgent: activeIdentity.userAgent,
        locale: activeIdentity.locale,
        viewport: activeIdentity.viewport,
        ...(activeProxy ? { proxy: { server: activeProxy } } : {})
      };

      context = await browser.newContext(contextOptions);
      await context.setExtraHTTPHeaders({
        'Accept-Language': activeIdentity.acceptLanguage,
        ...(activeIdentity.referer ? { Referer: activeIdentity.referer } : {})
      });
      await context.route('**/*', async (route) => {
        try {
          if (PLAYWRIGHT_BLOCKED_RESOURCE_TYPES.has(route.request().resourceType())) {
            await route.abort();
            return;
          }
        } catch {
          // If anything goes wrong while checking the request, continue normally.
        }
        await route.continue();
      });
      page = await context.newPage();
      
      // Use faster load strategy - 'domcontentloaded' is faster than 'networkidle'
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 15000 }); 
      
      // Minimal wait for dynamic content (reduced from 1000ms)
      await page.waitForTimeout(300); 

      const evaluationResult = await page.evaluate((candidateLimit) => {
        const toAbsolute = (href) => {
          try {
            return new URL(href, window.location.href).href;
          } catch {
            return null;
          }
        };

        const normalizeEmail = (value) => value.trim();
        const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
        const emailSet = new Set();
        const facebookSet = new Set();
        const candidateSet = new Set();

        document.querySelectorAll('a[href^="mailto:"]').forEach((anchor) => {
          const href = anchor.getAttribute('href');
          if (!href) return;
          const email = href.slice('mailto:'.length);
          if (email) {
            emailSet.add(normalizeEmail(email));
          }
        });

        const anchors = Array.from(document.querySelectorAll('a[href]'));
        anchors.forEach((anchor) => {
          const rawHref = anchor.getAttribute('href') || '';
          if (!rawHref || rawHref.startsWith('#')) {
            return;
          }
          const lowerRaw = rawHref.toLowerCase();
          if (lowerRaw.startsWith('javascript:')) {
            return;
          }
          const absoluteHref = toAbsolute(rawHref);
          if (absoluteHref) {
            if (!candidateSet.has(absoluteHref) && candidateSet.size < candidateLimit) {
              candidateSet.add(absoluteHref);
            }
            const lowerHref = absoluteHref.toLowerCase();
            if (lowerHref.includes('facebook.com') || lowerHref.includes('fb.com/')) {
              facebookSet.add(absoluteHref);
            }
          }
        });

        const bodyText = document.body ? document.body.innerText || '' : '';
        if (bodyText) {
          const inlineEmails = bodyText.match(emailRegex);
          if (inlineEmails) {
            inlineEmails.forEach((email) => emailSet.add(normalizeEmail(email)));
          }
          const facebookMatches = bodyText.match(/https?:\/\/(?:www\.)?(?:facebook\.com|fb\.com)[^\s"'<>]+/gi);
          if (facebookMatches) {
            facebookMatches.forEach((href) => {
              const absolute = toAbsolute(href) || href;
              facebookSet.add(absolute);
            });
          }
        }

        const uniqueCandidates = Array.from(candidateSet);
        return {
          emails: Array.from(emailSet),
          facebookUrls: Array.from(facebookSet),
          candidateLinks: uniqueCandidates.slice(0, candidateLimit)
        };
      }, MAX_LINKS_PER_PAGE);

      const pageEmails = Array.isArray(evaluationResult?.emails) ? evaluationResult.emails : [];
      const pageFacebookUrls = Array.isArray(evaluationResult?.facebookUrls) ? evaluationResult.facebookUrls : [];
      const candidateLinks = Array.isArray(evaluationResult?.candidateLinks) ? evaluationResult.candidateLinks : [];

      if (pageEmails.length > 0) {
        result.emails.push(...pageEmails);
      }
      if (pageFacebookUrls.length > 0) {
        result.facebookUrls.push(...pageFacebookUrls);
      }

      if (depth < MAX_DEPTH && result.emails.length === 0 && candidateLinks.length > 0) {
        const linkCollector = createSameDomainLinkCollector(url);
        linkCollector.addCommonPages();
        for (const link of candidateLinks) {
          linkCollector.addCandidateLink(link);
        }
        const collectedLinks = linkCollector.getLinks();
        if (collectedLinks.length > 0) {
          result.newUrls.push(...collectedLinks);
        }
      }
      
      console.log(`[Playwright] Extracted ${pageEmails.length} emails and ${candidateLinks.length} candidate link(s).`);

    } catch (error) {
      console.error(`[Playwright Error] Failed to process ${url}:`, error);
      if (!browser || !browser.isConnected()) {
        await resetSharedBrowser();
      }
      // Re-throw to let the caller handle the error
      throw error;
    } finally {
      if (page) {
        try {
          await page.close();
        } catch (closeErr) {
          console.error(`[Playwright] Failed to close page for ${url}: ${closeErr.message}`);
        }
      }
      if (context) {
        try {
          await context.close();
        } catch (closeErr) {
          console.error(`[Playwright] Failed to close context for ${url}: ${closeErr.message}`);
        }
      }
      if (semaphoreAcquired) {
        playwrightContextSemaphore.release();
      }
    }
  }

  // --- 3. Final Check & Fallback Extraction (if no emails found) ---
  if (result.emails.length === 0 && htmlContent) {
    // Only extract Facebook URLs if no emails were found on the primary page
    result.facebookUrls.push(...extractFacebookUrls(htmlContent));
  }

  return result;
}


// =========================================================================
// Direct scraping function
// =========================================================================
async function scrapeWebsite(url) {
  console.log(`Starting scrape for URL: ${url}`);
  
  const uniqueEmails = new Set();
  const uniqueFacebookUrls = new Set();
  const visitedUrls = new Set();

  try {
    // Step 1: Scrape the primary URL (depth 0)
    const primaryResult = await scrapeUrl(url, 0, visitedUrls);
    primaryResult.emails.forEach(e => uniqueEmails.add(e));
    primaryResult.facebookUrls.forEach(f => uniqueFacebookUrls.add(f));

    // Step 2: Optionally scrape a limited set of same-domain links (only if no emails found)
    if (MAX_DEPTH > 1 && uniqueEmails.size === 0) {
      const baseOrigin = new URL(url).origin;
      const subpageLimit = Math.min(MAX_SUBPAGE_CRAWLS, MAX_LINKS_PER_PAGE);
      const candidateLinks = (primaryResult.newUrls || [])
        .filter(link => {
          try {
            const linkUrl = new URL(link);
            return linkUrl.origin === baseOrigin && !visitedUrls.has(link);
          } catch {
            return false;
          }
        })
        .slice(0, subpageLimit);

      await runWithConcurrency(candidateLinks, SUBPAGE_CONCURRENCY, async (link) => {
        try {
          const subResult = await scrapeUrl(link, 1, visitedUrls);
          subResult.emails.forEach(e => uniqueEmails.add(e));
          subResult.facebookUrls.forEach(f => uniqueFacebookUrls.add(f));
          // Early exit if emails found
          if (uniqueEmails.size > 0) {
            return;
          }
        } catch (e) {
          console.error(`Error during scraping ${link}: ${e && e.message ? e.message : e}`);
          // Continue with other links even if one fails
        }
      });
    }

    // Remove duplicates and prepare results
    const finalEmails = Array.from(uniqueEmails);
    const finalFacebookUrls = Array.from(uniqueFacebookUrls);

    console.log(`Completed scrape: Found ${finalEmails.length} emails and ${finalFacebookUrls.length} Facebook URLs`);

    return {
      success: true,
      emails: finalEmails,
      facebook_urls: finalFacebookUrls,
      crawled_urls: Array.from(visitedUrls).slice(0, MAX_STORED_VISITED_URLS),
      pages_crawled: visitedUrls.size
    };

  } catch (error) {
    console.error(`Scrape failed for ${url}:`, error);
    throw error;
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
      'Handle JavaScript-rendered content',
      'Cheerio-first approach with Playwright fallback'
    ]
  });
});

// Initialize and start the server
async function startServer() {
  try {
    app.listen(PORT, () => {
      console.log(`Email extraction API running on port ${PORT}`);
      console.log(`Visit http://localhost:${PORT} for API documentation`);
    });
    
  } catch (error) {
    console.error('Failed to start server:', error);
    await closeSharedBrowser();
    process.exit(1);
  }
}

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('Shutting down gracefully...');
  await closeSharedBrowser();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('Shutting down gracefully...');
  await closeSharedBrowser();
  process.exit(0);
});

startServer();
