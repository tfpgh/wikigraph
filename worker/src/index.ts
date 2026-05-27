/**
 * wikigraph edge worker.
 *
 * Serves the PMTiles raster/metadata pyramids and meta.json straight from R2
 * (as before), and forwards /search and /path to the Rust container behind a
 * Cache API layer. Tiles and API responses are both edge-cached; the container
 * only ever sees cache misses.
 */
import {
  Compression,
  EtagMismatch,
  PMTiles,
  ResolvedValueCache,
  type RangeResponse,
  type Source,
  TileType,
} from "pmtiles";
import { Container, getContainer } from "@cloudflare/containers";

interface Env {
  BUCKET: R2Bucket;
  GRAPH_BACKEND: DurableObjectNamespace<GraphBackend>;
  ALLOWED_ORIGINS?: string;
  CACHE_CONTROL?: string;
  PUBLIC_HOSTNAME?: string;
  PMTILES_PATH?: string;
}

/** The container instance holding the graph + search index. */
export class GraphBackend extends Container {
  defaultPort = 8080;
  sleepAfter = "20m"; // keep warm so the CSR isn't reloaded mid-spike
}

// --- path parsing -----------------------------------------------------------

const TILE =
  /^\/(?<NAME>[0-9a-zA-Z\/!\-_\.\*\'\(\)]+)\/(?<Z>\d+)\/(?<X>\d+)\/(?<Y>\d+)\.(?<EXT>[a-z]+)$/;
const TILESET = /^\/(?<NAME>[0-9a-zA-Z\/!\-_\.\*\'\(\)]+)\.json$/;

type ParsedPath = { name: string; tile?: [number, number, number]; ext: string };

function parsePath(pathname: string): ParsedPath | null {
  const tileMatch = pathname.match(TILE);
  if (tileMatch) {
    const g = tileMatch.groups!;
    return { name: g.NAME, tile: [+g.Z, +g.X, +g.Y], ext: g.EXT };
  }
  const tilesetMatch = pathname.match(TILESET);
  if (tilesetMatch) {
    return { name: tilesetMatch.groups!.NAME, ext: "json" };
  }
  return null;
}

function pmtilesPath(name: string, setting?: string): string {
  return setting ? setting.replaceAll("{name}", name) : `${name}.pmtiles`;
}

// --- R2-backed PMTiles source ------------------------------------------------

class KeyNotFoundError extends Error {}

async function nativeDecompress(
  buf: ArrayBuffer,
  compression: Compression,
): Promise<ArrayBuffer> {
  if (compression === Compression.None || compression === Compression.Unknown) {
    return buf;
  }
  if (compression === Compression.Gzip) {
    const stream = new Response(buf).body!.pipeThrough(
      new DecompressionStream("gzip"),
    );
    return new Response(stream).arrayBuffer();
  }
  throw new Error("Compression method not supported");
}

class R2Source implements Source {
  constructor(
    private env: Env,
    private archiveName: string,
  ) {}

  getKey() {
    return this.archiveName;
  }

  async getBytes(
    offset: number,
    length: number,
    _signal?: AbortSignal,
    etag?: string,
  ): Promise<RangeResponse> {
    const resp = await this.env.BUCKET.get(
      pmtilesPath(this.archiveName, this.env.PMTILES_PATH),
      { range: { offset, length }, onlyIf: { etagMatches: etag } },
    );
    if (!resp) throw new KeyNotFoundError("Archive not found");
    // R2 returns the whole object (no body on the range result) when the etag
    // no longer matches — signal the pmtiles cache to re-read the header.
    if (!("body" in resp) || !resp.body) throw new EtagMismatch();
    return {
      data: await resp.arrayBuffer(),
      etag: resp.etag,
      cacheControl: resp.httpMetadata?.cacheControl,
      expires: resp.httpMetadata?.cacheExpiry?.toISOString(),
    };
  }
}

const TILE_CACHE = new ResolvedValueCache(25, undefined, nativeDecompress);

// --- CORS --------------------------------------------------------------------

function allowedOrigin(request: Request, env: Env): string {
  const origin = request.headers.get("Origin");
  for (const o of (env.ALLOWED_ORIGINS ?? "").split(",")) {
    if (o === origin || o === "*") return o;
  }
  return "";
}

function withCors(resp: Response, request: Request, env: Env): Response {
  const headers = new Headers(resp.headers);
  const origin = allowedOrigin(request, env);
  if (origin) headers.set("Access-Control-Allow-Origin", origin);
  headers.set("Vary", "Origin");
  return new Response(resp.body, { status: resp.status, headers });
}

// --- API forwarding ----------------------------------------------------------

async function handleApi(
  request: Request,
  env: Env,
  ctx: ExecutionContext,
): Promise<Response> {
  if (request.method.toUpperCase() !== "GET") {
    return new Response(undefined, { status: 405 });
  }

  const cache = caches.default;
  const cached = await cache.match(request.url);
  if (cached) return withCors(cached, request, env);

  // Directed /path queries are spiky + repetitive when shared — the cache
  // shields the container almost entirely under viral load.
  const upstream = await getContainer(env.GRAPH_BACKEND).fetch(request);
  const headers = new Headers(upstream.headers);
  headers.set("Cache-Control", env.CACHE_CONTROL ?? "public, max-age=86400");
  const body = await upstream.arrayBuffer();
  const response = new Response(body, { status: upstream.status, headers });
  if (upstream.ok) ctx.waitUntil(cache.put(request.url, response.clone()));
  return withCors(response, request, env);
}

// --- tile / metadata serving -------------------------------------------------

async function handleTiles(
  request: Request,
  env: Env,
  ctx: ExecutionContext,
): Promise<Response> {
  if (request.method.toUpperCase() === "POST") {
    return new Response(undefined, { status: 405 });
  }

  const url = new URL(request.url);
  const cache = caches.default;

  const cached = await cache.match(request.url);
  if (cached) return withCors(cached, request, env);

  const cacheable = (
    body: BodyInit | undefined,
    headers: Headers,
    status: number,
  ): Response => {
    headers.set("Cache-Control", env.CACHE_CONTROL ?? "public, max-age=86400");
    const toCache = new Response(body, { headers, status });
    ctx.waitUntil(cache.put(request.url, toCache.clone()));
    return withCors(toCache, request, env);
  };

  const headers = new Headers();

  // meta.json passthrough (overarching stats + cluster table).
  if (url.pathname === "/meta.json") {
    const obj = await env.BUCKET.get("meta.json");
    if (!obj) return cacheable("meta.json not found", headers, 404);
    headers.set("Content-Type", "application/json");
    return cacheable(obj.body, headers, 200);
  }

  const parsed = parsePath(url.pathname);
  if (!parsed) return new Response("Invalid URL", { status: 404 });

  const pmtiles = new PMTiles(
    new R2Source(env, parsed.name),
    TILE_CACHE,
    nativeDecompress,
  );

  try {
    const header = await pmtiles.getHeader();

    if (!parsed.tile) {
      headers.set("Content-Type", "application/json");
      const host = env.PUBLIC_HOSTNAME ?? url.hostname;
      const tj = await pmtiles.getTileJson(`https://${host}/${parsed.name}`);
      return cacheable(JSON.stringify(tj), headers, 200);
    }

    const [z, x, y] = parsed.tile;
    if (z < header.minZoom || z > header.maxZoom) {
      return cacheable(undefined, headers, 404);
    }

    const tile = await pmtiles.getZxy(z, x, y);
    switch (header.tileType) {
      case TileType.Png:
        headers.set("Content-Type", "image/png");
        break;
      case TileType.Webp:
        headers.set("Content-Type", "image/webp");
        break;
      case TileType.Jpeg:
        headers.set("Content-Type", "image/jpeg");
        break;
      case TileType.Mvt:
        headers.set("Content-Type", "application/x-protobuf");
        break;
      default:
        // UNKNOWN tile type (the gzipped-JSON metadata pyramids). The client
        // knows how to read these; pass them through as octet-stream.
        headers.set("Content-Type", "application/octet-stream");
    }

    if (tile) return cacheable(tile.data, headers, 200);
    return cacheable(undefined, headers, 204);
  } catch (e) {
    if (e instanceof KeyNotFoundError) {
      return cacheable("Archive not found", headers, 404);
    }
    throw e;
  }
}

export default {
  async fetch(
    request: Request,
    env: Env,
    ctx: ExecutionContext,
  ): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/search" || url.pathname === "/path") {
      return handleApi(request, env, ctx);
    }
    return handleTiles(request, env, ctx);
  },
};
