import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import fs from 'fs';
import os from 'os';
import path from 'path';
import type { AddressInfo } from 'net';
// the adapter gets the express app from AdminForth core, which runs express 5
import express from 'express5';

const afLogger = vi.hoisted(() => ({ warn: vi.fn(), debug: vi.fn(), error: vi.fn(), info: vi.fn() }));
vi.mock('adminforth', () => ({ default: class {}, afLogger }));

const Adapter = (await import('../index.js')).default;

const DAY = 24 * 60 * 60 * 1000;
const PNG = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==', 'base64');

let adapter: any;
let folder: string;
let baseUrl: string;
let server: ReturnType<ReturnType<typeof express>['listen']>;
let scheduler: () => void;

beforeAll(async () => {
  folder = fs.mkdtempSync(path.join(os.tmpdir(), 'af-local-adapter-'));
  const app = express();
  (globalThis as any).adminforth = { express: { expressApp: app }, config: { baseUrl: '' } };
  vi.spyOn(globalThis, 'setInterval').mockImplementation(((callback: () => void) => {
    scheduler = callback;
    return 0;
  }) as any);
  adapter = new Adapter({ fileSystemFolder: folder, signingSecret: 'secret', mode: 'public' });
  await adapter.setupLifecycle('test');
  server = app.listen(0);
  await new Promise((resolve) => server.once('listening', resolve));
  baseUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});

afterAll(async () => {
  server.close();
  await adapter.metadataDb.close();
  await adapter.candidatesForDeletionDb.close();
  fs.rmSync(folder, { recursive: true, force: true });
  vi.restoreAllMocks();
});

beforeEach(() => {
  afLogger.error.mockClear();
});

let keyCounter = 0;
const newKey = () => `photos/cat_${++keyCounter}.png`;

async function put(uploadUrl: string, body: Buffer, contentType = 'image/png') {
  const response = await fetch(`${baseUrl}${uploadUrl}`, { method: 'PUT', headers: { 'Content-Type': contentType }, body });
  return { status: response.status, text: await response.text() };
}

async function upload(key: string, contentLength?: number) {
  const { uploadUrl } = await adapter.getUploadSignedUrl(key, 'image/png', 600, contentLength);
  return put(uploadUrl, PNG);
}

const isCandidate = async (key: string) => (await adapter.candidatesForDeletionDb.get(key)) !== undefined;

describe('upload URL bound to the size', () => {
  it('accepts a file of exactly the signed size', async () => {
    expect(await upload(newKey(), PNG.length)).toEqual({ status: 200, text: 'File uploaded' });
  });

  it('rejects a bigger or a smaller file', async () => {
    const bigger = await adapter.getUploadSignedUrl(newKey(), 'image/png', 600, PNG.length);
    expect(await put(bigger.uploadUrl, Buffer.concat([PNG, Buffer.alloc(1000)]))).toEqual({ status: 400, text: 'Invalid content length' });

    const smaller = await adapter.getUploadSignedUrl(newKey(), 'image/png', 600, PNG.length);
    expect(await put(smaller.uploadUrl, PNG.subarray(0, 10))).toEqual({ status: 400, text: 'Invalid content length' });
  });

  it('rejects the URL when the size is removed from it', async () => {
    const { uploadUrl } = await adapter.getUploadSignedUrl(newKey(), 'image/png', 600, PNG.length);
    const withoutSize = uploadUrl.replace(/&?contentLength=\d+/, '');

    expect((await put(withoutSize, Buffer.alloc(5000))).status).toBe(403);
  });

  it('keeps URLs without a size working as before, for any size', async () => {
    expect((await upload(newKey())).status).toBe(200);
    expect((await upload(newKey(), null as any)).status).toBe(200);
  });

  it('still rejects another content type', async () => {
    const { uploadUrl } = await adapter.getUploadSignedUrl(newKey(), 'image/png', 600, PNG.length);

    expect(await put(uploadUrl, PNG, 'text/html')).toEqual({ status: 403, text: 'Invalid signature' });
  });
});

describe('serving files', () => {
  it('sends nosniff with the stored content type on GET and HEAD', async () => {
    const key = newKey();
    await upload(key, PNG.length);

    for (const method of ['GET', 'HEAD']) {
      const response = await fetch(`${baseUrl}${await adapter.getDownloadUrl(key)}`, { method });
      expect(response.status).toBe(200);
      expect(response.headers.get('content-type')).toBe('image/png');
      expect(response.headers.get('x-content-type-options')).toBe('nosniff');
    }
  });
});

describe('cleanup marks', () => {
  it('marks a new upload and unmarks it when it is saved', async () => {
    const key = newKey();
    await upload(key, PNG.length);
    expect(await isCandidate(key)).toBe(true);

    await adapter.markKeyForNotDeletion(key);
    expect(await isCandidate(key)).toBe(false);

    await adapter.markKeyForDeletion(key);
    expect(Date.now() - Number(await adapter.candidatesForDeletionDb.get(key))).toBeLessThan(1000);
  });

  it('never marks a file uploaded before cleanupEnabled existed', async () => {
    const key = newKey();
    fs.mkdirSync(path.join(folder, 'photos'), { recursive: true });
    fs.writeFileSync(path.join(folder, key), PNG);
    await adapter.metadataDb.put(key, JSON.stringify({ contentType: 'image/png', createdAt: Date.now() - 30 * DAY, size: PNG.length }));

    await adapter.markKeyForDeletion(key);

    expect(await isCandidate(key)).toBe(false);
  });

  it('answers 200 even when the mark can not be stored, the file is already saved', async () => {
    vi.spyOn(adapter, 'markKeyForDeletion').mockRejectedValueOnce(new Error('db is broken'));

    expect((await upload(newKey(), PNG.length)).status).toBe(200);
    expect(afLogger.error).toHaveBeenCalledWith(expect.stringContaining('db is broken'));
  });
});

describe('deleteExpiredCandidates', () => {
  async function storedFile(markedAgo: number) {
    const key = newKey();
    await upload(key, PNG.length);
    await adapter.candidatesForDeletionDb.put(key, String(Date.now() - markedAgo));
    return key;
  }
  const state = async (key: string) => ({
    file: fs.existsSync(path.join(folder, key)),
    metadata: (await adapter.metadataDb.get(key)) !== undefined,
    candidate: await isCandidate(key),
  });

  it('deletes files marked more than 24h ago and keeps fresh ones', async () => {
    const expired = await storedFile(DAY + 1000);
    const fresh = await storedFile(1000);

    await adapter.deleteExpiredCandidates();

    expect(await state(expired)).toEqual({ file: false, metadata: false, candidate: false });
    expect(await state(fresh)).toEqual({ file: true, metadata: true, candidate: true });
  });

  it('drops the records of a file removed by hand and does not fail on the next run', async () => {
    const removed = await storedFile(DAY + 1000);
    fs.unlinkSync(path.join(folder, removed));

    await adapter.deleteExpiredCandidates();
    await adapter.deleteExpiredCandidates();

    expect(await state(removed)).toEqual({ file: false, metadata: false, candidate: false });
  });

  it('the scheduler only logs a failed run, so it can not crash the process', async () => {
    vi.spyOn(adapter, 'deleteExpiredCandidates').mockRejectedValueOnce(new Error('db is broken'));

    scheduler();
    await vi.waitFor(() => expect(afLogger.error).toHaveBeenCalledWith(expect.stringContaining('db is broken')));
  });
});
