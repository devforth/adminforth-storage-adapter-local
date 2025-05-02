import fs from "fs/promises";
import path from "path";
import type { StorageAdapter } from "adminforth";

interface LocalStorageOptions {
  basePath: string;
  publicBaseUrl?: string; 
}

const CLEANUP_TAG_FILE = ".adminforth-cleanup";

export default class AdminForthAdapterLocalStorage implements StorageAdapter {
  private options: LocalStorageOptions;

  constructor(options: LocalStorageOptions) {
    this.options = options;
  }

  private resolveFilePath(key: string): string {
    return path.join(this.options.basePath, key);
  }

  private resolveTagFilePath(key: string): string {
    return this.resolveFilePath(key) + `.${CLEANUP_TAG_FILE}`;
  }

  async getUploadSignedUrl(
    key: string,
    _contentType: string,
    _expiresIn = 3600
  ): Promise<{ uploadUrl: string; uploadExtraParams: Record<string, string> }> {
    const filePath = this.resolveFilePath(key);
    await fs.mkdir(path.dirname(filePath), { recursive: true });

    return {
      uploadUrl: `file://${filePath}`, // not usable by browser, but valid for internal logic
      uploadExtraParams: {}, // not needed for local
    };
  }

  async getDownloadUrl(key: string, _expiresIn = 3600): Promise<string> {
    const filePath = this.resolveFilePath(key);
    if (this.options.publicBaseUrl) {
      return `${this.options.publicBaseUrl}/${key}`;
    }
    return `file://${filePath}`;
  }

  async markKeyForDeletation(key: string): Promise<string> {
    const tagFilePath = this.resolveTagFilePath(key);
    await fs.writeFile(tagFilePath, "true");
    return key;
  }

  async markKeyForNotDeletation(key: string): Promise<string> {
    const tagFilePath = this.resolveTagFilePath(key);
    await fs.rm(tagFilePath, { force: true });
    return key;
  }

  async setupLifecycle(): Promise<void> {
    console.log("ℹ️ Local storage lifecycle setup is a no-op.");
  }
}
