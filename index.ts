import fs from "fs/promises";
import path from "path";
import type { StorageAdapter, AdminForth } from "adminforth";
import crypto from "crypto";
import { createWriteStream } from 'fs';

declare global {
  var adminforth: AdminForth;
}

interface StorageLocalFilesystemOptions {
  fileSystemFolder: string; // folder where files will be stored
  mode: "public" | "private"; // public if all files should be accessible from the web, private only if could be accessed by temporary presigned links
  signingSecret: string; // secret used to generate presigned URLs
}

export default class AdminForthStorageAdapterLocalFilesystem implements StorageAdapter {
  private options: StorageLocalFilesystemOptions;
  private expressBase: string;

  constructor(options: StorageLocalFilesystemOptions) {
    this.options = options;
  }

  presignUrl(urlPath: string, expiresIn: number, payload: Record<string, string> = {}): string {
    const expires = Math.floor(Date.now() / 1000) + expiresIn;

    const params = new URLSearchParams({
      ...payload,
      expires: expires.toString(),
      signature: this.sign(urlPath, expires, payload),
    });
    return `${urlPath}?${params.toString()}`;
  }

  sign(urlPath: string, expires: number, payload: Record<string, string> = {}): string {
    const hmac = crypto.createHmac("sha256", this.options.signingSecret);
    hmac.update(urlPath);
    hmac.update(expires.toString());
    hmac.update(JSON.stringify(payload));
    return hmac.digest("hex");
  }


  /**
   * This method should return the presigned URL for the given key capable of upload (adapter user will call PUT multipart form data to this URL within expiresIn seconds after link generation).
   * By default file which will be uploaded on PUT should be marked for deletion. So if during 24h it is not marked for not deletion, it adapter should delete it forever.
   * The PUT method should fail if the file already exists.
   * 
   * Adapter user will always pass next parameters to the method:
   * @param key - The key of the file to be uploaded e.g. "uploads/file.txt"
   * @param expiresIn - The expiration time in seconds for the presigned URL
   * @param contentType - The content type of the file to be uploaded, e.g. "image/png"
   * 
   * @returns A promise that resolves to an object containing the upload URL and any extra parameters which should be sent with PUT multipart form data
   */
  async getUploadSignedUrl(
    key: string,
    contentType: string,
    expiresIn = 3600
  ): Promise<{ uploadUrl: string; uploadExtraParams: Record<string, string> }> {
    const urlPath = `${this.expressBase}/${key}`;

    return {
      uploadUrl: this.presignUrl(urlPath, expiresIn, { contentType }),
      uploadExtraParams: {}
    }
  }

  
  /**
   * This method should return the URL for the given key capable of download (200 GET request with response body or 200 HEAD request without response body).
   * If adapter configured to store objects publically, this method should return the public URL of the file.
   * If adapter configured to no allow public storing of images, this method should return the presigned URL for the file.
   * 
   * @param key - The key of the file to be downloaded e.g. "uploads/file.txt"
   * @param expiresIn - The expiration time in seconds for the presigned URL
   */
  async getDownloadUrl(key: string, _expiresIn = 3600): Promise<string> {
    const urlPath = `${this.expressBase}/${key}`;
    if (this.options.mode === "public") {
      return urlPath;
    } else {
      return this.presignUrl(key, _expiresIn);
    }
  }

  async markKeyForDeletation(key: string): Promise<void> {
  }

  /**
   * This method should mark the file for deletion.
   * If file is marked for delation and exists more then 24h (since creation date) it should be deleted.
   * This method should work even if the file does not exist yet (e.g. only presigned URL was generated).
   * @param key - The key of the file to be uploaded e.g. "uploads/file.txt"
   */
  async markKeyForNotDeletation(key: string): Promise<void> {
  }

  async setupLifecycle(userUniqueIntanceId): Promise<void> {

    if (!this.options.fileSystemFolder) {
      throw new Error("fileSystemFolder is not set in the options");
    }
    if (!this.options.signingSecret) {
      throw new Error("signingSecret is not set in the options");
    }

    // check if folder exists and try to create it if not
    // if it is not possible to create the folder, throw an error
    try {
      await fs.mkdir(this.options.fileSystemFolder, { recursive: true });
    } catch (e) {
      throw new Error(`Could not create folder ${this.options.fileSystemFolder}: ${e}`);
    }
    // check if folder is writable
    try {
      await fs.access(this.options.fileSystemFolder, fs.constants.W_OK);
    } catch (e) {
      throw new Error(`fileSystemFolder folder ${this.options.fileSystemFolder} is not writable: ${e}`);
    }
    // check if folder is readable
    try {
      await fs.access(this.options.fileSystemFolder, fs.constants.R_OK);
    } catch (e) {
      throw new Error(`fileSystemFolder folder ${this.options.fileSystemFolder} is not readable: ${e}`);
    }

    const expressInstance = global.adminforth.express.expressApp;
    const prefix = global.adminforth.config.baseUrl || '/';

    const slashedPrefix = prefix.endsWith('/') ? prefix : `${prefix}/`;

    this.expressBase = `${slashedPrefix}uploaded-static/${userUniqueIntanceId}`

    // add express PUT endpoint for uploading files
    expressInstance.put(`${this.expressBase}/*`, async (req, res) => {
      const key = req.params[0];
      const contentType = req.query.contentType as string;
      const filePath = path.join(this.options.fileSystemFolder, key);

      //verify presigned URL
      const expires = parseInt(req.query.expires as string);
      const signature = req.query.signature as string;
      const payload = {
        contentType: contentType,
      }
      console.log(`👐👐👐 verify sign for ${key}|${expires}|${JSON.stringify(payload)}`)

      const expectedSignature = this.sign(
        `${this.expressBase}/${key}`, expires, payload);
      if (signature !== expectedSignature) {
        return res.status(403).send("Invalid signature");
      }
      if (Date.now() / 1000 > expires) {
        return res.status(403).send("Signature expired");
      }
      // check if content type is valid
      if (contentType !== req.headers["content-type"]) {
        return res.status(400).send("Invalid content type");
      }

      // check if file already exists
      try {
        await fs.access(filePath);
        return res.status(409).send("File already exists");
      } catch (e) {
        // file does not exist, continue
      }
      // create folder if it does not exist
      const folderPath = path.dirname(filePath);
      try {
        await fs.mkdir(folderPath, { recursive: true });
      } catch (e) {
        return res.status(500).send(`Could not create folder ${folderPath}: ${e}`);
      }
      // write file to disk
      const writeStream = createWriteStream(filePath);
      req.pipe(writeStream);
      writeStream.on("finish", () => {
        res.status(200).send("File uploaded");
      });
    });

  }

  async objectCanBeAccesedPublicly(): Promise<boolean> {
    return this.options.mode === "public";
  }
}
