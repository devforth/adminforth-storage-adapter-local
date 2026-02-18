import fs from "fs/promises";
import path from "path";
import AdminForth, { StorageAdapter, afLogger } from "adminforth";
import crypto from "crypto";
import { createWriteStream } from 'fs';
import { Level } from 'level';
import { Express } from "express";

declare global {
  var adminforth: AdminForth;
}

interface StorageLocalFilesystemOptions {
  fileSystemFolder: string; // folder where files will be stored
  mode?: "public" | "private"; // public if all files should be accessible from the web, private only if could be accessed by temporary presigned links
  signingSecret: string; // secret used to generate presigned URLs
  adminServeBaseUrl?: string; // base URL for serving files e.g. static/uploads. If not defined will be generated automatically
    // please note that is adminforth base URL is set, files will be available on `${adminforth.config.baseUrl}/${adminServeBaseUrl}/{key}`
}

export default class AdminForthStorageAdapterLocalFilesystem implements StorageAdapter {
  static registredPrexises: string[] = [];

  private options: StorageLocalFilesystemOptions;
  private expressBase: string;
  private adminforthSlashedPrefix: string; // slashed prefix of the base URL

  private metadataDb: Level;
  private candidatesForDeletionDb: Level;

  constructor(options: StorageLocalFilesystemOptions) {
    this.options = options;
    if (!this.options.mode) {
      this.options.mode = "private";
    }
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
    afLogger.warn("Method \"markKeyForDeletation\" is deprecated. Please update upload plugin");
    this.markKeyForDeletion(key);
  }

  async markKeyForNotDeletation(key: string): Promise<void> {
    afLogger.warn("Method \"markKeyForNotDeletation\" is deprecated. Please update upload plugin");
    this.markKeyForNotDeletion(key);
  }

  async markKeyForDeletion(key: string): Promise<void> {
    const metadata = await this.metadataDb.get(key).catch((e) => {
      afLogger.error(`Could not read metadata from db: ${e}`);
      throw new Error(`Could not read metadata from db: ${e}`);
    });
    if (!metadata) {
      afLogger.error(`Metadata for key ${key} not found`);
      return;
    }
    const metadataParsed = JSON.parse(metadata);

    try {
      await this.candidatesForDeletionDb.get(key);
      // if key already exists, do nothing
      return;
    } catch (e) {
      // if key does not exist, continue
    }
    try {
      await this.candidatesForDeletionDb.put(key, metadataParsed.createdAt)
    } catch (e) {
      afLogger.error(`Could not write metadata to db: ${e}`);
      throw new Error(`Could not write metadata to db: ${e}`);
    }
  }

  /**
   * This method should mark the file for deletion.
   * If file is marked for delation and exists more then 24h (since creation date) it should be deleted.
   * This method should work even if the file does not exist yet (e.g. only presigned URL was generated).
   * @param key - The key of the file to be uploaded e.g. "uploads/file.txt"
   */
  async markKeyForNotDeletion(key: string): Promise<void> {
    try {
      // if key exists, delete it
      await this.candidatesForDeletionDb.del(key);
    } catch (e) {
      // if key does not exist, do nothing
    }
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

    this.metadataDb = new Level(path.join(this.options.fileSystemFolder, userUniqueIntanceId, 'metadata'));

    this.candidatesForDeletionDb = new Level(path.join(this.options.fileSystemFolder, userUniqueIntanceId, 'candidatesForDeletion'));

    const expressInstance: Express = global.adminforth.express.expressApp;
    const prefix = global.adminforth.config.baseUrl || '/';

    const slashedPrefix = prefix.endsWith('/') ? prefix : `${prefix}/`;
    this.adminforthSlashedPrefix = slashedPrefix;
    if (!this.options.adminServeBaseUrl) {
      this.expressBase = `${slashedPrefix}uploaded-static/${userUniqueIntanceId}`
    } else {
      if (AdminForthStorageAdapterLocalFilesystem.registredPrexises.includes(this.options.adminServeBaseUrl) || AdminForthStorageAdapterLocalFilesystem.registredPrexises.includes(`/${this.options.adminServeBaseUrl}`)) {
        throw new Error(`adminServeBaseUrl ${this.options.adminServeBaseUrl} already registered, by another instance of local filesystem adapter. 
          Each adapter instahce should have unique adminServeBaseUrl by design.
        `);
      }

      AdminForthStorageAdapterLocalFilesystem.registredPrexises.push(this.options.adminServeBaseUrl);
      this.expressBase = `${slashedPrefix}${this.options.adminServeBaseUrl}`;

    }
    

    // add express PUT endpoint for uploading files
    expressInstance.put(`${this.expressBase}/*`, async (req: any, res: any) => {
      const key = req.params[0];

      // get content type from headers
      const contentType = req.headers["content-type"] as string;
      if (!contentType) {
        return res.status(400).send("Content type is required");
      }

      const filePath = path.resolve(this.options.fileSystemFolder, key);

      // Ensure filePath is within fileSystemFolder
      const basePath = path.resolve(this.options.fileSystemFolder);
      if (!filePath.startsWith(basePath + path.sep)) {
        return res.status(400).send("Invalid key, access denied");
      }

      //verify presigned URL
      const expires = parseInt(req.query.expires as string);
      const signature = req.query.signature as string;
      const payload = {
        contentType: contentType,
      }

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
        // write metadata to db
        this.metadataDb.put(key, 
          JSON.stringify({
            contentType: contentType,
            createdAt: +Date.now(),
            size: writeStream.bytesWritten,
          })
        ).catch((e) => {
          afLogger.error(`Could not write metadata to db: ${e}`);
          throw new Error(`Could not write metadata to db: ${e}`);
        });

        this.markKeyForDeletion(key);

        res.status(200).send("File uploaded");
      });
    });

    // add express GET endpoint for downloading files
    expressInstance.get(`${this.expressBase}/*`, async (req: any, res: any) => {
      const key = req.params[0];
      const filePath = path.resolve(this.options.fileSystemFolder, key);

      // Ensure filePath is within fileSystemFolder
      const basePath = path.resolve(this.options.fileSystemFolder);
      if (!filePath.startsWith(basePath + path.sep)) {
        return res.status(400).send("Invalid key, access denied");
      }

      // check if file exists
      try {
        await fs.access(filePath);
      } catch (e) {
        return res.status(404).send("File not found");
      }

      // add metadata to response headers
      const metadata = await this.metadataDb.get(key).catch((e) => {
        throw new Error(`Could not read metadata for ${key} from db: ${e}`);
      });
      if (!metadata) {
        return res.status(404).send(`Metadata for ${key} not found`);
      }
      const metadataParsed = JSON.parse(metadata);
      // send file to client
      res.sendFile(
        filePath, 
        {
          headers: {
            "Content-Type": metadataParsed.contentType,
            "Content-Length": metadataParsed.size,
            "Last-Modified": new Date(metadataParsed.createdAt).toUTCString(),
            "ETag": crypto.createHash("md5").update(metadata).digest("hex"),
          },
        },
        (err) => {
          if (err) {
            afLogger.error(`Could not send file ${filePath}: ${err}`);
            res.status(500).send("Could not send file");
          }
        }
      );
    });

    this.putLastListenerToTheBeginningOfTheStack(expressInstance);



    // add HEAD endpoint for returning file metadata
    expressInstance.head(`${this.expressBase}/*`, async (req: any, res: any) => {
      const key = req.params[0];
      const filePath = path.resolve(this.options.fileSystemFolder, key);

      // Ensure filePath is within fileSystemFolder
      const basePath = path.resolve(this.options.fileSystemFolder);
      if (!filePath.startsWith(basePath + path.sep)) {
        return res.status(400).send("Invalid key, access denied");
      }

      // check if file exists
      try {
        await fs.access(filePath);
      } catch (e) {
        return res.status(404).send("File not found");
      }

      // add metadata to response headers
      const metadata = await this.metadataDb.get(key).catch((e) => {
        throw new Error(`Could not read metadata for ${key} from db: ${e}`);
      });
      if (!metadata) {
        return res.status(404).send(`Metadata for ${key} not found`);
      }
      const metadataParsed = JSON.parse(metadata);
      res.setHeader("Content-Type", metadataParsed.contentType);
      res.setHeader("Content-Length", metadataParsed.size);
      res.setHeader("Last-Modified", new Date(metadataParsed.createdAt).toUTCString());
      res.setHeader("ETag", crypto.createHash("md5").update(metadata).digest("hex"));
    });
    this.putLastListenerToTheBeginningOfTheStack(expressInstance);


    // run scheduler every 10 minutes to delete files marked for deletion
    setInterval(async () => {
      const now = +Date.now();
      const keys = await this.candidatesForDeletionDb.keys().all();
      for (const key of keys) {
        const createdAt = await this.candidatesForDeletionDb.get(key).catch((e) => {
          afLogger.error(`Could not read metadata from db: ${e}`);
          throw new Error(`Could not read metadata from db: ${e}`);
        });
        if (now - +createdAt > 24 * 60 * 60 * 1000) {
          // delete file
          try {
            await fs.unlink(path.resolve(this.options.fileSystemFolder, key));
          } catch (e) {
            afLogger.error(`Could not delete file ${key}: ${e}`);
            throw new Error(`Could not delete file ${key}: ${e}`);
          }
          // delete metadata
          try {
            await this.metadataDb.del(key);
          } catch (e) {
            afLogger.error(`Could not delete metadata from db: ${e}`);
            throw new Error(`Could not delete metadata from db: ${e}`);
          }
        }
      }
    }
      , 10 * 60 * 1000); // every 10 minutes

  }

  async objectCanBeAccesedPublicly(): Promise<boolean> {
    return this.options.mode === "public";
  }

  putLastListenerToTheBeginningOfTheStack(expressInstance) {
    // since adminforth might already registred /* endpoint we need to reorder the routes
    const stack = expressInstance._router.stack;
    const adpaterListnerLayer = stack.pop(); // route is last, just pop it
    // find route with ${this.adminforthSlashedPrefix}assets/*
    const wildcardIndex = stack.findIndex((layer) => {
      return layer.route && layer.route.path === `${this.adminforthSlashedPrefix}assets/*`;
    });
    if (wildcardIndex === -1) {
      // if not found, just push it to the end, e.g. if discover databse and this method executed before 
      // adminforth registered the wildcard route
      stack.push(adpaterListnerLayer);
    } else {
      stack.splice(wildcardIndex, 0, adpaterListnerLayer); // insert before wildcard
    }
  }

  /**
   * This method should return the key as a data URL (base64 encoded string).
   * @param key - The key of the file to be converted to a data URL
   * @returns A promise that resolves to a string containing the data URL
   */
   async getKeyAsDataURL(key: string): Promise<string> {
    const filePath = path.resolve(this.options.fileSystemFolder, key);

    // Ensure filePath is within fileSystemFolder
    const basePath = path.resolve(this.options.fileSystemFolder);
    if (!filePath.startsWith(basePath + path.sep)) {
      throw new Error("Invalid key, access denied");
    }

    // check if file exists
    try {
      await fs.access(filePath);
    } catch (e) {
      throw new Error("File not found");
    }

    // read file and convert to base64
    const fileBuffer = await fs.readFile(filePath);
    const base64 = fileBuffer.toString("base64");
    const metadata = await this.metadataDb.get(key).catch((e) => {
      afLogger.error(`Could not read metadata from db: ${e}`);
      throw new Error(`Could not read metadata from db: ${e}`);
    });
    if (!metadata) {
      throw new Error(`Metadata for key ${key} not found`);
    }
    const metadataParsed = JSON.parse(metadata);
    const dataUrl = `data:${metadataParsed.contentType};base64,${base64}`;
    return dataUrl;
  }

}
