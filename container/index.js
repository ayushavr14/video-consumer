import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import fs from "node:fs/promises";
import fsOld from "node:fs";
import path from "node:path";
import { MongoClient, ObjectId } from "mongodb";

// For fluent-ffmpeg (ES module workaround)
import { createRequire } from "module";
const require = createRequire(import.meta.url);
const ffmpeg = require("fluent-ffmpeg");

const RESOLUTIONS = [
  { name: "360p", width: 480, height: 360, bitrate: "800k" },
  { name: "480p", width: 858, height: 480, bitrate: "1400k" },
  { name: "720p", width: 1280, height: 720, bitrate: "2800k" },
];

const s3Client = new S3Client({
  region: "ap-south-1",
  credentials: {
    accessKeyId: process.env.S3_CLIENT_ACCESS_KEY_ID,
    secretAccessKey: process.env.S3_CLIENT_SECRET_ACCESS_KEY,
  },
});

//ENV
const BUCKET_NAME = process.env.BUCKET_NAME;
const KEY = process.env.KEY;
const POST_ID = process.env.POST_ID;

async function init() {

const client = new MongoClient(process.env.MONGODB_URI);

async function updateMediaUrl(postId, mediaUrl) {
  try {
    await client.connect();
    const db = client.db("dev-db"); 
    const postsCollection = db.collection("posts");

    const result = await postsCollection.updateOne(
      { _id: new ObjectId(postId) },
      { 
        $set: { 
          media: [{ url: mediaUrl }]
        } 
      }
    );

    if (result.modifiedCount > 0) {
      console.log(`✅ Updated Post ${postId} with new media URL: ${mediaUrl}`);
    } else {
      console.log(`⚠️ No document found with _id: ${postId}, or no changes made.`);
    }
  } catch (error) {
    console.error("❌ Error updating media URL:", error);
  } finally {
    await client.close();
  }
}

  
  const command = new GetObjectCommand({ Bucket: BUCKET_NAME, Key: KEY });
  const result = await s3Client.send(command);
  const originalFilePath = `original-video.mp4`;
  await fs.writeFile(originalFilePath, result.Body);
  const originalVideoPath = path.resolve(originalFilePath);

  const outputDir = "hls-output";
  await fs.mkdir(outputDir, { recursive: true });
  const masterPlaylist = path.join(outputDir, "index.m3u8");

  // Generate Thumbnails
  const thumbnailDir = "thumbnails";
  await fs.mkdir(thumbnailDir, { recursive: true });
  const thumbnailPattern = path.join(thumbnailDir, "thumbnail_%03d.png");

  await new Promise((resolve, reject) => {
    ffmpeg(originalVideoPath)
      .screenshots({
        count: 5,
        folder: thumbnailDir,
        filename: "thumbnail_%03d.png",
      })
      .on("end", resolve)
      .on("error", reject);
  });

  console.log("Thumbnails generated");

  // Extract MP3 Audio
  const audioOutput = "audio.mp3";
  await new Promise((resolve, reject) => {
    ffmpeg(originalVideoPath)
      .output(audioOutput)
      .audioCodec("libmp3lame")
      .on("end", resolve)
      .on("error", reject)
      .run();
  });

  console.log("MP3 audio extracted");

  const variantPlaylists = await Promise.all(
    RESOLUTIONS.map(async (res) => {
      const outputM3U8 = path.join(outputDir, `playlist_${res.name}.m3u8`);
      const outputPattern = path.join(outputDir, `segment_${res.name}_%03d.ts`);

      await new Promise((resolve, reject) => {
        ffmpeg(originalVideoPath)
          .output(outputM3U8)
          .videoCodec("libx264")
          .audioCodec("aac")
          .size(`${res.width}x${res.height}`)
          .outputOptions([
            "-preset fast",
            "-crf 20",
            `-b:v ${res.bitrate}`,
            "-maxrate 3000k",
            "-bufsize 6000k",
            "-hls_time 6",
            "-hls_playlist_type vod",
            `-hls_segment_filename ${outputPattern}`,
          ])
          .on("end", resolve)
          .on("error", reject)
          .run();
      });

      return { res, playlist: `playlist_${res.name}.m3u8` };
    })
  );

  const masterPlaylistContent = `#EXTM3U\n${variantPlaylists
    .map(
      (v) => `#EXT-X-STREAM-INF:BANDWIDTH=${v.res.bitrate.replace("k", "000")},RESOLUTION=${v.res.width}x${v.res.height}\n${v.playlist}`
    )
    .join("\n")}`;

  await fs.writeFile(masterPlaylist, masterPlaylistContent);

  const files = await fs.readdir(outputDir);
  for (const file of files) {
    const filePath = path.join(outputDir, file);
    const s3Key = `${POST_ID}/hls/${file}`;
    const putCommand = new PutObjectCommand({
      Bucket: process.env.S3_BUCKET_NAME,
      Key: s3Key,
      Body: fsOld.createReadStream(filePath),
      ContentType: file.endsWith(".m3u8")
        ? "application/vnd.apple.mpegurl"
        : "video/MP2T",
    });
    await s3Client.send(putCommand);
    console.log(`Uploaded ${s3Key}`);
    if (file === "index.m3u8") {
      const postId = POST_ID;
      const mediaUrl = `https://${process.env.S3_BUCKET_NAME}.s3.amazonaws.com/${postId}/hls/${file}`;
      await updateMediaUrl(postId, mediaUrl);
    }
  }


  // Upload Thumbnails
  const thumbnailFiles = await fs.readdir(thumbnailDir);
  for (const file of thumbnailFiles) {
    const filePath = path.join(thumbnailDir, file);
    const s3Key = `${POST_ID}/thumbnails/${file}`;
    const putCommand = new PutObjectCommand({
      Bucket: process.env.S3_BUCKET_NAME,
      Key: s3Key,
      Body: fsOld.createReadStream(filePath),
      ContentType: "image/png",
    });
    await s3Client.send(putCommand);
    console.log(`Uploaded ${s3Key}`);
  }

  // Upload MP3 Audio
  const audioFilePath = path.resolve(audioOutput);
  const audioS3Key = `${POST_ID}/audio/audio.mp3`;
  const audioPutCommand = new PutObjectCommand({
    Bucket: process.env.S3_BUCKET_NAME,
    Key: audioS3Key,
    Body: fsOld.createReadStream(audioFilePath),
    ContentType: "audio/mpeg",
  });
  await s3Client.send(audioPutCommand);
  console.log(`Uploaded ${audioS3Key}`);
}

init().catch(console.error);
