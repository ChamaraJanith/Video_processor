require('dotenv').config();
const { S3Client, GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');
const { SQSClient, ReceiveMessageCommand, DeleteMessageCommand } = require('@aws-sdk/client-sqs');
const ffmpeg = require('fluent-ffmpeg');
const fs = require('fs');
const path = require('path');
const os = require('os');
const { Readable } = require('stream');

// AWS Clients
const s3 = new S3Client({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

const sqs = new SQSClient({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

// Output format definitions
const FORMATS = {
  '720p':  { width: 1280, height: 720,  videoBitrate: '2500k', audioBitrate: '128k' },
  '480p':  { width: 854,  height: 480,  videoBitrate: '1000k', audioBitrate: '96k'  },
  '360p':  { width: 640,  height: 360,  videoBitrate: '500k',  audioBitrate: '64k'  },
};

// ─── Helper: Download S3 file to temp ─────────────────────────────────────────
async function downloadFromS3(bucket, key, destPath) {
  console.log(`[Worker] Downloading s3://${bucket}/${key}`);
  const { Body } = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  const stream = Readable.from(Body);
  await new Promise((resolve, reject) => {
    const file = fs.createWriteStream(destPath);
    stream.pipe(file);
    file.on('finish', resolve);
    file.on('error', reject);
  });
  console.log(`[Worker] Downloaded to ${destPath}`);
}

// ─── Helper: Upload file to S3 ─────────────────────────────────────────────────
async function uploadToS3(bucket, key, filePath) {
  console.log(`[Worker] Uploading ${filePath} → s3://${bucket}/${key}`);
  const fileStream = fs.createReadStream(filePath);
  await s3.send(new PutObjectCommand({
    Bucket: bucket,
    Key: key,
    Body: fileStream,
    ContentType: 'video/mp4',
  }));
  console.log(`[Worker] Uploaded: ${key}`);
}

// ─── Helper: Transcode video with FFmpeg ──────────────────────────────────────
function transcodeVideo(inputPath, outputPath, format) {
  const { width, height, videoBitrate, audioBitrate } = FORMATS[format];
  return new Promise((resolve, reject) => {
    console.log(`[Worker] Transcoding to ${format} (${width}x${height})`);
    ffmpeg(inputPath)
      .outputOptions([
        '-vf', `scale=${width}:${height}`,
        '-c:v', 'libx264',
        '-b:v', videoBitrate,
        '-c:a', 'aac',
        '-b:a', audioBitrate,
        '-movflags', '+faststart',
        '-preset', 'fast',
      ])
      .output(outputPath)
      .on('start', cmd => console.log(`[FFmpeg] Command: ${cmd}`))
      .on('progress', p => process.stdout.write(`\r[FFmpeg] Progress: ${Math.round(p.percent || 0)}%`))
      .on('end', () => { console.log(`\n[FFmpeg] Done: ${format}`); resolve(); })
      .on('error', err => { console.error(`\n[FFmpeg] Error:`, err); reject(err); })
      .run();
  });
}

// ─── Process a single job ─────────────────────────────────────────────────────
async function processJob(job) {
  const { jobId, inputKey, bucket, outputFormats } = job;
  const tmpDir = path.join(os.tmpdir(), `job-${jobId}`);
  fs.mkdirSync(tmpDir, { recursive: true });

  const inputExt = path.extname(inputKey) || '.mp4';
  const inputPath = path.join(tmpDir, `input${inputExt}`);

  try {
    // 1. Download input video from S3
    await downloadFromS3(bucket, inputKey, inputPath);

    // 2. Transcode for each requested format
    for (const format of outputFormats) {
      if (!FORMATS[format]) {
        console.warn(`[Worker] Unknown format "${format}", skipping.`);
        continue;
      }
      const outputPath = path.join(tmpDir, `${format}.mp4`);
      const outputKey  = `outputs/${jobId}/${format}.mp4`;

      await transcodeVideo(inputPath, outputPath, format);
      await uploadToS3(bucket, outputKey, outputPath);

      // Cleanup output file to save disk space
      fs.unlinkSync(outputPath);
      console.log(`[Worker] ✅ ${format} done → s3://${bucket}/${outputKey}`);
    }

    console.log(`[Worker] 🎉 Job ${jobId} completed all formats!`);
  } finally {
    // Cleanup temp directory
    fs.rmSync(tmpDir, { recursive: true, force: true });
    console.log(`[Worker] Temp files cleaned up for job ${jobId}`);
  }
}

// ─── Main Poll Loop ───────────────────────────────────────────────────────────
async function pollSQS() {
  console.log(`
╔══════════════════════════════════════════════╗
║   🔧 Video Transcoder Worker Running         ║
║   Region  : ${process.env.AWS_REGION}                    ║
║   Queue   : video-processing-queue           ║
║   Polling for jobs...                        ║
╚══════════════════════════════════════════════╝
  `);

  while (true) {
    try {
      const result = await sqs.send(new ReceiveMessageCommand({
        QueueUrl: process.env.SQS_QUEUE_URL,
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 20,        // Long polling - efficient & cost-saving
        VisibilityTimeout: 300,     // 5 minutes to process before requeue
        MessageAttributeNames: ['All'],
      }));

      if (!result.Messages || result.Messages.length === 0) {
        process.stdout.write('.');  // Show alive indicator
        continue;
      }

      const msg = result.Messages[0];
      const job = JSON.parse(msg.Body);

      console.log(`\n[Worker] 📨 Received job: ${job.jobId}`);
      console.log(`[Worker] Formats: ${job.outputFormats.join(', ')}`);

      await processJob(job);

      // Delete message from queue after successful processing
      await sqs.send(new DeleteMessageCommand({
        QueueUrl: process.env.SQS_QUEUE_URL,
        ReceiptHandle: msg.ReceiptHandle,
      }));
      console.log(`[Worker] Message deleted from SQS queue.`);

    } catch (err) {
      console.error('[Worker] Error in poll loop:', err.message);
      // Wait 5 seconds before retrying on error
      await new Promise(r => setTimeout(r, 5000));
    }
  }
}

// Start polling
pollSQS();
