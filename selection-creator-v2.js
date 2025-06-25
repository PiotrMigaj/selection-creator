// photo-selection-processor.js
import fs from "fs";
import path from "path";
import sharp from "sharp";
import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand, UpdateCommand } from "@aws-sdk/lib-dynamodb";
import { v4 as uuidv4 } from "uuid";
import dotenv from "dotenv";
import { fileURLToPath } from "url";
import readline from "readline";

// Load environment variables
dotenv.config();

// Get the current directory
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Create readline interface for user input
const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

// Function to get input from user
const getInput = (prompt) => {
  return new Promise((resolve) => {
    rl.question(prompt, (answer) => {
      resolve(answer);
    });
  });
};

// Main function
const main = async () => {
  try {
    // Get input from user or environment variables
    const awsRegion =
      process.env.AWS_REGION || (await getInput("Enter AWS region: "));
    const awsAccessKeyId =
      process.env.AWS_ACCESS_KEY_ID ||
      (await getInput("Enter AWS access key ID: "));
    const awsSecretAccessKey =
      process.env.AWS_SECRET_ACCESS_KEY ||
      (await getInput("Enter AWS secret access key: "));
    const bucketName =
      process.env.S3_BUCKET || (await getInput("Enter S3 bucket name: "));
    const username =
      process.env.USERNAME || (await getInput("Enter username: "));
    const eventId =
      process.env.EVENT_ID || (await getInput("Enter event ID: "));
    const eventTitle =
      process.env.EVENT_TITLE || (await getInput("Enter event title: "));
    const maxNumberOfPhotos =
      parseInt(process.env.MAX_NUMBER_OF_PHOTOS) || 
      parseInt(await getInput("Enter max number of photos: "));

    // Set up input directory
    const inputDir =
      process.env.INPUT_DIR || path.join(__dirname, "images/compressed");

    // Validate input directory exists
    if (!fs.existsSync(inputDir)) {
      console.error(`Input directory '${inputDir}' does not exist.`);
      rl.close();
      return;
    }

    // Configure AWS clients
    const s3Client = new S3Client({
      region: awsRegion,
      credentials: {
        accessKeyId: awsAccessKeyId,
        secretAccessKey: awsSecretAccessKey,
      },
    });

    const ddbClient = new DynamoDBClient({
      region: awsRegion,
      credentials: {
        accessKeyId: awsAccessKeyId,
        secretAccessKey: awsSecretAccessKey,
      },
    });

    const docClient = DynamoDBDocumentClient.from(ddbClient);

    // Step 1: Create Selection record
    console.log("Creating selection record...");
    const selectionId = uuidv4();
    await createSelectionRecord(docClient, {
      selectionId,
      username,
      eventId,
      eventTitle,
      maxNumberOfPhotos,
    });

    // Step 2: Get images from input directory and extract metadata
    console.log("Reading images and extracting metadata...");
    const imageFiles = await getImageFilesWithMetadata(inputDir);
    
    if (imageFiles.length === 0) {
      console.log("No images found to process. Exiting.");
      rl.close();
      return;
    }

    console.log(`Found ${imageFiles.length} images to process`);

    // Step 3: Upload images to S3 and generate presigned URLs
    console.log("Uploading images to S3...");
    const uploadResults = await uploadImagesToS3(
      s3Client,
      inputDir,
      imageFiles,
      bucketName,
      username,
      eventId
    );

    console.log("Generating presigned URLs...");
    const imagesWithUrls = await generatePresignedUrls(
      s3Client,
      uploadResults,
      bucketName
    );

    // Step 4: Create SelectionItem records
    console.log("Creating selection item records...");
    await createSelectionItemRecords(
      docClient,
      imagesWithUrls,
      selectionId,
      eventId,
      username
    );

    // Step 5: Update Events table to set selectionAvailable = true
    console.log("Updating Events table...");
    await updateEventSelectionAvailable(docClient, eventId);

    console.log("Photo selection processing complete!");
    console.log(`Selection ID: ${selectionId}`);
    console.log(`Processed ${imageFiles.length} images`);
    
    rl.close();
  } catch (error) {
    console.error("Error in main function:", error);
    rl.close();
  }
};

// Get image files from directory with metadata extraction
const getImageFilesWithMetadata = async (inputDir) => {
  try {
    const files = fs
      .readdirSync(inputDir)
      .filter((file) => /\.(jpe?g|png|webp)$/i.test(file));
    
    const imageFiles = [];
    
    for (const file of files) {
      const filePath = path.join(inputDir, file);
      
      try {
        // Extract image metadata using Sharp
        const metadata = await sharp(filePath).metadata();
        
        imageFiles.push({
          fileName: file,
          width: metadata.width,
          height: metadata.height,
          format: metadata.format,
          size: metadata.size
        });
        
        console.log(`✔ Extracted metadata for ${file}: ${metadata.width}x${metadata.height}`);
      } catch (metadataError) {
        console.error(`✖ Failed to extract metadata for ${file}:`, metadataError);
        // Still include the file but without dimensions
        imageFiles.push({
          fileName: file,
          width: null,
          height: null,
          format: null,
          size: null
        });
      }
    }
    
    return imageFiles;
  } catch (error) {
    console.error("Error reading image files:", error);
    return [];
  }
};

// Create Selection record in DynamoDB
const createSelectionRecord = async (docClient, selectionData) => {
  try {
    const tableName = process.env.DYNAMODB_TABLE_SELECTION || "Selection";
    
    const selectionRecord = {
      selectionId: selectionData.selectionId,
      username: selectionData.username,
      eventId: selectionData.eventId,
      eventTitle: selectionData.eventTitle,
      maxNumberOfPhotos: selectionData.maxNumberOfPhotos,
      selectedNumberOfPhotos: 0,
      blocked: false,
      createdAt: new Date().toISOString(),
      updatedAt: null,
      selectedImages: []
    };

    const command = new PutCommand({
      TableName: tableName,
      Item: selectionRecord,
    });

    await docClient.send(command);
    console.log(`✔ Selection record created with ID: ${selectionData.selectionId}`);
    
    return selectionRecord;
  } catch (error) {
    console.error("Error creating selection record:", error);
    throw error;
  }
};

// Upload images to S3
const uploadImagesToS3 = async (
  s3Client,
  inputDir,
  imageFiles,
  bucketName,
  username,
  eventId
) => {
  const uploadResults = [];
  const uploadPromises = [];

  for (const imageFile of imageFiles) {
    const filePath = path.join(inputDir, imageFile.fileName);
    const fileContent = fs.readFileSync(filePath);
    const objectKey = `${username}/${eventId}/selection/${imageFile.fileName}`;

    // Determine content type
    const ext = path.extname(imageFile.fileName).toLowerCase();
    let contentType = "image/jpeg"; // default
    if (ext === ".png") contentType = "image/png";
    if (ext === ".webp") contentType = "image/webp";

    const command = new PutObjectCommand({
      Bucket: bucketName,
      Key: objectKey,
      Body: fileContent,
      ContentType: contentType,
    });

    uploadPromises.push(
      s3Client
        .send(command)
        .then(() => {
          console.log(`✔ Uploaded: ${imageFile.fileName} to ${objectKey}`);
          uploadResults.push({
            fileName: imageFile.fileName,
            objectKey: objectKey,
            contentType: contentType,
            size: fileContent.length,
            width: imageFile.width,
            height: imageFile.height,
            format: imageFile.format
          });
        })
        .catch((err) => {
          console.error(`✖ Failed to upload: ${imageFile.fileName}`, err);
        })
    );
  }

  await Promise.all(uploadPromises);
  return uploadResults;
};

// Generate presigned URLs for images
const generatePresignedUrls = async (s3Client, images, bucketName) => {
  const expiration = 7 * 24 * 60 * 60; // 7 days in seconds

  for (const image of images) {
    const command = new GetObjectCommand({
      Bucket: bucketName,
      Key: image.objectKey,
    });

    try {
      const url = await getSignedUrl(s3Client, command, {
        expiresIn: expiration,
      });
      image.presignedUrl = url;
      console.log(`✔ Generated presigned URL for: ${image.fileName}`);
    } catch (error) {
      console.error(`✖ Error generating presigned URL for ${image.fileName}:`, error);
    }
  }
  
  return images;
};

// Create SelectionItem records
const createSelectionItemRecords = async (
  docClient,
  images,
  selectionId,
  eventId,
  username
) => {
  try {
    const tableName = process.env.DYNAMODB_TABLE_SELECTION_ITEM || "SelectionItem";
    const promises = [];

    for (const image of images) {
      // Extract image name without extension for the primary key
      const imageName = path.basename(image.fileName, path.extname(image.fileName));
      
      const selectionItemRecord = {
        imageName: imageName,
        selectionId: selectionId,
        eventId: eventId,
        username: username,
        objectKey: image.objectKey,
        presignedUrl: image.presignedUrl,
        selected: false,
        imageWidth: image.width,
        imageHeight: image.height
      };

      const command = new PutCommand({
        TableName: tableName,
        Item: selectionItemRecord,
      });

      promises.push(
        docClient
          .send(command)
          .then(() => {
            console.log(`✔ Created selection item record for: ${imageName} (${image.width}x${image.height})`);
          })
          .catch((err) => {
            console.error(`✖ Failed to create selection item record for ${imageName}:`, err);
          })
      );
    }

    await Promise.all(promises);
    console.log(`✔ Created ${images.length} selection item records`);
  } catch (error) {
    console.error("Error creating selection item records:", error);
    throw error;
  }
};

// Update Events table to set selectionAvailable = true
const updateEventSelectionAvailable = async (docClient, eventId) => {
  try {
    const tableName = process.env.EVENTS_TABLE || "Events";

    const command = new UpdateCommand({
      TableName: tableName,
      Key: {
        eventId: eventId,
      },
      UpdateExpression: "SET selectionAvailable = :selectionAvailable",
      ExpressionAttributeValues: {
        ":selectionAvailable": true,
      },
    });

    await docClient.send(command);
    console.log(`✔ Updated event ${eventId} - selectionAvailable set to true`);
  } catch (error) {
    console.error(`✖ Failed to update Events table for eventId ${eventId}:`, error);
    throw error;
  }
};

// Run the main function
main().catch(console.error);