// Imports the aws-sdk and jszip dependencies from the attached lambda layer
import AWS from "aws-sdk";
import JSZip from "jszip";
import vm from "vm";

const dynamodb = new AWS.DynamoDB.DocumentClient();
const s3 = new AWS.S3();

const BUCKET_NAME = "lambda-dynamodb-source-bucket";
const bucket2 = "lambda-dynamodb-destination-bucket";
const TABLE_NAME = "Movies";

// Function to get the latest partition key
const getLatestPartitionKey = async () => {
  const params = {
    TableName: TABLE_NAME,
    ProjectionExpression: "Movie_ID",
  };
  try {
    const result = await dynamodb.scan(params).promise();
    if (result.Items && result.Items.length > 0) {
      return Math.max(...result.Items.map((item) => item.Movie_ID));
    }
    return 0; // Default to 0 if no items exist
  } catch (error) {
    console.error("Error fetching latest partition key:", error);
    throw error;
  }
};

export const handler = async (event) => {
  try {
    console.log(
      "New zip file detected in :",
      BUCKET_NAME,
      JSON.stringify(event, null, 2)
    );
    const bucketName = event.Records[0].s3.bucket.name;
    const key = decodeURIComponent(
      event.Records[0].s3.object.key.replace(/\+/g, " ")
    );

    // Retrieve and unzip the file
    const zipFile = await s3
      .getObject({ Bucket: bucketName, Key: key })
      .promise();
    const zipContent = await JSZip.loadAsync(zipFile.Body);

    let jsFile;
    let images = [];
    let videos = [];

    for (const filename of Object.keys(zipContent.files)) {
      const file = zipContent.files[filename];
      if (filename.endsWith(".js")) {
        const jsContent = await file.async("string");
        const context = {};
        vm.createContext(context);
        const script = new vm.Script(`${jsContent}; testArray;`);
        jsFile = script.runInContext(context);
      } else if (
        filename.includes("Poster_Images") &&
        (filename.endsWith(".png") ||
          filename.endsWith(".jpg") ||
          filename.endsWith(".jpeg"))
      ) {
        images.push({ file, filename });
      } else if (
        filename.includes("Movie_Videos") &&
        (filename.endsWith(".mp4") ||
          filename.endsWith(".mov") ||
          filename.endsWith(".avi"))
      ) {
        videos.push({ file, filename });
      }
    }
    console.log("Items in the javascript file: ", jsFile);

    if (!jsFile || (images.length === 0 && videos.length === 0)) {
      throw new Error(
        "No JavaScript file, image files, or video files detected in the zip file"
      );
    }

    // Fetch latest partition key
    const latestPartitionKey = await getLatestPartitionKey();
    console.log("Latest Partition Key found: ", latestPartitionKey);

    // Process each movie entry
    for (let i = 0; i < jsFile.length; i++) {
      const movie = jsFile[i];
      const movieFolder = movie.Name.replace(/\s+/g, "_");
      let imageUrls = [];
      let videoUrls = [];

      for (const { file, filename } of images) {
        if (filename.includes(movie.Name.replace(/\s+/g, "_"))) {
          const newImageKey = `${movieFolder}/${filename}`;
          const contentType = filename.endsWith(".jpg")
            ? "image/jpeg"
            : "image/png";

          // Upload to S3 and overwrite existing
          await s3
            .upload({
              Bucket: bucket2,
              Key: newImageKey,
              Body: await file.async("nodebuffer"),
              ContentType: contentType,
            })
            .promise();

          // Generates the private S3 URIs
          imageUrls.push(`s3://${bucket2}/${newImageKey}`);
        }
      }

      for (const { file, filename } of videos) {
        if (filename.includes(movie.Name.replace(/\s+/g, "_"))) {
          const newVideoKey = `${movieFolder}/${filename}`;
          const contentType = filename.endsWith(".mp4")
            ? "video/mp4"
            : "video/mov";

          // Upload to S3
          await s3
            .upload({
              Bucket: bucket2,
              Key: newVideoKey,
              Body: await file.async("nodebuffer"),
              ContentType: contentType,
            })
            .promise();

          // Generate private S3 URI
          videoUrls.push(`s3://${bucket2}/${newVideoKey}`);
        }
      }

      // Creates a new DynamoDB item
      const newItem = {
        Movie_ID: latestPartitionKey + i + 1,
        ...movie,
        Image_url: imageUrls,
        Video_url: videoUrls,
      };
      await dynamodb.put({ TableName: TABLE_NAME, Item: newItem }).promise();
    }

    return {
      statusCode: 200,
      body: JSON.stringify({ message: "Items processed successfully." }),
    };
  } catch (error) {
    console.error("Error processing S3 event:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({
        message: "Failed to process item.",
        error: error.message,
      }),
    };
  }
};
