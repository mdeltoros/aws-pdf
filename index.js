const AWS = require('aws-sdk')
const hummus = require('hummus')
const memoryStreams = require('memory-streams')
const BUCKET_NAME = 'casestack-indigo-qa-indigo-upload'

var s3 = new AWS.S3({apiVersion: '2006-03-01'})

const generateDate = () => {
  const today = new Date()

  const year = today.getFullYear()
  const month = today.getMonth() + 1
  const day = today.getDate()

  return (month < 10 ? '0' + month : '' + month) + "-" + day + "-" + year
}



// List all objects from S3 Bucket 
const listObjects = (date) => {
  return s3.listObjects({
    Bucket: BUCKET_NAME,
    Delimiter: '/',
    Prefix: 'invoicesQueue/'
  }).promise()
}

// Filter all the objects that are not folders
const filterObjects = (objects) => {
  return objects.filter(object => object.Size > 0)
}

// Download all objects from S3 Bucket
const downloadObject = (object) => {
  return s3.getObject({
    Bucket: BUCKET_NAME, 
    Key: object.Key
  }).promise()
}

const appendPDF = (objects) => {
  const outStream = new memoryStreams.WritableStream()

  try {
    const pdfWriter = hummus.createWriterToModify(new hummus.PDFRStreamForBuffer(objects[0].Body), new hummus.PDFStreamForResponse(outStream));
    
    objects.forEach((object, index) => {
      index > 0 && pdfWriter.appendPDFPagesFromPDF(new hummus.PDFRStreamForBuffer(object.Body))
    })
  
    pdfWriter.end()
  
    var newBuffer = outStream.toBuffer();
    outStream.end();
  
    return newBuffer
  } catch (error) {
    outStream.end();
    throw new Error('Error during PDF combination: ' + error.message);
  }
}

const uploadObject = async (buffer) => {
  const key = 'temp/Merged.pdf'
    await s3.putObject({
        Body: buffer,
        Bucket: BUCKET_NAME,
        ACL: 'private',
        ContentType: 'application/pdf',
        Key: key,
        ContentDisposition: 'attachment'
      })
      .promise()

    return s3.getSignedUrl('getObject', {
      Bucket: BUCKET_NAME,
      Key: key,
      Expires: 900
    })
}

const runScript = async () => {
  const objects = filterObjects(await listObjects().then(response => response.Contents))

  const objectsDownloaded = objects.map(object => downloadObject(object))

  const dude = await Promise.all(objectsDownloaded)

  await uploadObject(appendPDF(dude))
   
  
}

console.log(generateDate())
// runScript()