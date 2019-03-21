require('dotenv').config()
const AWS = require('aws-sdk')
const hummus = require('hummus')
const memoryStreams = require('memory-streams')
const BUCKET_NAME = process.env.BUCKET_NAME

var s3 = new AWS.S3({apiVersion: '2006-03-01'})

const generateDate = () => {
  const today = new Date()

  const year = today.getFullYear()
  const month = today.getMonth() + 1
  const day = today.getDate()

  return (month < 10 ? '0' + month : '' + month) + "-" + day + "-" + year
}

// List all objects from S3 Bucket 
const listInvoices = (date) => {
  return s3.listObjects({
    Bucket: BUCKET_NAME,
    Delimiter: '/invoicesQueue',
    Prefix: `invoicesQueue/${date}/`
  }).promise()
}

// Group Invoices by Customer
const groupInvoices = (objects) => {
  let invoicesByCustomer = {}
  let prefix = ""

  objects.forEach(object => {
    if (object.Size === 0) {
      invoicesByCustomer[object.Key] = []
      prefix = object.Key
    } else {
      if (object.Key.indexOf('__CompiledInvoices.pdf') === -1) {
        invoicesByCustomer[prefix].push(object)
      }
    }
  })

  return invoicesByCustomer
}

const promisifyInvoices = (invoicesByCustomer) => {
  let invoicesPromises = {}

  for (const key in invoicesByCustomer) {
    const invoices = invoicesByCustomer[key]
    
    if (invoices.length > 0) {
      invoicesPromises[key] = invoices.map(invoice => {
        return s3.getObject({
          Bucket: BUCKET_NAME, 
          Key: invoice.Key
        }).promise()
      })
    }
  }  

  return invoicesPromises
}

const downloadInvoices = async (invoicesByCustomer) => {
  let invoicesPromises = {}

  for (const key in invoicesByCustomer) { 
    invoicesPromises[key] = await Promise.all(invoicesByCustomer[key])
  }

  return invoicesPromises
}

const mergeInvoices = (downloadedInvoices) => {  
  let mergedInvoices = {}

  for (const key in downloadedInvoices) {
    const invoicesByCustomer = downloadedInvoices[key]

    const outStream = new memoryStreams.WritableStream()
  
    try {
      const pdfWriter = hummus.createWriterToModify(new hummus.PDFRStreamForBuffer(invoicesByCustomer[0].Body), new hummus.PDFStreamForResponse(outStream));
      
      invoicesByCustomer.forEach((invoice, index) => {
        index > 0 && pdfWriter.appendPDFPagesFromPDF(new hummus.PDFRStreamForBuffer(invoice.Body))
      })
    
      pdfWriter.end()
    
      var newBuffer = outStream.toBuffer();
      outStream.end();
    
      mergedInvoices[key] = newBuffer
    } catch (error) {
      outStream.end();
      throw new Error('Error during PDF merging: ' + error.message);
    }
  }

  return mergedInvoices
}

const uploadMergedInvoices = async (mergedInvoices) => {
  for (const key in mergedInvoices) {
    const pdfName = `${key}__CompiledInvoices.pdf`

    await s3.putObject({
      Body: mergedInvoices[key],
      Bucket: BUCKET_NAME,
      ACL: 'private',
      ContentType: 'application/pdf',
      Key: pdfName,
      ContentDisposition: 'attachment'
    }).promise()
  }
}

const runScript = async () => {
  const date = '03-16-2019' // Just for testing purposes. Use the generateDate() function

  const invoices = groupInvoices(await listInvoices(date).then(response => response.Contents))
 
  const promisifiedInvoices = promisifyInvoices(invoices)

  const downloadedInvoices = await downloadInvoices(promisifiedInvoices)

  const mergedInvoices = mergeInvoices(downloadedInvoices)

  await uploadMergedInvoices(mergedInvoices)
}

runScript()

// const deleteObject = () => {
//   s3.deleteObject({
//     Bucket: BUCKET_NAME,
//     Key: // key of the file you want to delete
//   })
//   .promise()
// }

// deleteObject()