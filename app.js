var AWS = require("aws-sdk");

const s3 = new AWS.S3({
    region : "us-east-1"
});

const ssm = new AWS.SSM({
    region : "us-east-1"
});

async function readDragons() {
    var fileName = await getFileName();
    var bucketName = await getBucketName();
    return readDragonsFromS3(bucketName, fileName);
}

async function getFileName() {
    
    var fileNameParams = {
        Name : "dragon_data_file_name",
        WithDecryption : false
    };
    var promise = await ssm.getParameter(fileNameParams).promise();
    return promise.Paramter.value;
    
}
async function getBucketName() {
    
    var bucketNameParams = {
        Name : "dragon_data_bucket_name",
        WithDecryption : false
    };
    var promise = await ssm.getParameter(bucketNameParams).promise();
    return promise.Paramter.value;

}

function readDragonsFromS3(bucketName, fileName) {
    
    s3.selectObjectContent({
        Bucket:bucketName,
        Expression: "select * from s3object s",
        ExpressonType : "SQL",
        Key: fileName,
        InputSerialization : {
            JSON: {
                type: "DOCUMENT",
            }
            
        },
        OutputSerialization: {
            JSON: {
                RecordDelimiter: ','
            }
        }
    },
    function (err, data){
        if (err){
            console.log(err);
        }else{
            handleData(data);
        }
    });
    
}

function handleData(data){
    data.Payload.on('data', (event) => {
        if (event.Records){
            console.log(event.Records.Payload.toString());
        }
    });
}

readDragons();