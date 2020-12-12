echo "Copying lambda functions..."

cp s3proc.py ../textract-pipeline/lambda/s3_processor/main.py
cp s3batchproc.py ../textract-pipeline/lambda/s3_batch_processor/main.py
cp docproc.py ../textract-pipeline/lambda/document_processor/main.py
cp syncproc.py ../textract-pipeline/lambda/sync_processor/main.py
cp asyncproc.py ../textract-pipeline/lambda/async_processor/main.py
cp jobresultsproc.py ../textract-pipeline/lambda/job_result_processor/main.py

cp helper.py ../textract-pipeline/layers/helper/python/helper.py
cp datastore.py ../textract-pipeline/layers/helper/python/datastore.py
cp trp.py ../textract-pipeline/layers/textractor/python/trp.py
cp og.py ../textract-pipeline/layers/textractor/python/og.py

echo "Done!"
