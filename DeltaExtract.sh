#!/bin/bash

# Set the current date in the format ddmmyyyy
currentdate=$(date +'%d%m%Y')

# Temporary file to store the query results
ordertempfile=$(mktemp)
simastempfile=$(mktemp)

# Execute the query and save the results to the temporary file

PGPASSWORD=abc psql -h host -p 54320 -d db -U user -c "\COPY (SELECT s.id,s.sim,s.package_status,s.category_id,s.pool_id as sim_pool_id ,i.imsi , i.status as imsi_status,i.pool_id as imsi_pool_id, s.attributes as sim_attributes, i.phlr, i.region, i.shlr, s.order_attr_id FROM simas_db.sim s join simas_db.imsi i on s.imsi_id = i.id where s.created_dt::date = CURRENT_DATE - INTERVAL '1 day' or s.modified_dt::date = CURRENT_DATE - INTERVAL '1 day' ) TO STDOUT WITH (DELIMITER ',', FORMAT CSV, HEADER)"  > "$simastempfile" 
PGPASSWORD=abc psql -h host -p 54320 -d db -U user -c "\COPY (SELECT id, created_by, created_dt, modified_by, modified_dt, version, attributes, order_id   FROM simas_db.order_attributes_formatted   WHERE created_dt::date = CURRENT_DATE - INTERVAL '1 day'   OR modified_dt::date = CURRENT_DATE - INTERVAL '1 day' ) TO STDOUT WITH (DELIMITER ',', FORMAT CSV, HEADER)" > "$ordertempfile"

# Check if the file is empty (excluding the header)
if [[ $(wc -l < "$ordertempfile") -le 1 ]]; then
  # Create a file with only headers if no records found
  echo "id,created_by,created_dt,modified_by,modified_dt,version,attributes,order_id" > "$ordertempfile"
fi

if [[ $(wc -l < "$simastempfile") -le 1 ]]; then
  # Create a file with only headers if no records found
  echo "id,sim,package_status,category_id,sim_pool_id,imsi,imsi_status,imsi_pool_id,sim_attributes,phlr,region,shlr,order_attr_id" > "$simastempfile"
fi

# Push the file to S3
aws s3 cp "$ordertempfile" "s3://bucket/order_attributes_$currentdate.csv"
aws s3 cp "$simastempfile" s3://bucket/simas_data_$currentdate.csv
# Remove the temporary file
rm "$ordertempfile"
rm "$simastempfile"
#0 7 * * * /temp/DeltaExtract.sh  after crontab -e
