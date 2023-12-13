#!/bin/bash

# Your database connection details
DB_HOST="host"
DB_USER="user"
DB_NAME="db"


# PL/pgSQL block as a string
QUERY=$(cat <<EOF
DO \$\$

BEGIN
    INSERT INTO tablea
(created_by, created_dt, modified_by, modified_dt, "version", "attributes", order_id)
SELECT 'admin',
       '2023-12-08 10:32:19.684285'::timestamp,
       NULL,
       NULL,
       0,
       attributes,
       order_id
FROM (
    SELECT DISTINCT ON (attributes->>'Order ID') 
    jsonb_build_object(
        'SIM Artwork', attributes->'SIM Artwork',
		'Is Ordered', attributes->'Is Ordered',
		'Region', attributes->'Region'        
    ) AS attributes,
    attributes->>'Order ID' AS order_id
FROM tableb WHERE id BETWEEN \$1 AND \$2
ORDER BY attributes->>'Order ID', id
) AS subquery
ON CONFLICT (order_id) DO NOTHING;
END \$\$;
EOF
)

# Function to execute the migration in parallel
execute_parallel_migration() {
    local parallel_processes=$1
    local from_=$2

    # Run the migration function in parallel with different parameters
    for ((i = 1; i <= parallel_processes; i++)); do
        #local offset=$(( (i - 1) * total_rows ))
		#local from=197
		local to=$(( from_ + 100000 ))
        echo "id from $from_  to  $to"
                # Replace \$1 with the actual offset value
                #local replaced_query=$(echo "$QUERY" | sed "s/\\\$1/$offset/")
				local replaced_query=$(echo "$QUERY" | sed -e "s/\\\$1/$from_/" -e "s/\\\$2/$to/")
                #echo "$replaced_query"
        PGPASSWORD=password psql -h $DB_HOST -U $DB_USER -d $DB_NAME -p 54321 -c "$replaced_query" &
    done

    # Wait for all background processes to finish
    wait

    current_time=$(date +"%T")
    echo "Order attr populated: $current_time"
}

# Number of parallel processes
parallel_processes=1
from_=100197

# Run the migration at intervals of 30 minutes for a total duration of 13 hours
total_duration_hours=723
interval_minutes=2

for ((hour = 1; hour <= total_duration_hours; hour++)); do
    from_=$(( 100000 + from_ ))
	execute_parallel_migration $parallel_processes $from_
    #sleep $((interval_minutes * 60))  # Convert minutes to seconds for sleep
done
