#!/bin/bash

# Your database connection details
DB_HOST="host"
DB_USER="user"
DB_NAME="db"


# PL/pgSQL block as a string
QUERY=$(cat <<EOF
DO \$\$
DECLARE
    TABLE_RECORD RECORD;
    chunk_size INTEGER := 30000;
    offset_val INTEGER := 0;
    total_rows INTEGER = 30000;
BEGIN
    WHILE offset_val < total_rows LOOP
        FOR TABLE_RECORD IN (
            SELECT rr_id, imsi_id, sim_id
            FROM public.simas_resource_relationship
            WHERE updated_by IS NULL
            ORDER BY rr_id
            LIMIT chunk_size OFFSET \$1  -- Using \$1 as the offset parameter
        )
        LOOP
            INSERT INTO simas.imsi (created_by, created_dt, modified_by, modified_dt, version, imsi, status, pool_id, rr_id)
            SELECT
                created_by,
                resource_id,
                resource_lifecycle_status_id,
                resource_pool_id,
                TABLE_RECORD.rr_id
            FROM public.simas_imsi
            WHERE imsi_id = TABLE_RECORD.imsi_id;

            INSERT INTO simas.sim (created_by, created_dt, modified_by, modified_dt, version, last_activity_date, last_activity_name, msisdn, package_status, sim, category_id, pool_id, rr_id)
            SELECT
                created_by,
                created_date,
                updated_by,
                updated_date,
                0,
                null,
                null,
                null,
                resource_lifecycle_status_id,
                resource_id,
                resource_type_id,
                resource_pool_id,
                TABLE_RECORD.rr_id
            FROM public.simas_sim
            WHERE sim_id = TABLE_RECORD.sim_id;

            UPDATE public.simas_resource_relationship SET updated_by = 'Data Migrated' WHERE rr_id = TABLE_RECORD.rr_id;
        END LOOP;
        COMMIT;
        offset_val := offset_val + chunk_size;
    END LOOP;
END \$\$;
EOF
)

# Function to execute the migration in parallel
execute_parallel_migration() {
    local parallel_processes=$1
    local total_rows=$2

    # Run the migration function in parallel with different parameters
    for ((i = 1; i <= parallel_processes; i++)); do
        local offset=$(( (i - 1) * total_rows ))
        echo "Executing query with offset: $offset"
                # Replace \$1 with the actual offset value
                local replaced_query=$(echo "$QUERY" | sed "s/\\\$1/$offset/")
                #echo "$replaced_query"
        PGPASSWORD=password psql -h $DB_HOST -U $DB_USER -d $DB_NAME -p 54321 -c "$replaced_query" &
    done

    # Wait for all background processes to finish
    wait

    current_time=$(date +"%T")
    echo "Migration completed for all processes at: $current_time"
}

# Number of parallel processes
parallel_processes=5
total_rows=30000

# Run the migration at intervals of 30 minutes for a total duration of 13 hours
total_duration_hours=31
interval_minutes=2

for ((hour = 1; hour <= total_duration_hours; hour++)); do
    execute_parallel_migration $parallel_processes $total_rows
    #sleep $((interval_minutes * 60))  # Convert minutes to seconds for sleep
done
