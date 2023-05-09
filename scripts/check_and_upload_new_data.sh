s3_file_list=$(s3cmd ls s3://mlops-data/fraud-data/ | awk '{print $4}' | awk -F" +|/" '{print $NF}' | sort)
hdfs_file_list=$(hdfs dfs -ls -C /user/fraud-data/ | awk -F" +|/" '{print $NF}' | sort)

# Get first missing file
file_to_add=$(comm -23 <(echo "$s3_file_list" | sort) <(echo "$hdfs_file_list" | sort) | head -n 1)

if [ -z "$file_to_add" ]
then
    echo "No files to download"
else
    echo "Downloading $file_to_add"
    s3cmd get s3://mlops-data/fraud-data/$file_to_add ./$file_to_add
    hdfs dfs -put ./$file_to_add /user/fraud-data/
    rm ./$file_to_add
fi
