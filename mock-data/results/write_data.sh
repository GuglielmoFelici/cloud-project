TABLE_NAME="measurements6"
file="results.txt"
while read line; do
    aws dynamodb put-item \
    --table-name $TABLE_NAME \
    --item "$line" \
    --return-consumed-capacity TOTAL || exit
done < "${file}"