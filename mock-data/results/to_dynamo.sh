file="2022-06-29_23-09.txt"
while read line; do
    aws dynamodb put-item \
    --table-name measurements \
    --item "$line" \
    --return-consumed-capacity TOTAL
done < "${file}"