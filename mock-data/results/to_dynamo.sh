TABLE_NAME="measurements"
read -p "Do you want to create the table? [y/n] " create
if [[ $create == 'y' ]]; then
    aws dynamodb create-table \
    --table-name $TABLE_NAME \
    --attribute-definitions \
    AttributeName=dynamo_id,AttributeType=S \
    --key-schema \
    AttributeName=dynamo_id,KeyType=HASH \
    --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1 \
    && echo "Table created"
fi

file="results.txt"
while read line; do
    aws dynamodb put-item \
    --table-name $TABLE_NAME \
    --item "$line" \
    --return-consumed-capacity TOTAL
done < "${file}"