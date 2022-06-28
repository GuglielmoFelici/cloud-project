number=$1
for (( i=1; i<number/100; i++ )); do
    ( for (( j=1; j<100; j++ )); do aws lambda invoke --function-name sensors --payload '{  }' response.json > /dev/null) &
    sleep $2
done