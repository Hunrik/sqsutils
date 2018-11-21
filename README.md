# Another sqs cli utility tool

This project has no tests and it was thrown together quickly so be careful

## Install

`go get github.com/Hunrik/sqsutils`


## Usage

### To send messages to an sqs queue from a file

`sqsutils load ./file https://sqs.eu-central-1.amazonaws.com/123456789/test`

You can also format the messages during loading

```
75735492429828740
98935086832012740
88607231878692200
80069749431358050
```

`sqsutils load ./file https://sqs.eu-central-1.amazonaws.com/123456789/test  --format "{\"id\": %s}"`

```
{"id":75735492429828740}
{"id":98935086832012740}
{"id":88607231878692200}
{"id":80069749431358050}
```

### To persist sqs messages to a file
*It will remove the messages from the queue*

`sqsutils move https://sqs.eu-central-1.amazonaws.com/123456789/test ./out`

You can use the  `--regex` parameter to filter messages which you want to persist. (Which is not matched by the regex will fall back to the queue with 10 min visibility timeout)

`sqsutils move https://sqs.eu-central-1.amazonaws.com/123456789/test ./out --regex "data"`