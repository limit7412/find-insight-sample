package cloudwatch

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
)

// InsightRepository is repo for cloudwatch insight.
type InsightRepository interface {
	FindLogByRange(logGroup string, from, to int64) ([]*Log, error)
}

type insightRepoImpl struct {
	cfg aws.Config
}

// NewInsightRepoImpl returns a repository implementation.
func NewInsightRepoImpl() (InsightRepository, error) {
	// Get aws default config
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		panic(err)
	}

	return &insightRepoImpl{
		cfg: cfg,
	}, nil
}

func (r *insightRepoImpl) sendQuery(logGroup, query string, from, to int64) (*cloudwatchlogs.GetQueryResultsOutput, error) {
	client := cloudwatchlogs.NewFromConfig(r.cfg)
	startQueryInput := &cloudwatchlogs.StartQueryInput{
		StartTime:     aws.Int64(from),
		EndTime:       aws.Int64(to),
		LogGroupNames: []string{logGroup},
		QueryString:   aws.String(query),
	}
	startQueryOutput, err := client.StartQuery(context.TODO(), startQueryInput)
	if err != nil {
		panic(err)
	}

	// wait end query on insight
	for {
		getQueryResultInput := &cloudwatchlogs.GetQueryResultsInput{
			QueryId: startQueryOutput.QueryId,
		}
		getQueryResultOutput, err := client.GetQueryResults(context.TODO(), getQueryResultInput)
		if err != nil {
			panic(err)
		}

		switch getQueryResultOutput.Status {
		case types.QueryStatusRunning, types.QueryStatusScheduled:
			time.Sleep(5 * time.Second)
		case types.QueryStatusComplete:
			return getQueryResultOutput, nil
		default:
			panic(err)
		}
	}
}

type Log struct {
	RequestId string
	Type      string
	Message   string
}

func (r *insightRepoImpl) FindLogByRange(logGroup string, from, to int64) ([]*Log, error) {
	query := `
		fields @timestamp, @requestId, @timestamp, @type, @message
			| sort @timestamp desc
	`

	res, err := r.sendQuery(logGroup, query, from, to)
	if err != nil {
		panic(err)
	}

	purchaseRequests := []*Log{}
	for _, results := range res.Results {
		purchaseRequest := &Log{}
		for _, result := range results {
			switch *result.Field {
			case "@requestId":
				purchaseRequest.RequestId = *result.Value
			case "@type":
				purchaseRequest.Type = *result.Value
			case "@message":
				purchaseRequest.Message = *result.Value
			default:
				continue
			}
		}
		purchaseRequests = append(purchaseRequests, purchaseRequest)
	}

	return purchaseRequests, nil
}
