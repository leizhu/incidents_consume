package main

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	json "github.com/bitly/go-simplejson"
	"github.com/bsm/sarama-cluster"
	elastic "gopkg.in/olivere/elastic.v5"
	"sync"
	"time"
)

// channelSize is the number of events Channel can buffer before blocking will occur.
const channelSize = 100

func init() {
}

type ElasticsearchOutput struct {
	Channel              chan *sarama.ConsumerMessage
	messages             []*sarama.ConsumerMessage // Events being held by the ElasticsearchOutput.
	workerNo             int
	KafkaConsumer        *cluster.Consumer
	flushSize            int
	flushIntervalSeconds int
	wg                   sync.WaitGroup
	esClient             *elastic.Client
	esCtx                context.Context
}

func NewElasticsearchOutput(size int, intervalSeconds int, consumer *cluster.Consumer, no int, url string, sniff bool) *ElasticsearchOutput {
	client, err := elastic.NewClient(elastic.SetURL(url), elastic.SetSniff(sniff))
	if err != nil {
		return nil
	}
	ctx := context.Background()
	info, code, err := client.Ping(url).Do(ctx)
	if err != nil {
		logrus.Infof("Elasticsearch returned with code %d and version %s", code, info.Version.Number)
		return nil
	}
	return &ElasticsearchOutput{
		Channel:              make(chan *sarama.ConsumerMessage, channelSize),
		messages:             make([]*sarama.ConsumerMessage, 0, size),
		KafkaConsumer:        consumer,
		flushSize:            size,
		flushIntervalSeconds: intervalSeconds,
		workerNo:             no,
		esClient:             client,
		esCtx:                ctx,
	}
}

func (e *ElasticsearchOutput) Start() {
	e.wg.Add(1)
	go e.run()
}

type doc struct {
	Content   string    `json:"content"`
	Timestamp time.Time `json:"@timestamp"`
}

func parseMsgContent(content []byte) (interface{}, string, string) {
	js, _ := json.NewJson(content)
	tenant := js.Get("tenant").MustString()
	t := time.Now()
	js.Set("incidentTime", t.Unix()*1000)
	js.Set("localeIncidentTime", t.Format("2006-01-02 15:04:05"))
	return js.Interface(), tenant, "network"
}

func (e *ElasticsearchOutput) flush() int {
	count := len(e.messages)
	if count == 0 {
		return 0
	}

	t := time.Now()

	// copy buffer
	tmpCopy := make([]*sarama.ConsumerMessage, count)
	copy(tmpCopy, e.messages)
	// clear buffer
	e.messages = e.messages[:0]

	logrus.Debugf("---copy messages duration (%s) ", time.Now().Sub(t).String())
	t = time.Now()

	// send batched events to es
	offsetStash := cluster.NewOffsetStash()
	//bulk := e.esClient.Bulk().Index("test").Type("test")
	bulk := e.esClient.Bulk()
	for _, msg := range tmpCopy {
		//logrus.Debugf("[worker-%d] %s/%d/%d\t%s", e.workerNo, msg.Topic, msg.Partition, msg.Offset, msg.Value)
		offsetStash.MarkOffset(msg, "")
		//d := doc{
		//	Content:   fmt.Sprintf("%s", msg.Value),
		//	Timestamp: time.Now(),
		//}
		//req := elastic.NewBulkIndexRequest().Doc(d)
		d, tenant, docType := parseMsgContent(msg.Value)
		bulk.Add(elastic.NewBulkIndexRequest().Index("incidents-" + tenant).Type(docType).Doc(d))
	}

	logrus.Debugf("---parse/add incidents to bulk queue duration (%s) ", time.Now().Sub(t).String())
	t = time.Now()

	res, err := bulk.Do(e.esCtx)
	if err == nil && !res.Errors {
		logrus.Infof("bulk commit success, bulk size is %d", count)
		e.KafkaConsumer.MarkOffsets(offsetStash)
	} else {
		logrus.Error("bulk commit failed")
	}

	logrus.Debugf("---es bulk action duration (%s) ", time.Now().Sub(t).String())

	return count
}

func (e *ElasticsearchOutput) queue(event *sarama.ConsumerMessage) bool {
	flushed := false
	e.messages = append(e.messages, event)
	if len(e.messages) == cap(e.messages) {
		logrus.Infof("[worker-%d] Flushing because queue is full. Events flushed: %d", e.workerNo, len(e.messages))
		e.flush()
		flushed = true
	}
	return flushed
}

func (e *ElasticsearchOutput) run() {
	logrus.Infof("[worker-%d] Starting ElasticsearchOutput: FlushSize: %d; FlushIntervalSeconds: %d", e.workerNo, e.flushSize, e.flushIntervalSeconds)

	defer e.flush()
	defer e.wg.Done()

	timer := time.NewTimer(time.Duration(e.flushIntervalSeconds) * time.Second)
	defer timer.Stop()

	for {
		select {
		case event, ok := <-e.Channel:
			if !ok {
				time.Sleep(time.Duration(1) * time.Second)
				return
			}
			if event != nil {
				flushed := e.queue(event)
				if flushed {
					// Stop timer and drain channel. See https://golang.org/pkg/time/#Timer.Reset
					if !timer.Stop() {
						<-timer.C
					}
					timer.Reset(time.Duration(e.flushIntervalSeconds) * time.Second)
				}
			}
		case <-timer.C:
			logrus.Debugf("[worker-%d] Flushing buffered incidents because of timeout. Events flushed: %v", e.workerNo, len(e.messages))
			e.flush()
			timer.Reset(time.Duration(e.flushIntervalSeconds) * time.Second)
		}
	}
}

func (e *ElasticsearchOutput) Stop() {
	logrus.Infof("[worker-%d] Stopping ElasticsearchOutput...", e.workerNo)

	// Signal to the run method that it should stop.
	// Stop accepting writes. Any events in the channel will be flushed.
	close(e.Channel)

	// Wait for spooler shutdown to complete.
	e.wg.Wait()
	logrus.Infof("[worker-%d] ElasticsearchOutput has stopped", e.workerNo)
}
