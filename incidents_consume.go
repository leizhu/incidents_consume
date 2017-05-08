package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
)

var (
	groupID              = flag.String("group", "", "REQUIRED: The shared consumer group name")
	brokerList           = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The comma separated list of brokers in the Kafka cluster")
	topicList            = flag.String("topics", "", "REQUIRED: The comma separated list of topics to consume")
	offset               = flag.String("offset", "newest", "The offset to start with. Can be `oldest`, `newest`")
	verbose              = flag.Bool("verbose", false, "Whether to turn on sarama logging")
	workers              = flag.Int("workers", 1, "How many consumers will be started")
	flushSize            = flag.Int("flushSize", 100, "Flush size of es output")
	flushIntervalSeconds = flag.Int("flushIntervalSeconds", 10, "Flush Interval Seconds of es output")

	logger = log.New(os.Stderr, "", log.LstdFlags)
)

type Worker struct {
	No            int
	wg            *sync.WaitGroup
	KafkaConsumer *cluster.Consumer
	esOutput      *ElasticsearchOutput
}

func NewWorker(num int, w *sync.WaitGroup, brokerList *string, groupID *string, topicList *string, config *cluster.Config) *Worker {
	worker := &Worker{No: num, wg: w}
	consumer, err := cluster.NewConsumer(strings.Split(*brokerList, ","), *groupID, strings.Split(*topicList, ","), config)
	if err != nil {
		logger.Fatalln(fmt.Sprintf("Failed to start consumer-%d, %s", num, err.Error()))
		w.Done()
		return nil
	}
	worker.KafkaConsumer = consumer
	worker.esOutput = NewElasticsearchOutput(*flushSize, *flushIntervalSeconds, consumer, num)
	return worker
}

func (worker *Worker) run() {
	defer worker.wg.Done()

	// start es output
	worker.esOutput.Start()

	defer func() {
		worker.esOutput.Stop()
		worker.KafkaConsumer.Close()
		logger.Println("Closing consumer " + fmt.Sprintf("%d", worker.No))
	}()

	// Create signal channel
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	// Consume all channels, wait for signal to exit
	for {
		select {
		case msg, more := <-worker.KafkaConsumer.Messages():
			if more {
				worker.esOutput.Channel <- msg
				//logger.Println(fmt.Sprintf("[consumer-%d] %s/%d/%d\t%s", worker.No, msg.Topic, msg.Partition, msg.Offset, msg.Value))
				//worker.KafkaConsumer.MarkOffset(msg, "")
				//err := worker.KafkaConsumer.CommitOffsets()
				//if err != nil {
				//	fmt.Fprintf(os.Stderr, "ERROR: "+err.Error())
				//}
			}
		case ntf, more := <-worker.KafkaConsumer.Notifications():
			if more {
				logger.Printf("Rebalanced: %+v\n", ntf)
			}
		case err, more := <-worker.KafkaConsumer.Errors():
			if more {
				logger.Printf("Error: %s\n", err.Error())
			}
		case <-sigchan:
			return
		}
	}
}

func main() {
	flag.Parse()

	if *groupID == "" {
		printUsageErrorAndExit("You have to provide a -group name.")
	} else if *brokerList == "" {
		printUsageErrorAndExit("You have to provide -brokers as a comma-separated list, or set the KAFKA_PEERS environment variable.")
	} else if *topicList == "" {
		printUsageErrorAndExit("You have to provide -topics as a comma-separated list.")
	}
	// Init config
	config := cluster.NewConfig()
	if *verbose {
		sarama.Logger = logger
	} else {
		config.Consumer.Return.Errors = true
		config.Group.Return.Notifications = true
	}
	switch *offset {
	case "oldest":
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	case "newest":
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	default:
		printUsageErrorAndExit("-offset should be `oldest` or `newest`")
	}

	var wg sync.WaitGroup
	for i := 1; i <= *workers; i++ {
		wg.Add(1)
		go func(no int) {
			worker := NewWorker(no, &wg, brokerList, groupID, topicList, config)
			if worker != nil {
				worker.run()
			}
		}(i)
	}
	wg.Wait()
	logger.Println(">>>>>Exit<<<<<")
}

func printUsageErrorAndExit(format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: "+format+"\n\n", values...)
	flag.Usage()
	os.Exit(64)
}
