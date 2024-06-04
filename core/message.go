package core_pubsub

type Message struct {
	topic     string
	data      string
	partition int
}

func CreateMessage(topic string, data string, partition int) *Message {
	return &Message{
		topic:     topic,
		data:      data,
		partition: partition,
	}
}
