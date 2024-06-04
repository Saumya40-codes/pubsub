package core_pubsub

type Message struct {
	topic string
	data  string
}

func CreateMessage(topic string, data string) *Message {
	return &Message{
		topic: topic,
		data:  data,
	}
}
