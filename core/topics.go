package core_pubsub

type Topic struct {
	name       string
	partitions int
}

func CreateTopic(name string, partitions int) *Topic {
	return &Topic{
		name:       name,
		partitions: partitions,
	}
}
