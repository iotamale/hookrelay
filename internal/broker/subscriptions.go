package broker

func (b *Broker) Subscribe(topic string, sub Subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.subscribers[topic] = append(b.subscribers[topic], sub)
}

func (b *Broker) UnsubscribeByUrl(topic, targetURL string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	subs, exists := b.subscribers[topic]
	if !exists {
		return false
	}

	n := 0 // new size of subs array
	removed := false

	for _, s := range subs {
		if s.TargetURL != targetURL {
			subs[n] = s
			n++
		} else {
			removed = true
		}
	}

	if removed {
		// cut the array to fit only not-removed subs
		b.subscribers[topic] = subs[:n]
	}

	return removed
}

func (b *Broker) GetSubscribers(topic string) []Subscriber {
	b.mu.RLock()
	defer b.mu.RUnlock()

	subs := b.subscribers[topic]
	targets := make([]Subscriber, len(subs))
	copy(targets, subs)

	return targets
}
