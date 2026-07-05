package main

import (
	"fmt"
	"sync"
	"time"
)

type Event struct {
	ID        int
	Type      string
	Data      string
	Timestamp time.Time
}

type EventStore struct {
	mu            sync.Mutex
	Events        map[int]Event
	TypesMap      map[string][]int
	ElementsCount int
}

func NewEventStore() *EventStore {
	return &EventStore{
		Events:   make(map[int]Event),
		TypesMap: make(map[string][]int),
	}
}

func (es *EventStore) Add(eventType string, data string) int {
	es.mu.Lock()
	defer es.mu.Unlock()
	es.ElementsCount++
	es.TypesMap[eventType] = append(es.TypesMap[eventType], es.ElementsCount)

	es.Events[es.ElementsCount] = Event{
		ID:        es.ElementsCount,
		Type:      eventType,
		Timestamp: time.Now(),
		Data:      data,
	}

	return es.ElementsCount
}

func (es *EventStore) GetAll() []Event {
	var events []Event
	es.mu.Lock()
	defer es.mu.Unlock()
	for _, e := range es.Events {
		events = append(events, e)
	}
	return events
}

func (es *EventStore) GetByID(id int) (Event, bool) {
	es.mu.Lock()
	defer es.mu.Unlock()
	event, ok := es.Events[id]
	return event, ok
}

func (es *EventStore) Count() int {
	return es.ElementsCount
}

func (es *EventStore) GetByType(eventType string) []Event {
	var events []Event
	es.mu.Lock()
	defer es.mu.Unlock()
	if ids, ok := es.TypesMap[eventType]; ok {
		for _, id := range ids {
			events = append(events, es.Events[id])
		}
		return events
	}
	return nil
}

func (es *EventStore) FindAfter(timestamp time.Time) []Event {
	var events []Event
	es.mu.Lock()
	defer es.mu.Unlock()
	for i := 1; i <= es.ElementsCount; i++ {
		event, ok := es.Events[i]
		if ok {
			if event.Timestamp.After(timestamp) {
				events = append(events, event)
			}
		}
	}
	return events
}

func (es *EventStore) GetRange(startID, endID int) []Event {
	var events []Event
	if startID > endID {
		startID, endID = endID, startID
		//или return nil
	}
	es.mu.Lock()
	defer es.mu.Unlock()
	for i := startID; i <= endID; i++ {
		if e, ok := es.Events[i]; ok {
			events = append(events, e)
		}
	}
	return events
}

func (es *EventStore) Filter(predicate func(Event) bool) []Event {
	var events []Event
	es.mu.Lock()
	defer es.mu.Unlock()

	for i := 1; i <= es.ElementsCount; i++ {
		e, ok := es.Events[i]
		if ok {
			if predicate(e) {
				events = append(events, e)
			}
		}
	}
	return events
}

func main() {
	store := NewEventStore()

	//id1 := store.Add("user.login", "user: alice")
	//id2 := store.Add("user.logout", "user: alice")

	//if event, ok := store.GetByID(id1); ok {
	//	fmt.Printf("Event %d: %s - %s at %v\n",
	//		event.ID, event.Type, event.Data, event.Timestamp)
	//}
	//
	//if event, ok := store.GetByID(id2); ok {
	//	fmt.Printf("Event %d: %s - %s at %v\n",
	//		event.ID, event.Type, event.Data, event.Timestamp)
	//}
	_ = store.Add("user.login", "user: alice")
	_ = store.Add("user.logout", "user: alice")
	_ = store.Add("user.login", "user: bob")
	_ = store.Add("user.logout", "user: bob")
	_ = store.Add("test", "u1")
	_ = store.Add("test", "new")
	fmt.Printf("%v\n", store.GetRange(4, 4))
	//event, ok := store.GetByID(6)
	//fmt.Printf("event: %v, ok: %v\n", event, ok)
	fmt.Printf("Total events: %d\n", store.Count())
}
