package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/gorilla/mux"
)


type MovieEvent struct {
	MovieID     int      `json:"movie_id"`
	Title       string   `json:"title"`
	Action      string   `json:"action"`
	UserID      *int     `json:"user_id,omitempty"`
	Rating      *float64 `json:"rating,omitempty"`
	Genres      []string `json:"genres,omitempty"`
	Description string   `json:"description,omitempty"`
}

type UserEvent struct {
	UserID    int       `json:"user_id"`
	Username  string    `json:"username,omitempty"`
	Email     string    `json:"email,omitempty"`
	Action    string    `json:"action"`
	Timestamp time.Time `json:"timestamp"`
}

type PaymentEvent struct {
	PaymentID  int       `json:"payment_id"`
	UserID     int       `json:"user_id"`
	Amount     float64   `json:"amount"`
	Status     string    `json:"status"`
	Timestamp  time.Time `json:"timestamp"`
	MethodType string    `json:"method_type,omitempty"`
}

type Event struct {
	ID        string      `json:"id"`
	Type      string      `json:"type"`
	Timestamp time.Time   `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}

type EventResponse struct {
	Status    string      `json:"status"`
	Partition int32       `json:"partition"`
	Offset    int64       `json:"offset"`
	Event     Event       `json:"event"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}


var (
	kafkaProducer sarama.SyncProducer
	kafkaConsumer sarama.Consumer
)

func main() {
	brokers := []string{os.Getenv("KAFKA_BROKERS")}
	if brokers[0] == "" {
		brokers = []string{"kafka:9092"}
	}

    config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to init Kafka producer: %v", err)
	}
	kafkaProducer = producer
	defer kafkaProducer.Close()

	consumer, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		log.Fatalf("Failed to init Kafka consumer: %v", err)
	}
	kafkaConsumer = consumer
	defer kafkaConsumer.Close()


	topics := []string{"movie-events", "user-events", "payment-events"}
	for _, topic := range topics {
		partitions, err := kafkaConsumer.Partitions(topic)
		if err != nil {
			log.Printf("Can't get partitions for %s: %v", topic, err)
			continue
		}
		for _, p := range partitions {
			go consumeKafka(topic, p)
		}
	}

	r := mux.NewRouter()
	r.HandleFunc("/api/events/health", healthHandler).Methods("GET")
	r.HandleFunc("/api/events/movie", movieEventHandler).Methods("POST")
	r.HandleFunc("/api/events/user", userEventHandler).Methods("POST")
	r.HandleFunc("/api/events/payment", paymentEventHandler).Methods("POST")

	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}
	srv := &http.Server{Addr: ":" + port, Handler: r}

	go func() {
		log.Printf("Events service started on port %s", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down events service...")
	srv.Shutdown(context.Background())
}


func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}


func movieEventHandler(w http.ResponseWriter, r *http.Request) {
	var ev MovieEvent
	if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
		writeError(w, "Некорректный запрос: "+err.Error(), http.StatusBadRequest)
		return
	}
	if ev.MovieID == 0 || ev.Title == "" || ev.Action == "" {
		writeError(w, "Некорректный запрос: отсутствуют обязательные поля", http.StatusBadRequest)
		return
	}
	event := Event{
		ID:        fmt.Sprintf("movie-%d-%s", ev.MovieID, ev.Action),
		Type:      "movie",
		Timestamp: time.Now().UTC(),
		Payload:   ev,
	}
	produceEvent(w, "movie-events", event)
}


func userEventHandler(w http.ResponseWriter, r *http.Request) {
	var ev UserEvent
	if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
		writeError(w, "Некорректный запрос: "+err.Error(), http.StatusBadRequest)
		return
	}
	if ev.UserID == 0 || ev.Action == "" {
		writeError(w, "Некорректный запрос: отсутствуют обязательные поля", http.StatusBadRequest)
		return
	}

	if ev.Timestamp.IsZero() {
		ev.Timestamp = time.Now().UTC()
	}
	event := Event{
		ID:        fmt.Sprintf("user-%d-%s", ev.UserID, ev.Action),
		Type:      "user",
		Timestamp: time.Now().UTC(),
		Payload:   ev,
	}
	produceEvent(w, "user-events", event)
}


func paymentEventHandler(w http.ResponseWriter, r *http.Request) {
	var ev PaymentEvent
	if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
		writeError(w, "Некорректный запрос: "+err.Error(), http.StatusBadRequest)
		return
	}
	if ev.PaymentID == 0 || ev.UserID == 0 || ev.Status == "" || ev.Amount == 0 {
		writeError(w, "Некорректный запрос: отсутствуют обязательные поля", http.StatusBadRequest)
		return
	}
	if ev.Timestamp.IsZero() {
		ev.Timestamp = time.Now().UTC()
	}
	event := Event{
		ID:        fmt.Sprintf("payment-%d-%s", ev.PaymentID, ev.Status),
		Type:      "payment",
		Timestamp: time.Now().UTC(),
		Payload:   ev,
	}
	produceEvent(w, "payment-events", event)
}


func produceEvent(w http.ResponseWriter, topic string, event Event) {
	eventBytes, err := json.Marshal(event)
	if err != nil {
		writeError(w, "Ошибка сериализации", http.StatusInternalServerError)
		return
	}
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(eventBytes),
	}
	partition, offset, err := kafkaProducer.SendMessage(msg)
	if err != nil {
		writeError(w, "Ошибка Kafka: "+err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("Event sent to Kafka topic=%s partition=%d offset=%d: %+v", topic, partition, offset, event)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	resp := EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     event,
	}
	json.NewEncoder(w).Encode(resp)
}

func writeError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(ErrorResponse{Error: msg})
}


func consumeKafka(topic string, partition int32) {
	pc, err := kafkaConsumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
	if err != nil {
		log.Printf("Kafka consume error (%s:%d): %v", topic, partition, err)
		return
	}
	defer pc.Close()
	for msg := range pc.Messages() {
		log.Printf("[KAFKA][%s] Consumed: %s", topic, string(msg.Value))
	}
}
