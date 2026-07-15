// Package chat provides an in-process recent-message cache built on cache233.
package chat

import (
	"errors"
	"sync"
	"time"

	cache233 "github.com/neko233-com/cache233-go"
)

var ErrInvalidCapacity = errors.New("chat: capacities must be greater than zero")

// Message is a cached message. ID makes Append idempotent.
type Message[ID comparable, Payload any] struct {
	ID        ID
	Sender    string
	Payload   Payload
	CreatedAt time.Time
}

// Config exposes business-specific extension points without leaking Store internals.
// ConfigureCache can enable stats, custom expiry, deletion listeners, weights,
// refresh behavior, and executors available from cache233.Options.
type Config[Conversation comparable, ID comparable, Payload any] struct {
	ConversationCapacity    int
	MessagesPerConversation int
	TTL                     time.Duration
	ConfigureCache          func(*cache233.Options[Conversation, []Message[ID, Payload]])
	Accept                  func(Conversation, Message[ID, Payload]) bool
	OnAppend                func(Conversation, Message[ID, Payload], bool)
}

// Store bounds both the number of conversations and messages per conversation.
// Conversation eviction uses cache233's adaptive W-TinyLFU policy.
type Store[Conversation comparable, ID comparable, Payload any] struct {
	mu                      sync.Mutex
	messagesPerConversation int
	conversations           *cache233.Cache[Conversation, []Message[ID, Payload]]
	accept                  func(Conversation, Message[ID, Payload]) bool
	onAppend                func(Conversation, Message[ID, Payload], bool)
}

func New[Conversation comparable, ID comparable, Payload any](conversationCapacity, messagesPerConversation int, ttl time.Duration) (*Store[Conversation, ID, Payload], error) {
	return NewWithConfig(Config[Conversation, ID, Payload]{ConversationCapacity: conversationCapacity, MessagesPerConversation: messagesPerConversation, TTL: ttl})
}

// NewWithConfig constructs a store with optional application hooks and cache options.
func NewWithConfig[Conversation comparable, ID comparable, Payload any](config Config[Conversation, ID, Payload]) (*Store[Conversation, ID, Payload], error) {
	if config.ConversationCapacity <= 0 || config.MessagesPerConversation <= 0 {
		return nil, ErrInvalidCapacity
	}
	options := &cache233.Options[Conversation, []Message[ID, Payload]]{MaximumSize: config.ConversationCapacity}
	if config.TTL > 0 {
		options.ExpiryCalculator = cache233.ExpiryWriting[Conversation, []Message[ID, Payload]](config.TTL)
	}
	if config.ConfigureCache != nil {
		config.ConfigureCache(options)
	}
	cache, err := cache233.New(options)
	if err != nil {
		return nil, err
	}
	return &Store[Conversation, ID, Payload]{messagesPerConversation: config.MessagesPerConversation, conversations: cache, accept: config.Accept, onAppend: config.OnAppend}, nil
}

// Append inserts or replaces by message ID. It returns false when Accept rejects it.
func (s *Store[Conversation, ID, Payload]) Append(conversation Conversation, message Message[ID, Payload]) bool {
	if s.accept != nil && !s.accept(conversation, message) {
		return false
	}
	s.mu.Lock()
	messages, _ := s.conversations.GetIfPresent(conversation)
	replaced := false
	for index := range messages {
		if messages[index].ID == message.ID {
			messages[index] = message
			s.conversations.Set(conversation, messages)
			replaced = true
			s.mu.Unlock()
			if s.onAppend != nil {
				s.onAppend(conversation, message, replaced)
			}
			return true
		}
	}
	messages = append(messages, message)
	if len(messages) > s.messagesPerConversation {
		messages = messages[len(messages)-s.messagesPerConversation:]
	}
	s.conversations.Set(conversation, messages)
	s.mu.Unlock()
	if s.onAppend != nil {
		s.onAppend(conversation, message, replaced)
	}
	return true
}

// Recent returns up to limit messages, oldest first.
func (s *Store[Conversation, ID, Payload]) Recent(conversation Conversation, limit int) []Message[ID, Payload] {
	s.mu.Lock()
	defer s.mu.Unlock()
	messages, ok := s.conversations.GetIfPresent(conversation)
	if !ok || limit <= 0 {
		return nil
	}
	if limit < len(messages) {
		messages = messages[len(messages)-limit:]
	}
	return append([]Message[ID, Payload](nil), messages...)
}
func (s *Store[Conversation, ID, Payload]) Invalidate(conversation Conversation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conversations.Invalidate(conversation)
}
func (s *Store[Conversation, ID, Payload]) CleanUp() { s.conversations.CleanUp() }
