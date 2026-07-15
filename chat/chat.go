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

// Store bounds both the number of conversations and messages per conversation.
// Conversation eviction uses cache233's adaptive W-TinyLFU policy.
type Store[Conversation comparable, ID comparable, Payload any] struct {
	mu                      sync.Mutex
	messagesPerConversation int
	conversations           *cache233.Cache[Conversation, []Message[ID, Payload]]
}

func New[Conversation comparable, ID comparable, Payload any](conversationCapacity, messagesPerConversation int, ttl time.Duration) (*Store[Conversation, ID, Payload], error) {
	if conversationCapacity <= 0 || messagesPerConversation <= 0 {
		return nil, ErrInvalidCapacity
	}
	options := &cache233.Options[Conversation, []Message[ID, Payload]]{MaximumSize: conversationCapacity}
	if ttl > 0 {
		options.ExpiryCalculator = cache233.ExpiryWriting[Conversation, []Message[ID, Payload]](ttl)
	}
	cache, err := cache233.New(options)
	if err != nil {
		return nil, err
	}
	return &Store[Conversation, ID, Payload]{messagesPerConversation: messagesPerConversation, conversations: cache}, nil
}

// Append inserts or replaces by message ID.
func (s *Store[Conversation, ID, Payload]) Append(conversation Conversation, message Message[ID, Payload]) {
	s.mu.Lock()
	defer s.mu.Unlock()
	messages, _ := s.conversations.GetIfPresent(conversation)
	for index := range messages {
		if messages[index].ID == message.ID {
			messages[index] = message
			s.conversations.Set(conversation, messages)
			return
		}
	}
	messages = append(messages, message)
	if len(messages) > s.messagesPerConversation {
		messages = messages[len(messages)-s.messagesPerConversation:]
	}
	s.conversations.Set(conversation, messages)
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
