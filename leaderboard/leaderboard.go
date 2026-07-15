// Package leaderboard provides a bounded, concurrent score-board read model.
// Keep the durable score in a database or stream; Board is optimized for fast
// in-process ranking reads.
package leaderboard

import (
	"errors"
	"math"
	"sort"
	"sync"
)

var (
	ErrInvalidCapacity = errors.New("leaderboard: capacity must be greater than zero")
	ErrInvalidScore    = errors.New("leaderboard: score must not be NaN")
)

// Rank is a member score and its one-based position.
type Rank[K comparable] struct {
	Member   K
	Score    float64
	Position int
}

// Change describes a successful score mutation and whether it remains in the board.
type Change[K comparable] struct {
	Member   K
	Score    float64
	Retained bool
}

// Config provides normalization and event hooks for domain rules such as score
// caps, season multipliers, audit events, or external leaderboard replication.
type Config[K comparable] struct {
	Capacity  int
	Normalize func(K, float64) (float64, error)
	OnChange  func(Change[K])
}

// Board retains at most the highest-scoring capacity members.
type Board[K comparable] struct {
	mu        sync.RWMutex
	capacity  int
	scores    map[K]float64
	normalize func(K, float64) (float64, error)
	onChange  func(Change[K])
}

func New[K comparable](capacity int) (*Board[K], error) {
	return NewWithConfig(Config[K]{Capacity: capacity})
}

func NewWithConfig[K comparable](config Config[K]) (*Board[K], error) {
	if config.Capacity <= 0 {
		return nil, ErrInvalidCapacity
	}
	return &Board[K]{capacity: config.Capacity, scores: make(map[K]float64), normalize: config.Normalize, onChange: config.OnChange}, nil
}

// Set assigns a score. Members outside the top capacity are not retained.
func (b *Board[K]) Set(member K, score float64) error {
	if b.normalize != nil {
		var err error
		score, err = b.normalize(member, score)
		if err != nil {
			return err
		}
	}
	if math.IsNaN(score) {
		return ErrInvalidScore
	}
	b.mu.Lock()
	b.scores[member] = score
	b.trimLocked()
	_, retained := b.scores[member]
	b.mu.Unlock()
	if b.onChange != nil {
		b.onChange(Change[K]{Member: member, Score: score, Retained: retained})
	}
	return nil
}

// Add applies delta and returns the resulting score.
func (b *Board[K]) Add(member K, delta float64) (float64, error) {
	if math.IsNaN(delta) {
		return 0, ErrInvalidScore
	}
	b.mu.Lock()
	score := b.scores[member] + delta
	if b.normalize != nil {
		var err error
		score, err = b.normalize(member, score)
		if err != nil {
			b.mu.Unlock()
			return 0, err
		}
	}
	if math.IsNaN(score) {
		b.mu.Unlock()
		return 0, ErrInvalidScore
	}
	b.scores[member] = score
	b.trimLocked()
	_, retained := b.scores[member]
	b.mu.Unlock()
	if b.onChange != nil {
		b.onChange(Change[K]{Member: member, Score: score, Retained: retained})
	}
	return score, nil
}
func (b *Board[K]) Remove(member K) { b.mu.Lock(); defer b.mu.Unlock(); delete(b.scores, member) }
func (b *Board[K]) Top(limit int) []Rank[K] {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.rankedLocked(limit)
}
func (b *Board[K]) RankOf(member K) (Rank[K], bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, rank := range b.rankedLocked(len(b.scores)) {
		if rank.Member == member {
			return rank, true
		}
	}
	var zero Rank[K]
	return zero, false
}
func (b *Board[K]) Len() int { b.mu.RLock(); defer b.mu.RUnlock(); return len(b.scores) }
func (b *Board[K]) rankedLocked(limit int) []Rank[K] {
	if limit <= 0 {
		return nil
	}
	ranks := make([]Rank[K], 0, len(b.scores))
	for member, score := range b.scores {
		ranks = append(ranks, Rank[K]{Member: member, Score: score})
	}
	sort.SliceStable(ranks, func(i, j int) bool { return ranks[i].Score > ranks[j].Score })
	if limit < len(ranks) {
		ranks = ranks[:limit]
	}
	for i := range ranks {
		ranks[i].Position = i + 1
	}
	return ranks
}
func (b *Board[K]) trimLocked() {
	if len(b.scores) <= b.capacity {
		return
	}
	for _, rank := range b.rankedLocked(len(b.scores))[b.capacity:] {
		delete(b.scores, rank.Member)
	}
}
