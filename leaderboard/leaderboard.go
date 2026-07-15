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

// Board retains at most the highest-scoring capacity members.
type Board[K comparable] struct {
	mu       sync.RWMutex
	capacity int
	scores   map[K]float64
}

func New[K comparable](capacity int) (*Board[K], error) {
	if capacity <= 0 {
		return nil, ErrInvalidCapacity
	}
	return &Board[K]{capacity: capacity, scores: make(map[K]float64)}, nil
}

// Set assigns a score. Members outside the top capacity are not retained.
func (b *Board[K]) Set(member K, score float64) error {
	if math.IsNaN(score) {
		return ErrInvalidScore
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.scores[member] = score
	b.trimLocked()
	return nil
}

// Add applies delta and returns the resulting score.
func (b *Board[K]) Add(member K, delta float64) (float64, error) {
	if math.IsNaN(delta) {
		return 0, ErrInvalidScore
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	score := b.scores[member] + delta
	if math.IsNaN(score) {
		return 0, ErrInvalidScore
	}
	b.scores[member] = score
	b.trimLocked()
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
